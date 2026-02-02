import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
from pyspark.sql import types as T
from awsglue import DynamicFrame
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

"""
Organization: iNeuron Intelligence Private Limited
author: avnish@ineuron.ai
Created date: 08/02/2023
Order: PROJECT-746
Update date: 08/02/2023
"""
def directJDBCSource(
    glueContext,
    connectionName,
    connectionType,
    database,
    table,
    redshiftTmpDir,
    transformation_ctx,
) -> DynamicFrame:

    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": table,
        "connectionName": connectionName,
    }

    if redshiftTmpDir:
        connection_options["redshiftTmpDir"] = redshiftTmpDir

    return glueContext.create_dynamic_frame.from_options(
        connection_type=connectionType,
        connection_options=connection_options,
        transformation_ctx=transformation_ctx,
    )


def read_tsv(file_path):
    return spark.read.option("sep","\t").csv(file_path)

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

job.init(args['JOB_NAME'], args)



#declaring variables

logger = glueContext.get_logger()

logger.info(f"Declaring variables")
FILE_NAME="ProductCategory.csv"
PRODUCT_CATEGORY_COLUMN = ["productCategoryKey","name","rowGuid","modifiedDate"]
BUCKET_NAME = "data-source-ineuron"
TABLE_NAME = "public.DimProductCategory"
DATABASE_NAME = "ecommercedw"
ROOT_DIR = f"s3://{BUCKET_NAME}/data/"
FILES = []
FILE_PATH =f"{ROOT_DIR}{FILE_NAME}"

REDSHIFT_DATABASE_CONNECTION_NAME  = "ineuron-datawarehouse"
REDSHIFT_TEMP_DIR="s3://data-source-ineuron/redshift/tmp"
logger.info(f"Variable declaration completed.")



"""
-----------------------------------------------------------------
       Dimenstion Product category 
-----------------------------------------------------------------
"""

logger.info(f"Reading product category file from {FILE_PATH}")
df_product_category = read_tsv(file_path=FILE_PATH)
logger.info(f"File read successfully and has {df_product_category.count()} rows and columns are: {df_product_category.columns}")

logger.info(f"Assigning original column name to producrt category dataframe")
for old_column,new_column in zip(df_product_category.columns,PRODUCT_CATEGORY_COLUMN):
    logger.info(f"Renaming column {old_column}--->{new_column}")
    df_product_category =  df_product_category.withColumnRenamed(old_column,new_column)


df_product_category =  df_product_category.select(["productCategorykey","rowGuid"])
logger.info(f"Renaming column rowGuid to productCategoryAlternateKey")
df_product_category =  df_product_category.withColumnRenamed("rowGuid","productCategoryAlternateKey")
logger.info(f"Type casting column to productCategoryKey to integer.")
df_product_category =  df_product_category.withColumn("productCategorykey",F.col("productCategorykey").cast(T.IntegerType()))

my_conn_options = {
"dbtable":TABLE_NAME,
"database":DATABASE_NAME
}
logger.info(f"Definining redshift database connection options {my_conn_options}")

logger.info(f"Reading data frame redshift")
exitingDimProductCategory = directJDBCSource(
    glueContext,
    connectionName=REDSHIFT_DATABASE_CONNECTION_NAME,
    connectionType="redshift",
    database=DATABASE_NAME,
    table=TABLE_NAME,
    redshiftTmpDir=REDSHIFT_TEMP_DIR,
    transformation_ctx="exitingDimProductCategory",
)

df_existing_product_category = exitingDimProductCategory.toDF()

if df_existing_product_category.count()>0:
    logger.info(f"NUmber of row avaible in redshift table: {df_existing_product_category.count()}")
    
    logger.info(f"Dropping exisiting row from s3 file that is already available in redshift table.")
    df_existing_product_category= df_existing_product_category.withColumnRenamed("productCategoryKey","existingProductCategoryKey").select("existingProductCategoryKey")
    
    df_new_product_category  =(df_product_category.join(df_existing_product_category,
    F.col("productCategorykey")==F.col("existingProductCategoryKey"),how="left").filter("existingProductCategoryKey is null")
    .drop("existingProductCategoryKey"))
    new_record_to_insert = df_new_product_category.count()
    logger.info(f" Number of new record to insert into redshift table : {new_record_to_insert}")
    logger.info(f"{df_new_product_category.count()} rows will be inseryed into redshift table. ")
    dyf_new_product_category = DynamicFrame.fromDF(df_new_product_category,glueContext,"df_new_product_category")

else:
    df_product_category.printSchema()
    new_record_to_insert = df_product_category.count()
    logger.info(f" Number of new record to insert into redshift table : {new_record_to_insert}")
    logger.info(f"Columns: {df_product_category.columns}")
    dyf_new_product_category = DynamicFrame.fromDF(df_product_category,glueContext,"df_product_category")



if new_record_to_insert>0:
    logger.info(f"Started writing data into redshift table")
    redshift_result = glueContext.write_dynamic_frame.from_jdbc_conf(
            frame=dyf_new_product_category,
            catalog_connection=REDSHIFT_DATABASE_CONNECTION_NAME,
            connection_options=my_conn_options,
            redshift_tmp_dir=REDSHIFT_TEMP_DIR
        )
else:
    logger.info(f"New records not found insert")
    
FILES.append(FILE_PATH)
"""
-----------------------------------------------------------------
        Product sub category 
-----------------------------------------------------------------
"""
PRODUCT_SUB_CATEGORY_COLUMNS = [
    "productSubCategoryId",
    "productCategoryId",
    "name",
    "rowGuid",
    "modifiedDate"
]

DIM_PRODUCT_SUB_CATEGORY_COLUMNS = [
    "productSubCategoryKey",
    "productSubCategoryAlternateKey",
    "productCategoryKey"
]

FILE_NAME = "ProductSubcategory.csv"
FILE_PATH = f"{ROOT_DIR}{FILE_NAME}"
TABLE_NAME = "public.dimproductsubcategory"


logger.info(f"Reading product sub category file from {FILE_PATH}")
df_product_sub_category = read_tsv(file_path=FILE_PATH)
logger.info(f"File read successfully and has {df_product_sub_category.count()} rows and columns are: {df_product_sub_category.columns}")

logger.info(f"Assigning original column name to producrt category dataframe")
for old_column,new_column in zip(df_product_sub_category.columns,PRODUCT_SUB_CATEGORY_COLUMNS):
    logger.info(f"Renaming column {old_column}--->{new_column}")
    df_product_sub_category =  df_product_sub_category.withColumnRenamed(old_column,new_column)


logger.info(f"Renaming column rowGuid to productCategoryAlternateKey")

df_product_sub_category =(
    df_product_sub_category.withColumnRenamed("productSubCategoryId","productSubCategoryKey")
    .withColumnRenamed("productCategoryId","productCategoryKey")
    .withColumnRenamed("rowGuid","productSubCategoryAlternateKey")
).select(DIM_PRODUCT_SUB_CATEGORY_COLUMNS)

logger.info(f"Type casting column to productCategoryKey and productSubCategoryKey to integer.")
df_product_sub_category = (
     df_product_sub_category.withColumn("productSubCategoryKey",
                                        F.col("productSubCategoryKey")
                                        .cast(T.IntegerType()))
                            .withColumn("productCategoryKey",
                                        F.col("productCategoryKey")
                                        .cast(T.IntegerType()))
                                                            )

my_conn_options = {
"dbtable":TABLE_NAME,
"database":DATABASE_NAME
}
logger.info(f"Definining redshift database connection options {my_conn_options}")

logger.info(f"Reading data frame redshift")
exitingDimProductSubCategory = directJDBCSource(
    glueContext,
    connectionName=REDSHIFT_DATABASE_CONNECTION_NAME,
    connectionType="redshift",
    database=DATABASE_NAME,
    table=TABLE_NAME,
    redshiftTmpDir=REDSHIFT_TEMP_DIR,
    transformation_ctx="exitingDimProductSubCategory",
)

df_existing_product_sub_category = exitingDimProductSubCategory.toDF()

if df_existing_product_sub_category.count()>0:
    logger.info(f"NUmber of row avaible in redshift table: {df_existing_product_sub_category.count()}")
    
    logger.info(f"Dropping exisiting row from s3 file that is already available in redshift table.")
    

    df_new_product_sub_category = (
                                    df_product_sub_category.alias("prod_sub_cat").join(
                                    df_existing_product_sub_category.alias("existing_prod_cat"),
                                    F.col("prod_sub_cat.productSubCategoryKey")==F.col("existing_prod_cat.productSubCategoryKey"),
                                    how="left")
                                    .filter("existing_prod_cat.productSubCategoryKey is null")
                                    .select(["prod_sub_cat.productSubCategoryKey","prod_sub_cat.productSubCategoryAlternateKey",
                                            "prod_sub_cat.productCategoryKey"])
                                    )


    new_record_to_insert = df_new_product_sub_category.count()
    logger.info(f" Number of new record to insert into redshift table : {new_record_to_insert}")
    logger.info(f"{new_record_to_insert} rows will be inseryed into redshift table. ")
    dyf_new_product_sub_category = DynamicFrame.fromDF(df_new_product_sub_category,glueContext,"df_new_product_sub_category")

else:
    df_product_sub_category.printSchema()
    new_record_to_insert = df_product_sub_category.count()
    logger.info(f" Number of new record to insert into redshift table : {new_record_to_insert}")
    logger.info(f"Columns: {df_product_sub_category.columns}")
    dyf_new_product_sub_category = DynamicFrame.fromDF(df_product_sub_category,glueContext,"df_product_sub_category")



if new_record_to_insert>0:
    logger.info(f"Started writing data into redshift table")
    redshift_result = glueContext.write_dynamic_frame.from_jdbc_conf(
            frame=dyf_new_product_sub_category,
            catalog_connection=REDSHIFT_DATABASE_CONNECTION_NAME,
            connection_options=my_conn_options,
            redshift_tmp_dir=REDSHIFT_TEMP_DIR
        )
else:
    logger.info(f"New records not found insert")
    

"""
===========================================================
Dimension Product table
========================================================
"""

FILE_NAME = "Product.csv"
FILE_PATH = f"{ROOT_DIR}{FILE_NAME}"
TABLE_NAME = "public.dimproduct"


PRODUCT_COLUMNS =["productId","Name","ProductNumber",
                  "MakeFlag","FinishedGoodsFlag",
                  "Color","SafetyStockLevel",
                  "ReorderPoint","StandardCost",
                  "ListPrice","Size",
                  "SizeUnitMeasureCode",
                  "WeightUnitMeasureCode",
                  "Weight",
                  "DaysToManufacture",
                  "ProductLine",
                  "Class","Style",
                  "ProductSubcategoryId",
                  "ProductModelId",
                  "SellStartDate",
                  "SellEndDate",
                  "DiscountinuedDate",
                  "rowguid",
                  "ModifiedDate"]

DIM_PRODUCT_COLUMNS = [
    "productKey",
    "productAlternateKey",
    "productSubCategoryKey",
    "startDate"
]



logger.info(f"Reading product  file from {FILE_PATH}")
df_product = read_tsv(file_path=FILE_PATH)
logger.info(f"File read successfully and has {df_product.count()} rows and columns are: {df_product.columns}")

logger.info(f"Assigning original column name to df_product dataframe")
for old_column,new_column in zip(df_product.columns,PRODUCT_COLUMNS):
    logger.info(f"Renaming column {old_column}--->{new_column}")
    df_product =  df_product.withColumnRenamed(old_column,new_column)


logger.info(f"Renaming column rowGuid to productAlternateKey")

df_product =(
    df_product.withColumnRenamed("productId","productKey")
    .withColumnRenamed("ProductSubCategoryId","productSubCategoryKey")
    .withColumnRenamed("rowGuid","productAlternateKey")
    .withColumnRenamed("SellStartDate","startDate")
).select(DIM_PRODUCT_COLUMNS)

logger.info(f"Type casting column to productCategoryKey and productSubCategoryKey to integer.")
df_product = (
                df_product
                .withColumn("productSubCategoryKey",F.col("productSubCategoryKey").cast(T.IntegerType()))
                .withColumn("productKey",F.col("productKey").cast(T.IntegerType()))
                .withColumn("startDate",F.col("startDate").cast(T.TimestampType()))
                )

my_conn_options = {
"dbtable":TABLE_NAME,
"database":DATABASE_NAME
}
logger.info(f"Definining redshift database connection options {my_conn_options}")

logger.info(f"Reading data frame redshift")
existingDimProduct = directJDBCSource(
    glueContext,
    connectionName=REDSHIFT_DATABASE_CONNECTION_NAME,
    connectionType="redshift",
    database=DATABASE_NAME,
    table=TABLE_NAME,
    redshiftTmpDir=REDSHIFT_TEMP_DIR,
    transformation_ctx="exitingDimProduct",
)

df_existing_product = existingDimProduct.toDF()

if df_existing_product.count()>0:
    logger.info(f"NUmber of row avaible in redshift table: {df_existing_product.count()}")
    logger.info(f"Dropping exisiting row from s3 file that is already available in redshift table.")
    df_new_product = (
                    df_product.alias("prod").join(
                    df_existing_product.alias("existing_prod"),
                    F.col("prod.productKey")==F.col("existing_prod.productKey"),
                    how="left")
                    .filter("existing_prod.productKey is null")
                    .select(["prod.productKey","prod.productAlternateKey",
                                "prod.productSubCategoryKey",
                                "prod.startDate"])
                    )
    new_record_to_insert = df_new_product.count()
    logger.info(f" Number of new record to insert into redshift table : {new_record_to_insert}")
    logger.info(f"{new_record_to_insert} rows will be inseryed into redshift table. ")
    dyf_new_product = DynamicFrame.fromDF(df_new_product,glueContext,"df_new_product")

else:
    df_product.printSchema()
    new_record_to_insert = df_product.count()
    logger.info(f" Number of new record to insert into redshift table : {new_record_to_insert}")
    logger.info(f"Columns: {df_product.columns}")
    dyf_new_product = DynamicFrame.fromDF(df_product,glueContext,"df_product")



if new_record_to_insert>0:
    logger.info(f"Started writing data into redshift table")
    redshift_result = glueContext.write_dynamic_frame.from_jdbc_conf(
            frame=dyf_new_product,
            catalog_connection=REDSHIFT_DATABASE_CONNECTION_NAME,
            connection_options=my_conn_options,
            redshift_tmp_dir=REDSHIFT_TEMP_DIR
        )
else:
    logger.info(f"New records not found insert")
    
"""
------------------------------------------------------------------------
                        Dimension Currency
-------------------------------------------------------------------------
"""

FILE_NAME = "Currency.csv"
FILE_PATH = f"{ROOT_DIR}{FILE_NAME}"
TABLE_NAME = "public.DimCurrency"


CURRENCY_COLUMNS =["currencyCode",
                  "name",
                  "modifiedDate"
                  ]

DIM_CURRENCY_COLUMNS = [
    "CurrencyKey",
    "CurrencyAlternateKey",
   
]


logger.info(f"Reading currency file from {FILE_PATH}")
df_currency = read_tsv(file_path=FILE_PATH)
logger.info(f"File read successfully and has {df_currency.count()} rows and columns are: {df_currency.columns}")

logger.info(f"Assigning original column name to df_product dataframe")
for old_column,new_column in zip(df_currency.columns,CURRENCY_COLUMNS):
    logger.info(f"Renaming column {old_column}--->{new_column}")
    df_currency =  df_currency.withColumnRenamed(old_column,new_column)


logger.info(f"Renaming column rowGuid to productAlternateKey")

df_currency =(
    df_currency.withColumnRenamed("currencyCode","CurrencyKey")
    .withColumnRenamed("name","CurrencyAlternateKey")

).select(DIM_CURRENCY_COLUMNS)

my_conn_options = {
"dbtable":TABLE_NAME,
"database":DATABASE_NAME
}
logger.info(f"Definining redshift database connection options {my_conn_options}")

logger.info(f"Reading data frame redshift")
exitingDimCurrency = directJDBCSource(
    glueContext,
    connectionName=REDSHIFT_DATABASE_CONNECTION_NAME,
    connectionType="redshift",
    database=DATABASE_NAME,
    table=TABLE_NAME,
    redshiftTmpDir=REDSHIFT_TEMP_DIR,
    transformation_ctx="exitingDimCurrency",
)

df_existing_currency = exitingDimCurrency.toDF()

if df_existing_currency.count()>0:
    logger.info(f"NUmber of row avaible in redshift table: {df_existing_currency.count()}")
    logger.info(f"Dropping exisiting row from s3 file that is already available in redshift table.")
    df_new_currency = (
                    df_currency.alias("currency").join(
                    df_existing_currency.alias("existing_currency"),
                    F.col("currency.CurrencyKey")==F.col("existing_currency.CurrencyKey"),
                    how="left")
                    .filter("existing_currency.CurrencyKey is null")
                    .select(["currency.CurrencyKey","currency.CurrencyAlternateKey",
                               ])
                    )
    new_record_to_insert = df_new_currency.count()
    logger.info(f" Number of new record to insert into redshift table : {new_record_to_insert}")
    logger.info(f"{new_record_to_insert} rows will be inseryed into redshift table. ")
    dyf_new_currency = DynamicFrame.fromDF(df_new_currency,glueContext,"df_new_currency")

else:
    df_currency.printSchema()
    new_record_to_insert = df_currency.count()
    logger.info(f" Number of new record to insert into redshift table : {new_record_to_insert}")
    logger.info(f"Columns: {df_currency.columns}")
    dyf_new_currency = DynamicFrame.fromDF(df_currency,glueContext,"df_currency")



if new_record_to_insert>0:
    logger.info(f"Started writing data into redshift table")
    redshift_result = glueContext.write_dynamic_frame.from_jdbc_conf(
            frame=dyf_new_currency,
            catalog_connection=REDSHIFT_DATABASE_CONNECTION_NAME,
            connection_options=my_conn_options,
            redshift_tmp_dir=REDSHIFT_TEMP_DIR
        )
else:
    logger.info(f"New records not found insert")
    
"""
-----------------------------------------------------
Dim Promotion
-----------------------------------------------------
"""


FILE_NAME = "SpecialOfferProduct.csv"
FILE_PATH = f"{ROOT_DIR}{FILE_NAME}"
TABLE_NAME = "public.DimPromotion"


PROMOTION_COLUMNS = ["SpecialOfferId",
"ProductId",
"rowGuid",
"ModifiedDate"]

DIM_PROMOTION_COLUMNS = [
    "PromotionKey",
    "PromotionAlternateKey"
]





logger.info(f"Reading currency file from {FILE_PATH}")
df_promotion = read_tsv(file_path=FILE_PATH)
logger.info(f"File read successfully and has {df_promotion.count()} rows and columns are: {df_promotion.columns}")

logger.info(f"Assigning original column name to df_product dataframe")
for old_column,new_column in zip(df_promotion.columns,PROMOTION_COLUMNS):
    logger.info(f"Renaming column {old_column}--->{new_column}")
    df_promotion =  df_promotion.withColumnRenamed(old_column,new_column)


logger.info(f"Renaming column rowGuid to productAlternateKey")

df_promotion =(
    df_promotion.withColumnRenamed("SpecialOfferId","PromotionKey")
    .withColumnRenamed("rowGuid","PromotionAlternateKey")

).select(DIM_PROMOTION_COLUMNS)

df_promotion = df_promotion.withColumn("PromotionKey",F.col("PromotionKey").cast(T.IntegerType()))

my_conn_options = {
"dbtable":TABLE_NAME,
"database":DATABASE_NAME
}
logger.info(f"Definining redshift database connection options {my_conn_options}")

logger.info(f"Reading data frame redshift")
exitingDimPromotion = directJDBCSource(
    glueContext,
    connectionName=REDSHIFT_DATABASE_CONNECTION_NAME,
    connectionType="redshift",
    database=DATABASE_NAME,
    table=TABLE_NAME,
    redshiftTmpDir=REDSHIFT_TEMP_DIR,
    transformation_ctx="exitingDimPromotion",
)

df_existing_promotion = exitingDimPromotion.toDF()

if df_existing_promotion.count()>0:
    logger.info(f"NUmber of row avaible in redshift table: {df_existing_promotion.count()}")
    logger.info(f"Dropping exisiting row from s3 file that is already available in redshift table.")
    df_new_promotion = (
                    df_promotion.alias("promotion").join(
                    df_existing_promotion.alias("existing_promotion"),
                    F.col("promotion.PromotionKey")==F.col("existing_promotion.PromotionKey"),
                    how="left")
                    .filter("existing_promotion.PromotionKey is null")
                    .select(["promotion.PromotionKey","promotion.PromotionAlternateKey",
                               ])
                    )
    new_record_to_insert = df_new_promotion.count()
    logger.info(f" Number of new record to insert into redshift table : {new_record_to_insert}")
    logger.info(f"{new_record_to_insert} rows will be inseryed into redshift table. ")
    dyf_new_promotion = DynamicFrame.fromDF(df_new_promotion,glueContext,"df_new_promotion")

else:
    df_promotion.printSchema()
    new_record_to_insert = df_promotion.count()
    logger.info(f" Number of new record to insert into redshift table : {new_record_to_insert}")
    logger.info(f"Columns: {df_promotion.columns}")
    dyf_new_promotion = DynamicFrame.fromDF(df_promotion,glueContext,"df_promotion")



if new_record_to_insert>0:
    logger.info(f"Started writing data into redshift table")
    redshift_result = glueContext.write_dynamic_frame.from_jdbc_conf(
            frame=dyf_new_promotion,
            catalog_connection=REDSHIFT_DATABASE_CONNECTION_NAME,
            connection_options=my_conn_options,
            redshift_tmp_dir=REDSHIFT_TEMP_DIR
        )
else:
    logger.info(f"New records not found insert")

"""
-----------------------------------------------------------
DimSalesterritory
-----------------------------------------------------------
"""
FILE_NAME = "SalesTerritory.csv"
FILE_PATH = f"{ROOT_DIR}{FILE_NAME}"
TABLE_NAME = "public.DimSalesTerritory"


SALES_TERRITORY_COLUMNS = ["TerritoryId","Name","CountryRegionCode","Group","SalesYTD","SalesLastYear","CostYTD","CostLastYear","rowguid","ModifiedDate"]

DIM_SALES_TERRITORY_COLUMNS = [
    "SalesTerritoryKey",
    "SalesTerritoryAlternateKey"
]






logger.info(f"Reading sales territory file from {FILE_PATH}")
df_sales_territory= read_tsv(file_path=FILE_PATH)
logger.info(f"File read successfully and has {df_sales_territory.count()} rows and columns are: {df_sales_territory.columns}")

logger.info(f"Assigning original column name to df_product dataframe")
for old_column,new_column in zip(df_sales_territory.columns,SALES_TERRITORY_COLUMNS):
    logger.info(f"Renaming column {old_column}--->{new_column}")
    df_sales_territory =  df_sales_territory.withColumnRenamed(old_column,new_column)


df_sales_territory =(
    df_sales_territory.withColumnRenamed("TerritoryId","SalesTerritoryKey")
    .withColumnRenamed("rowGuid","SalesTerritoryAlternateKey")

).select(DIM_SALES_TERRITORY_COLUMNS)


df_sales_territory = df_sales_territory.withColumn("SalesTerritoryKey",F.col("SalesTerritoryKey").cast(T.IntegerType()))
my_conn_options = {
"dbtable":TABLE_NAME,
"database":DATABASE_NAME
}
logger.info(f"Definining redshift database connection options {my_conn_options}")

logger.info(f"Reading data frame redshift")
exitingDimSalesTerritory = directJDBCSource(
    glueContext,
    connectionName=REDSHIFT_DATABASE_CONNECTION_NAME,
    connectionType="redshift",
    database=DATABASE_NAME,
    table=TABLE_NAME,
    redshiftTmpDir=REDSHIFT_TEMP_DIR,
    transformation_ctx="exitingDimSalesTerritory",
)

df_existing_sales_territory = exitingDimSalesTerritory.toDF()

if df_existing_sales_territory.count()>0:
    logger.info(f"NUmber of row avaible in redshift table: {df_existing_sales_territory.count()}")
    logger.info(f"Dropping exisiting row from s3 file that is already available in redshift table.")
    df_new_sales_territory = (
                    df_sales_territory.alias("sales_territory").join(
                    df_existing_sales_territory.alias("existing_sales_territory"),
                    F.col("sales_territory.SalesTerritoryKey")==F.col("existing_sales_territory.SalesTerritoryKey"),
                    how="left")
                    .filter("existing_sales_territory.SalesTerritoryKey is null")
                    .select(["sales_territory.SalesTerritoryKey","sales_territory.SalesTerritoryKey",
                               ])
                    )
    new_record_to_insert = df_new_sales_territory.count()
    logger.info(f" Number of new record to insert into redshift table : {new_record_to_insert}")
    logger.info(f"{new_record_to_insert} rows will be inseryed into redshift table. ")
    dyf_new_sales_territory = DynamicFrame.fromDF(df_new_sales_territory,glueContext,"df_new_sales_territory")

else:
    df_sales_territory.printSchema()
    new_record_to_insert = df_sales_territory.count()
    logger.info(f" Number of new record to insert into redshift table : {new_record_to_insert}")
    logger.info(f"Columns: {df_sales_territory.columns}")
    dyf_new_sales_territory = DynamicFrame.fromDF(df_sales_territory,glueContext,"df_promotion")



if new_record_to_insert>0:
    logger.info(f"Started writing data into redshift table")
    redshift_result = glueContext.write_dynamic_frame.from_jdbc_conf(
            frame=dyf_new_sales_territory,
            catalog_connection=REDSHIFT_DATABASE_CONNECTION_NAME,
            connection_options=my_conn_options,
            redshift_tmp_dir=REDSHIFT_TEMP_DIR
        )
else:
    logger.info(f"New records not found insert")

"""
----------------------------------------
Dim Customer
----------------------------------------
"""
FILE_NAME = "Customer.csv"
FILE_PATH = f"{ROOT_DIR}{FILE_NAME}"
TABLE_NAME = "public.DimCustomer"


CUSTOMER_COLUMNS = [
"customerId",
"territoryId",
"accountNumber",
"customerType",
"rowGuid",
"modifiedDate"
]

DIM_CUSTOMER_COLUMN = [
"customerKey",
"salesTerritoryKey",
"customerAlternateKey"
]



logger.info(f"Reading customer file from {FILE_PATH}")
df_customer= read_tsv(file_path=FILE_PATH)
logger.info(f"File read successfully and has {df_customer.count()} rows and columns are: {df_customer.columns}")

logger.info(f"Assigning original column name to df_product dataframe")
for old_column,new_column in zip(df_customer.columns,CUSTOMER_COLUMNS):
    logger.info(f"Renaming column {old_column}--->{new_column}")
    df_customer =  df_customer.withColumnRenamed(old_column,new_column)


logger.info(f"Renaming column rowGuid to productAlternateKey")

df_customer =(
    df_customer.withColumnRenamed("customerId","customerKey")
    .withColumnRenamed("rowGuid","customerAlternateKey")
    .withColumnRenamed("territoryId","salesTerritoryKey")

).select(DIM_CUSTOMER_COLUMN)

df_customer =   (
    df_customer.withColumn("customerKey",F.col("customerKey").cast(T.IntegerType()))
    .withColumn("salesTerritoryKey",F.col("salesTerritoryKey").cast(T.IntegerType()))
)

my_conn_options = {
"dbtable":TABLE_NAME,
"database":DATABASE_NAME
}
logger.info(f"Definining redshift database connection options {my_conn_options}")

logger.info(f"Reading data frame redshift")

exitingDimCustomer= directJDBCSource(
    glueContext,
    connectionName=REDSHIFT_DATABASE_CONNECTION_NAME,
    connectionType="redshift",
    database=DATABASE_NAME,
    table=TABLE_NAME,
    redshiftTmpDir=REDSHIFT_TEMP_DIR,
    transformation_ctx="exitingDimCustomer",
)

df_existing_customer = exitingDimCustomer.toDF()

if df_existing_customer.count()>0:
    logger.info(f"NUmber of row avaible in redshift table: {df_existing_customer.count()}")
    logger.info(f"Dropping exisiting row from s3 file that is already available in redshift table.")
    df_new_customer = (
                    df_customer.alias("customer").join(
                    df_existing_customer.alias("existing_customer"),
                    F.col("customer.customerKey")==F.col("existing_customer.customerKey"),
                    how="left")
                    .filter("existing_customer.customerKey is null")
                    .select(["customer.customerKey","customer.customerAlternateKey",
                             "customer.salesTerritoryKey"
                               ]))
    new_record_to_insert = df_new_customer.count()
    logger.info(f" Number of new record to insert into redshift table : {new_record_to_insert}")
    logger.info(f"{new_record_to_insert} rows will be inseryed into redshift table. ")
    dyf_new_customer = DynamicFrame.fromDF(df_new_customer,glueContext,"df_new_customer")

else:
    df_customer.printSchema()
    new_record_to_insert = df_customer.count()
    logger.info(f" Number of new record to insert into redshift table : {new_record_to_insert}")
    logger.info(f"Columns: {df_customer.columns}")
    dyf_new_customer = DynamicFrame.fromDF(df_customer,glueContext,"df_customer")



if new_record_to_insert>0:
    logger.info(f"Started writing data into redshift table")
    redshift_result = glueContext.write_dynamic_frame.from_jdbc_conf(
            frame=dyf_new_customer,
            catalog_connection=REDSHIFT_DATABASE_CONNECTION_NAME,
            connection_options=my_conn_options,
            redshift_tmp_dir=REDSHIFT_TEMP_DIR
        )
else:
    logger.info(f"New records not found insert")





"""
------------------------------------------------
Fact_Internet_Sales
------------------------------------------------
"""


TABLE_NAME ="public.FactInternetSales"

timestamp_columns = [
"OrderDate",
"DueDate",
"ShipDate",
]

real_value_columns = [
"OrderQuantity",
"UnitPrice",
"UnitPriceDiscount",
"TaxAmt",
"Freight",
]

integer_columns = [
"ProductKey",
"CustomerKey",
"PromotionKey",
"SalesTerritoryKey",
"RevisionNumber"
]






df_sales_order_detail = read_tsv(f"{ROOT_DIR}SalesOrderDetail.csv")
df_sales_order_header = read_tsv(f"{ROOT_DIR}SalesOrderHeader.csv")



SALES_ORDER_DETAIL_COLUMN = ["salesOrderId",
"salesOrderDetailId",
"carrierTrackingNumber",
"orderQty",
"productId",
"specialOfferId",
"unitPrice",
"unitPriceDiscount",
"lineTotal",
"rowGuid",
"modifiedDate",
]

logger.info(f"File read successfully and has {df_sales_order_detail.count()} rows and columns are: {df_sales_order_detail.columns}")

logger.info(f"Assigning original column name to df_product dataframe")
for old_column,new_column in zip(df_sales_order_detail.columns,SALES_ORDER_DETAIL_COLUMN):
    logger.info(f"Renaming column {old_column}--->{new_column}")
    df_sales_order_detail =  df_sales_order_detail.withColumnRenamed(old_column,new_column)



SALES_ORDER_HEADER_COLUMN = [
"SalesOrderId",
"RevisionNumber",
"OrderDate",
"DueDate",
"ShipDate",
"Status",
"OnlineOrderFlag",
"SalesOrderNumber",
"PurchaseOrderNumber",
"AccountNumber",
"CustomerId",
"ContactId",
"SalesPersonId",
"TerritoryId",
"BillToAddressId",
"ShipMethodId",
"CreditCardApprovalCode",
"CurrencyRateId",
"SubTotal",
"TaxAmt",
"Freight",
"TotalDue",
"Comment",
"rowGuid"
"ModifiedDate",
"CreditCardId"
]

logger.info(f"Assigning original column name to df_sales_order_header dataframe")
for old_column,new_column in zip(df_sales_order_header.columns,SALES_ORDER_HEADER_COLUMN):
    logger.info(f"Renaming column {old_column}--->{new_column}")
    df_sales_order_header =  df_sales_order_header.withColumnRenamed(old_column,new_column)


mapping={
"productId":"productKey",
"CustomerId":"CustomerKey",
"specialOfferId":"PromotionKey",
"TerritoryId":"salesTerritoryKey",
"SalesOrderNumber":"SalesOrderNumber",
"RevisionNumber":"RevisionNumber",
"orderQty":"OrderQuantity",
"unitPrice":"UnitPrice",
"unitPriceDiscount":"UnitPriceDiscount",
"TaxAmt":"TaxAmt",
"Freight":"Freight",
"carrierTrackingNumber":"CarrierTrackingNumber",
"OrderDate":"OrderDate",
"DueDate":"DueDate",
"ShipDate":"ShipDate"
}

FACT_INTERNET_SALES_COLUMNS = [
"ProductKey",
"CustomerKey",
"PromotionKey",
"SalesTerritoryKey",
"SalesOrderNumber",
"RevisionNumber",
"OrderQuantity",
"UnitPrice",
"UnitPriceDiscount",
"TaxAmt",
"Freight",
"CarrierTrackingNumber",
"OrderDate",
"DueDate",
"ShipDate"
]


df_sales = (df_sales_order_detail.alias("sod")
 .join(df_sales_order_header.alias("soh"),
       F.col("sod.SalesOrderId")==F.col("soh.SalesOrderId"),
       how="inner"
       )
 ).drop("soh.SalesOrderId")

for old_column,new_column in mapping.items():
    df_sales = df_sales.withColumnRenamed(old_column,new_column)

for column in timestamp_columns:
    df_sales=df_sales.withColumn(column,F.col(column).cast(T.TimestampType()))

for column in integer_columns:
    df_sales = df_sales.withColumn(column,F.col(column).cast(T.IntegerType()))

for column in real_value_columns:
    df_sales = df_sales.withColumn(column,F.col(column).cast(T.FloatType()))


df_sales = df_sales.select(FACT_INTERNET_SALES_COLUMNS)

exitingFactSales= directJDBCSource(
    glueContext,
    connectionName=REDSHIFT_DATABASE_CONNECTION_NAME,
    connectionType="redshift",
    database=DATABASE_NAME,
    table=TABLE_NAME,
    redshiftTmpDir=REDSHIFT_TEMP_DIR,
    transformation_ctx="exitingFactSales",
)

my_conn_options = {
"dbtable":TABLE_NAME,
"database":DATABASE_NAME
}

df_existing_sales = exitingFactSales.toDF()

if df_existing_sales.count()>0:
    logger.info(f"NUmber of row avaible in redshift table: {df_existing_sales.count()}")
    logger.info(f"Dropping exisiting row from s3 file that is already available in redshift table.")
    df_new_sales = (
                    df_sales.alias("sales").join(
                    df_existing_sales.alias("existing_sales"),
                    ((F.col("sales.ProductKey")==F.col("existing_sales.ProductKey"))  
                    & (F.col("sales.CustomerKey")==F.col("existing_sales.CustomerKey")) 
                    & (F.col("sales.PromotionKey")==F.col("existing_sales.PromotionKey")) 
                    & (F.col("sales.SalesTerritoryKey")==F.col("existing_sales.SalesTerritoryKey"))),
                    how="left")
                    .filter("existing_sales.ProductKey is null"
                            " and  existing_sales.customerKey is null"
                            " and  existing_sales.PromotionKey is null"
                             " and  existing_sales.SalesTerritoryKey is null"
                            )
                    .select(["sales.*"
                               ]))
    new_record_to_insert = df_new_sales.count()
    logger.info(f" Number of new record to insert into redshift table : {new_record_to_insert}")
    logger.info(f"{new_record_to_insert} rows will be inseryed into redshift table. ")
    dyf_new_sales = DynamicFrame.fromDF(df_new_sales,glueContext,"df_new_sales")

else:
    df_sales.printSchema()
    new_record_to_insert = df_sales.count()
    logger.info(f" Number of new record to insert into redshift table : {new_record_to_insert}")
    logger.info(f"Columns: {df_sales.columns}")
    dyf_new_sales = DynamicFrame.fromDF(df_sales,glueContext,"df_sales")



if new_record_to_insert>0:
    logger.info(f"Started writing data into redshift table")
    redshift_result = glueContext.write_dynamic_frame.from_jdbc_conf(
            frame=dyf_new_sales,
            catalog_connection=REDSHIFT_DATABASE_CONNECTION_NAME,
            connection_options=my_conn_options,
            redshift_tmp_dir=REDSHIFT_TEMP_DIR
        )
else:
    logger.info(f"New records not found insert")



job.commit() 


