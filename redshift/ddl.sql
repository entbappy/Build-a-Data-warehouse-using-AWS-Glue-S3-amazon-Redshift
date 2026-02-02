
--Create table FactInternetSalesReason
CREATE TABLE "public"."FactInternetSalesReason"
( "SalesOrderNumber" INTEGER NOT NULL,
"SalesOrderLineNumber" INTEGER NOT NULL,
"SalesReasonKey" INTEGER NOT NULL,
Primary Key ("SalesOrderNumber","SalesOrderLineNumber","SalesReasonKey") 
)SORTKEY("SalesOrderNumber") ENCODE AUTO;

CREATE TABLE "public"."DimProductCategory"
( "ProductCategoryKey" INTEGER NULL,
"ProductCategoryAlternateKey" VARCHAR NULL,
PRIMARY KEY("ProductCategoryKey")
) ENCODE AUTO;

CREATE TABLE "public"."DimProductSubCategory"
( "ProductSubCategoryKey" INTEGER NULL,
"ProductSubCategoryAlternateKey" VARCHAR NULL,
"ProductCategoryKey" INTEGER NULL,
PRIMARY KEY("ProductSubCategoryKey"),
FOREIGN KEY ("ProductCategoryKey") REFERENCES "public"."DimProductCategory" ("ProductCategoryKey"))
 ENCODE AUTO;

CREATE TABLE "public"."DimProduct"
( "Productkey" INTEGER NULL,
"ProductAlternateKey" VARCHAR NULL,
"ProductSubCategoryKey" INTEGER NULL,
"StartDate" TIMESTAMP NULL,
PRIMARY KEY("Productkey") ,
FOREIGN KEY("ProductSubCategoryKey") REFERENCES  "public"."DimProductSubCategory" ("ProductSubCategoryKey")
) ENCODE AUTO;


CREATE TABLE "public"."DimCurrency"
( "CurrencyKey" VARCHAR NULL,
"CurrencyAlternateKey" VARCHAR NULL,
PRIMARY KEY("CurrencyKey") 
) ENCODE AUTO;

CREATE TABLE "public"."DimDate"
( "Datekey" INTEGER NULL,
"FullDateAlternateKey" VARCHAR NULL,
PRIMARY KEY("Datekey") 
) ENCODE AUTO;

CREATE TABLE "public"."DimPromotion"
( "PromotionKey" INTEGER NOT NULL,
"PromotionAlternateKey" VARCHAR NULL,
PRIMARY KEY("PromotionKey") 
);



CREATE TABLE "public"."DimSalesTerritory"
( "SalesTerritoryKey" INTEGER NULL,
"SalesTerritoryAlternateKey" VARCHAR NULL,
PRIMARY KEY("SalesTerritoryKey") 
) ENCODE AUTO;

CREATE TABLE "public"."DimGeography"
( "GeographyKey" INTEGER NULL,
"SalesTerritoryKey" INTEGER NULL,
PRIMARY KEY("GeographyKey") ,
FOREIGN KEY ("SalesTerritoryKey") REFERENCES "public"."DimSalesTerritory"("SalesTerritoryKey")
) ENCODE AUTO;


CREATE TABLE "public"."DimCustomer"
( 
    "CustomerKey" INTEGER NULL,
    "CustomerAlternateKey" VARCHAR NULL,
    "GeographyKey" INTEGER NULL,
    PRIMARY KEY("CustomerKey"),
    FOREIGN KEY ("GeographyKey") REFERENCES "public"."DimGeography"("geographykey")
) ENCODE AUTO;


CREATE TABLE "public"."FactInternetSales"(
"ProductKey" INTEGER NULL,
"CustomerKey" INTEGER NULL ,
"PromotionKey" INTEGER NULL,
"SalesTerritoryKey" INTEGER NULL,
"SalesOrderNumber" VARCHAR NULL,
"RevisionNumber" INTEGER,
"OrderQuantity" REAL NULL,
"UnitPrice" REAL NULL,
"UnitPriceDiscount" REAL NULL,
"TaxAmt" REAL NULL,
"Freight" REAL NULL ,
"CarrierTrackingNumber" VARCHAR NULL,
"OrderDate" TIMESTAMP NULL,
"DueDate" TIMESTAMP NULL,
"ShipDate" TIMESTAMP NULL)
