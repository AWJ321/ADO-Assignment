-- 1. Create a File Format object
-- This acts as a template for reading our specific CSV structure.
USE DATABASE ADVENTUREWORKS_RAW;
USE SCHEMA PUBLIC;
USE WAREHOUSE DATAOPS_WH; 


CREATE OR REPLACE FILE FORMAT CSV_FORMAT
  TYPE = 'CSV'
  FIELD_DELIMITER = ','           -- Columns are separated by commas
  SKIP_HEADER = 1                 -- Skip the first row (Column Names)
  FIELD_OPTIONALLY_ENCLOSED_BY = '"' -- Handles text fields like "City, Country"
  NULL_IF = ('NULL', 'null', '')  -- Converts text 'NULL' or empty strings to actual SQL NULLs
  TRIM_SPACE = TRUE;              -- Removes extra spaces from text fields

-- 2. Create an Internal Stage
-- A Stage is like a cloud folder where we upload files before loading them into tables.
CREATE OR REPLACE STAGE MY_DATA_STAGE
  FILE_FORMAT = CSV_FORMAT;

/* 
   ===================================================================
   PHASE: DATA DEFINITION (DDL)
   DESCRIPTION: Creating Raw tables for ALL AdventureWorks CSV files.
   ===================================================================
*/

-- 1. LOCATION & ADDRESS ----------------------------------------------
CREATE OR REPLACE TABLE RAW_ADDRESS (
    AddressID INT,
    AddressLine1 VARCHAR(60),
    AddressLine2 VARCHAR(60),
    City VARCHAR(30),
    StateProvinceID INT,
    PostalCode VARCHAR(15),
    SpatialLocation VARCHAR(100), 
    rowguid VARCHAR(64),
    ModifiedDate VARCHAR(50) -- Changed to Text
);

CREATE OR REPLACE TABLE RAW_STATEPROVINCE (
    StateProvinceID INT,
    StateProvinceCode VARCHAR(3),
    CountryRegionCode VARCHAR(3),
    IsOnlyStateProvinceFlag BOOLEAN,
    Name VARCHAR(50),
    TerritoryID INT,
    rowguid VARCHAR(64),
    ModifiedDate VARCHAR(50) -- Changed to Text
);

CREATE OR REPLACE TABLE RAW_COUNTRYREGION (
    CountryRegionCode VARCHAR(3),
    Name VARCHAR(50),
    ModifiedDate VARCHAR(50) -- Changed to Text
);

-- 2. PEOPLE & CUSTOMERS ----------------------------------------------
CREATE OR REPLACE TABLE RAW_PERSON (
    BusinessEntityID INT,      
    PersonType VARCHAR(2),
    NameStyle BOOLEAN,
    Title VARCHAR(8),
    FirstName VARCHAR(50),
    MiddleName VARCHAR(50),
    LastName VARCHAR(50),
    Suffix VARCHAR(10),
    EmailPromotion INT,
    AdditionalContactInfo VARCHAR(5000), 
    Demographics VARCHAR(5000),          
    rowguid VARCHAR(64),
    ModifiedDate VARCHAR(50) -- Changed to Text
);

CREATE OR REPLACE TABLE RAW_CUSTOMER (
    CustomerID INT,
    PersonID INT,
    StoreID INT,
    TerritoryID INT,
    AccountNumber VARCHAR(20),
    rowguid VARCHAR(64),
    ModifiedDate VARCHAR(50) -- Changed to Text
);

CREATE OR REPLACE TABLE RAW_STORE (
    BusinessEntityID INT,
    Name VARCHAR(50),
    SalesPersonID INT,
    Demographics VARCHAR(5000),
    rowguid VARCHAR(64),
    ModifiedDate VARCHAR(50) -- Changed to Text
);

-- 3. PRODUCT CATALOG -------------------------------------------------
CREATE OR REPLACE TABLE RAW_PRODUCT (
    ProductID INT,
    Name VARCHAR(100),
    ProductNumber VARCHAR(25),
    MakeFlag BOOLEAN,
    FinishedGoodsFlag BOOLEAN,
    Color VARCHAR(15),
    SafetyStockLevel INT,
    ReorderPoint INT,
    StandardCost FLOAT,
    ListPrice FLOAT,
    Size VARCHAR(5),
    SizeUnitMeasureCode VARCHAR(3),
    WeightUnitMeasureCode VARCHAR(3),
    Weight FLOAT,
    DaysToManufacture INT,
    ProductLine VARCHAR(2),
    Class VARCHAR(2),
    Style VARCHAR(2),
    ProductSubcategoryID INT,
    ProductModelID INT,
    SellStartDate VARCHAR(50), -- Changed to Text
    SellEndDate VARCHAR(50),   -- Changed to Text
    DiscontinuedDate VARCHAR(50), -- Changed to Text
    rowguid VARCHAR(64),
    ModifiedDate VARCHAR(50) -- Changed to Text
);

CREATE OR REPLACE TABLE RAW_PRODUCTSUBCATEGORY (
    ProductSubcategoryID INT,
    ProductCategoryID INT,
    Name VARCHAR(50),
    rowguid VARCHAR(64),
    ModifiedDate VARCHAR(50) -- Changed to Text
);

CREATE OR REPLACE TABLE RAW_PRODUCTCATEGORY (
    ProductCategoryID INT,
    Name VARCHAR(50),
    rowguid VARCHAR(64),
    ModifiedDate VARCHAR(50) -- Changed to Text
);

CREATE OR REPLACE TABLE RAW_PRODUCTMODEL (
    ProductModelID INT,
    Name VARCHAR(50),
    CatalogDescription VARCHAR(5000), 
    Instructions VARCHAR(5000),       
    rowguid VARCHAR(64),
    ModifiedDate VARCHAR(50) -- Changed to Text
);

-- 4. SALES TRANSACTIONS ----------------------------------------------
CREATE OR REPLACE TABLE RAW_SALESORDERHEADER (
    SalesOrderID INT,
    RevisionNumber INT,
    OrderDate VARCHAR(50), -- Changed to Text
    DueDate VARCHAR(50),   -- Changed to Text
    ShipDate VARCHAR(50),  -- Changed to Text
    Status INT,
    OnlineOrderFlag BOOLEAN,
    SalesOrderNumber VARCHAR(25),
    PurchaseOrderNumber VARCHAR(25),
    AccountNumber VARCHAR(15),
    CustomerID INT,
    SalesPersonID INT,
    TerritoryID INT,
    BillToAddressID INT,
    ShipToAddressID INT,
    ShipMethodID INT,
    CreditCardID INT,
    CreditCardApprovalCode VARCHAR(15),
    CurrencyRateID INT,
    SubTotal FLOAT,
    TaxAmt FLOAT,
    Freight FLOAT,
    TotalDue FLOAT,
    Comment VARCHAR(500),
    rowguid VARCHAR(64),
    ModifiedDate VARCHAR(50) -- Changed to Text
);

CREATE OR REPLACE TABLE RAW_SALESORDERDETAIL (
    SalesOrderID INT,
    SalesOrderDetailID INT,
    CarrierTrackingNumber VARCHAR(25),
    OrderQty INT,
    ProductID INT,
    SpecialOfferID INT,
    UnitPrice FLOAT,
    UnitPriceDiscount FLOAT,
    LineTotal FLOAT,
    rowguid VARCHAR(64),
    ModifiedDate VARCHAR(50) -- Changed to Text
);

-- 5. SALES SUPPORT TABLES --------------------------------------------
CREATE OR REPLACE TABLE RAW_SALESTERRITORY (
    TerritoryID INT,
    Name VARCHAR(50),
    CountryRegionCode VARCHAR(3),
    "Group" VARCHAR(50),    
    SalesYTD FLOAT,
    SalesLastYear FLOAT,
    CostYTD FLOAT,
    CostLastYear FLOAT,
    rowguid VARCHAR(64),
    ModifiedDate VARCHAR(50) -- Changed to Text
);

CREATE OR REPLACE TABLE RAW_SALESPERSON (
    BusinessEntityID INT,
    TerritoryID INT,
    SalesQuota FLOAT,
    Bonus FLOAT,
    CommissionPct FLOAT,
    SalesYTD FLOAT,
    SalesLastYear FLOAT,
    rowguid VARCHAR(64),
    ModifiedDate VARCHAR(50) -- Changed to Text
);

CREATE OR REPLACE TABLE RAW_SALESPERSONQUOTAHISTORY (
    BusinessEntityID INT,
    QuotaDate VARCHAR(50), -- Changed to Text
    SalesQuota FLOAT,
    rowguid VARCHAR(64),
    ModifiedDate VARCHAR(50) -- Changed to Text
);

CREATE OR REPLACE TABLE RAW_SALESREASON (
    SalesReasonID INT,
    Name VARCHAR(50),
    ReasonType VARCHAR(50),
    ModifiedDate VARCHAR(50) -- Changed to Text
);

CREATE OR REPLACE TABLE RAW_SALESORDERHEADERSALESREASON (
    SalesOrderID INT,
    SalesReasonID INT,
    ModifiedDate VARCHAR(50) -- Changed to Text
);

CREATE OR REPLACE TABLE RAW_SHIPMETHOD (
    ShipMethodID INT,
    Name VARCHAR(50),
    ShipBase FLOAT,
    ShipRate FLOAT,
    rowguid VARCHAR(64),
    ModifiedDate VARCHAR(50) -- Changed to Text
);

-- 6. SPECIAL OFFERS --------------------------------------------------
CREATE OR REPLACE TABLE RAW_SPECIALOFFER (
    SpecialOfferID INT,
    Description VARCHAR(255),
    DiscountPct FLOAT,
    Type VARCHAR(50),
    Category VARCHAR(50),
    StartDate VARCHAR(50), -- Changed to Text
    EndDate VARCHAR(50),   -- Changed to Text
    MinQty INT,
    MaxQty INT,
    rowguid VARCHAR(64),
    ModifiedDate VARCHAR(50) -- Changed to Text
);

CREATE OR REPLACE TABLE RAW_SPECIALOFFERPRODUCT (
    SpecialOfferID INT,
    ProductID INT,
    rowguid VARCHAR(64),
    ModifiedDate VARCHAR(50) -- Changed to Text
);

/* 
   ===================================================================
   PHASE: DATA INGESTION
   DESCRIPTION: Loading data from Stage into Raw Tables.
   NOTE: We use ON_ERROR = 'CONTINUE' to skip bad rows without crashing.
   ===================================================================
*/

-- 1. Location Data
COPY INTO RAW_ADDRESS FROM @MY_DATA_STAGE/Address.csv ON_ERROR = 'CONTINUE';
COPY INTO RAW_STATEPROVINCE FROM @MY_DATA_STAGE/StateProvince.csv ON_ERROR = 'CONTINUE';
COPY INTO RAW_COUNTRYREGION FROM @MY_DATA_STAGE/CountryRegion.csv ON_ERROR = 'CONTINUE';

-- 2. People Data
COPY INTO RAW_PERSON FROM @MY_DATA_STAGE/Person.csv ON_ERROR = 'CONTINUE';
COPY INTO RAW_CUSTOMER FROM @MY_DATA_STAGE/Customer.csv ON_ERROR = 'CONTINUE';
COPY INTO RAW_STORE FROM @MY_DATA_STAGE/Store.csv ON_ERROR = 'CONTINUE';

-- 3. Product Data
COPY INTO RAW_PRODUCT FROM @MY_DATA_STAGE/Product.csv ON_ERROR = 'CONTINUE';
COPY INTO RAW_PRODUCTSUBCATEGORY FROM @MY_DATA_STAGE/ProductSubcategory.csv ON_ERROR = 'CONTINUE';
COPY INTO RAW_PRODUCTCATEGORY FROM @MY_DATA_STAGE/ProductCategory.csv ON_ERROR = 'CONTINUE';
COPY INTO RAW_PRODUCTMODEL FROM @MY_DATA_STAGE/ProductModel.csv ON_ERROR = 'CONTINUE';

-- 4. Sales Transactions
COPY INTO RAW_SALESORDERHEADER FROM @MY_DATA_STAGE/SalesOrderHeader.csv ON_ERROR = 'CONTINUE';
COPY INTO RAW_SALESORDERDETAIL FROM @MY_DATA_STAGE/SalesOrderDetail.csv ON_ERROR = 'CONTINUE';

-- 5. Sales Support
COPY INTO RAW_SALESTERRITORY FROM @MY_DATA_STAGE/SalesTerritory.csv ON_ERROR = 'CONTINUE';
COPY INTO RAW_SALESPERSON FROM @MY_DATA_STAGE/SalesPerson.csv ON_ERROR = 'CONTINUE';
COPY INTO RAW_SALESPERSONQUOTAHISTORY FROM @MY_DATA_STAGE/SalesPersonQuotaHistory.csv ON_ERROR = 'CONTINUE';
COPY INTO RAW_SALESREASON FROM @MY_DATA_STAGE/SalesReason.csv ON_ERROR = 'CONTINUE';
COPY INTO RAW_SALESORDERHEADERSALESREASON FROM @MY_DATA_STAGE/SalesOrderHeaderSalesReason.csv ON_ERROR = 'CONTINUE';
COPY INTO RAW_SHIPMETHOD FROM @MY_DATA_STAGE/ShipMethod.csv ON_ERROR = 'CONTINUE';

-- 6. Special Offers
COPY INTO RAW_SPECIALOFFER FROM @MY_DATA_STAGE/SpecialOffer.csv ON_ERROR = 'CONTINUE';
COPY INTO RAW_SPECIALOFFERPRODUCT FROM @MY_DATA_STAGE/SpecialOfferProduct.csv ON_ERROR = 'CONTINUE';

/* 
   STEP 6: FINAL VERIFICATION
   -----------------------------------------------------------
   Objective: Confirm that all key tables have data.
   Expected Result: Numbers greater than 0 for all tables.
*/

SELECT 'SalesOrderHeader' AS Table_Name, COUNT(*) AS Row_Count FROM RAW_SALESORDERHEADER
UNION ALL
SELECT 'SalesOrderDetail', COUNT(*) FROM RAW_SALESORDERDETAIL
UNION ALL
SELECT 'Product', COUNT(*) FROM RAW_PRODUCT
UNION ALL
SELECT 'Customer', COUNT(*) FROM RAW_CUSTOMER
UNION ALL

SELECT 'SpecialOfferProduct', COUNT(*) FROM RAW_SPECIALOFFERPRODUCT;
