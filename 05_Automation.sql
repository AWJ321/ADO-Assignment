/* 
   ===================================================================
   PHASE 4: AUTOMATION (DATAOPS PIPELINE)
   Objective: Create a scheduled task that runs the entire Transformation 
   logic automatically.
   ===================================================================
*/
USE DATABASE ADVENTUREWORKS_ANALYTICS;

-- Create the Master Task
CREATE OR REPLACE TASK MASTER_ELT_PIPELINE_TASK
  WAREHOUSE = DATAOPS_WH
  SCHEDULE = 'USING CRON 0 0 * * SUN UTC' -- Safe Schedule: Runs only on Sundays at Midnight
AS
BEGIN
CREATE OR REPLACE TASK MASTER_ELT_PIPELINE_TASK
  WAREHOUSE = DATAOPS_WH
  SCHEDULE = 'USING CRON 0 0 * * SUN UTC' -- Safe Schedule: Runs only on Sundays at Midnight
AS
BEGIN
    -- 1. Refresh Product Dimension
    CREATE OR REPLACE TABLE DIM_PRODUCT AS
    SELECT 
        p.ProductID,
        p.Name AS ProductName,
        p.ProductNumber,
        COALESCE(p.Color, 'Multi/Other') AS Color,
        p.StandardCost,
        p.ListPrice,
        -- p.Size,
        COALESCE(s.Name, 'Uncategorized') AS SubCategory,
        COALESCE(c.Name, 'Uncategorized') AS Category,
        p.ProductLine,
        p.Class,
        p.Style,
        TRY_TO_DATE(LEFT(p.SellStartDate, 10)) AS SellStartDate,
        CASE WHEN p.SellEndDate IS NULL THEN 'Active' ELSE 'Discontinued' END AS ProductStatus
    FROM ADVENTUREWORKS_RAW.PUBLIC.RAW_PRODUCT p
    LEFT JOIN ADVENTUREWORKS_RAW.PUBLIC.RAW_PRODUCTSUBCATEGORY s ON p.ProductSubcategoryID = s.ProductSubcategoryID
    LEFT JOIN ADVENTUREWORKS_RAW.PUBLIC.RAW_PRODUCTCATEGORY c ON s.ProductCategoryID = c.ProductCategoryID;
    
    

    -- 2. Refresh Customer Dimension
    CREATE OR REPLACE TABLE DIM_CUSTOMER AS
    SELECT 
        c.CustomerID,
        c.AccountNumber,
        COALESCE(s.Name, CONCAT(p.FirstName, ' ', p.LastName), 'Unknown') AS CustomerName,
        CASE WHEN s.Name IS NOT NULL THEN 'Reseller/Store' ELSE 'Individual' END AS CustomerType,
        t.Name AS TerritoryName,
        t."Group" AS RegionGroup, 
        cr.Name AS CountryName
    FROM ADVENTUREWORKS_RAW.PUBLIC.RAW_CUSTOMER c
    LEFT JOIN ADVENTUREWORKS_RAW.PUBLIC.RAW_PERSON p ON c.PersonID = p.BusinessEntityID
    LEFT JOIN ADVENTUREWORKS_RAW.PUBLIC.RAW_STORE s ON c.StoreID = s.BusinessEntityID
    LEFT JOIN ADVENTUREWORKS_RAW.PUBLIC.RAW_SALESTERRITORY t ON c.TerritoryID = t.TerritoryID
    LEFT JOIN ADVENTUREWORKS_RAW.PUBLIC.RAW_COUNTRYREGION cr ON t.CountryRegionCode = cr.CountryRegionCode;

    -- 3. Refresh Location Dimension
    CREATE OR REPLACE TABLE DIM_LOCATION AS
    SELECT 
        a.AddressID,
        a.City,
        a.PostalCode,
        sp.Name AS StateProvince,
        cr.Name AS Country
    FROM ADVENTUREWORKS_RAW.PUBLIC.RAW_ADDRESS a
    JOIN ADVENTUREWORKS_RAW.PUBLIC.RAW_STATEPROVINCE sp ON a.StateProvinceID = sp.StateProvinceID
    JOIN ADVENTUREWORKS_RAW.PUBLIC.RAW_COUNTRYREGION cr ON sp.CountryRegionCode = cr.CountryRegionCode;
    
    -- 4. Refresh SalesPerson Dimension
    CREATE OR REPLACE TABLE DIM_SALESPERSON AS
    SELECT 
        sp.BusinessEntityID AS SalesPersonID,
        CONCAT(COALESCE(p.FirstName, ''), ' ', COALESCE(p.LastName, '')) AS SalesPersonName,
        COALESCE(sp.SalesQuota, 0) AS SalesQuota,
        sp.Bonus,
        sp.CommissionPct,
        COALESCE(t.Name, 'No Territory') AS TerritoryName,
        COALESCE(t."Group", 'No Region') AS RegionGroup
    FROM ADVENTUREWORKS_RAW.PUBLIC.RAW_SALESPERSON sp
    JOIN ADVENTUREWORKS_RAW.PUBLIC.RAW_PERSON p ON sp.BusinessEntityID = p.BusinessEntityID
    LEFT JOIN ADVENTUREWORKS_RAW.PUBLIC.RAW_SALESTERRITORY t ON sp.TerritoryID = t.TerritoryID;

    -- 5. Refresh Sales Fact (The Big One)
    CREATE OR REPLACE TABLE FACT_SALES AS
    SELECT DISTINCT 
        h.SalesOrderID,
        d.SalesOrderDetailID,
        h.CustomerID,
        h.TerritoryID,
        d.ProductID,
        h.SalesPersonID,
        h.BillToAddressID AS LocationID,
        TRY_TO_DATE(LEFT(h.OrderDate, 10)) AS OrderDate,
        TRY_TO_DATE(LEFT(h.ShipDate, 10)) AS ShipDate,
        d.OrderQty,
        d.UnitPrice,
        d.UnitPriceDiscount,
        (d.OrderQty * d.UnitPrice) * (1 - d.UnitPriceDiscount) AS LineTotal,
        h.TotalDue,
        h.TaxAmt,
        h.Freight
    FROM ADVENTUREWORKS_RAW.PUBLIC.RAW_SALESORDERHEADER h
    JOIN ADVENTUREWORKS_RAW.PUBLIC.RAW_SALESORDERDETAIL d ON h.SalesOrderID = d.SalesOrderID
    WHERE TRY_TO_DATE(LEFT(h.OrderDate, 10)) IS NOT NULL;
END;

-- 1. Turn the task ON
ALTER TASK MASTER_ELT_PIPELINE_TASK RESUME;

-- 2. Force it to run right now (Manually)
EXECUTE TASK MASTER_ELT_PIPELINE_TASK;

-- 3. Check the history to see if it ran successfully
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME=>'MASTER_ELT_PIPELINE_TASK', 
    RESULT_LIMIT => 10
));

/* 
   SAFETY: SUSPEND TASK
   Turn off the task to save credits after verification.
*/
--ALTER TASK MASTER_ELT_PIPELINE_TASK SUSPEND;
