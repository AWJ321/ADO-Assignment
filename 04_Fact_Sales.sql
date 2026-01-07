/* 
   TRANSFORMATION 4: FACT_SALES (The Money Table)
   -----------------------------------------------------------
   Cleaning: Deduplication, Date Parsing, Revenue Calculation.
*/

CREATE OR REPLACE TABLE FACT_SALES AS
SELECT DISTINCT 
    -- PRIMARY KEYS
    h.SalesOrderID,
    d.SalesOrderDetailID,
    
    -- FOREIGN KEYS (Links to Dimensions)
    h.CustomerID,
    h.TerritoryID,
    d.ProductID,
    h.SalesPersonID,
    h.BillToAddressID AS LocationID, -- Link to DIM_LOCATION
    
    -- DATES (CLEANING: Convert Text to Date)
    TRY_TO_DATE(LEFT(h.OrderDate, 10)) AS OrderDate,
    TRY_TO_DATE(LEFT(h.ShipDate, 10)) AS ShipDate,
    
    -- METRICS
    d.OrderQty,
    d.UnitPrice,
    d.UnitPriceDiscount,
    
    -- CALCULATED REVENUE (Qty * Price * (1 - Discount))
    (d.OrderQty * d.UnitPrice) * (1 - d.UnitPriceDiscount) AS LineTotal,
    
    h.TotalDue,
    h.TaxAmt,
    h.Freight

FROM ADVENTUREWORKS_RAW.PUBLIC.RAW_SALESORDERHEADER h
JOIN ADVENTUREWORKS_RAW.PUBLIC.RAW_SALESORDERDETAIL d 
    ON h.SalesOrderID = d.SalesOrderID
    
-- Filter out bad dates
WHERE TRY_TO_DATE(LEFT(h.OrderDate, 10)) IS NOT NULL;

-- Final Verification
SELECT 'FACT_SALES', COUNT(*) FROM FACT_SALES
UNION ALL
SELECT 'DIM_PRODUCT', COUNT(*) FROM DIM_PRODUCT;