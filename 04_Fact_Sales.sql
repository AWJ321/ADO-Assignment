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



/* QA CHECK 1: Referential Integrity */
SELECT COUNT(*) AS Orphan_Records
FROM ADVENTUREWORKS_ANALYTICS.PUBLIC.FACT_SALES f
LEFT JOIN ADVENTUREWORKS_ANALYTICS.PUBLIC.DIM_PRODUCT p 
    ON f.ProductID = p.ProductID
WHERE p.ProductID IS NULL;

/* QA CHECK 2: Date Range Validity */
SELECT 
    MIN(OrderDate) AS First_Sale,
    MAX(OrderDate) AS Last_Sale,
    COUNT(*) AS Total_Rows
FROM ADVENTUREWORKS_ANALYTICS.PUBLIC.FACT_SALES;

/* QA CHECK 3: Total Revenue Sanity Check */
SELECT 
    SUM(LineTotal) AS Total_Revenue_Excl_Tax,
    SUM(TotalDue) AS Total_Revenue_Incl_Tax
FROM ADVENTUREWORKS_ANALYTICS.PUBLIC.FACT_SALES;

/* QA CHECK 4: Sales Person Data Quality */
SELECT * FROM ADVENTUREWORKS_ANALYTICS.PUBLIC.DIM_SALESPERSON LIMIT 10;

/* QA CHECK 5: Top Sales Agents (Integration Test) */
SELECT 
    sp.SalesPersonName,
    COUNT(f.SalesOrderID) AS Total_Orders,
    SUM(f.LineTotal) AS Total_Revenue
FROM ADVENTUREWORKS_ANALYTICS.PUBLIC.DIM_SALESPERSON sp
JOIN ADVENTUREWORKS_ANALYTICS.PUBLIC.FACT_SALES f 
    ON sp.SalesPersonID = f.SalesPersonID
GROUP BY sp.SalesPersonName
ORDER BY Total_Revenue DESC
LIMIT 5;
