/* 
   TRANSFORMATION: DIM_CUSTOMER
   -----------------------------------------------------------
   Source: RAW_CUSTOMER, RAW_PERSON, RAW_STORE, RAW_TERRITORY
   Cleaning: Merge Names, clean Location data.
*/

CREATE OR REPLACE TABLE DIM_CUSTOMER AS
SELECT 
    c.CustomerID,
    c.AccountNumber,
    
    -- SMART NAME LOGIC: If Store Name exists, use it. Otherwise, First + Last Name.
    COALESCE(s.Name, CONCAT(p.FirstName, ' ', p.LastName), 'Unknown') AS CustomerName,
    
    -- LOGIC: Define the Type of Customer
    CASE 
        WHEN s.Name IS NOT NULL THEN 'Reseller/Store'
        ELSE 'Individual'
    END AS CustomerType,
    
    -- LOCATION DATA (Crucial for Maps)
    t.Name AS TerritoryName,
    t."Group" AS RegionGroup, 
    cr.Name AS CountryName
    
FROM ADVENTUREWORKS_RAW.PUBLIC.RAW_CUSTOMER c
LEFT JOIN ADVENTUREWORKS_RAW.PUBLIC.RAW_PERSON p 
    ON c.PersonID = p.BusinessEntityID
LEFT JOIN ADVENTUREWORKS_RAW.PUBLIC.RAW_STORE s 
    ON c.StoreID = s.BusinessEntityID
LEFT JOIN ADVENTUREWORKS_RAW.PUBLIC.RAW_SALESTERRITORY t 
    ON c.TerritoryID = t.TerritoryID
LEFT JOIN ADVENTUREWORKS_RAW.PUBLIC.RAW_COUNTRYREGION cr 
    ON t.CountryRegionCode = cr.CountryRegionCode;

/* 
   TRANSFORMATION: DIM_SALESPERSON
   -----------------------------------------------------------
   Source: RAW_SALESPERSON, RAW_PERSON, RAW_TERRITORY
   Why: Required for "Sales Agent Tracking" report (Page 5 of PDF).
*/

CREATE OR REPLACE TABLE DIM_SALESPERSON AS
SELECT 
    sp.BusinessEntityID AS SalesPersonID,
    
    -- CLEANING: Combine First and Last Name
    CONCAT(COALESCE(p.FirstName, ''), ' ', COALESCE(p.LastName, '')) AS SalesPersonName,
    
    -- METRICS: Targets and Commission
    COALESCE(sp.SalesQuota, 0) AS SalesQuota,
    sp.Bonus,
    sp.CommissionPct,
    
    -- CONTEXT: Where do they work?
    COALESCE(t.Name, 'No Territory') AS TerritoryName,
    COALESCE(t."Group", 'No Region') AS RegionGroup

FROM ADVENTUREWORKS_RAW.PUBLIC.RAW_SALESPERSON sp
JOIN ADVENTUREWORKS_RAW.PUBLIC.RAW_PERSON p 
    ON sp.BusinessEntityID = p.BusinessEntityID
LEFT JOIN ADVENTUREWORKS_RAW.PUBLIC.RAW_SALESTERRITORY t 
    ON sp.TerritoryID = t.TerritoryID;