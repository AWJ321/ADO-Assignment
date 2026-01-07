/* 
   TRANSFORMATION: DIM_PRODUCT
   -----------------------------------------------------------
   Source: RAW_PRODUCT, RAW_PRODUCTSUBCATEGORY, RAW_PRODUCTCATEGORY
   Cleaning: Cast Dates, Handle Null Colors, Denormalize hierarchy.
*/

CREATE OR REPLACE TABLE DIM_PRODUCT AS
SELECT 
    p.ProductID,
    p.Name AS ProductName,
    p.ProductNumber,
    
    -- CLEANING: Handle Null Colors
    COALESCE(p.Color, 'Multi/Other') AS Color,
    
    p.StandardCost,
    p.ListPrice,
    p.Size,
    
    -- JOINING: Bring in the Subcategory Name
    COALESCE(s.Name, 'Uncategorized') AS SubCategory,
    
    -- JOINING: Bring in the Category Name
    COALESCE(c.Name, 'Uncategorized') AS Category,
    
    p.ProductLine,
    p.Class,
    p.Style,
    
    -- CLEANING: Convert Text Date back to Real Date
    TRY_TO_DATE(LEFT(p.SellStartDate, 10)) AS SellStartDate,
    
    -- Status Flag (Calculated Column)
    CASE 
        WHEN p.SellEndDate IS NULL THEN 'Active'
        ELSE 'Discontinued'
    END AS ProductStatus

FROM ADVENTUREWORKS_RAW.PUBLIC.RAW_PRODUCT p
LEFT JOIN ADVENTUREWORKS_RAW.PUBLIC.RAW_PRODUCTSUBCATEGORY s 
    ON p.ProductSubcategoryID = s.ProductSubcategoryID
LEFT JOIN ADVENTUREWORKS_RAW.PUBLIC.RAW_PRODUCTCATEGORY c 
    ON s.ProductCategoryID = c.ProductCategoryID;

/* 
   TRANSFORMATION: DIM_LOCATION
   -----------------------------------------------------------
   Source: RAW_ADDRESS, RAW_STATEPROVINCE, RAW_COUNTRYREGION
   Cleaning: Create a clean hierarchy of City -> State -> Country.
*/

CREATE OR REPLACE TABLE DIM_LOCATION AS
SELECT 
    a.AddressID,
    a.City,
    a.PostalCode,
    sp.Name AS StateProvince,
    cr.Name AS Country
FROM ADVENTUREWORKS_RAW.PUBLIC.RAW_ADDRESS a
JOIN ADVENTUREWORKS_RAW.PUBLIC.RAW_STATEPROVINCE sp 
    ON a.StateProvinceID = sp.StateProvinceID
JOIN ADVENTUREWORKS_RAW.PUBLIC.RAW_COUNTRYREGION cr 
    ON sp.CountryRegionCode = cr.CountryRegionCode;