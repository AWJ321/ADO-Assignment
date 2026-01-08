/* 
   STEP 1: INFRASTRUCTURE SETUP
   -----------------------------------------------------------
   Objective: Create a dedicated database for raw data ingestion 
   and a warehouse (compute engine) to process the queries.
*/

-- 1. Create the database for the Raw Layer (The "Landing Zone")
-- We use "RAW" to indicate this data is untouched and potentially dirty.
CREATE OR REPLACE DATABASE ADVENTUREWORKS_RAW;

/* 
   Objective: Create a separate database for the clean, modeled data.
   This separates "Raw Data" (Bronze) from "Reporting Data" (Gold).
*/

CREATE OR REPLACE DATABASE ADVENTUREWORKS_ANALYTICS;

-- 2. Create a Virtual Warehouse
-- "X-SMALL" is sufficient for our dataset size and saves credits.
-- "AUTO_SUSPEND" saves money by turning off after 60s of inactivity.
CREATE OR REPLACE WAREHOUSE DATAOPS_WH 
WITH 
  WAREHOUSE_SIZE = 'X-SMALL' 
  AUTO_SUSPEND = 60 
  AUTO_RESUME = TRUE;

-- 3. Set the context for the session
-- This ensures all subsequent commands run in this DB and Warehouse.
USE DATABASE ADVENTUREWORKS_RAW;
USE WAREHOUSE DATAOPS_WH;

