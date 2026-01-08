import snowflake.connector
import os

account = os.environ['SNOWFLAKE_ACCOUNT']
user = os.environ['SNOWFLAKE_CICD_USER']
password = os.environ['SNOWFLAKE_CICD_PASSWORD']
role = os.environ['SNOWFLAKE_ROLE']
warehouse = os.environ['SNOWFLAKE_WAREHOUSE']
database = os.environ['SNOWFLAKE_DATABASE']
schema = os.environ['SNOWFLAKE_SCHEMA']

ctx = snowflake.connector.connect(
    user=user,
    password=password,
    account=account,
    role=role,
    warehouse=warehouse,
    database=database,
    schema=schema
)

ctx.cursor().execute("USE ROLE CI_CD_ROLE;")  # Explicitly switch to the role

sql_files = [
    "00_Setup_and_Automation.sql",
    "01_Raw_Ingestion.sql",
    "02_Dims_Customer_SalesPerson.sql",
    "03_Dims_Product_Location.sql",
    "04_Fact_Sales.sql"
]


for file in sql_files:
    print(f"Running {file}...")
    with open(file, 'r') as f:
        sql = f.read()
        # Split the file into statements using semicolon
        statements = [s.strip() for s in sql.split(';') if s.strip()]
        for stmt in statements:
            cs.execute(stmt)


cs.close()
ctx.close()
