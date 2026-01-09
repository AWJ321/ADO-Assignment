# import snowflake.connector
# import os

# account = os.environ['SNOWFLAKE_ACCOUNT']
# user = os.environ['SNOWFLAKE_CICD_USER']
# password = os.environ['SNOWFLAKE_CICD_PASSWORD']
# role = os.environ['SNOWFLAKE_ROLE']
# warehouse = os.environ['SNOWFLAKE_WAREHOUSE']
# database = os.environ['SNOWFLAKE_DATABASE']
# schema = os.environ['SNOWFLAKE_SCHEMA']

# ctx = snowflake.connector.connect(
#     user=user,
#     password=password,
#     account=account,
#     role=role,
#     warehouse=warehouse,
#     database=database,
#     schema=schema
# )

# # cs =  ctx.cursor().execute("USE ROLE CI_CD_ROLE;")  # Explicitly switch to the role

# cs = ctx.cursor()  # Initialize the cursor
# cs.execute("USE ROLE CI_CD_ROLE;")  # Explicitly switch to the role

# sql_files = [
#     "01_Raw_Ingestion.sql",
#     "02_Dims_Customer_SalesPerson.sql",
#     "03_Dims_Product_Location.sql",
#     "04_Fact_Sales.sql",
#     "05_Automation.sql"
# ]


# for file in sql_files:
#     print(f"Running {file}...")
#     with open(file, 'r') as f:
#         sql = f.read()
#         # Split the file into statements using semicolon
#         statements = [s.strip() for s in sql.split(';') if s.strip()]
#         for stmt in statements:
#             print(f"Executing SQL statement: {stmt}")  # Add this for debugging
#             try:
#                 cs.execute(stmt)
#             except Exception as e:
#                 print(f"Failed to execute: {stmt}")
#                 print(f"Error: {e}")  # Print the error for debugging
#                 raise

# cs.close()
# ctx.close()




# import snowflake.connector
# import os

# account = os.environ['SNOWFLAKE_ACCOUNT']
# user = os.environ['SNOWFLAKE_CICD_USER']
# password = os.environ['SNOWFLAKE_CICD_PASSWORD']
# role = os.environ['SNOWFLAKE_ROLE']
# warehouse = os.environ['SNOWFLAKE_WAREHOUSE']
# database = os.environ['SNOWFLAKE_DATABASE']
# schema = os.environ['SNOWFLAKE_SCHEMA']

# ctx = snowflake.connector.connect(
#     user=user,
#     password=password,
#     account=account,
#     role=role,
#     warehouse=warehouse,
#     database=database,
#     schema=schema
# )

# cs = ctx.cursor()
# cs.execute("USE ROLE CI_CD_ROLE;")  # Explicitly switch to the role

# sql_files = [
#     "01_Raw_Ingestion.sql",
#     "02_Dims_Customer_SalesPerson.sql",
#     "03_Dims_Product_Location.sql",
#     "04_Fact_Sales.sql",
#     "05_Automation.sql"
# ]

# def smart_split_sql(sql_content):
#     """
#     Split SQL statements while preserving BEGIN...END blocks
#     """
#     statements = []
#     current_stmt = []
#     in_begin_end = 0  # Counter for nested BEGIN...END
    
#     lines = sql_content.split('\n')
    
#     for line in lines:
#         stripped_upper = line.strip().upper()
        
#         # Check for BEGIN keyword (start of block)
#         if 'BEGIN' in stripped_upper and not stripped_upper.startswith('--'):
#             in_begin_end += 1
        
#         # Check for END keyword (end of block)
#         if stripped_upper.startswith('END;') or stripped_upper == 'END':
#             in_begin_end -= 1
        
#         current_stmt.append(line)
        
#         # Only split on semicolon if we're NOT inside a BEGIN...END block
#         if ';' in line and in_begin_end == 0:
#             stmt = '\n'.join(current_stmt).strip()
#             if stmt and not stmt.startswith('/*') and not stmt.startswith('--'):
#                 statements.append(stmt)
#             current_stmt = []
    
#     # Add any remaining statement
#     if current_stmt:
#         stmt = '\n'.join(current_stmt).strip()
#         if stmt and not stmt.startswith('/*') and not stmt.startswith('--'):
#             statements.append(stmt)
    
#     return statements

# for file in sql_files:
#     print(f"\n{'='*60}")
#     print(f"Running {file}...")
#     print('='*60)
    
#     with open(file, 'r') as f:
#         sql_content = f.read()
    
#     # Use smart splitter to preserve BEGIN...END blocks
#     statements = smart_split_sql(sql_content)
    
#     for stmt in statements:
#         stmt = stmt.strip().rstrip(';')
#         if stmt:
#             # Show first 150 chars for debugging
#             preview = stmt[:150].replace('\n', ' ')
#             print(f"Executing: {preview}...")
#             try:
#                 cs.execute(stmt)
#                 print("✓ Success")
#             except Exception as e:
#                 print(f"✗ Failed to execute statement")
#                 print(f"Error: {e}")
#                 print(f"Statement preview: {stmt[:500]}")
#                 raise
    
#     print(f"✓ {file} completed successfully")

# cs.close()
# ctx.close()

# print("\n All SQL files executed successfully!")








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

cs = ctx.cursor()
cs.execute("USE ROLE CI_CD_ROLE;")

sql_files = [
    "01_Raw_Ingestion.sql",
    "02_Dims_Customer_SalesPerson.sql",
    "03_Dims_Product_Location.sql",
    "04_Fact_Sales.sql",
    "05_Automation.sql"
]

def remove_comments(sql_content):
    """
    Remove SQL comments from content
    """
    lines = []
    in_multiline_comment = False
    
    for line in sql_content.split('\n'):
        # Handle multi-line comments
        if '/*' in line:
            in_multiline_comment = True
        if '*/' in line:
            in_multiline_comment = False
            continue
        
        # Skip comment lines
        if in_multiline_comment or line.strip().startswith('--'):
            continue
        
        lines.append(line)
    
    return '\n'.join(lines)

def smart_split_sql(sql_content):
    """
    Split SQL statements while preserving BEGIN...END blocks
    """
    # First remove all comments
    sql_content = remove_comments(sql_content)
    
    statements = []
    current_stmt = []
    in_begin_end = 0
    
    lines = sql_content.split('\n')
    
    for line in lines:
        stripped_upper = line.strip().upper()
        
        # Skip empty lines
        if not stripped_upper:
            continue
        
        # Check for BEGIN keyword (start of block)
        if 'BEGIN' in stripped_upper:
            in_begin_end += 1
        
        # Check for END keyword (end of block)
        if stripped_upper.startswith('END;') or stripped_upper == 'END':
            in_begin_end -= 1
        
        current_stmt.append(line)
        
        # Only split on semicolon if NOT inside BEGIN...END block
        if ';' in line and in_begin_end == 0:
            stmt = '\n'.join(current_stmt).strip()
            if stmt:
                statements.append(stmt)
            current_stmt = []
    
    # Add any remaining statement
    if current_stmt:
        stmt = '\n'.join(current_stmt).strip()
        if stmt:
            statements.append(stmt)
    
    return statements

for file in sql_files:
    print(f"\n{'='*60}")
    print(f"Running {file}...")
    print('='*60)
    
    with open(file, 'r') as f:
        sql_content = f.read()
    
    # Use smart splitter to preserve BEGIN...END blocks
    statements = smart_split_sql(sql_content)
    
    for stmt in statements:
        stmt = stmt.strip().rstrip(';')
        if stmt:
            # Show first 150 chars for debugging
            preview = stmt[:150].replace('\n', ' ')
            print(f"Executing: {preview}...")
            try:
                cs.execute(stmt)
                print("✓ Success")
            except Exception as e:
                print(f"✗ Failed to execute statement")
                print(f"Error: {e}")
                print(f"Statement preview: {stmt[:500]}")
                raise
    
    print(f"✓ {file} completed successfully")

cs.close()
ctx.close()
print("\n All SQL files executed successfully!")
