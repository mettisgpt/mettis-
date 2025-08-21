from sqlalchemy import create_engine, text
import pandas as pd
import urllib

# Server & database config
server = 'MUHAMMADUSMAN'      # Or your SQL Server name
database = 'MGFinancials'     # Replace with your DB name

# Windows Authentication via ODBC Driver
params = urllib.parse.quote_plus(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"Trusted_Connection=yes;"
)

# Create engine
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

# Get query from user
query = input("Enter your SQL query: ")

# Execute and display result
with engine.connect() as connection:
    try:
        result = pd.read_sql_query(text(query), con=connection)
        print("\nQuery Results:\n")
        print(result.to_string(index=False))
    except Exception as e:
        print("Error:", e)