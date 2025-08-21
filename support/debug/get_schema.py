import pyodbc
import pandas as pd
from sqlalchemy import create_engine
import urllib

# Database connection parameters
server = 'DESKTOP-IQPVGVL'
database = 'FinancialData'
username = 'sa'
password = '123456'

# Create connection string
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

# Create connection
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# Get all tables
print("\n=== TABLES ===\n")
cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
tables = cursor.fetchall()
for table in tables:
    print(table[0])

# For each table, get its columns
for table in tables:
    table_name = table[0]
    print(f"\n=== COLUMNS IN {table_name} ===\n")
    cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'")
    columns = cursor.fetchall()
    for column in columns:
        print(column[0])

# Close connection
conn.close()