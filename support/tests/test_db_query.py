'''
Test script to directly query the database and check what data is available for Mari Energies Limited
'''

import urllib
from sqlalchemy import create_engine, text
import pandas as pd
from typing import Dict, List, Optional, Tuple, Any
import os
from utils import logger

# Database connection parameters
server = 'MUHAMMADUSMAN'
database = 'MGFinancials'

# Create connection string
params = urllib.parse.quote_plus(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes')
conn_str = f'mssql+pyodbc:///?odbc_connect={params}'

# Create engine
engine = create_engine(conn_str)

# Function to execute query and print results
def execute_query(query):
    try:
        print(f"Executing query:\n{query}")
        with engine.connect() as connection:
            result = connection.execute(text(query))
            rows = result.fetchall()
            if rows:
                # Get column names
                columns = result.keys()
                # Create DataFrame
                df = pd.DataFrame(rows, columns=columns)
                print(f"\nResults ({len(df)} rows):")
                print(df)
                return df
            else:
                print("No results found")
                return None
    except Exception as e:
        print(f"Error executing query: {e}")
        return None

# Test queries

# 1. Check available companies
print("\n=== Available Companies ===")
company_query = """
SELECT TOP 10 CompanyID, CompanyName, Symbol 
FROM tbl_companieslist 
WHERE CompanyName LIKE '%Mari%' OR Symbol LIKE '%MARI%'
"""
execute_query(company_query)

# 2. Check available consolidation types
print("\n=== Available Consolidation Types ===")
consolidation_query = """
SELECT * FROM tbl_consolidation
"""
execute_query(consolidation_query)

# 3. Check available terms
print("\n=== Available Terms ===")
terms_query = """
SELECT TOP 10 * FROM tbl_terms
"""
execute_query(terms_query)

# 4. Check available ratio heads
print("\n=== Available Ratio Heads ===")
ratio_heads_query = """
SELECT TOP 10 * FROM tbl_ratiosheadmaster WHERE HeadNames LIKE '%ROE%' OR HeadNames LIKE '%Return on Equity%'
"""
execute_query(ratio_heads_query)

# 5. Check ratio data for Mari Energies
print("\n=== Ratio Data for Mari Energies ===")
ratio_data_query = """
SELECT TOP 10 r.*, c.CompanyName, rh.HeadNames 
FROM tbl_ratiorawdata r
JOIN tbl_companieslist c ON r.CompanyID = c.CompanyID
JOIN tbl_ratiosheadmaster rh ON r.SubHeadID = rh.SubHeadID
WHERE c.CompanyName LIKE '%Mari%'
ORDER BY r.PeriodEnd DESC
"""
execute_query(ratio_data_query)

# 6. Check table structure of tbl_ratiorawdata
print("\n=== Table Structure of tbl_ratiorawdata ===")
table_structure_query = """
SELECT COLUMN_NAME, DATA_TYPE 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = 'tbl_ratiorawdata'
"""
execute_query(table_structure_query)

# 7. Check table structure of tbl_unitofmeasurement
print("\n=== Table Structure of tbl_unitofmeasurement ===")
table_structure_query = """
SELECT COLUMN_NAME, DATA_TYPE 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = 'tbl_unitofmeasurement'
"""
execute_query(table_structure_query)

# 8. Check table structure of tbl_ratiosheadmaster
print("\n=== Table Structure of tbl_ratiosheadmaster ===")
table_structure_query = """
SELECT COLUMN_NAME, DATA_TYPE 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = 'tbl_ratiosheadmaster'
"""
execute_query(table_structure_query)