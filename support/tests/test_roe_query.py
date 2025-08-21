'''
Test script to directly query the database for ROE data for Mari Energies Limited
'''

import urllib
from sqlalchemy import create_engine, text
import pandas as pd
from typing import Dict, List, Optional, Tuple, Any
import os

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

# 1. Get Mari Energies company ID
print("\n=== Mari Energies Company ID ===")
company_query = """
SELECT CompanyID, CompanyName, Symbol 
FROM tbl_companieslist 
WHERE CompanyName LIKE '%Mari%'
"""
company_df = execute_query(company_query)

if company_df is not None and not company_df.empty:
    company_id = company_df.iloc[0]['CompanyID']
    print(f"\nUsing Company ID: {company_id}")
    
    # 2. Get ROE ratio head ID
    print("\n=== ROE Ratio Head ID ===")
    ratio_head_query = """
    SELECT SubHeadID, HeadNames 
    FROM tbl_ratiosheadmaster 
    WHERE HeadNames LIKE '%ROE%' OR HeadNames LIKE '%Return on Equity%'
    """
    ratio_head_df = execute_query(ratio_head_query)
    
    if ratio_head_df is not None and not ratio_head_df.empty:
        ratio_head_id = ratio_head_df.iloc[0]['SubHeadID']
        print(f"\nUsing Ratio Head ID: {ratio_head_id}")
        
        # 3. Get available terms
        print("\n=== Available Terms ===")
        terms_query = """
        SELECT TermID, term 
        FROM tbl_terms
        """
        terms_df = execute_query(terms_query)
        
        # 4. Get available consolidation types
        print("\n=== Available Consolidation Types ===")
        consolidation_query = """
        SELECT ConsolidationID, consolidationname 
        FROM tbl_consolidation
        """
        consolidation_df = execute_query(consolidation_query)
        
        # 5. Try different combinations of term and consolidation to find data
        print("\n=== Searching for ROE Data with Different Parameters ===")
        
        if terms_df is not None and consolidation_df is not None:
            found_data = False
            
            for _, term_row in terms_df.iterrows():
                term_id = term_row['TermID']
                term_name = term_row['term']
                
                for _, cons_row in consolidation_df.iterrows():
                    cons_id = cons_row['ConsolidationID']
                    cons_name = cons_row['consolidationname']
                    
                    print(f"\nTrying Term: {term_name} (ID: {term_id}), Consolidation: {cons_name} (ID: {cons_id})")
                    
                    # Query for ROE data with these parameters
                    roe_query = f"""
                    SELECT r.Value_, r.PeriodEnd, r.FY, t.term, con.consolidationname, rh.HeadNames
                    FROM tbl_ratiorawdata r
                    JOIN tbl_ratiosheadmaster rh ON r.SubHeadID = rh.SubHeadID
                    JOIN tbl_terms t ON r.TermID = t.TermID
                    JOIN tbl_consolidation con ON r.ConsolidationID = con.ConsolidationID
                    WHERE r.CompanyID = {company_id}
                    AND r.SubHeadID = {ratio_head_id}
                    AND r.TermID = {term_id}
                    AND r.ConsolidationID = {cons_id}
                    ORDER BY r.PeriodEnd DESC
                    """
                    
                    result_df = execute_query(roe_query)
                    
                    if result_df is not None and not result_df.empty:
                        found_data = True
                        print(f"\n*** FOUND DATA for Term: {term_name}, Consolidation: {cons_name} ***")
                        break
                
                if found_data:
                    break
            
            if not found_data:
                print("\nNo ROE data found for any combination of term and consolidation")
                
                # 6. Check if any ratio data exists for this company
                print("\n=== Checking for Any Ratio Data for this Company ===")
                any_ratio_query = f"""
                SELECT TOP 10 r.Value_, r.PeriodEnd, r.FY, r.SubHeadID, t.term, con.consolidationname, rh.HeadNames
                FROM tbl_ratiorawdata r
                JOIN tbl_ratiosheadmaster rh ON r.SubHeadID = rh.SubHeadID
                JOIN tbl_terms t ON r.TermID = t.TermID
                JOIN tbl_consolidation con ON r.ConsolidationID = con.ConsolidationID
                WHERE r.CompanyID = {company_id}
                ORDER BY r.PeriodEnd DESC
                """
                
                execute_query(any_ratio_query)
        
        # 7. Check if the table structure is correct
        print("\n=== Verifying Table Structure ===")
        structure_query = """
        SELECT TOP 1 * FROM tbl_ratiorawdata
        """
        execute_query(structure_query)