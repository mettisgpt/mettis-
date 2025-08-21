'''
Author: AI Assistant
Date: 2023-07-10
Description: Test script for the Financial Database module
'''

import sys
import os

# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.core.database.financial_db import FinancialDatabase

def test_database_connection():
    """
    Test the database connection and metadata loading
    """
    print("Testing database connection...")
    
    # Initialize the database with the server and database name
    db = FinancialDatabase(
        server='MUHAMMADUSMAN',
        database='MGFinancials'
    )
    
    # Test connection by executing a simple query
    try:
        result = db.execute_query("SELECT TOP 5 * FROM tbl_companieslist")
        print("\nConnection successful! Sample companies:")
        print(result)
    except Exception as e:
        print(f"Connection error: {e}")
        return
    
    # Test metadata loading
    try:
        print("\nLoading metadata...")
        db.load_metadata()
        print("Metadata loaded successfully!")
        
        # Print some metadata statistics
        for key, df in db.metadata_cache.items():
            print(f"- {key}: {len(df)} records")
    except Exception as e:
        print(f"Metadata loading error: {e}")
        return
    
    # Test entity lookup functions
    print("\nTesting entity lookups:")
    
    # Test company lookup
    company_name = "HBL"
    company_id = db.get_company_id(company_name)
    print(f"Company ID for '{company_name}': {company_id}")
    
    # Test head lookup
    metric_name = "EPS"
    head_id, is_ratio = db.get_head_id(metric_name)
    print(f"Head ID for '{metric_name}': {head_id} (is_ratio: {is_ratio})")
    
    # Test term lookup
    term_desc = "Q2 2023"
    term_id = db.get_term_id(term_desc, company_id)
    print(f"Term ID for '{term_desc}': {term_id}")
    
    # Test consolidation lookup
    cons_desc = "standalone"
    cons_id = db.get_consolidation_id(cons_desc)
    print(f"Consolidation ID for '{cons_desc}': {cons_id}")
    
    # Test financial data retrieval
    if all([company_id, head_id, term_id, cons_id]):
        print("\nTesting financial data retrieval:")
        query = db.build_financial_query(company_id, head_id, term_id, cons_id, is_ratio)
        print(f"Generated query:\n{query}")
        
        try:
            result = db.execute_query(query)
            print("\nQuery result:")
            print(result)
        except Exception as e:
            print(f"Query execution error: {e}")
    
    print("\nTest completed!")

if __name__ == "__main__":
    test_database_connection()