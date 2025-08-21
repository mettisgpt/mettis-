#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.database.financial_db import FinancialDatabase
from utils import logger

def find_correct_equity():
    """Find the correct equity metrics in the database"""
    
    # Initialize the database connection
    db = FinancialDatabase(
        server='MUHAMMADUSMAN',
        database='MGFinancials'
    )
    
    print("Finding correct equity metrics...")
    print("=" * 50)
    
    try:
        # First, let's check the structure of tbl_ratiosheadmaster
        print("Checking tbl_ratiosheadmaster structure...")
        query = "SELECT TOP 5 * FROM tbl_ratiosheadmaster"
        result = db.execute_query(query)
        
        if not result.empty:
            print(f"Columns in tbl_ratiosheadmaster: {list(result.columns)}")
            print("Sample data:")
            print(result.head())
        
        # Search for equity in tbl_headsmaster
        print("\nSearching for 'equity' in tbl_headsmaster...")
        query = "SELECT SubHeadID, SubHeadName FROM tbl_headsmaster WHERE SubHeadName LIKE '%equity%'"
        result = db.execute_query(query)
        
        if not result.empty:
            print(f"Found {len(result)} metrics containing 'equity':")
            for _, row in result.iterrows():
                print(f"  SubHeadID: {row['SubHeadID']}, SubHeadName: '{row['SubHeadName']}'")
        else:
            print("No metrics containing 'equity' found")
            
        # Search for 'total' in tbl_headsmaster
        print("\nSearching for 'total' in tbl_headsmaster...")
        query = "SELECT SubHeadID, SubHeadName FROM tbl_headsmaster WHERE SubHeadName LIKE '%total%'"
        result = db.execute_query(query)
        
        if not result.empty:
            print(f"Found {len(result)} metrics containing 'total':")
            for _, row in result.iterrows():
                print(f"  SubHeadID: {row['SubHeadID']}, SubHeadName: '{row['SubHeadName']}'")
        else:
            print("No metrics containing 'total' found")
            
        # Let's also check what SubHeadID 113 actually is
        print("\nChecking SubHeadID 113...")
        query = "SELECT SubHeadID, SubHeadName FROM tbl_headsmaster WHERE SubHeadID = 113"
        result = db.execute_query(query)
        
        if not result.empty:
            print(f"SubHeadID 113:")
            for _, row in result.iterrows():
                print(f"  SubHeadID: {row['SubHeadID']}, SubHeadName: '{row['SubHeadName']}'")
        else:
            print("SubHeadID 113 not found")
            
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    find_correct_equity()