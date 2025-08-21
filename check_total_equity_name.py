#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.database.financial_db import FinancialDatabase
from utils import logger

def check_total_equity():
    """Check the exact name of SubHeadID 3 in the database"""
    
    # Initialize the database connection
    db = FinancialDatabase(
        server='MUHAMMADUSMAN',
        database='MGFinancials'
    )
    
    print("Checking SubHeadID 3 details...")
    print("=" * 50)
    
    try:
        # Check in tbl_headsmaster
        query = "SELECT SubHeadID, SubHeadName FROM tbl_headsmaster WHERE SubHeadID = 3"
        result = db.execute_query(query)
        
        if not result.empty:
            print(f"Found in tbl_headsmaster:")
            for _, row in result.iterrows():
                print(f"  SubHeadID: {row['SubHeadID']}, SubHeadName: '{row['SubHeadName']}'")
        else:
            print("Not found in tbl_headsmaster")
            
        # Check in tbl_ratiosheadmaster
        query = "SELECT SubHeadID, SubHeadName FROM tbl_ratiosheadmaster WHERE SubHeadID = 3"
        result = db.execute_query(query)
        
        if not result.empty:
            print(f"Found in tbl_ratiosheadmaster:")
            for _, row in result.iterrows():
                print(f"  SubHeadID: {row['SubHeadID']}, SubHeadName: '{row['SubHeadName']}'")
        else:
            print("Not found in tbl_ratiosheadmaster")
            
        # Also check what metrics contain 'equity'
        print("\nSearching for metrics containing 'equity'...")
        query = "SELECT SubHeadID, SubHeadName FROM tbl_headsmaster WHERE SubHeadName LIKE '%equity%'"
        result = db.execute_query(query)
        
        if not result.empty:
            print(f"Found {len(result)} metrics containing 'equity' in tbl_headsmaster:")
            for _, row in result.iterrows():
                print(f"  SubHeadID: {row['SubHeadID']}, SubHeadName: '{row['SubHeadName']}'")
        else:
            print("No metrics containing 'equity' found in tbl_headsmaster")
            
        # Check ratios too
        query = "SELECT SubHeadID, SubHeadName FROM tbl_ratiosheadmaster WHERE SubHeadName LIKE '%equity%'"
        result = db.execute_query(query)
        
        if not result.empty:
            print(f"Found {len(result)} metrics containing 'equity' in tbl_ratiosheadmaster:")
            for _, row in result.iterrows():
                print(f"  SubHeadID: {row['SubHeadID']}, SubHeadName: '{row['SubHeadName']}'")
        else:
            print("No metrics containing 'equity' found in tbl_ratiosheadmaster")
            
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    check_total_equity()