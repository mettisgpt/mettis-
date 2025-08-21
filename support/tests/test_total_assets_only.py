#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.rag.financial_rag import FinancialRAG
from utils import logger

def test_total_assets():
    """Test the Last Available Total Assets query specifically"""
    
    # Initialize the RAG system
    rag = FinancialRAG(
        server='MUHAMMADUSMAN',
        database='MGFinancials'
    )
    
    query = "Last Available Total Assets for Engro Corp"
    print(f"\nTesting query: {query}")
    print("=" * 50)
    
    try:
        # Process the query
        response = rag.process_query(query)
        print(f"Response: {response}")
        
        # Check if response contains value and date information
        if any(keyword in response.lower() for keyword in ['million', 'billion', 'thousand', 'pkr', 'rs.', 'value']):
            if any(keyword in response.lower() for keyword in ['2023', '2024', 'march', 'december', 'quarter', 'year']):
                print("[PASS] Test PASSED: Got a valid response with value and date")
                return True
            else:
                print("[FAIL] Test FAILED: Response missing date information")
                return False
        else:
            print("[FAIL] Test FAILED: Response missing value information")
            return False
            
    except Exception as e:
        print(f"[FAIL] Test FAILED: Exception occurred: {str(e)}")
        return False

if __name__ == "__main__":
    test_total_assets()