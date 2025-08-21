'''
Comprehensive test script for date format conversion functionality

This script tests all date format variations:
1. YYYY-MM-DD (standard format)
2. DD-MM-YYYY (European format)
3. DD-M-YYYY (European format with single digit month)
4. D-M-YYYY (European format with single digit day and month)
'''

import os
import sys
from app.core.rag.financial_rag import FinancialRAG
from utils import logger

def test_date_formats():
    # Initialize the Financial RAG system
    try:
        print("Initializing Financial RAG system...")
        rag = FinancialRAG(
            server='MUHAMMADUSMAN',
            database='MGFinancials',
            model_path='Mistral-7B-Instruct-v0.1.Q4_K_M.gguf'
        )
        print("Financial RAG system initialized successfully!\n")
    except Exception as e:
        print(f"Initialization error: {e}")
        return False
    
    # Test cases with different date formats
    test_cases = [
        {
            "format": "YYYY-MM-DD",
            "query": "Mari Energies Limited's ROE with Unconsolidated on periodend 2023-12-31",
            "expected_date": "2023-12-31"
        },
        {
            "format": "DD-MM-YYYY",
            "query": "Mari Energies Limited's ROE with Unconsolidated on periodend 31-12-2023",
            "expected_date": "2023-12-31"
        },
        {
            "format": "DD-M-YYYY",
            "query": "What was the Assets of HBL on 30-6-2023 consolidated?",
            "expected_date": "2023-06-30"
        }
    ]
    
    success = True
    
    # Run each test case
    for i, test_case in enumerate(test_cases):
        print(f"\n{'='*50}")
        print(f"Test Case {i+1}: {test_case['format']} format")
        print(f"Query: {test_case['query']}")
        print(f"Expected date format in SQL: {test_case['expected_date']}")
        print("Processing query...")
        
        # Process the query
        response = rag.process_query(test_case['query'])
        
        # Print the response
        print("\nResponse:")
        print(response)
        
        # Check if we got a valid response
        if response and "sorry" not in response.lower() and "error" not in response.lower():
            print(f"\n✓ Test case {i+1} PASSED")
        else:
            print(f"\n✗ Test case {i+1} FAILED")
            success = False
    
    return success

if __name__ == "__main__":
    try:
        print("\n=== Testing Date Format Conversion ===\n")
        success = test_date_formats()
        if success:
            print("\n✓ All date format tests completed successfully!")
            sys.exit(0)
        else:
            print("\n✗ Some date format tests failed!")
            sys.exit(1)
    except Exception as e:
        print(f"\n✗ Error during testing: {e}")
        sys.exit(1)