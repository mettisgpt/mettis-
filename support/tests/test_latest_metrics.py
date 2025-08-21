'''
Test script to validate the Financial RAG system's ability to fetch the latest financial metrics
'''

import os
import sys
import time
from app.core.rag.financial_rag import FinancialRAG
from utils import logger

def test_latest_metrics():
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
    
    # Test cases for latest metrics using relative terms
    test_cases = [
        {
            "category": "Latest EPS",
            "query": "What is the latest EPS of HBL?",
            "description": "Testing relative term 'latest' for EPS metric"
        },
        {
            "category": "Last Reported Net Income",
            "query": "Tell me the last reported net income of UBL.",
            "description": "Testing relative term 'last reported' for Net Income metric"
        },
        {
            "category": "Most Recent Revenue",
            "query": "Give me the most recent revenue of Lucky Cement.",
            "description": "Testing relative term 'most recent' for Revenue metric"
        },
        {
            "category": "Latest Gross Profit Margin",
            "query": "What was the latest reported gross profit margin of MCB?",
            "description": "Testing relative term 'latest reported' for ratio metric"
        },
        {
            "category": "Last Available Total Assets",
            "query": "What's the last available value of total assets for Engro Corp?",
            "description": "Testing relative term 'last available' for assets metric"
        },
        {
            "category": "Latest Book Value",
            "query": "Show me the latest book value of Meezan Bank.",
            "description": "Testing relative term 'latest' for book value metric"
        },
        {
            "category": "Latest ROE",
            "query": "What is the latest ROE of TRG?",
            "description": "Testing relative term 'latest' for ROE ratio metric"
        },
        {
            "category": "Last Reported Operating Expenses",
            "query": "Tell me the last reported operating expenses of HBL.",
            "description": "Testing relative term 'last reported' for operating expenses metric"
        },
        {
            "category": "Most Recent Dividend Payout",
            "query": "What's the most recent dividend payout of OGDC?",
            "description": "Testing relative term 'most recent' for dividend payout metric"
        },
        {
            "category": "Latest TTM EPS",
            "query": "Give me the latest TTM EPS for BAHL.",
            "description": "Testing relative term 'latest' for TTM EPS metric"
        }
    ]
    
    results = []
    
    # Run each test case
    for i, test_case in enumerate(test_cases):
        print(f"\n{'='*50}")
        print(f"Test Case {i+1}: {test_case['category']}")
        print(f"Query: {test_case['query']}")
        print(f"Description: {test_case['description']}")
        print("Processing query...")
        
        # Process the query
        try:
            start_time = time.time()
            response = rag.process_query(test_case['query'])
            end_time = time.time()
            
            # Print the response
            print("\nResponse:")
            print(response)
            print(f"\nQuery processing time: {end_time - start_time:.2f} seconds")
            
            # Check if response contains error or no data
            error_indicators = [
                "error", "i'm sorry", "couldn't retrieve", "no data found", 
                "couldn't extract", "couldn't find", "not available", "unable to"
            ]
            
            has_error = any(indicator in response.lower() for indicator in error_indicators)
            
            if has_error:
                print("❌ Test FAILED: Error in response")
                results.append(False)
            else:
                # Check if response contains a value and period end date
                has_value = any(term in response.lower() for term in ["is", "was", "reported"])
                has_date = any(term in response.lower() for term in ["period", "quarter", "year", "month", "20"])
                
                if has_value and has_date:
                    print("[PASS] Test PASSED: Got a valid response with value and date")
                    results.append(True)
                else:
                    print("[FAIL] Test FAILED: Response missing value or date information")
                    results.append(False)
        except Exception as e:
            print(f"\n[FAIL] Test FAILED: Exception occurred: {e}")
            results.append(False)
    
    # Print summary
    print(f"\n{'='*50}")
    print(f"Test Summary: {sum(results)}/{len(results)} tests passed")
    
    # Print detailed results
    print("\nDetailed Results:")
    for i, (test_case, result) in enumerate(zip(test_cases, results)):
        status = "PASSED" if result else "FAILED"
        print(f"{i+1}. {test_case['category']}: {status}")
    
    return all(results)

if __name__ == "__main__":
    success = test_latest_metrics()
    sys.exit(0 if success else 1)