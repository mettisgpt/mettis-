#!/usr/bin/env python3
"""
Test script for dissection group handling in EnhancedFinancialRAG
"""

from support.debug.enhanced_financial_rag import EnhancedFinancialRAG

# Initialize the RAG system
rag = EnhancedFinancialRAG('localhost', 'FinancialData')

# Test queries for different dissection groups
test_queries = [
    # Per Share
    "What was Lucky Cement's EPS per share for 2023-06-30 (Q4)?",
    
    # Annual Growth
    "Give me the annual growth of HBL's Net Sales for 2023-12-31.",
    
    # Percentage Of Asset
    "What is UBL's Loan-to-Assets ratio as a percentage of assets for 2023-12-31?",
    
    # Percentage Of Sales/Revenue
    "How much was Engro Fertilizer's Cost of Goods Sold as a percentage of sales in 2022-12-31?",
    
    # Quarterly Growth
    "What is the quarterly growth in Profit After Tax for MCB in 2023-Q3?",
    
    # Per Share with Consolidation
    "What is Lucky Cement's Book Value per share (consolidated) for 2023-12-31?",
    
    # Annual Growth with Industry Join
    "What is the annual growth of HBL's Retention Ratio for 2023-12-31?"
]

# Run each test query and print the results
for i, query in enumerate(test_queries, 1):
    print(f"\n{i}. TEST QUERY: {query}")
    print("-" * 80)
    
    # Process the query and get the result
    result = rag.process_query(query)
    
    # Print the result
    print("RESULT:")
    print(result)
    print("=" * 80)

print("\nAll test queries completed.")