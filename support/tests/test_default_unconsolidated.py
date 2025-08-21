'''
Test script to verify that the default consolidation is set to unconsolidated (ConsolidationID=2)
unless explicitly specified as consolidated in the query.
'''

import os
import sys
import logging

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Import the FinancialRAG class
from app.core.rag.financial_rag import FinancialRAG

def test_default_unconsolidated():
    # Initialize the FinancialRAG system
    rag = FinancialRAG(server='MUHAMMADUSMAN', database='MGFinancials')
    
    print("\n=== Test 1: Query without specifying consolidation (should default to unconsolidated) ===\n")
    # Test query without specifying consolidation
    query1 = "What was the Assets of HBL on 30-6-2023?"
    print(f"Query: {query1}")
    
    # Process the query
    response1 = rag.process_query(query1)
    print(f"Response: {response1}")
    
    print("\n=== Test 2: Query explicitly specifying consolidated ===\n")
    # Test query explicitly specifying consolidated
    query2 = "What was the Assets of HBL on 30-6-2023 consolidated?"
    print(f"Query: {query2}")
    
    # Process the query
    response2 = rag.process_query(query2)
    print(f"Response: {response2}")
    
    print("\n=== Test 3: Query explicitly specifying unconsolidated ===\n")
    # Test query explicitly specifying unconsolidated
    query3 = "What was the Assets of HBL on 30-6-2023 unconsolidated?"
    print(f"Query: {query3}")
    
    # Process the query
    response3 = rag.process_query(query3)
    print(f"Response: {response3}")
    
    print("\nTest completed!")

if __name__ == "__main__":
    test_default_unconsolidated()