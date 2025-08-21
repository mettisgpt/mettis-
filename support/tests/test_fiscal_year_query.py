#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Test script for fiscal year query functionality in Financial RAG

This script tests the ability to query financial data using fiscal year filtering.
It demonstrates how the system extracts fiscal year information and uses it to filter
the SQL query results.
"""

import os
import sys
import logging
from loguru import logger

# Add the parent directory to the path so we can import the app modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the Financial RAG components
from app.core.rag.financial_rag import FinancialRAG
from app.core.chat.mistral_chat import MistralChat
from app.core.database.financial_db import FinancialDatabase

# Configure logging
logging.basicConfig(level=logging.INFO)

# Initialize the components
def test_fiscal_year_query():
    # Get the model path
    model_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'Mistral-7B-Instruct-v0.1.Q4_K_M.gguf'))
    if not os.path.exists(model_path):
        logger.error(f"Model file not found at {model_path}")
        raise FileNotFoundError(f"Model file not found at {model_path}")
    
    # Initialize the Financial RAG system directly
    logger.info("Initializing Financial RAG...")
    financial_rag = FinancialRAG(
        server='MUHAMMADUSMAN',
        database='MGFinancials',
        model_path=model_path
    )
    
    # Test query with fiscal year
    test_query = "What was the Assets of HBL 6M in FY 2023 consolidated?"
    logger.info(f"Testing query: {test_query}")
    
    # Process the query
    response = financial_rag.process_query(test_query)
    
    # Print the response
    logger.info("Response:")
    print("\n" + response + "\n")
    
    # Return success if we got a response
    return response is not None and len(response) > 0

if __name__ == "__main__":
    try:
        success = test_fiscal_year_query()
        if success:
            logger.info("Test completed successfully!")
            sys.exit(0)
        else:
            logger.error("Test failed!")
            sys.exit(1)
    except Exception as e:
        logger.error(f"Error during test: {e}")
        sys.exit(1)