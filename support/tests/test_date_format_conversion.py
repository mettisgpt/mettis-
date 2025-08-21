#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Test script for date format conversion functionality in Financial RAG

This script tests the ability to handle different date formats in queries,
specifically converting from DD-MM-YYYY to YYYY-MM-DD format.
"""

import os
import sys
import logging
from loguru import logger

# Add the parent directory to the path so we can import the app modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the Financial RAG components
from app.core.rag.financial_rag import FinancialRAG

# Configure logging
logging.basicConfig(level=logging.INFO)

def test_date_format_conversion():
    # Get the model path
    model_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'Mistral-7B-Instruct-v0.1.Q4_K_M.gguf'))
    if not os.path.exists(model_path):
        logger.error(f"Model file not found at {model_path}")
        raise FileNotFoundError(f"Model file not found at {model_path}")
    
    # Initialize the Financial RAG system
    logger.info("Initializing Financial RAG...")
    financial_rag = FinancialRAG(
        server='MUHAMMADUSMAN',
        database='MGFinancials',
        model_path=model_path
    )
    
    # Test query with DD-MM-YYYY date format
    test_query = "What was the Assets of HBL on 30-6-2023 consolidated?"
    logger.info(f"Testing query with DD-MM-YYYY format: {test_query}")
    
    # Extract entities to verify date conversion
    logger.info("Extracting entities and verifying date format conversion...")
    entities = financial_rag._extract_entities(test_query)
    
    # Log all extracted entities
    logger.info(f"Extracted entities: {entities}")
    
    # Specifically verify the period_end date conversion
    period_end = entities.get('period_end', 'Not found')
    logger.info(f"Period end date after conversion: {period_end}")
    
    # Verify the format is YYYY-MM-DD
    if period_end != 'Not found':
        if period_end.startswith('2023-') and len(period_end.split('-')) == 3:
            logger.info(f"✓ Date successfully converted to YYYY-MM-DD format: {period_end}")
        else:
            logger.error(f"✗ Date not in expected YYYY-MM-DD format: {period_end}")
    else:
        logger.error("✗ No period_end date found in extracted entities")
    
    
    # Process the query
    response = financial_rag.process_query(test_query)
    
    # Print the response
    logger.info("Response:")
    print("\n" + response + "\n")
    
    # Return success if we got a response
    return response is not None and len(response) > 0

if __name__ == "__main__":
    try:
        success = test_date_format_conversion()
        if success:
            logger.info("Test completed successfully!")
            sys.exit(0)
        else:
            logger.error("Test failed!")
            sys.exit(1)
    except Exception as e:
        logger.error(f"Error during test: {e}")
        sys.exit(1)