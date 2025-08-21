'''
Test script to verify term and consolidation mapping
'''

import os
import sys
from app.core.database.financial_db import FinancialDatabase
from utils import logger

# Initialize database connection with the same parameters as financial_rag_cli.py
server = 'MUHAMMADUSMAN'
database = 'MGFinancials'
db = FinancialDatabase(server, database)

# Test term mapping
def test_term_mapping():
    print("\n=== Testing Term Mapping ===")
    test_cases = [
        ("FY 2023", 2),  # Should map to 6M (TermID=2)
        ("6M", 2),
        ("1Y", 4),
        ("3M", 1),
    ]
    
    for term_str, expected_id in test_cases:
        # Use a default company_id of 1 for testing
        company_id = 1
        term_id = db.get_term_id(term_str, company_id)
        print(f"Term: {term_str} -> TermID: {term_id} (Expected: {expected_id})")
        assert term_id == expected_id, f"Term mapping failed for {term_str}"

# Test consolidation mapping
def test_consolidation_mapping():
    print("\n=== Testing Consolidation Mapping ===")
    test_cases = [
        ("Consolidated", 1),
        ("Unconsolidated", 2),
        ("Standalone", 2),
    ]
    
    for flag, expected_id in test_cases:
        consolidation_id = db.get_consolidation_id(flag)
        print(f"Consolidation: {flag} -> ConsolidationID: {consolidation_id} (Expected: {expected_id})")
        assert consolidation_id == expected_id, f"Consolidation mapping failed for {flag}"

# Run tests
if __name__ == "__main__":
    try:
        test_term_mapping()
        test_consolidation_mapping()
        print("\nAll tests passed!")
    except Exception as e:
        print(f"\nTest failed: {e}")