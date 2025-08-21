'''
Test script to verify the fix for the duplicate term_id issue in improved_query_approach.py
'''

import sys
import os

# Import the fixed implementation
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from fixed_improved_query_approach import query_dissection_data, query_ttm_data

# Test the fixed implementation
print("\n=== Testing Fixed Implementation ===\n")

# Test dissection data query with a date string
print("\n1. Testing dissection data query with a date string:\n")
result = query_dissection_data(
    company_ticker='HBL',
    metric_name='PAT Per Share',
    period_term='2023-12-31',
    dissection_group_id=2,  # Per Share group
    consolidation_type='Unconsolidated',
    data_type='regular'
)

# Test TTM data query with a date string
print("\n2. Testing TTM data query with a date string:\n")
result = query_ttm_data(
    company_ticker='UBL',
    metric_name='Net Income',
    period_term='2023-12-31',
    consolidation_type='Unconsolidated'
)

print("\n=== Tests Completed ===\n")