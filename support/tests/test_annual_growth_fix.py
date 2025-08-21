'''
Test script to verify the fix for the issue with 'annual growth' being treated as a metric instead of a DisectionGroup
'''

import os
import sys
from improved_query_approach import improved_query_approach
from utils import logger

def test_annual_growth_fix():
    print("\n=== Testing Annual Growth Fix ===\n")
    
    # Test with HBL's Investments Annual Growth
    print("Testing HBL's Investments Annual Growth...")
    result = improved_query_approach(
        company_ticker='HBL',
        metric_name='Investments Annual Growth',
        term_description='2024-12-31',
        consolidation_type='Unconsolidated'
    )
    
    if result is not None and not result.empty:
        print("\nQuery successful! Results:")
        print(result)
    else:
        print("\nNo results found for Investments Annual Growth query.")
    
    # Test with just 'Annual Growth' as the metric name
    print("\nTesting with just 'Annual Growth' as the metric name...")
    result = improved_query_approach(
        company_ticker='HBL',
        metric_name='Annual Growth',
        term_description='2024-12-31',
        consolidation_type='Unconsolidated'
    )
    
    if result is not None and not result.empty:
        print("\nQuery successful! Results:")
        print(result)
    else:
        print("\nNo results found for Annual Growth query.")

# Run the test
if __name__ == "__main__":
    test_annual_growth_fix()