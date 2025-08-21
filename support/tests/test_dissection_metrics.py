'''
Test script for dissection metrics handling in improved_query_approach.py
'''

import sys
from improved_query_approach import improved_query_approach
from app.core.database.detect_dissection_metrics import is_dissection_metric

def test_dissection_metrics():
    print("\n=== Testing Dissection Metrics Detection ===\n")
    
    test_metrics = [
        "EPS Annual Growth",
        "PAT Per Share",
        "ROI/Asset",
        "Revenue Percentage of Sales",
        "QoQ Revenue Growth",
        "Net Income",  # Not a dissection metric
    ]
    
    for metric in test_metrics:
        is_dissection, group_id, data_type = is_dissection_metric(metric)
        if is_dissection:
            print(f"{metric}: Dissection metric with Group ID {group_id}, Data Type: {data_type}")
        else:
            print(f"{metric}: Not a dissection metric")

def test_annual_growth_query():
    print("\n=== Testing Annual Growth Query ===\n")
    
    # Test with a known annual growth metric
    result = improved_query_approach(
        company_ticker='HBL',
        metric_name='EPS Annual Growth',
        term_description='2024-12-31',
        consolidation_type='Unconsolidated'
    )
    
    if result is not None and not result.empty:
        print("\nQuery successful! Results:")
        print(result)
    else:
        print("\nNo results found for Annual Growth query.")

def test_per_share_query():
    print("\n=== Testing Per Share Query ===\n")
    
    # Test with a known per share metric
    result = improved_query_approach(
        company_ticker='HBL',
        metric_name='PAT Per Share',
        term_description='2024-12-31',
        consolidation_type='Unconsolidated'
    )
    
    if result is not None and not result.empty:
        print("\nQuery successful! Results:")
        print(result)
    else:
        print("\nNo results found for Per Share query.")

def test_percentage_of_asset_query():
    print("\n=== Testing Percentage of Asset Query ===\n")
    
    # Test with a percentage of asset metric
    result = improved_query_approach(
        company_ticker='HBL',
        metric_name='ROI/Asset',
        term_description='2024-12-31',
        consolidation_type='Unconsolidated'
    )
    
    if result is not None and not result.empty:
        print("\nQuery successful! Results:")
        print(result)
    else:
        print("\nNo results found for Percentage of Asset query.")

def test_percentage_of_sales_query():
    print("\n=== Testing Percentage of Sales Query ===\n")
    
    # Test with a percentage of sales metric
    result = improved_query_approach(
        company_ticker='HBL',
        metric_name='Revenue Percentage of Sales',
        term_description='2024-12-31',
        consolidation_type='Unconsolidated'
    )
    
    if result is not None and not result.empty:
        print("\nQuery successful! Results:")
        print(result)
    else:
        print("\nNo results found for Percentage of Sales query.")

def test_quarterly_growth_query():
    print("\n=== Testing Quarterly Growth Query ===\n")
    
    # Test with a quarterly growth metric
    result = improved_query_approach(
        company_ticker='HBL',
        metric_name='QoQ Revenue Growth',
        term_description='2024-12-31',
        consolidation_type='Unconsolidated'
    )
    
    if result is not None and not result.empty:
        print("\nQuery successful! Results:")
        print(result)
    else:
        print("\nNo results found for Quarterly Growth query.")

# Run tests
if __name__ == "__main__":
    try:
        # Test dissection metrics detection
        test_dissection_metrics()
        
        # Test each type of dissection metric query
        test_annual_growth_query()
        test_per_share_query()
        test_percentage_of_asset_query()
        test_percentage_of_sales_query()
        test_quarterly_growth_query()
        
    except Exception as e:
        print(f"\nTest failed: {e}")