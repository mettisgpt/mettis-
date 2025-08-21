from app.core.database.metric_classification import classify_metric, get_metric_type_info

def test_classification():
    # Test cases for dissection metrics (per share)
    per_share_metrics = [
        "PAT per share",
        "EPS",
        "Earnings Per Share",
        "Revenue per share",
        "Book Value per Share",
        "pat per share",  # lowercase test
        "Per Share PAT",  # different word order
    ]
    
    # Test cases for ratio metrics
    ratio_metrics = [
        "Debt to Equity",
        "Debt/Equity",
        "D/E Ratio",
        "Profit Margin",
        "Return on Equity",
        "Interest Coverage",
        "debt to equity",  # lowercase test
    ]
    
    # Test cases for other metrics
    other_metrics = [
        "Revenue",
        "Net Income",
        "Total Assets",
        "Operating Profit",
    ]
    
    print("\n=== Testing Per Share Metrics (should all be 'dissection') ===")
    for metric in per_share_metrics:
        metric_type = classify_metric(metric)
        print(f"'{metric}' classified as: {metric_type}")
        if metric_type == "dissection":
            is_dissection, group_id, data_type = get_metric_type_info(metric)
            print(f"  - DisectionGroupID: {group_id}, data_type: {data_type}")
        assert metric_type == "dissection", f"Failed: '{metric}' should be classified as 'dissection'"
    
    print("\n=== Testing Ratio Metrics (should all be 'ratio') ===")
    for metric in ratio_metrics:
        metric_type = classify_metric(metric)
        print(f"'{metric}' classified as: {metric_type}")
        assert metric_type == "ratio", f"Failed: '{metric}' should be classified as 'ratio'"
    
    print("\n=== Testing Other Metrics (should be 'unknown') ===")
    for metric in other_metrics:
        metric_type = classify_metric(metric)
        print(f"'{metric}' classified as: {metric_type}")
        assert metric_type == "unknown", f"Failed: '{metric}' should be classified as 'unknown'"
    
    print("\nAll tests passed!")

if __name__ == "__main__":
    test_classification()