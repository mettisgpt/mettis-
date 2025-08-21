from app.core.database.updated_fix_head_id import get_available_head_id
from app.core.database.metric_classification import classify_metric
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Test metrics
metrics_to_test = [
    'PROFIT AFTER TAX per share',
    'UBL PROFIT AFTER TAX per share',
    'Debt to Equity'
]

# Test classification
print("\nTesting metric classification:")
for metric in metrics_to_test:
    metric_type = classify_metric(metric)
    print(f"Metric '{metric}' is classified as: {metric_type}")
    
    # Simulate the logic in updated_fix_head_id.py
    is_ratio_metric = (metric_type == "ratio")
    
    # Check forced ratio metrics
    normalized_metric_name = metric.lower().strip()
    forced_ratio_metrics = ['debt to equity', 'debt/equity', 'd/e ratio']
    if any(ratio_term in normalized_metric_name for ratio_term in forced_ratio_metrics):
        is_ratio_metric = True
        print(f"  - Forced to be treated as a ratio")
    
    # Apply the fix: Check if this is a dissection metric
    if metric_type == "dissection":
        is_ratio_metric = False
        print(f"  - Is a dissection metric, forcing classification as Regular Financial Metric")
    
    # Log the final classification
    print(f"  - Final classification: {'Ratio Metric' if is_ratio_metric else 'Regular Financial Metric'}")
    print(f"  - Will use {'tbl_ratiosheadmaster' if is_ratio_metric else 'tbl_headsmaster'} for lookup")
    print()