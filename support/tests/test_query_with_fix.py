from app.core.database.financial_db import FinancialDatabase
from app.core.database.updated_fix_head_id import get_available_head_id
from app.core.database.metric_classification import classify_metric
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Initialize database connection
server = 'DESKTOP-UFQMR5D'
database = 'FinancialData'
db = FinancialDatabase(server, database)

# Test metrics
metrics_to_test = [
    'PROFIT AFTER TAX per share',
    'Debt to Equity'
]

# Test company ID (assuming UBL has ID 1 for testing)
company_id = 1

# Test queries
print("\nTesting metric classification with the fix:")
for metric in metrics_to_test:
    print(f"\nMetric: {metric}")
    
    # Check classification
    metric_type = classify_metric(metric)
    print(f"Classification: {metric_type}")
    
    # Simulate the logic in updated_fix_head_id.py
    is_ratio_metric = (metric_type == "ratio")
    
    # Check forced ratio metrics
    normalized_metric_name = metric.lower().strip()
    forced_ratio_metrics = ['debt to equity', 'debt/equity', 'd/e ratio']
    if any(ratio_term in normalized_metric_name for ratio_term in forced_ratio_metrics):
        is_ratio_metric = True
        print(f"Forced to be treated as a ratio")
    
    # Apply the fix: Check if this is a dissection metric
    if metric_type == "dissection":
        is_ratio_metric = False
        print(f"Is a dissection metric, forcing classification as Regular Financial Metric")
    
    # Log the final classification
    print(f"Final classification: {'Ratio Metric' if is_ratio_metric else 'Regular Financial Metric'}")
    print(f"Will use {'tbl_ratiosheadmaster' if is_ratio_metric else 'tbl_headsmaster'} for lookup")
    
    # Test get_available_head_id with correct parameter order
    try:
        # Correct parameter order: db, company_id, metric_name, period_end=None, consolidation_id=None
        head_id_info = get_available_head_id(db, company_id, metric)
        print(f"\nHead ID info: {head_id_info}")
    except Exception as e:
        print(f"Error getting head ID: {e}")