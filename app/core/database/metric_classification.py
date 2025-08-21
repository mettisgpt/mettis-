import re
import logging
from functools import lru_cache

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@lru_cache(maxsize=128)
def classify_metric(metric_name):
    """
    Classify a metric as dissection, ratio, or unknown based on its name.
    Uses a priority-based approach to ensure consistent classification.
    
    Args:
        metric_name: The name of the metric to classify
        
    Returns:
        str: 'dissection', 'ratio', or 'unknown'
    """
    if not metric_name:
        return "unknown"
    
    # Normalize the metric name for case-insensitive matching
    name_lower = metric_name.lower().strip()
    
    # Highest priority: Per Share detection
    # Special case for EPS (Earnings Per Share)
    if name_lower == 'eps' or name_lower == 'earnings per share':
        logger.info(f"Metric '{metric_name}' classified as dissection (per share - EPS)")
        return "dissection"
        
    if any(term in name_lower for term in ['per share', '/share', 'per-share']):
        logger.info(f"Metric '{metric_name}' classified as dissection (per share)")
        return "dissection"
    
    # Other dissection keywords (second priority)
    if any(term in name_lower for term in [
        'annual growth', 'yoy growth', 'year over year growth',
        'percentage of asset', '% of asset', 'percent of asset', 'of asset',
        'percentage of sales', '% of sales', 'percent of sales',
        'percentage of revenue', '% of revenue', 'percent of revenue',
        'of sales', 'of revenue',
        'quarterly growth', 'qoq growth', 'quarter over quarter growth',
        'per unit', 'per branch', 'per customer'
    ]):
        logger.info(f"Metric '{metric_name}' classified as dissection (other)")
        return "dissection"
    
    # Ratio keywords (lowest priority)
    if any(keyword in name_lower for keyword in [
        'ratio', 'margin', 'return on', 'roe', 'roa', 'roce', 'yield', 
        'coverage', 'to equity', 'debt to', 'debt/equity', 'd/e ratio'
    ]):
        logger.info(f"Metric '{metric_name}' classified as ratio")
        return "ratio"
    
    # Default case
    logger.info(f"Metric '{metric_name}' classification is unknown")
    return "unknown"


# For backward compatibility with existing code
def get_metric_type_info(metric_name):
    """
    Get detailed information about a metric's type based on its classification.
    
    Args:
        metric_name: The name of the metric to check
        
    Returns:
        Tuple of (is_dissection, dissection_group_id, data_type) where:
            - is_dissection: Boolean indicating if this is a dissection metric
            - dissection_group_id: The appropriate DisectionGroupID (1-5) or None
            - data_type: The appropriate data_type ('regular', 'ratio', 'quarter', 'ttm') or None
    """
    if not metric_name:
        return False, None, None
    
    # Normalize the metric name for better matching
    normalized_metric = metric_name.lower().strip()
    
    # First classify the metric
    metric_type = classify_metric(metric_name)
    
    if metric_type == "dissection":
        # Now determine the specific dissection type and group ID
        
        # Check for Annual Growth metrics
        if any(term in normalized_metric for term in ['annual growth', 'yoy growth', 'year over year growth', 'year-over-year growth']):
            logger.info(f"Metric '{metric_name}' identified as Annual Growth dissection metric (Group ID: 2)")
            return True, 2, 'ratio'
        
        # Check for Per Share metrics
        if any(term in normalized_metric for term in ['per share', '/share', 'per-share']):
            logger.info(f"Metric '{metric_name}' identified as Per Share dissection metric (Group ID: 1)")
            return True, 1, 'regular'
        
        # Check for Percentage Of Asset metrics
        if any(term in normalized_metric for term in ['percentage of asset', '% of asset', 'percent of asset', 'of asset']):
            logger.info(f"Metric '{metric_name}' identified as Percentage Of Asset dissection metric (Group ID: 3)")
            return True, 3, 'ratio'
        
        # Check for Percentage Of Sales/Revenue metrics
        if any(term in normalized_metric for term in ['percentage of sales', '% of sales', 'percent of sales', 
                                                     'percentage of revenue', '% of revenue', 'percent of revenue',
                                                     'of sales', 'of revenue']):
            logger.info(f"Metric '{metric_name}' identified as Percentage Of Sales/Revenue dissection metric (Group ID: 4)")
            return True, 4, 'ratio'
        
        # Check for Quarterly Growth metrics
        if any(term in normalized_metric for term in ['quarterly growth', 'qoq growth', 'quarter over quarter growth', 
                                                     'quarter-over-quarter growth', 'q/q growth']):
            logger.info(f"Metric '{metric_name}' identified as Quarterly Growth dissection metric (Group ID: 5)")
            return True, 5, 'quarter'
    
    # Not a dissection metric or couldn't determine specific type
    return False, None, None


# Example usage
if __name__ == "__main__":
    test_metrics = [
        "EPS Annual Growth",
        "PAT Per Share",
        "ROI/Asset",
        "Revenue Percentage of Sales",
        "QoQ Revenue Growth",
        "Net Income",  # Not a dissection metric
        "Debt to Equity",  # Ratio metric
    ]
    
    for metric in test_metrics:
        metric_type = classify_metric(metric)
        print(f"{metric}: Classified as {metric_type}")
        
        # For backward compatibility
        is_dissection, group_id, data_type = get_metric_type_info(metric)
        if is_dissection:
            print(f"  - Dissection metric with Group ID {group_id}, Data Type: {data_type}")