import re
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def is_dissection_metric(metric_name):
    """
    Determine if a metric is a dissection metric and return its appropriate DisectionGroupID.
    
    Dissection metrics are categorized as follows:
    - Annual Growth (DisectionGroupID = 2)
    - Per Share (DisectionGroupID = 1)
    - Percentage Of Asset (DisectionGroupID = 3)
    - Percentage Of Sales/Revenue (DisectionGroupID = 4)
    - Quarterly Growth (DisectionGroupID = 5)
    
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
    
    # Not a dissection metric
    logger.info(f"Metric '{metric_name}' is not identified as a dissection metric")
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
    ]
    
    for metric in test_metrics:
        is_dissection, group_id, data_type = is_dissection_metric(metric)
        if is_dissection:
            print(f"{metric}: Dissection metric with Group ID {group_id}, Data Type: {data_type}")
        else:
            print(f"{metric}: Not a dissection metric")