import sys
import os
import pandas as pd
import re

# Add the project root directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
from app.core.database.financial_db import FinancialDatabase
from app.core.database.detect_dissection_metrics import is_dissection_metric
from app.core.database.metric_classification import classify_metric, get_metric_type_info
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_available_head_id(db, company_id, metric_name, period_end=None, consolidation_id=None):
    """
    Get a head_id that actually has data for the specified company and parameters.
    
    This function first determines if the metric is a ratio or dissection metric based on its name,
    then gets all possible SubHeadIDs for the given metric name from the appropriate table,
    filters them based on the company's industry and sector,
    and finally checks which one actually has data for the specified company.
    
    Args:
        db: FinancialDatabase instance
        company_id: Company ID
        metric_name: Name of the financial metric
        period_end: Optional specific period end date (format: 'YYYY-MM-DD')
        consolidation_id: Optional consolidation ID
        
    Returns:
        Tuple of (head_id, is_ratio, is_dissection, dissection_group_id, data_type) if found, 
        (None, False, False, None, None) otherwise
    """
    
    # Metric name mappings for common aliases
    METRIC_ALIASES = {
        'total assets': 'Total Assets Of Window Takaful Operations - Operator\'s Fund',
        'assets': 'Total Assets Of Window Takaful Operations - Operator\'s Fund',
        'book value': 'Total Assets Of Window Takaful Operations - Operator\'s Fund'
    }
    
    # Check if metric needs to be mapped to an alias
    metric_key = metric_name.strip().lower()
    if metric_key in METRIC_ALIASES:
        original_metric = metric_name
        metric_name = METRIC_ALIASES[metric_key]
        logger.info(f"Mapping metric '{original_metric}' → '{metric_name}'")
    
    # Special handling for TTM EPS
    if metric_name.lower() == 'ttm eps' or metric_name.lower() == 'eps ttm':
        logger.info(f"Special handling for TTM EPS metric")
        # First try to find EPS in regular heads (tbl_headsmaster)
        query = "SELECT SubHeadID, SubHeadName FROM tbl_headsmaster WHERE LOWER(SubHeadName) LIKE '%eps%'"
        eps_heads = db.execute_query(query)
        
        if not eps_heads.empty:
            logger.info(f"Found {len(eps_heads)} EPS-related regular heads")
            
            # Try each EPS head to find one with data
            for _, row in eps_heads.iterrows():
                sub_head_id = row['SubHeadID']
                head_name = row['SubHeadName']
                
                # Build a query to check if data exists
                where_clauses = [f"f.CompanyID = {company_id}", f"f.SubHeadID = {sub_head_id}"]
                
                if consolidation_id is not None:
                    where_clauses.append(f"f.ConsolidationID = {consolidation_id}")
                    
                where_clause = " AND ".join(where_clauses)
                
                query = f"""
                SELECT COUNT(*) as count FROM tbl_financialrawdata f 
                WHERE {where_clause}
                """
                
                result = db.execute_query(query)
                count = result.iloc[0]['count'] if not result.empty else 0
                
                logger.info(f"EPS head: Regular SubHeadID {sub_head_id} ({head_name}) has {count} rows of data")
                
                if count > 0:
                    logger.info(f"Found data for EPS regular SubHeadID {sub_head_id} ({head_name})")
                    return sub_head_id, False, False, None, None
        else:
            logger.info(f"No EPS-related regular heads found, trying ratio heads")
            
        # If no regular heads with data found, try ratio heads
        # Get all EPS-related ratio heads
        query = "SELECT SubHeadID, HeadNames FROM tbl_ratiosheadmaster WHERE LOWER(HeadNames) LIKE '%eps%'"
        eps_ratio_heads = db.execute_query(query)
        
        if not eps_ratio_heads.empty:
            logger.info(f"Found {len(eps_ratio_heads)} EPS-related ratio heads")
            
            # Try each EPS ratio head to find one with data
            for _, row in eps_ratio_heads.iterrows():
                sub_head_id = row['SubHeadID']
                head_name = row['HeadNames']
                
                # Build a query to check if data exists
                where_clauses = [f"r.CompanyID = {company_id}", f"r.SubHeadID = {sub_head_id}"]
                
                if consolidation_id is not None:
                    where_clauses.append(f"r.ConsolidationID = {consolidation_id}")
                    
                where_clause = " AND ".join(where_clauses)
                
                query = f"""
                SELECT COUNT(*) as count FROM tbl_ratiorawdata r 
                WHERE {where_clause}
                """
                
                result = db.execute_query(query)
                count = result.iloc[0]['count'] if not result.empty else 0
                
                logger.info(f"EPS head: Ratio SubHeadID {sub_head_id} ({head_name}) has {count} rows of data")
                
                if count > 0:
                    logger.info(f"Found data for EPS ratio SubHeadID {sub_head_id} ({head_name})")
                    return sub_head_id, True, False, None, None
        else:
            logger.error(f"No EPS-related ratio heads found")
                
        logger.error(f"No data found for any EPS head (regular or ratio)")
        return None, False, False, None, None
    
    # Check if this is a dissection metric using the new classification system
    metric_type = classify_metric(metric_name)
    is_dissection_result, dissection_group_id, data_type = get_metric_type_info(metric_name)
    
    # Always prioritize dissection classification
    if metric_type == "dissection" or is_dissection_result:
        logger.info(f"Metric '{metric_name}' identified as a dissection metric with group ID {dissection_group_id} and data type {data_type}")
        
        # Extract the base metric name (remove the dissection part)
        base_metric = metric_name
        
        # For Per Share metrics
        if dissection_group_id == 1:
            base_metric = re.sub(r'\s*per\s*share\s*', '', metric_name.lower(), flags=re.IGNORECASE).strip()
            logger.info(f"Extracted base metric '{base_metric}' from Per Share metric '{metric_name}'")
        
        # For Annual Growth metrics
        elif dissection_group_id == 2:
            base_metric = re.sub(r'\s*annual\s*growth\s*', '', metric_name.lower(), flags=re.IGNORECASE).strip()
            logger.info(f"Extracted base metric '{base_metric}' from Annual Growth metric '{metric_name}'")
        
        # For Percentage Of Asset metrics
        elif dissection_group_id == 3:
            base_metric = re.sub(r'\s*(percentage|%)\s*of\s*asset\s*', '', metric_name.lower(), flags=re.IGNORECASE).strip()
            logger.info(f"Extracted base metric '{base_metric}' from Percentage Of Asset metric '{metric_name}'")
        
        # For Percentage Of Sales/Revenue metrics
        elif dissection_group_id == 4:
            base_metric = re.sub(r'\s*(percentage|%)\s*of\s*(sales|revenue)\s*', '', metric_name.lower(), flags=re.IGNORECASE).strip()
            logger.info(f"Extracted base metric '{base_metric}' from Percentage Of Sales/Revenue metric '{metric_name}'")
        
        # For Quarterly Growth metrics
        elif dissection_group_id == 5:
            base_metric = re.sub(r'\s*(quarterly|qoq)\s*growth\s*', '', metric_name.lower(), flags=re.IGNORECASE).strip()
            logger.info(f"Extracted base metric '{base_metric}' from Quarterly Growth metric '{metric_name}'")
        
        # If base_metric is empty, use the original metric name
        if not base_metric:
            base_metric = metric_name
            logger.warning(f"Could not extract base metric from '{metric_name}', using original name")
        
        # Get sector and industry information for the company
        sector_query = f"""
        SELECT c.SectorID, s.SectorName 
        FROM tbl_companieslist c
        JOIN tbl_sectornames s ON c.SectorID = s.SectorID
        WHERE c.CompanyID = {company_id}
        """
        sector_result = db.execute_query(sector_query)
        
        sector_id = None
        industry_id = None
        
        if not sector_result.empty:
            sector_id = sector_result.iloc[0]['SectorID']
            logger.info(f"Found SectorID: {sector_id} for company ID: {company_id}")
            
            # Get industry information
            industry_query = f"""
            SELECT i.IndustryID, i.IndustryName 
            FROM tbl_industrynames i
            JOIN tbl_industryandsectormapping m ON i.IndustryID = m.industryid
            WHERE m.sectorid = {sector_id}
            """
            industry_result = db.execute_query(industry_query)
            
            if not industry_result.empty:
                industry_id = industry_result.iloc[0]['IndustryID']
                logger.info(f"Found IndustryID: {industry_id} for SectorID: {sector_id}")
        
        # Search for the base metric in the appropriate heads table
        # For ratio dissection data, search in tbl_ratiosheadmaster
        if data_type.lower() == 'ratio':
            if sector_id is not None and industry_id is not None:
                query = f"""
                SELECT r.SubHeadID, r.HeadNames 
                FROM tbl_ratiosheadmaster r
                JOIN tbl_industryandsectormapping m ON r.IndustryID = m.industryid
                WHERE LOWER(r.HeadNames) LIKE '%{base_metric.lower()}%'
                AND m.sectorid = {sector_id}
                """
            else:
                query = f"SELECT SubHeadID, HeadNames FROM tbl_ratiosheadmaster WHERE LOWER(HeadNames) LIKE '%{base_metric.lower()}%'"
            
            possible_heads = db.execute_query(query)
            
            if possible_heads.empty:
                logger.warning(f"No ratio heads found for base metric: {base_metric}")
                return None, False, False, None, None
            
            logger.info(f"Found {len(possible_heads)} possible ratio heads for base metric: {base_metric}")
            
            # Determine which dissection table to use
            if data_type.lower() == 'ratio':
                table_name = "tbl_disectionrawdata_Ratios"
            elif data_type.lower() == 'quarter':
                table_name = "tbl_disectionrawdata_Quarter"
            elif data_type.lower() == 'ttm':
                table_name = "tbl_disectionrawdataTTM"
            else:
                table_name = "tbl_disectionrawdata"
            
            # Check each SubHeadID to see if it has dissection data for the company
            for _, row in possible_heads.iterrows():
                sub_head_id = row['SubHeadID']
                sub_head_name = row['HeadNames']
                
                # Build a query to check if dissection data exists
                where_clauses = [
                    f"d.CompanyID = {company_id}", 
                    f"d.SubHeadID = {sub_head_id}",
                    f"d.DisectionGroupID = {dissection_group_id}"
                ]
                
                if period_end is not None:
                    where_clauses.append(f"d.PeriodEnd = '{period_end}'")
                    
                if consolidation_id is not None:
                    where_clauses.append(f"d.ConsolidationID = {consolidation_id}")
                    
                where_clause = " AND ".join(where_clauses)
                
                query = f"""
                SELECT COUNT(*) as count FROM {table_name} d 
                WHERE {where_clause}
                """
                
                result = db.execute_query(query)
                count = result.iloc[0]['count'] if not result.empty else 0
                
                logger.info(f"Ratio SubHeadID {sub_head_id} ({sub_head_name}) has {count} rows of dissection data")
                
                if count > 0:
                    logger.info(f"Found dissection data for ratio SubHeadID {sub_head_id} ({sub_head_name})")
                    return sub_head_id, True, True, dissection_group_id, data_type
        
        # For regular dissection data, search in tbl_headsmaster
        else:
            if sector_id is not None and industry_id is not None:
                query = f"""
                SELECT h.SubHeadID, h.SubHeadName 
                FROM tbl_headsmaster h
                JOIN tbl_industryandsectormapping m ON h.IndustryID = m.industryid
                WHERE LOWER(h.SubHeadName) LIKE '%{base_metric.lower()}%'
                AND m.sectorid = {sector_id}
                """
            else:
                query = f"SELECT SubHeadID, SubHeadName FROM tbl_headsmaster WHERE LOWER(SubHeadName) LIKE '%{base_metric.lower()}%'"
            
            possible_heads = db.execute_query(query)
            
            if possible_heads.empty:
                logger.warning(f"No regular heads found for base metric: {base_metric}")
                return None, False, False, None, None
            
            logger.info(f"Found {len(possible_heads)} possible regular heads for base metric: {base_metric}")
            
            # Determine which dissection table to use
            if data_type.lower() == 'ratio':
                table_name = "tbl_disectionrawdata_Ratios"
            elif data_type.lower() == 'quarter':
                table_name = "tbl_disectionrawdata_Quarter"
            elif data_type.lower() == 'ttm':
                table_name = "tbl_disectionrawdataTTM"
            else:
                table_name = "tbl_disectionrawdata"
            
            # Check each SubHeadID to see if it has dissection data for the company
            for _, row in possible_heads.iterrows():
                sub_head_id = row['SubHeadID']
                sub_head_name = row['SubHeadName']
                
                # Build a query to check if dissection data exists
                where_clauses = [
                    f"d.CompanyID = {company_id}", 
                    f"d.SubHeadID = {sub_head_id}",
                    f"d.DisectionGroupID = {dissection_group_id}"
                ]
                
                if period_end is not None:
                    where_clauses.append(f"d.PeriodEnd = '{period_end}'")
                    
                if consolidation_id is not None:
                    where_clauses.append(f"d.ConsolidationID = {consolidation_id}")
                    
                where_clause = " AND ".join(where_clauses)
                
                query = f"""
                SELECT COUNT(*) as count FROM {table_name} d 
                WHERE {where_clause}
                """
                
                result = db.execute_query(query)
                count = result.iloc[0]['count'] if not result.empty else 0
                
                logger.info(f"Regular SubHeadID {sub_head_id} ({sub_head_name}) has {count} rows of dissection data")
                
                if count > 0:
                    logger.info(f"Found dissection data for regular SubHeadID {sub_head_id} ({sub_head_name})")
                    return sub_head_id, False, True, dissection_group_id, data_type
        
        # If we get here, we couldn't find any dissection data for this metric
        logger.warning(f"No dissection data found for metric: {metric_name}")
        
        # Try to find available SubHeadIDs with any dissection group
        if data_type.lower() == 'ratio':
            query = f"""
            SELECT DISTINCT d.SubHeadID, r.HeadNames, d.DisectionGroupID, COUNT(*) as count
            FROM {table_name} d
            JOIN tbl_ratiosheadmaster r ON d.SubHeadID = r.SubHeadID
            WHERE d.CompanyID = {company_id}
            AND LOWER(r.HeadNames) LIKE '%{base_metric.lower()}%'
            GROUP BY d.SubHeadID, r.HeadNames, d.DisectionGroupID
            ORDER BY count DESC
            """
        else:
            query = f"""
            SELECT DISTINCT d.SubHeadID, h.SubHeadName, d.DisectionGroupID, COUNT(*) as count
            FROM {table_name} d
            JOIN tbl_headsmaster h ON d.SubHeadID = h.SubHeadID
            WHERE d.CompanyID = {company_id}
            AND LOWER(h.SubHeadName) LIKE '%{base_metric.lower()}%'
            GROUP BY d.SubHeadID, h.SubHeadName, d.DisectionGroupID
            ORDER BY count DESC
            """
        
        available_heads = db.execute_query(query)
        
        if not available_heads.empty:
            logger.info(f"Found {len(available_heads)} available SubHeadIDs with dissection data for base metric: {base_metric}")
            logger.info(f"Available SubHeadIDs: {available_heads.to_dict()}")
            
            # Return the first available SubHeadID with the most data
            sub_head_id = available_heads.iloc[0]['SubHeadID']
            actual_group_id = available_heads.iloc[0]['DisectionGroupID']
            
            if data_type.lower() == 'ratio':
                sub_head_name = available_heads.iloc[0]['HeadNames']
                logger.info(f"Using ratio SubHeadID {sub_head_id} ({sub_head_name}) with DisectionGroupID {actual_group_id}")
                return sub_head_id, True, True, actual_group_id, data_type
            else:
                sub_head_name = available_heads.iloc[0]['SubHeadName']
                logger.info(f"Using regular SubHeadID {sub_head_id} ({sub_head_name}) with DisectionGroupID {actual_group_id}")
                return sub_head_id, False, True, actual_group_id, data_type
    
    # If not a dissection metric, continue with the original logic for ratio/regular metrics
    # Use the new classification system to determine if this is a ratio metric
    normalized_metric_name = metric_name.lower().strip()
    
    # If we haven't already classified the metric, do so now
    if 'metric_type' not in locals():
        metric_type = classify_metric(metric_name)
    
    # Determine if the metric is a ratio based on the classification
    is_ratio_metric = (metric_type == "ratio")
    
    # For backward compatibility, also check specific forced ratio metrics
    forced_ratio_metrics = ['debt to equity', 'debt/equity', 'd/e ratio']
    if any(ratio_term in normalized_metric_name for ratio_term in forced_ratio_metrics):
        is_ratio_metric = True
        logger.info(f"Metric '{metric_name}' is forced to be treated as a ratio")
    
    # Check if this is a dissection metric that was missed earlier
    if metric_type == "dissection":
        is_ratio_metric = False
        logger.info(f"Metric '{metric_name}' is a dissection metric, forcing classification as Regular Financial Metric")
    
    # Log the classification decision
    logger.info(f"Final classification for '{metric_name}': {'Ratio Metric' if is_ratio_metric else 'Regular Financial Metric'}")
    logger.info(f"Will use {'tbl_ratiosheadmaster' if is_ratio_metric else 'tbl_headsmaster'} for lookup")
    
    # Get sector and industry information for the company
    sector_query = f"""
    SELECT c.SectorID, s.SectorName 
    FROM tbl_companieslist c
    JOIN tbl_sectornames s ON c.SectorID = s.SectorID
    WHERE c.CompanyID = {company_id}
    """
    sector_result = db.execute_query(sector_query)
    
    sector_id = None
    industry_id = None
    
    if not sector_result.empty:
        sector_id = sector_result.iloc[0]['SectorID']
        logger.info(f"Found SectorID: {sector_id} for company ID: {company_id}")
        
        # Get industry information
        industry_query = f"""
        SELECT i.IndustryID, i.IndustryName 
        FROM tbl_industrynames i
        JOIN tbl_industryandsectormapping m ON i.IndustryID = m.industryid
        WHERE m.sectorid = {sector_id}
        """
        industry_result = db.execute_query(industry_query)
        
        if not industry_result.empty:
            industry_id = industry_result.iloc[0]['IndustryID']
            logger.info(f"Found IndustryID: {industry_id} for SectorID: {sector_id}")
    
    # If we couldn't determine sector or industry, log a warning but continue with the search
    if sector_id is None or industry_id is None:
        logger.warning(f"Could not determine sector or industry for company ID: {company_id}")
        logger.warning("Will search for SubHeadIDs without industry-sector validation")

    # Initialize variables to store search results
    exact_heads = pd.DataFrame()
    exact_ratio_heads = pd.DataFrame()
    
    # Prioritize search based on whether the metric is likely a ratio or not
    if is_ratio_metric:
        # For ratio metrics, ONLY search in ratio heads - never fall back to regular heads
        if sector_id is not None and industry_id is not None:
            # First try exact match with industry validation
            query = f"""
            SELECT r.SubHeadID, r.HeadNames 
            FROM tbl_ratiosheadmaster r
            JOIN tbl_industryandsectormapping m ON r.IndustryID = m.industryid
            WHERE LOWER(r.HeadNames) = LOWER('{normalized_metric_name}')
            AND m.sectorid = {sector_id}
            """
            exact_ratio_heads = db.execute_query(query)
            
            # If no exact matches, try contains match with industry validation
            if exact_ratio_heads.empty:
                logger.info(f"No exact ratio matches with industry validation, trying contains match with industry filter")
                query = f"""
                SELECT r.SubHeadID, r.HeadNames 
                FROM tbl_ratiosheadmaster r
                JOIN tbl_industryandsectormapping m ON r.IndustryID = m.industryid
                WHERE LOWER(r.HeadNames) LIKE '%{normalized_metric_name}%'
                OR LOWER(r.HeadNames) LIKE '%debt%equity%'
                AND m.sectorid = {sector_id}
                """
                exact_ratio_heads = db.execute_query(query)
            
            # If still no matches with industry validation, try without it as fallback
            if exact_ratio_heads.empty:
                logger.info(f"No ratio matches with industry validation, trying without industry filter")
                query = f"SELECT SubHeadID, HeadNames FROM tbl_ratiosheadmaster WHERE LOWER(HeadNames) = LOWER('{normalized_metric_name}')"
                exact_ratio_heads = db.execute_query(query)
                
                # If still no exact matches, try contains match without industry validation
                if exact_ratio_heads.empty:
                    logger.info(f"No exact ratio matches without industry filter, trying contains match")
                    query = f"SELECT SubHeadID, HeadNames FROM tbl_ratiosheadmaster WHERE LOWER(HeadNames) LIKE '%{normalized_metric_name}%' OR LOWER(HeadNames) LIKE '%debt%equity%'"
                    exact_ratio_heads = db.execute_query(query)
        else:
            # If sector or industry is not available, search without validation
            query = f"SELECT SubHeadID, HeadNames FROM tbl_ratiosheadmaster WHERE LOWER(HeadNames) = LOWER('{normalized_metric_name}')"
            exact_ratio_heads = db.execute_query(query)
            
            # If no exact matches, try contains match
            if exact_ratio_heads.empty:
                logger.info(f"No exact ratio matches without industry info, trying contains match")
                query = f"SELECT SubHeadID, HeadNames FROM tbl_ratiosheadmaster WHERE LOWER(HeadNames) LIKE '%{normalized_metric_name}%' OR LOWER(HeadNames) LIKE '%debt%equity%'"
                exact_ratio_heads = db.execute_query(query)
        
        # For ratio metrics, we do NOT fall back to regular heads
        # This ensures ratio metrics are always looked up in tbl_ratiosheadmaster
    else:
        # For regular metrics, first try regular heads
        if sector_id is not None and industry_id is not None:
            query = f"""
            SELECT h.SubHeadID, h.SubHeadName 
            FROM tbl_headsmaster h
            JOIN tbl_industryandsectormapping m ON h.IndustryID = m.industryid
            WHERE LOWER(h.SubHeadName) = LOWER('{metric_name}')
            AND m.sectorid = {sector_id}
            """
            exact_heads = db.execute_query(query)
            
            # If no matches with industry validation, try without it as fallback
            if exact_heads.empty:
                logger.info(f"No exact matches with industry validation, trying without industry filter")
                query = f"SELECT SubHeadID, SubHeadName FROM tbl_headsmaster WHERE LOWER(SubHeadName) = LOWER('{metric_name}')"
                exact_heads = db.execute_query(query)
        else:
            # If sector or industry is not available, search without validation
            query = f"SELECT SubHeadID, SubHeadName FROM tbl_headsmaster WHERE LOWER(SubHeadName) = LOWER('{metric_name}')"
            exact_heads = db.execute_query(query)
        
        # Only if we don't find in regular heads, try ratio heads as fallback
        if exact_heads.empty:
            logger.info(f"No exact matches in regular heads, trying ratio heads as fallback")
            if sector_id is not None and industry_id is not None:
                query = f"""
                SELECT r.SubHeadID, r.HeadNames 
                FROM tbl_ratiosheadmaster r
                JOIN tbl_industryandsectormapping m ON r.IndustryID = m.industryid
                WHERE LOWER(r.HeadNames) = LOWER('{metric_name}')
                AND m.sectorid = {sector_id}
                """
                exact_ratio_heads = db.execute_query(query)
                
                # If no matches with industry validation, try without it as fallback
                if exact_ratio_heads.empty:
                    logger.info(f"No exact ratio matches with industry validation, trying without industry filter")
                    query = f"SELECT SubHeadID, HeadNames FROM tbl_ratiosheadmaster WHERE LOWER(HeadNames) = LOWER('{metric_name}')"
                    exact_ratio_heads = db.execute_query(query)
            else:
                # If sector or industry is not available, search without validation
                query = f"SELECT SubHeadID, HeadNames FROM tbl_ratiosheadmaster WHERE LOWER(HeadNames) = LOWER('{metric_name}')"
                exact_ratio_heads = db.execute_query(query)
    
    # Initialize variables for contains matches
    contains_heads = pd.DataFrame()
    contains_ratio_heads = pd.DataFrame()
    
    # If we have exact matches, use them directly
    if not exact_heads.empty:
        contains_heads = exact_heads
    if not exact_ratio_heads.empty:
        contains_ratio_heads = exact_ratio_heads
    
    # If no exact matches, try contains match based on metric type
    if is_ratio_metric:
        # For ratio metrics, ONLY search in ratio heads - never fall back to regular heads
        if exact_ratio_heads.empty:
            if sector_id is not None and industry_id is not None:
                query = f"""
                SELECT r.SubHeadID, r.HeadNames 
                FROM tbl_ratiosheadmaster r
                JOIN tbl_industryandsectormapping m ON r.IndustryID = m.industryid
                WHERE LOWER(r.HeadNames) LIKE '%{normalized_metric_name}%'
                AND m.sectorid = {sector_id}
                """
                contains_ratio_heads = db.execute_query(query)
                
                # If no matches with industry validation, try without it as fallback
                if contains_ratio_heads.empty:
                    logger.info(f"No contains ratio matches with industry validation, trying without industry filter")
                    query = f"SELECT SubHeadID, HeadNames FROM tbl_ratiosheadmaster WHERE LOWER(HeadNames) LIKE '%{normalized_metric_name}%'"
                    contains_ratio_heads = db.execute_query(query)
            else:
                # If sector or industry is not available, search without validation
                query = f"SELECT SubHeadID, HeadNames FROM tbl_ratiosheadmaster WHERE LOWER(HeadNames) LIKE '%{normalized_metric_name}%'"
                contains_ratio_heads = db.execute_query(query)
    else:
        # For regular metrics, first try regular heads
        if exact_heads.empty:
            if sector_id is not None and industry_id is not None:
                query = f"""
                SELECT h.SubHeadID, h.SubHeadName 
                FROM tbl_headsmaster h
                JOIN tbl_industryandsectormapping m ON h.IndustryID = m.industryid
                WHERE LOWER(h.SubHeadName) LIKE '%{normalized_metric_name}%'
                AND m.sectorid = {sector_id}
                """
                contains_heads = db.execute_query(query)
                
                # If no matches with industry validation, try without it as fallback
                if contains_heads.empty:
                    logger.info(f"No contains matches with industry validation, trying without industry filter")
                    query = f"SELECT SubHeadID, SubHeadName FROM tbl_headsmaster WHERE LOWER(SubHeadName) LIKE '%{normalized_metric_name}%'"
                    contains_heads = db.execute_query(query)
            else:
                # If sector or industry is not available, search without validation
                query = f"SELECT SubHeadID, SubHeadName FROM tbl_headsmaster WHERE LOWER(SubHeadName) LIKE '%{normalized_metric_name}%'"
                contains_heads = db.execute_query(query)
            
            # Only if we don't find in regular heads, try ratio heads as fallback
            if contains_heads.empty:
                logger.info(f"No contains matches in regular heads, trying ratio heads as fallback")
                if sector_id is not None and industry_id is not None:
                    query = f"""
                    SELECT r.SubHeadID, r.HeadNames 
                    FROM tbl_ratiosheadmaster r
                    JOIN tbl_industryandsectormapping m ON r.IndustryID = m.industryid
                    WHERE LOWER(r.HeadNames) LIKE '%{normalized_metric_name}%'
                    AND m.sectorid = {sector_id}
                    """
                    contains_ratio_heads = db.execute_query(query)
                    
                    # If no matches with industry validation, try without it as fallback
                    if contains_ratio_heads.empty:
                        logger.info(f"No contains ratio matches with industry validation, trying without industry filter")
                        query = f"SELECT SubHeadID, HeadNames FROM tbl_ratiosheadmaster WHERE LOWER(HeadNames) LIKE '%{normalized_metric_name}%'"
                        contains_ratio_heads = db.execute_query(query)
                else:
                    # If sector or industry is not available, search without validation
                    query = f"SELECT SubHeadID, HeadNames FROM tbl_ratiosheadmaster WHERE LOWER(HeadNames) LIKE '%{normalized_metric_name}%'"
                    contains_ratio_heads = db.execute_query(query)
    
    # Check if we found any matches
    if contains_heads.empty and contains_ratio_heads.empty:
        logger.error(f"No matches found for metric: {metric_name}")
        return None, False, False, None, None
    
    # Log the number of matches found
    if not contains_heads.empty:
        logger.info(f"Found {len(contains_heads)} regular head matches for metric: {metric_name}")
        logger.info(f"Regular matches: {contains_heads.to_dict()}")
    
    if not contains_ratio_heads.empty:
        logger.info(f"Found {len(contains_ratio_heads)} ratio head matches for metric: {metric_name}")
        logger.info(f"Ratio matches: {contains_ratio_heads.to_dict()}")
    
    # Check each SubHeadID to see if it has data for the company
    # First check regular heads if available and not a ratio metric
    if not contains_heads.empty and not is_ratio_metric:
        for _, row in contains_heads.iterrows():
            sub_head_id = row['SubHeadID']
            sub_head_name = row['SubHeadName']
            
            # Build a query to check if data exists
            where_clauses = [f"f.CompanyID = {company_id}", f"f.SubHeadID = {sub_head_id}"]
            
            if period_end is not None:
                where_clauses.append(f"f.PeriodEnd = '{period_end}'")
                
            if consolidation_id is not None:
                where_clauses.append(f"f.ConsolidationID = {consolidation_id}")
                
            where_clause = " AND ".join(where_clauses)
            
            query = f"""
            SELECT COUNT(*) as count FROM tbl_financialrawdata f 
            WHERE {where_clause}
            """
            
            result = db.execute_query(query)
            count = result.iloc[0]['count'] if not result.empty else 0
            
            logger.info(f"Regular SubHeadID {sub_head_id} ({sub_head_name}) has {count} rows of data")
            
            if count > 0:
                logger.info(f"Found data for regular SubHeadID {sub_head_id} ({sub_head_name})")
                return sub_head_id, False, False, None, None
    
    # Then check ratio heads
    if not contains_ratio_heads.empty:
        for _, row in contains_ratio_heads.iterrows():
            sub_head_id = row['SubHeadID']
            sub_head_name = row['HeadNames']
            
            # Build a query to check if data exists
            where_clauses = [f"r.CompanyID = {company_id}", f"r.SubHeadID = {sub_head_id}"]
            
            if period_end is not None:
                where_clauses.append(f"r.PeriodEnd = '{period_end}'")
                
            if consolidation_id is not None:
                where_clauses.append(f"r.ConsolidationID = {consolidation_id}")
                
            where_clause = " AND ".join(where_clauses)
            
            query = f"""
            SELECT COUNT(*) as count FROM tbl_ratiorawdata r 
            WHERE {where_clause}
            """
            
            result = db.execute_query(query)
            count = result.iloc[0]['count'] if not result.empty else 0
            
            logger.info(f"Ratio SubHeadID {sub_head_id} ({sub_head_name}) has {count} rows of data")
            
            if count > 0:
                logger.info(f"Found data for ratio SubHeadID {sub_head_id} ({sub_head_name})")
                return sub_head_id, True, False, None, None
    
    # If we get here, we couldn't find any data for this metric
    logger.error(f"No data found for any SubHeadID for metric: {metric_name}")
    
    # Return the first SubHeadID we found, even though it doesn't have data
    # This allows the caller to at least know what the SubHeadID is
    if not contains_heads.empty and not is_ratio_metric:
        sub_head_id = contains_heads.iloc[0]['SubHeadID']
        sub_head_name = contains_heads.iloc[0]['SubHeadName']
        logger.warning(f"Returning regular SubHeadID {sub_head_id} ({sub_head_name}) even though it has no data")
        return sub_head_id, False, False, None, None
    elif not contains_ratio_heads.empty:
        sub_head_id = contains_ratio_heads.iloc[0]['SubHeadID']
        sub_head_name = contains_ratio_heads.iloc[0]['HeadNames']
        logger.warning(f"Returning ratio SubHeadID {sub_head_id} ({sub_head_name}) even though it has no data")
        return sub_head_id, True, False, None, None
    else:
        return None, False, False, None, None


# Simplified version for backward compatibility
def get_available_head_id_simple(db, company_id, metric_name, period_end=None, consolidation_id=None):
    """
    Simplified version of get_available_head_id that returns only head_id and is_ratio for backward compatibility.
    """
    head_id, is_ratio, is_dissection, dissection_group_id, data_type = get_available_head_id(db, company_id, metric_name, period_end, consolidation_id)
    return head_id, is_ratio


# Example usage
if __name__ == "__main__":
    db = FinancialDatabase('MUHAMMADUSMAN', 'MGFinancials')
    company_id = db.get_company_id('UBL')
    period_end = '2021-03-31'
    consolidation_id = db.get_consolidation_id('unconsolidated')
    
    # Get company's sector and industry information for context
    sector_query = f"""
    SELECT c.SectorID, s.SectorName 
    FROM tbl_companieslist c
    JOIN tbl_sectornames s ON c.SectorID = s.SectorID
    WHERE c.CompanyID = {company_id}
    """
    sector_result = db.execute_query(sector_query)
    
    if not sector_result.empty:
        sector_id = sector_result.iloc[0]['SectorID']
        sector_name = sector_result.iloc[0]['SectorName']
        print(f"\nCompany is in sector: {sector_name} (ID: {sector_id})")
        
        # Get industry information
        industry_query = f"""
        SELECT i.IndustryID, i.IndustryName 
        FROM tbl_industrynames i
        JOIN tbl_industryandsectormapping m ON i.IndustryID = m.industryid
        WHERE m.sectorid = {sector_id}
        """
        industry_result = db.execute_query(industry_query)
        
        if not industry_result.empty:
            industry_id = industry_result.iloc[0]['IndustryID']
            industry_name = industry_result.iloc[0]['IndustryName']
            print(f"Company is in industry: {industry_name} (ID: {industry_id})")
    
    # Example 1: Test with a regular financial metric
    regular_metric = 'Depreciation and Amortisation'
    head_id, is_ratio, is_dissection, dissection_group_id, data_type = get_available_head_id(db, company_id, regular_metric, period_end, consolidation_id)
    print(f"\nMetric: {regular_metric}")
    print(f"Head ID: {head_id}, Is Ratio: {is_ratio}, Is Dissection: {is_dissection}, Group ID: {dissection_group_id}, Data Type: {data_type}")
    
    # Example 2: Test with a ratio metric
    ratio_metric = 'Debt to Equity'
    head_id, is_ratio, is_dissection, dissection_group_id, data_type = get_available_head_id(db, company_id, ratio_metric, period_end, consolidation_id)
    print(f"\nMetric: {ratio_metric}")
    print(f"Head ID: {head_id}, Is Ratio: {is_ratio}, Is Dissection: {is_dissection}, Group ID: {dissection_group_id}, Data Type: {data_type}")
    
    # Example 3: Test with a dissection metric
    dissection_metric = 'PAT Per Share'
    head_id, is_ratio, is_dissection, dissection_group_id, data_type = get_available_head_id(db, company_id, dissection_metric, period_end, consolidation_id)
    print(f"\nMetric: {dissection_metric}")
    print(f"Head ID: {head_id}, Is Ratio: {is_ratio}, Is Dissection: {is_dissection}, Group ID: {dissection_group_id}, Data Type: {data_type}")