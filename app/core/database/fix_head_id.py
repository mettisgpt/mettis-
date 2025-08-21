import sys
import os
import pandas as pd
import re

# Add the project root directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
from app.core.database.financial_db import FinancialDatabase
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_available_head_id(db, company_id, metric_name, period_end=None, consolidation_id=None):
    """
    Get a head_id that actually has data for the specified company and parameters.
    
    This function first determines if the metric is a ratio based on its name,
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
        Tuple of (head_id, is_ratio) if found, (None, False) otherwise
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
                    return sub_head_id, False
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
                    return sub_head_id, True
        else:
            logger.error(f"No EPS-related ratio heads found")
                
        logger.error(f"No data found for any EPS head (regular or ratio)")
        return None, False
    
    # Normalize the metric name for better matching
    normalized_metric_name = metric_name.lower().strip()
    
    # Determine if the metric is likely a ratio based on its name
    is_ratio_metric = False
    ratio_keywords = ['ratio', 'margin', 'return on', 'roe', 'roa', 'roce', 'per ', 'yield', 'to ', 'coverage']
    
    if any(keyword in normalized_metric_name for keyword in ratio_keywords):
        is_ratio_metric = True
        logger.info(f"Metric '{metric_name}' appears to be a ratio based on its name")
    else:
        logger.info(f"Metric '{metric_name}' appears to be a regular financial metric based on its name")
        
    # Force specific metrics to be treated as ratios regardless of naming
    forced_ratio_metrics = ['debt to equity', 'debt/equity', 'd/e ratio']
    if any(ratio_term in normalized_metric_name for ratio_term in forced_ratio_metrics):
        is_ratio_metric = True
        logger.info(f"Metric '{metric_name}' is forced to be treated as a ratio")
        
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
                WHERE r.HeadNames LIKE '%{metric_name}%'
                AND m.sectorid = {sector_id}
                """
                contains_ratio_heads = db.execute_query(query)
                
                # If no matches with industry validation, try without it as fallback
                if contains_ratio_heads.empty:
                    logger.info(f"No contains ratio matches with industry validation, trying without industry filter")
                    query = f"SELECT SubHeadID, HeadNames FROM tbl_ratiosheadmaster WHERE HeadNames LIKE '%{metric_name}%'"
                    contains_ratio_heads = db.execute_query(query)
            else:
                # If sector or industry is not available, search without validation
                query = f"SELECT SubHeadID, HeadNames FROM tbl_ratiosheadmaster WHERE HeadNames LIKE '%{metric_name}%'"
                contains_ratio_heads = db.execute_query(query)
        
        # For ratio metrics, we do NOT fall back to regular heads
        # This ensures ratio metrics are always looked up in tbl_ratiosheadmaster
    else:
        # For regular metrics, prioritize searching in regular heads
        if exact_heads.empty:
            if sector_id is not None and industry_id is not None:
                query = f"""
                SELECT h.SubHeadID, h.SubHeadName 
                FROM tbl_headsmaster h
                JOIN tbl_industryandsectormapping m ON h.IndustryID = m.industryid
                WHERE h.SubHeadName LIKE '%{metric_name}%'
                AND m.sectorid = {sector_id}
                """
                contains_heads = db.execute_query(query)
                
                # If no matches with industry validation, try without it as fallback
                if contains_heads.empty:
                    logger.info(f"No contains matches with industry validation, trying without industry filter")
                    query = f"SELECT SubHeadID, SubHeadName FROM tbl_headsmaster WHERE SubHeadName LIKE '%{metric_name}%'"
                    contains_heads = db.execute_query(query)
            else:
                # If sector or industry is not available, search without validation
                query = f"SELECT SubHeadID, SubHeadName FROM tbl_headsmaster WHERE SubHeadName LIKE '%{metric_name}%'"
                contains_heads = db.execute_query(query)
        
        # Only if we don't find in regular heads, try ratio heads as fallback
        if contains_heads.empty and exact_ratio_heads.empty:
            logger.info(f"No contains matches in regular heads, trying ratio heads as fallback")
            if sector_id is not None and industry_id is not None:
                query = f"""
                SELECT r.SubHeadID, r.HeadNames 
                FROM tbl_ratiosheadmaster r
                JOIN tbl_industryandsectormapping m ON r.IndustryID = m.industryid
                WHERE r.HeadNames LIKE '%{metric_name}%'
                AND m.sectorid = {sector_id}
                """
                contains_ratio_heads = db.execute_query(query)
                
                # If no matches with industry validation, try without it as fallback
                if contains_ratio_heads.empty:
                    logger.info(f"No contains ratio matches with industry validation, trying without industry filter")
                    query = f"SELECT SubHeadID, HeadNames FROM tbl_ratiosheadmaster WHERE HeadNames LIKE '%{metric_name}%'"
                    contains_ratio_heads = db.execute_query(query)
            else:
                # If sector or industry is not available, search without validation
                query = f"SELECT SubHeadID, HeadNames FROM tbl_ratiosheadmaster WHERE HeadNames LIKE '%{metric_name}%'"
                contains_ratio_heads = db.execute_query(query)

    # Check if we found any potential matches
    if contains_heads.empty and contains_ratio_heads.empty:
        logger.error(f"No SubHeadIDs found for metric: {metric_name}")
        return None, False
    
    # Prioritize checking based on whether the metric is a ratio or not
    if is_ratio_metric:
        # First check exact matches in ratio heads for ratio metrics
        if not exact_ratio_heads.empty:
            logger.info(f"Found {len(exact_ratio_heads)} exact matches in ratio heads for metric: {metric_name}")
            
            for _, row in exact_ratio_heads.iterrows():
                sub_head_id = row['SubHeadID']
                head_name = row['HeadNames']
                
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
                
                logger.info(f"Exact match: Ratio SubHeadID {sub_head_id} ({head_name}) has {count} rows of data")
                
                if count > 0:
                    logger.info(f"Found data for exact match ratio SubHeadID {sub_head_id} ({head_name})")
                    return sub_head_id, True
        
        # Then check contains matches in ratio heads for ratio metrics
        if not contains_ratio_heads.empty and contains_ratio_heads.equals(exact_ratio_heads) == False:
            logger.info(f"Found {len(contains_ratio_heads)} contains matches in ratio heads for metric: {metric_name}")
            
            for _, row in contains_ratio_heads.iterrows():
                sub_head_id = row['SubHeadID']
                head_name = row['HeadNames']
                
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
                
                logger.info(f"Contains match: Ratio SubHeadID {sub_head_id} ({head_name}) has {count} rows of data")
                
                if count > 0:
                    logger.info(f"Found data for contains match ratio SubHeadID {sub_head_id} ({head_name})")
                    return sub_head_id, True
        
        # For ratio metrics, we do NOT check regular heads
        # This ensures ratio metrics are always queried from tbl_ratiorawdata
    else:
        # For regular metrics, first check exact matches in regular heads
        if not exact_heads.empty:
            logger.info(f"Found {len(exact_heads)} exact matches in regular heads for metric: {metric_name}")
            
            for _, row in exact_heads.iterrows():
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
                
                logger.info(f"Exact match: SubHeadID {sub_head_id} ({sub_head_name}) has {count} rows of data")
                
                if count > 0:
                    logger.info(f"Found data for exact match SubHeadID {sub_head_id} ({sub_head_name})")
                    return sub_head_id, False
        
        # Then check contains matches in regular heads for regular metrics
        if not contains_heads.empty and contains_heads.equals(exact_heads) == False:
            logger.info(f"Found {len(contains_heads)} contains matches in regular heads for metric: {metric_name}")
            
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
                
                logger.info(f"Contains match: SubHeadID {sub_head_id} ({sub_head_name}) has {count} rows of data")
                
                if count > 0:
                    logger.info(f"Found data for contains match SubHeadID {sub_head_id} ({sub_head_name})")
                    return sub_head_id, False
        
        # As fallback for regular metrics, check ratio heads
        if not exact_ratio_heads.empty:
            logger.info(f"Checking ratio heads as fallback for regular metric: {metric_name}")
            
            for _, row in exact_ratio_heads.iterrows():
                sub_head_id = row['SubHeadID']
                head_name = row['HeadNames']
                
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
                
                logger.info(f"Fallback exact match: Ratio SubHeadID {sub_head_id} ({head_name}) has {count} rows of data")
                
                if count > 0:
                    logger.info(f"Found data for fallback exact match ratio SubHeadID {sub_head_id} ({head_name})")
                    return sub_head_id, True
        
        # Finally check contains matches in ratio heads as last resort for regular metrics
        if not contains_ratio_heads.empty and contains_ratio_heads.equals(exact_ratio_heads) == False:
            logger.info(f"Checking contains matches in ratio heads as last resort for regular metric: {metric_name}")
            
            for _, row in contains_ratio_heads.iterrows():
                sub_head_id = row['SubHeadID']
                head_name = row['HeadNames']
                
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
                
                logger.info(f"Fallback contains match: Ratio SubHeadID {sub_head_id} ({head_name}) has {count} rows of data")
                
                if count > 0:
                    logger.info(f"Found data for fallback contains match ratio SubHeadID {sub_head_id} ({head_name})")
                    return sub_head_id, True
    
    logger.error(f"No data found for any SubHeadID for metric: {metric_name}")
    return None, False

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
    print(f"\n--- Example 1: Regular Financial Metric ---")
    print(f"Searching for metric: {regular_metric}")
    
    # Get the head_id using the new method with industry-sector validation
    regular_head_id, regular_is_ratio = get_available_head_id(db, company_id, regular_metric, period_end, consolidation_id)
    print(f"Result: Head ID: {regular_head_id}, Is Ratio: {regular_is_ratio}")
    
    # Example 2: Test with a ratio metric
    ratio_metric = 'Debt to Equity Ratio'
    print(f"\n--- Example 2: Ratio Metric ---")
    print(f"Searching for metric: {ratio_metric}")
    
    # Get the head_id using the new method with industry-sector validation
    ratio_head_id, ratio_is_ratio = get_available_head_id(db, company_id, ratio_metric, period_end, consolidation_id)
    print(f"Result: Head ID: {ratio_head_id}, Is Ratio: {ratio_is_ratio}")
    
    # Test the query with one of the head_ids
    test_head_id = regular_head_id if regular_head_id is not None else ratio_head_id
    is_ratio = regular_is_ratio if regular_head_id is not None else ratio_is_ratio
    
    if test_head_id is not None:
        # Determine table and column names based on whether it's a ratio or not
        if is_ratio:
            # For ratio metrics, use ratio tables and columns with correct column names
            query = f"""
            SELECT r.Value_ as Value, u.unitname AS Unit, t.term AS Term, c.CompanyName AS Company, 
                   rh.HeadNames AS Metric, con.consolidationname AS Consolidation, r.PeriodEnd AS PeriodEnd
            FROM tbl_ratiorawdata r
            JOIN tbl_ratiosheadmaster rh ON r.SubHeadID = rh.SubHeadID
            JOIN tbl_unitofmeasurement u ON rh.UnitID = u.UnitID
            JOIN tbl_terms t ON r.TermID = t.TermID
            JOIN tbl_companieslist c ON r.CompanyID = c.CompanyID
            JOIN tbl_consolidation con ON r.ConsolidationID = con.ConsolidationID
            WHERE r.CompanyID = {company_id}
            AND r.SubHeadID = {test_head_id}
            AND r.RatioDate = '{period_end}'
            AND r.ConsolidationID = {consolidation_id}
            ORDER BY r.RatioDate DESC
            """
        else:
            # For regular financial metrics, use financial tables and columns
            query = f"""
            SELECT f.Value as Value, u.unitname, t.term, c.CompanyName, 
                   h.SubHeadName, con.consolidationname, f.PeriodEnd
            FROM tbl_financialrawdata f
            JOIN tbl_headsmaster h ON f.SubHeadID = h.SubHeadID
            JOIN tbl_unitofmeasurement u ON h.UnitID = u.UnitID
            JOIN tbl_terms t ON f.TermID = t.TermID
            JOIN tbl_companieslist c ON f.CompanyID = c.CompanyID
            JOIN tbl_consolidation con ON f.ConsolidationID = con.ConsolidationID
            WHERE f.CompanyID = {company_id}
            AND f.SubHeadID = {test_head_id}
            AND f.PeriodEnd = '{period_end}'
            AND f.ConsolidationID = {consolidation_id}
            ORDER BY f.PeriodEnd DESC
        """
        
        print(f"\nQuery with Head ID:\n{query}\n")
        result = db.execute_query(query)
        print(f"\nQuery Result:\n{result}")
    else:
        print("\nNo valid head_id found for either metric")