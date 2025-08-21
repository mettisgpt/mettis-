'''
Test Calculated and Dissection Data Queries for Mettis Financial Database

This script tests the enhanced query functionality for calculated non-ratio data
(quarterly, TTM) and dissection data (regular, ratio, quarterly, TTM).
'''

import os
import sys
import pandas as pd
from app.core.database.financial_db import FinancialDatabase
from app.core.database.fix_head_id import get_available_head_id
from improved_query_approach import (
    query_quarterly_data,
    query_ttm_data,
    query_dissection_data,
    resolve_period_end
)
from utils import logger

# Initialize database connection
server = 'MUHAMMADUSMAN'
database = 'MGFinancials'
db = FinancialDatabase(server, database)

def get_company_id(ticker):
    """Get company ID from ticker symbol."""
    query = f"SELECT CompanyID FROM tbl_companieslist WHERE Symbol = '{ticker}'"
    result = db.execute_query(query)
    if result.empty:
        return None
    return result.iloc[0]['CompanyID']

def test_query_1():
    """Test Query 1: Latest Quarterly Revenue (Calculated Non-Ratio)
    "What is the most recent quarterly Revenue for Atlas Honda (unconsolidated)?"
    """
    print("\n=== Test Query 1: Latest Quarterly Revenue ===\n")
    
    company = "ATLH"  # Atlas Honda
    metric = "Revenue"
    period = "most recent quarter"
    consolidation = "Unconsolidated"
    
    try:
        # Get company ID
        company_id = get_company_id(company)
        if not company_id:
            print(f"Company not found: {company}")
            return
        
        # Get SubHeadID
        consolidation_id = db.get_consolidation_id(consolidation)
        head_id, is_ratio = get_available_head_id(db, company_id, metric, None, consolidation_id)
        
        if not head_id:
            print(f"No SubHeadID found for {metric}")
            # List available metrics
            query = f"""
            SELECT TOP 10 h.SubHeadName
            FROM tbl_headsmaster h
            JOIN tbl_financialrawdata_Quarter f ON h.SubHeadID = f.SubHeadID
            WHERE f.CompanyID = {company_id}
            AND f.ConsolidationID = {consolidation_id}
            GROUP BY h.SubHeadName
            ORDER BY COUNT(*) DESC
            """
            available_metrics = db.execute_query(query)
            print("Available metrics:")
            for _, row in available_metrics.iterrows():
                print(f"  - {row['SubHeadName']}")
            return
        
        # Resolve period
        period_end, term_id, fiscal_year = resolve_period_end(db, company_id, period)
        
        if not period_end:
            print(f"Could not resolve period: {period}")
            # List available periods
            query = f"""
            SELECT TOP 5 PeriodEnd
            FROM tbl_financialrawdata_Quarter
            WHERE CompanyID = {company_id}
            AND SubHeadID = {head_id}
            AND ConsolidationID = {consolidation_id}
            ORDER BY PeriodEnd DESC
            """
            available_periods = db.execute_query(query)
            print("Available periods:")
            for _, row in available_periods.iterrows():
                print(f"  - {row['PeriodEnd']}")
            return
        
        # Build and execute query
        query = f"""
        SELECT f.Value_ AS Value,
               u.unitname AS Unit,
               t.term AS Term,
               c.CompanyName AS Company,
               h.SubHeadName AS Metric,
               con.consolidationname AS Consolidation,
               f.PeriodEnd AS PeriodEnd
        FROM tbl_financialrawdata_Quarter f
        JOIN tbl_headsmaster h ON f.SubHeadID = h.SubHeadID
        JOIN tbl_unitofmeasurement u ON h.UnitID = u.UnitID
        JOIN tbl_terms t ON f.TermID = t.TermID
        JOIN tbl_companieslist c ON f.CompanyID = c.CompanyID
        JOIN tbl_consolidation con ON f.ConsolidationID = con.ConsolidationID
        WHERE f.CompanyID = {company_id}
        AND f.SubHeadID = {head_id}
        AND f.PeriodEnd = '{period_end}'
        AND f.ConsolidationID = {consolidation_id}
        """
        
        print("SQL Query:")
        print(query)
        
        result = db.execute_query(query)
        
        if result.empty:
            print("No data found.")
        else:
            print("\nResult:")
            print(result)
            print(f"\nLatest quarterly {metric} for {company} ({consolidation}): {result.iloc[0]['Value']} {result.iloc[0]['Unit']}")
        
        return result
    except Exception as e:
        print(f"Error: {str(e)}")
        return None

def test_query_2():
    """Test Query 2: TTM Net Income (Calculated Non-Ratio)
    "Show me the trailing-twelve-month Net Income of UBL for the latest period."
    """
    print("\n=== Test Query 2: TTM Net Income ===\n")
    
    company = "UBL"
    metric = "Net Income"
    period = "ttm"
    consolidation = "Unconsolidated"
    
    try:
        result = query_ttm_data(
            company_ticker=company,
            metric_name=metric,
            period_term=period,
            consolidation_type=consolidation
        )
        
        if result is not None and not result.empty:
            print("SQL Query: (Generated by query_ttm_data)")
            print("\nResult:")
            print(result)
            print(f"\nTTM {metric} for {company} ({consolidation}): {result.iloc[0]['Value']} {result.iloc[0]['Unit']}")
        else:
            print("No data found.")
            
            # Get company ID
            company_id = get_company_id(company)
            if not company_id:
                print(f"Company not found: {company}")
                return
            
            # List available metrics
            consolidation_id = db.get_consolidation_id(consolidation)
            query = f"""
            SELECT TOP 10 h.SubHeadName
            FROM tbl_headsmaster h
            JOIN tbl_financialrawdataTTM f ON h.SubHeadID = f.SubHeadID
            WHERE f.CompanyID = {company_id}
            AND f.ConsolidationID = {consolidation_id}
            GROUP BY h.SubHeadName
            ORDER BY COUNT(*) DESC
            """
            available_metrics = db.execute_query(query)
            print("Available metrics:")
            for _, row in available_metrics.iterrows():
                print(f"  - {row['SubHeadName']}")
        
        return result
    except Exception as e:
        print(f"Error: {str(e)}")
        return None

def test_query_3():
    """Test Query 3: Annual EPS Growth (Dissection Group)
    "Fetch HBL's EPS annual growth (DisectionGroupID = 2) for 2024-12-31."
    """
    print("\n=== Test Query 3: Annual EPS Growth (Dissection Group) ===\n")
    
    company = "HBL"
    metric = "EPS Annual Growth"
    period = "2024-12-31"
    group_id = 2
    consolidation = "Unconsolidated"
    data_type = "ratio"
    
    try:
        result = query_dissection_data(
            company_ticker=company,
            metric_name=metric,
            period_term=period,
            dissection_group_id=group_id,
            consolidation_type=consolidation,
            data_type=data_type
        )
        
        if result is not None and not result.empty:
            print("SQL Query: (Generated by query_dissection_data)")
            print("\nResult:")
            print(result)
            print(f"\nEPS Annual Growth for {company} (DisectionGroupID={group_id}): Found {len(result)} records")
        else:
            print("No data found.")
            
            # Get company ID
            company_id = get_company_id(company)
            if not company_id:
                print(f"Company not found: {company}")
                return
            
            # Get SubHeadID
            consolidation_id = db.get_consolidation_id(consolidation)
            head_id, is_ratio = get_available_head_id(db, company_id, metric, period, consolidation_id)
            
            if head_id:
                # Check available dissection groups
                query = f"""
                SELECT DISTINCT DisectionGroupID
                FROM tbl_disectionrawdata_Ratios
                WHERE CompanyID = {company_id}
                AND SubHeadID = {head_id}
                AND PeriodEnd = '{period}'
                AND ConsolidationID = {consolidation_id}
                """
                groups = db.execute_query(query)
                print(f"Available dissection groups for {metric}:")
                for _, row in groups.iterrows():
                    print(f"  - DisectionGroupID: {row['DisectionGroupID']}")
            else:
                print(f"No SubHeadID found for {metric}")
        
        return result
    except Exception as e:
        print(f"Error: {str(e)}")
        return None

def test_query_4():
    """Test Query 4: PAT Per Share (Dissection Group)
    "Retrieve HBL's PAT per share (DisectionGroupID = 1) for 2024-12-31."
    """
    print("\n=== Test Query 4: PAT Per Share (Dissection Group) ===\n")
    
    company = "HBL"
    metric = "PAT Per Share"
    period = "2024-12-31"
    group_id = 1
    consolidation = "Unconsolidated"
    data_type = "regular"
    
    try:
        result = query_dissection_data(
            company_ticker=company,
            metric_name=metric,
            period_term=period,
            dissection_group_id=group_id,
            consolidation_type=consolidation,
            data_type=data_type
        )
        
        if result is not None and not result.empty:
            print("SQL Query: (Generated by query_dissection_data)")
            print("\nResult:")
            print(result)
            print(f"\nPAT Per Share for {company} (DisectionGroupID={group_id}): Found {len(result)} records")
        else:
            print("No data found.")
            
            # Get company ID
            company_id = get_company_id(company)
            if not company_id:
                print(f"Company not found: {company}")
                return
            
            # Get SubHeadID
            consolidation_id = db.get_consolidation_id(consolidation)
            head_id, is_ratio = get_available_head_id(db, company_id, metric, period, consolidation_id)
            
            if head_id:
                # Check available dissection groups
                query = f"""
                SELECT DISTINCT DisectionGroupID
                FROM tbl_disectionrawdata
                WHERE CompanyID = {company_id}
                AND SubHeadID = {head_id}
                AND PeriodEnd = '{period}'
                AND ConsolidationID = {consolidation_id}
                """
                groups = db.execute_query(query)
                print(f"Available dissection groups for {metric}:")
                for _, row in groups.iterrows():
                    print(f"  - DisectionGroupID: {row['DisectionGroupID']}")
            else:
                print(f"No SubHeadID found for {metric}")
        
        return result
    except Exception as e:
        print(f"Error: {str(e)}")
        return None

def test_query_5():
    """Test Query 5: YoY ROI Breakdown (Dissection Ratios)
    "Get UBL's year-over-year ROI breakdown (tbl_disectionrawdata_Ratios) for Q3 2021."
    """
    print("\n=== Test Query 5: YoY ROI Breakdown (Dissection Ratios) ===\n")
    
    company = "UBL"
    metric = "ROI"
    period = "Q3 2021"
    consolidation = "Unconsolidated"
    data_type = "ratio"
    
    try:
        # Get company ID
        company_id = get_company_id(company)
        if not company_id:
            print(f"Company not found: {company}")
            return
        
        # Resolve period
        period_end, term_id, fiscal_year = resolve_period_end(db, company_id, period)
        
        if not period_end:
            print(f"Could not resolve period: {period}")
            return
        
        # Get SubHeadID
        consolidation_id = db.get_consolidation_id(consolidation)
        head_id, is_ratio = get_available_head_id(db, company_id, metric, period_end, consolidation_id)
        
        if not head_id:
            print(f"No SubHeadID found for {metric}")
            return
        
        # Find available dissection groups
        groups_query = f"""
        SELECT DISTINCT DisectionGroupID
        FROM tbl_disectionrawdata_Ratios
        WHERE CompanyID = {company_id}
        AND SubHeadID = {head_id}
        AND PeriodEnd = '{period_end}'
        AND ConsolidationID = {consolidation_id}
        """
        
        groups = db.execute_query(groups_query)
        
        if groups.empty:
            print(f"No dissection groups found for {metric} on {period_end}")
            return
        
        # Use the first available group
        group_id = groups.iloc[0]['DisectionGroupID']
        
        # Build and execute query
        query = f"""
        SELECT f.Value_ AS Value,
               u.unitname AS Unit,
               t.term AS Term,
               c.CompanyName AS Company,
               h.SubHeadName AS Metric,
               con.consolidationname AS Consolidation,
               f.PeriodEnd AS PeriodEnd,
               f.DisectionGroupID AS GroupID,
               f.DisectionID AS DisectionID
        FROM tbl_disectionrawdata_Ratios f
        JOIN tbl_ratiosheadmaster h ON f.SubHeadID = h.SubHeadID
        JOIN tbl_unitofmeasurement u ON h.UnitID = u.UnitID
        JOIN tbl_terms t ON f.TermID = t.TermID
        JOIN tbl_companieslist c ON f.CompanyID = c.CompanyID
        JOIN tbl_consolidation con ON f.ConsolidationID = con.ConsolidationID
        WHERE f.CompanyID = {company_id}
        AND f.SubHeadID = {head_id}
        AND f.PeriodEnd = '{period_end}'
        AND f.ConsolidationID = {consolidation_id}
        AND f.DisectionGroupID = {group_id}
        """
        
        print("SQL Query:")
        print(query)
        
        result = db.execute_query(query)
        
        if result.empty:
            print("No data found.")
        else:
            print("\nResult:")
            print(result)
            print(f"\nYoY ROI Breakdown for {company} (DisectionGroupID={group_id}): Found {len(result)} records")
        
        return result
    except Exception as e:
        print(f"Error: {str(e)}")
        return None

def test_query_6():
    """Test Query 6: QoQ Asset Return (Dissection Ratios)
    "What is the quarter-over-quarter ROI/Asset for Engro Corp in Q2 2022?"
    """
    print("\n=== Test Query 6: QoQ Asset Return (Dissection Ratios) ===\n")
    
    company = "ENGRO"
    metric = "ROI/Asset"
    period = "Q2 2022"
    consolidation = "Unconsolidated"
    data_type = "ratio"
    
    try:
        # Get company ID
        company_id = get_company_id(company)
        if not company_id:
            print(f"Company not found: {company}")
            return
        
        # Resolve period
        period_end, term_id, fiscal_year = resolve_period_end(db, company_id, period)
        
        if not period_end:
            print(f"Could not resolve period: {period}")
            return
        
        # Get SubHeadID
        consolidation_id = db.get_consolidation_id(consolidation)
        head_id, is_ratio = get_available_head_id(db, company_id, metric, period_end, consolidation_id)
        
        if not head_id:
            print(f"No SubHeadID found for {metric}")
            # Try similar metrics
            query = f"""
            SELECT TOP 10 h.SubHeadName
            FROM tbl_ratiosheadmaster h
            JOIN tbl_disectionrawdata_Ratios f ON h.SubHeadID = f.SubHeadID
            WHERE f.CompanyID = {company_id}
            AND f.ConsolidationID = {consolidation_id}
            AND h.SubHeadName LIKE '%ROI%' OR h.SubHeadName LIKE '%Return%'
            GROUP BY h.SubHeadName
            ORDER BY COUNT(*) DESC
            """
            similar_metrics = db.execute_query(query)
            print("Similar metrics available:")
            for _, row in similar_metrics.iterrows():
                print(f"  - {row['SubHeadName']}")
            return
        
        # Find available dissection groups
        groups_query = f"""
        SELECT DISTINCT DisectionGroupID
        FROM tbl_disectionrawdata_Ratios
        WHERE CompanyID = {company_id}
        AND SubHeadID = {head_id}
        AND PeriodEnd = '{period_end}'
        AND ConsolidationID = {consolidation_id}
        """
        
        groups = db.execute_query(groups_query)
        
        if groups.empty:
            print(f"No dissection groups found for {metric} on {period_end}")
            return
        
        # Use the first available group
        group_id = groups.iloc[0]['DisectionGroupID']
        
        result = query_dissection_data(
            company_ticker=company,
            metric_name=metric,
            period_term=period,
            dissection_group_id=group_id,
            consolidation_type=consolidation,
            data_type=data_type
        )
        
        if result is not None and not result.empty:
            print("SQL Query: (Generated by query_dissection_data)")
            print("\nResult:")
            print(result)
            print(f"\nQoQ ROI/Asset for {company} (DisectionGroupID={group_id}): Found {len(result)} records")
        else:
            print("No data found.")
        
        return result
    except Exception as e:
        print(f"Error: {str(e)}")
        return None

def test_query_7():
    """Test Query 7: Quarterly Operating Cash Flow (Calculated Non-Ratio)
    "Give me the latest quarterly Operating Cash Flow of Lucky Cement."
    """
    print("\n=== Test Query 7: Quarterly Operating Cash Flow ===\n")
    
    company = "LUCK"
    metric = "Operating Cash Flow"
    period = "most recent quarter"
    consolidation = "Unconsolidated"
    
    try:
        result = query_quarterly_data(
            company_ticker=company,
            metric_name=metric,
            period_term=period,
            consolidation_type=consolidation
        )
        
        if result is not None and not result.empty:
            print("SQL Query: (Generated by query_quarterly_data)")
            print("\nResult:")
            print(result)
            print(f"\nLatest quarterly {metric} for {company}: {result.iloc[0]['Value']} {result.iloc[0]['Unit']}")
        else:
            print("No data found.")
            
            # Get company ID
            company_id = get_company_id(company)
            if not company_id:
                print(f"Company not found: {company}")
                return
            
            # Try similar metrics
            consolidation_id = db.get_consolidation_id(consolidation)
            query = f"""
            SELECT TOP 10 h.SubHeadName
            FROM tbl_headsmaster h
            JOIN tbl_financialrawdata_Quarter f ON h.SubHeadID = f.SubHeadID
            WHERE f.CompanyID = {company_id}
            AND f.ConsolidationID = {consolidation_id}
            AND h.SubHeadName LIKE '%Cash Flow%'
            GROUP BY h.SubHeadName
            ORDER BY COUNT(*) DESC
            """
            similar_metrics = db.execute_query(query)
            print("Similar metrics available:")
            for _, row in similar_metrics.iterrows():
                print(f"  - {row['SubHeadName']}")
        
        return result
    except Exception as e:
        print(f"Error: {str(e)}")
        return None

def test_query_8():
    """Test Query 8: YTD Gross Profit (Calculated Non-Ratio)
    "Show Year-to-Date Gross Profit for MCB (unconsolidated)."
    """
    print("\n=== Test Query 8: YTD Gross Profit ===\n")
    
    company = "MCB"
    metric = "Gross Profit"
    period = "ytd"
    consolidation = "Unconsolidated"
    
    try:
        # Get company ID
        company_id = get_company_id(company)
        if not company_id:
            print(f"Company not found: {company}")
            return
        
        # Resolve period
        period_end, term_id, fiscal_year = resolve_period_end(db, company_id, period)
        
        if not period_end:
            print(f"Could not resolve period: {period}")
            return
        
        # Get SubHeadID
        consolidation_id = db.get_consolidation_id(consolidation)
        head_id, is_ratio = get_available_head_id(db, company_id, metric, period_end, consolidation_id)
        
        if not head_id:
            print(f"No SubHeadID found for {metric}")
            return
        
        # Build and execute query
        query = f"""
        SELECT f.Value_ AS Value,
               u.unitname AS Unit,
               t.term AS Term,
               c.CompanyName AS Company,
               h.SubHeadName AS Metric,
               con.consolidationname AS Consolidation,
               f.PeriodEnd AS PeriodEnd
        FROM tbl_financialrawdata f
        JOIN tbl_headsmaster h ON f.SubHeadID = h.SubHeadID
        JOIN tbl_unitofmeasurement u ON h.UnitID = u.UnitID
        JOIN tbl_terms t ON f.TermID = t.TermID
        JOIN tbl_companieslist c ON f.CompanyID = c.CompanyID
        JOIN tbl_consolidation con ON f.ConsolidationID = con.ConsolidationID
        WHERE f.CompanyID = {company_id}
        AND f.SubHeadID = {head_id}
        AND f.PeriodEnd = '{period_end}'
        AND f.ConsolidationID = {consolidation_id}
        """
        
        print("SQL Query:")
        print(query)
        
        result = db.execute_query(query)
        
        if result.empty:
            print("No data found.")
        else:
            print("\nResult:")
            print(result)
            print(f"\nYTD {metric} for {company}: {result.iloc[0]['Value']} {result.iloc[0]['Unit']}")
        
        return result
    except Exception as e:
        print(f"Error: {str(e)}")
        return None

def test_query_9():
    """Test Query 9: TTM Revenue vs. TTM Cost (Dissection Quarter)
    "Compare MCB's TTM Revenue and TTM Cost (tbl_disectionrawdata_Quarter) for 2023-09-30."
    """
    print("\n=== Test Query 9: TTM Revenue vs. TTM Cost (Dissection Quarter) ===\n")
    
    company = "MCB"
    metric = "Revenue"
    period = "2023-09-30"
    consolidation = "Unconsolidated"
    data_type = "quarter"
    
    try:
        # Get company ID
        company_id = get_company_id(company)
        if not company_id:
            print(f"Company not found: {company}")
            return
        
        # Get SubHeadID
        consolidation_id = db.get_consolidation_id(consolidation)
        head_id, is_ratio = get_available_head_id(db, company_id, metric, period, consolidation_id)
        
        if not head_id:
            print(f"No SubHeadID found for {metric}")
            return
        
        # Find available dissection groups
        groups_query = f"""
        SELECT DISTINCT DisectionGroupID
        FROM tbl_disectionrawdata_Quarter
        WHERE CompanyID = {company_id}
        AND SubHeadID = {head_id}
        AND PeriodEnd = '{period}'
        AND ConsolidationID = {consolidation_id}
        """
        
        groups = db.execute_query(groups_query)
        
        if groups.empty:
            print(f"No dissection groups found for {metric} on {period}")
            return
        
        # Use the first available group
        group_id = groups.iloc[0]['DisectionGroupID']
        
        result = query_dissection_data(
            company_ticker=company,
            metric_name=metric,
            period_term=period,
            dissection_group_id=group_id,
            consolidation_type=consolidation,
            data_type=data_type
        )
        
        if result is not None and not result.empty:
            print("SQL Query: (Generated by query_dissection_data)")
            print("\nResult:")
            print(result)
            print(f"\nTTM Revenue vs. Cost for {company} (DisectionGroupID={group_id}): Found {len(result)} records")
        else:
            print("No data found.")
        
        return result
    except Exception as e:
        print(f"Error: {str(e)}")
        return None

def test_query_10():
    """Test Query 10: Annual Dividend Yield (Dissection Ratios)
    "Fetch HBL's annual Dividend Yield ratio breakdown for 2024-12-31."
    """
    print("\n=== Test Query 10: Annual Dividend Yield (Dissection Ratios) ===\n")
    
    company = "HBL"
    metric = "Dividend Yield"
    period = "2024-12-31"
    consolidation = "Unconsolidated"
    data_type = "ratio"
    
    try:
        # Get company ID
        company_id = get_company_id(company)
        if not company_id:
            print(f"Company not found: {company}")
            return
        
        # Get SubHeadID
        consolidation_id = db.get_consolidation_id(consolidation)
        head_id, is_ratio = get_available_head_id(db, company_id, metric, period, consolidation_id)
        
        if not head_id:
            print(f"No SubHeadID found for {metric}")
            # Try similar metrics
            query = f"""
            SELECT TOP 10 h.SubHeadName
            FROM tbl_ratiosheadmaster h
            JOIN tbl_disectionrawdata_Ratios f ON h.SubHeadID = f.SubHeadID
            WHERE f.CompanyID = {company_id}
            AND f.ConsolidationID = {consolidation_id}
            AND h.SubHeadName LIKE '%Dividend%'
            GROUP BY h.SubHeadName
            ORDER BY COUNT(*) DESC
            """
            similar_metrics = db.execute_query(query)
            print("Similar metrics available:")
            for _, row in similar_metrics.iterrows():
                print(f"  - {row['SubHeadName']}")
            return
        
        # Find available dissection groups
        groups_query = f"""
        SELECT DISTINCT DisectionGroupID
        FROM tbl_disectionrawdata_Ratios
        WHERE CompanyID = {company_id}
        AND SubHeadID = {head_id}
        AND PeriodEnd = '{period}'
        AND ConsolidationID = {consolidation_id}
        """
        
        groups = db.execute_query(groups_query)
        
        if groups.empty:
            print(f"No dissection groups found for {metric} on {period}")
            return
        
        # Use the first available group
        group_id = groups.iloc[0]['DisectionGroupID']
        
        result = query_dissection_data(
            company_ticker=company,
            metric_name=metric,
            period_term=period,
            dissection_group_id=group_id,
            consolidation_type=consolidation,
            data_type=data_type
        )
        
        if result is not None and not result.empty:
            print("SQL Query: (Generated by query_dissection_data)")
            print("\nResult:")
            print(result)
            print(f"\nDividend Yield for {company} (DisectionGroupID={group_id}): Found {len(result)} records")
        else:
            print("No data found.")
        
        return result
    except Exception as e:
        print(f"Error: {str(e)}")
        return None

# Run all tests
if __name__ == "__main__":
    try:
        print("\n=== TESTING CALCULATED & DISSECTION DATA QUERIES ===\n")
        
        # Run all test queries
        test_query_1()  # Latest Quarterly Revenue
        test_query_2()  # TTM Net Income
        test_query_3()  # Annual EPS Growth (Dissection)
        test_query_4()  # PAT Per Share (Dissection)
        test_query_5()  # YoY ROI Breakdown
        test_query_6()  # QoQ Asset Return
        test_query_7()  # Quarterly Operating Cash Flow
        test_query_8()  # YTD Gross Profit
        test_query_9()  # TTM Revenue vs. TTM Cost
        test_query_10() # Annual Dividend Yield
        
        print("\n=== ALL TESTS COMPLETED ===\n")
    except Exception as e:
        print(f"\nTests failed: {e}")