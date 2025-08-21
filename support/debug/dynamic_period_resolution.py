# Dynamic Period Resolution for Mettis Financial Database

import sqlite3
import datetime
from typing import Tuple, Optional, Union


def get_company_id(symbol_or_name: str, conn) -> int:
    """Get company ID from symbol or name."""
    cursor = conn.cursor()
    cursor.execute(
        "SELECT CompanyID FROM tbl_companieslist WHERE Symbol = ? OR CompanyName LIKE ?",
        (symbol_or_name, f"%{symbol_or_name}%")
    )
    result = cursor.fetchone()
    if not result:
        raise ValueError(f"Company '{symbol_or_name}' not found")
    return result[0]


def resolve_period_end(natural_language_term: str, company_id: int, conn) -> Tuple[Optional[str], Optional[int], Optional[int]]:
    """Translate natural language period terms into concrete PeriodEnd or TermID + FY.
    
    Args:
        natural_language_term: Natural language term like "most recent quarter", "current period", "YTD", etc.
        company_id: The company ID to query for
        conn: Database connection
        
    Returns:
        Tuple of (period_end_date, term_id, fiscal_year) where:
            - period_end_date: A date string in the format 'YYYY-MM-DD' or None if using term_id + fy
            - term_id: Term ID from tbl_terms or None if using period_end_date
            - fiscal_year: Fiscal year or None if using period_end_date
    """
    cursor = conn.cursor()
    
    # Current date for relative calculations
    today = datetime.date.today()
    
    # Handle different natural language terms
    if "recent" in natural_language_term.lower() and "quarter" in natural_language_term.lower():
        # Most recent quarter
        cursor.execute(
            "SELECT MAX(PeriodEnd) FROM tbl_financialrawdata_Quarter WHERE CompanyID = ?",
            (company_id,)
        )
        result = cursor.fetchone()
        if result and result[0]:
            return result[0], None, None
    
    elif "ytd" in natural_language_term.lower() or "year to date" in natural_language_term.lower():
        # Year to date - find the most recent period in the current year
        current_year = today.year
        cursor.execute(
            "SELECT MAX(PeriodEnd) FROM tbl_financialrawdata WHERE CompanyID = ? AND PeriodEnd LIKE ?",
            (company_id, f"{current_year}-%")
        )
        result = cursor.fetchone()
        if result and result[0]:
            return result[0], None, None
    
    elif "ttm" in natural_language_term.lower() or "trailing twelve month" in natural_language_term.lower():
        # Trailing twelve months
        cursor.execute(
            "SELECT MAX(PeriodEnd) FROM tbl_financialrawdataTTM WHERE CompanyID = ?",
            (company_id,)
        )
        result = cursor.fetchone()
        if result and result[0]:
            return result[0], None, None
    
    elif "current" in natural_language_term.lower() and "fiscal" in natural_language_term.lower():
        # Current fiscal year
        # First determine the company's fiscal year end month
        cursor.execute(
            "SELECT MAX(PeriodEnd) FROM tbl_financialrawdata WHERE CompanyID = ? AND TermID = 4",  # TermID 4 is typically annual
            (company_id,)
        )
        result = cursor.fetchone()
        if result and result[0]:
            # Get the term ID for annual (typically 4)
            cursor.execute("SELECT TermID FROM tbl_terms WHERE term = '12M'")
            term_result = cursor.fetchone()
            if term_result:
                # Extract the year from the most recent annual report
                last_annual_date = datetime.datetime.strptime(result[0], "%Y-%m-%d").date()
                current_fiscal_year = last_annual_date.year
                if today > last_annual_date.replace(year=today.year):
                    current_fiscal_year = today.year
                else:
                    current_fiscal_year = today.year - 1
                    
                return None, term_result[0], current_fiscal_year
    
    elif any(quarter in natural_language_term.lower() for quarter in ["q1", "q2", "q3", "q4"]):
        # Specific quarter
        for q_num, q_text in [(1, "q1"), (2, "q2"), (3, "q3"), (4, "q4")]:
            if q_text in natural_language_term.lower():
                # Get the term ID for this quarter
                cursor.execute(f"SELECT TermID FROM tbl_terms WHERE term = '{q_text.upper()}'")
                term_result = cursor.fetchone()
                if term_result:
                    # Try to extract year from the query, otherwise use current year
                    year_match = None
                    for word in natural_language_term.split():
                        if word.isdigit() and len(word) == 4:  # Looks like a year
                            year_match = int(word)
                            break
                    
                    fiscal_year = year_match if year_match else today.year
                    return None, term_result[0], fiscal_year
    
    # Default: return the most recent period end date for the company
    cursor.execute(
        "SELECT MAX(PeriodEnd) FROM tbl_financialrawdata WHERE CompanyID = ?",
        (company_id,)
    )
    result = cursor.fetchone()
    if result and result[0]:
        return result[0], None, None
    
    # If all else fails
    return None, None, None


def build_period_condition(period_info: Tuple[Optional[str], Optional[int], Optional[int]]) -> str:
    """Build the SQL condition for period filtering based on period resolution results.
    
    Args:
        period_info: Tuple of (period_end_date, term_id, fiscal_year) from resolve_period_end
        
    Returns:
        SQL condition string for the WHERE clause
    """
    period_end, term_id, fiscal_year = period_info
    
    if period_end:
        return f"f.PeriodEnd = '{period_end}'"
    elif term_id is not None and fiscal_year is not None:
        return f"f.TermID = {term_id} AND f.FY = {fiscal_year}"
    else:
        # Fallback to a condition that will return no results rather than all results
        return "1=0"


def example_usage():
    """Example of how to use the dynamic period resolution."""
    # Connect to the database
    conn = sqlite3.connect("mettis_financial.db")
    
    # Example 1: Most recent quarter for HBL
    company_id = get_company_id("HBL", conn)
    period_info = resolve_period_end("most recent quarter", company_id, conn)
    period_condition = build_period_condition(period_info)
    
    print(f"Company ID: {company_id}")
    print(f"Period Info: {period_info}")
    print(f"SQL Condition: {period_condition}")
    
    # Example query using the resolved period
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
      JOIN tbl_industryandsectormapping im 
           ON im.SectorID = c.SectorID
          AND h.IndustryID = im.IndustryID
      JOIN tbl_consolidation con ON f.ConsolidationID = con.ConsolidationID
     WHERE f.CompanyID = {company_id}
       AND f.SubHeadID = 123  -- Example SubHeadID
       AND {period_condition}
       AND f.ConsolidationID = 1  -- Example ConsolidationID
     ORDER BY f.PeriodEnd DESC;
    """
    
    print("\nExample Query:")
    print(query)
    
    # Example 2: Current fiscal year for UBL
    company_id = get_company_id("UBL", conn)
    period_info = resolve_period_end("current fiscal year", company_id, conn)
    period_condition = build_period_condition(period_info)
    
    print(f"\nCompany ID: {company_id}")
    print(f"Period Info: {period_info}")
    print(f"SQL Condition: {period_condition}")
    
    conn.close()


if __name__ == "__main__":
    example_usage()