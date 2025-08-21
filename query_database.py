'''
Script to query the financial database for specific information
'''

import os
import sys
import pandas as pd

# Add the project root directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

from app.core.database.financial_db import FinancialDatabase

def setup_database():
    """Initialize database connection"""
    server = 'MUHAMMADUSMAN'  # Replace with your server name
    database = 'MGFinancials'  # Replace with your database name
    return FinancialDatabase(server, database)

def query_company_info(db, company_name):
    """Query company information including industry and sector"""
    query = f"""
    SELECT c.CompanyName, i.IndustryName, s.SectorName, cn.CountryName
    FROM tbl_companieslist c
    JOIN tbl_industryandsectormapping ism ON c.SectorID = ism.sectorid
    JOIN tbl_industrynames i ON ism.industryid = i.IndustryID
    JOIN tbl_sectornames s ON ism.sectorid = s.SectorID
    JOIN tbl_countriesnames cn ON c.CountryID = cn.CountryID
    WHERE c.CompanyName LIKE '%{company_name}%' OR c.CompanyTicker LIKE '%{company_name}%'
    """
    
    try:
        result = db.execute_query(query)
        if not result.empty:
            print(f"\nCompany Information for {company_name}:")
            print(f"Company: {result['CompanyName'].values[0]}")
            print(f"Industry: {result['IndustryName'].values[0]}")
            print(f"Sector: {result['SectorName'].values[0]}")
            print(f"Country: {result['CountryName'].values[0]}")
        else:
            print(f"No information found for {company_name}")
    except Exception as e:
        print(f"Error querying company info: {e}")

def query_companies_by_sector(db, sector_name):
    """Query companies belonging to a specific sector"""
    query = f"""
    SELECT c.CompanyName, c.CompanyTicker, i.IndustryName
    FROM tbl_companieslist c
    JOIN tbl_industryandsectormapping ism ON c.SectorID = ism.sectorid
    JOIN tbl_industrynames i ON ism.industryid = i.IndustryID
    JOIN tbl_sectornames s ON ism.sectorid = s.SectorID
    WHERE s.SectorName LIKE '%{sector_name}%'
    ORDER BY c.CompanyName
    """
    
    try:
        result = db.execute_query(query)
        if not result.empty:
            print(f"\nCompanies in {sector_name} sector:")
            for idx, row in result.iterrows():
                print(f"{row['CompanyName']} ({row['CompanyTicker']}) - {row['IndustryName']}")
        else:
            print(f"No companies found in {sector_name} sector")
    except Exception as e:
        print(f"Error querying companies by sector: {e}")

def query_companies_by_country_and_sector(db, country_name, sector_name):
    """Query companies from a specific country and sector"""
    query = f"""
    SELECT c.CompanyName, c.CompanyTicker, i.IndustryName
    FROM tbl_companieslist c
    JOIN tbl_industryandsectormapping ism ON c.SectorID = ism.sectorid
    JOIN tbl_industrynames i ON ism.industryid = i.IndustryID
    JOIN tbl_sectornames s ON ism.sectorid = s.SectorID
    JOIN tbl_countriesnames cn ON c.CountryID = cn.CountryID
    WHERE cn.CountryName LIKE '%{country_name}%' AND s.SectorName LIKE '%{sector_name}%'
    ORDER BY c.CompanyName
    """
    
    try:
        result = db.execute_query(query)
        if not result.empty:
            print(f"\nCompanies in {country_name} from {sector_name} sector:")
            for idx, row in result.iterrows():
                print(f"{row['CompanyName']} ({row['CompanyTicker']}) - {row['IndustryName']}")
        else:
            print(f"No companies found in {country_name} from {sector_name} sector")
    except Exception as e:
        print(f"Error querying companies by country and sector: {e}")

def query_industries_by_sector(db, sector_name):
    """Query industries under a specific sector"""
    query = f"""
    SELECT DISTINCT i.IndustryName
    FROM tbl_industryandsectormapping ism
    JOIN tbl_industrynames i ON ism.industryid = i.IndustryID
    JOIN tbl_sectornames s ON ism.sectorid = s.SectorID
    WHERE s.SectorName LIKE '%{sector_name}%'
    ORDER BY i.IndustryName
    """
    
    try:
        result = db.execute_query(query)
        if not result.empty:
            print(f"\nIndustries under {sector_name} sector:")
            for idx, row in result.iterrows():
                print(f"- {row['IndustryName']}")
        else:
            print(f"No industries found under {sector_name} sector")
    except Exception as e:
        print(f"Error querying industries by sector: {e}")

def query_statement_heads(db, statement_name):
    """Query heads belonging to a specific financial statement"""
    query = f"""
    SELECT h.SubHeadName
    FROM tbl_headsmaster h
    JOIN tbl_statementsname s ON h.StatementID = s.StatementID
    WHERE s.StatementName LIKE '%{statement_name}%'
    ORDER BY h.SubHeadName
    """
    
    try:
        result = db.execute_query(query)
        if not result.empty:
            print(f"\nHeads in {statement_name}:")
            for idx, row in result.iterrows():
                print(f"- {row['SubHeadName']}")
        else:
            print(f"No heads found for {statement_name}")
    except Exception as e:
        print(f"Error querying statement heads: {e}")

def query_ratio_heads(db):
    """Query all ratio heads available in the database"""
    query = """
    SELECT SubHeadName
    FROM tbl_ratiosheadmaster
    ORDER BY SubHeadName
    """
    
    try:
        result = db.execute_query(query)
        if not result.empty:
            print("\nAvailable Ratio Heads:")
            for idx, row in result.iterrows():
                print(f"- {row['SubHeadName']}")
        else:
            print("No ratio heads found")
    except Exception as e:
        print(f"Error querying ratio heads: {e}")

def query_financial_data(db, company_name, metric_name, term, consolidation_type="Consolidated"):
    """Query financial data for a specific company, metric, term, and consolidation"""
    try:
        # Get IDs
        company_id = db.get_company_id(company_name)
        head_id = db.get_head_id(metric_name, company_id)
        consolidation_id = db.get_consolidation_id(consolidation_type)
        
        # Get financial data
        result = db.get_financial_data(company_name, metric_name, term, consolidation_type)
        
        if result:
            print(f"\nFinancial Data for {company_name} - {metric_name} ({term}, {consolidation_type}):")
            print(f"Value: {result['Value']} {result['Unit']}")
            print(f"Period End: {result['PeriodEnd']}")
        else:
            print(f"No financial data found for {company_name} - {metric_name} ({term}, {consolidation_type})")
    except Exception as e:
        print(f"Error querying financial data: {e}")

def query_ratio_data(db, company_name, ratio_name, term, consolidation_type="Consolidated"):
    """Query ratio data for a specific company, ratio, term, and consolidation"""
    try:
        # Get IDs
        company_id = db.get_company_id(company_name)
        consolidation_id = db.get_consolidation_id(consolidation_type)
        
        # Direct SQL query for ratio data
        query = f"""
        SELECT r.Value_ as Value, u.unitname AS Unit, t.term AS Term, c.CompanyName AS Company,
               h.SubHeadName AS Metric, con.consolidationname AS Consolidation, r.PeriodEnd as PeriodEnd
        FROM tbl_ratiorawdata r
        JOIN tbl_ratiosheadmaster h ON r.SubHeadID = h.SubHeadID
        JOIN tbl_unitofmeasurement u ON h.UnitID = u.UnitID
        JOIN tbl_terms t ON r.TermID = t.TermID
        JOIN tbl_companieslist c ON r.CompanyID = c.CompanyID
        JOIN tbl_industryandsectormapping im ON im.sectorid = c.SectorID
        JOIN tbl_consolidation con ON r.ConsolidationID = con.ConsolidationID
        WHERE c.CompanyName LIKE '%{company_name}%'
        AND h.SubHeadName LIKE '%{ratio_name}%'
        AND t.term LIKE '%{term}%'
        AND con.consolidationname LIKE '%{consolidation_type}%'
        ORDER BY r.PeriodEnd DESC
        """
        
        result = db.execute_query(query)
        
        if not result.empty:
            print(f"\nRatio Data for {company_name} - {ratio_name} ({term}, {consolidation_type}):")
            for idx, row in result.iterrows():
                print(f"Value: {row['Value']} {row['Unit']}")
                print(f"Period End: {row['PeriodEnd']}")
                print(f"Metric: {row['Metric']}")
        else:
            print(f"No ratio data found for {company_name} - {ratio_name} ({term}, {consolidation_type})")
    except Exception as e:
        print(f"Error querying ratio data: {e}")

def query_subheads_for_ratio(db, ratio_name):
    """Query subheads available under a specific ratio"""
    query = f"""
    SELECT s.SubHeadName
    FROM tbl_ratio_subhead_mapping m
    JOIN tbl_ratiosheadmaster r ON m.RatioHeadID = r.SubHeadID
    JOIN tbl_headsmaster s ON m.SubHeadID = s.SubHeadID
    WHERE r.SubHeadName LIKE '%{ratio_name}%'
    """
    
    try:
        result = db.execute_query(query)
        if not result.empty:
            print(f"\nSubheads for {ratio_name}:")
            for idx, row in result.iterrows():
                print(f"- {row['SubHeadName']}")
        else:
            print(f"No subheads found for {ratio_name}")
    except Exception as e:
        print(f"Error querying subheads for ratio: {e}")

def query_statement_for_head(db, head_name):
    """Query which financial statement a specific head appears in"""
    query = f"""
    SELECT s.StatementName
    FROM tbl_headsmaster h
    JOIN tbl_statementsname s ON h.StatementID = s.StatementID
    WHERE h.SubHeadName LIKE '%{head_name}%'
    """
    
    try:
        result = db.execute_query(query)
        if not result.empty:
            print(f"\n'{head_name}' appears in:")
            for idx, row in result.iterrows():
                print(f"- {row['StatementName']}")
        else:
            print(f"'{head_name}' not found in any statement")
    except Exception as e:
        print(f"Error querying statement for head: {e}")

def query_heads_for_metric(db, metric_name):
    """Query which heads are mapped to a specific metric"""
    query = f"""
    SELECT h.SubHeadName
    FROM tbl_keystatssubheadsmapping m
    JOIN tbl_keystatsmaster k ON m.KeyStatsID = k.KeyStatsID
    JOIN tbl_headsmaster h ON m.SubHeadID = h.SubHeadID
    WHERE k.KeyStatsName LIKE '%{metric_name}%'
    """
    
    try:
        result = db.execute_query(query)
        if not result.empty:
            print(f"\nHeads mapped to {metric_name}:")
            for idx, row in result.iterrows():
                print(f"- {row['SubHeadName']}")
        else:
            print(f"No heads found mapped to {metric_name}")
    except Exception as e:
        print(f"Error querying heads for metric: {e}")

def query_most_recent_data(db, company_name, metric_name, consolidation_type="Consolidated"):
    """Query the most recent data for a specific company and metric"""
    query = f"""
    SELECT TOP 1 f.Value_ as Value, u.unitname AS Unit, t.term AS Term, c.CompanyName AS Company,
           h.SubHeadName AS Metric, con.consolidationname AS Consolidation, f.PeriodEnd as PeriodEnd
    FROM tbl_financialrawdata f
    JOIN tbl_headsmaster h ON f.SubHeadID = h.SubHeadID
    JOIN tbl_unitofmeasurement u ON h.UnitID = u.UnitID
    JOIN tbl_terms t ON f.TermID = t.TermID
    JOIN tbl_companieslist c ON f.CompanyID = c.CompanyID
    JOIN tbl_industryandsectormapping im ON im.sectorid = c.SectorID
    JOIN tbl_consolidation con ON f.ConsolidationID = con.ConsolidationID
    WHERE c.CompanyName LIKE '%{company_name}%'
    AND h.SubHeadName LIKE '%{metric_name}%'
    AND con.consolidationname LIKE '%{consolidation_type}%'
    ORDER BY f.PeriodEnd DESC
    """
    
    try:
        result = db.execute_query(query)
        if not result.empty:
            print(f"\nMost Recent {metric_name} for {company_name} ({consolidation_type}):")
            for idx, row in result.iterrows():
                print(f"Value: {row['Value']} {row['Unit']}")
                print(f"Period End: {row['PeriodEnd']}")
                print(f"Term: {row['Term']}")
        else:
            print(f"No data found for {company_name} - {metric_name}")
    except Exception as e:
        print(f"Error querying most recent data: {e}")

def query_compare_periods(db, company_name, metric_name, period1, period2, consolidation_type="Consolidated"):
    """Compare data for a company and metric between two periods"""
    query1 = f"""
    SELECT f.Value_ as Value, u.unitname AS Unit, t.term AS Term, f.PeriodEnd as PeriodEnd
    FROM tbl_financialrawdata f
    JOIN tbl_headsmaster h ON f.SubHeadID = h.SubHeadID
    JOIN tbl_unitofmeasurement u ON h.UnitID = u.UnitID
    JOIN tbl_terms t ON f.TermID = t.TermID
    JOIN tbl_companieslist c ON f.CompanyID = c.CompanyID
    JOIN tbl_consolidation con ON f.ConsolidationID = con.ConsolidationID
    WHERE c.CompanyName LIKE '%{company_name}%'
    AND h.SubHeadName LIKE '%{metric_name}%'
    AND t.term LIKE '%{period1}%'
    AND con.consolidationname LIKE '%{consolidation_type}%'
    ORDER BY f.PeriodEnd DESC
    """
    
    query2 = f"""
    SELECT f.Value_ as Value, u.unitname AS Unit, t.term AS Term, f.PeriodEnd as PeriodEnd
    FROM tbl_financialrawdata f
    JOIN tbl_headsmaster h ON f.SubHeadID = h.SubHeadID
    JOIN tbl_unitofmeasurement u ON h.UnitID = u.UnitID
    JOIN tbl_terms t ON f.TermID = t.TermID
    JOIN tbl_companieslist c ON f.CompanyID = c.CompanyID
    JOIN tbl_consolidation con ON f.ConsolidationID = con.ConsolidationID
    WHERE c.CompanyName LIKE '%{company_name}%'
    AND h.SubHeadName LIKE '%{metric_name}%'
    AND t.term LIKE '%{period2}%'
    AND con.consolidationname LIKE '%{consolidation_type}%'
    ORDER BY f.PeriodEnd DESC
    """
    
    try:
        result1 = db.execute_query(query1)
        result2 = db.execute_query(query2)
        
        print(f"\nComparing {metric_name} for {company_name} between {period1} and {period2}:")
        
        if not result1.empty:
            row1 = result1.iloc[0]
            print(f"{period1}: {row1['Value']} {row1['Unit']} (Period End: {row1['PeriodEnd']})")
        else:
            print(f"No data found for {period1}")
            
        if not result2.empty:
            row2 = result2.iloc[0]
            print(f"{period2}: {row2['Value']} {row2['Unit']} (Period End: {row2['PeriodEnd']})")
        else:
            print(f"No data found for {period2}")
            
        if not result1.empty and not result2.empty:
            row1 = result1.iloc[0]
            row2 = result2.iloc[0]
            if row1['Unit'] == row2['Unit']:
                change = row2['Value'] - row1['Value']
                pct_change = (change / row1['Value']) * 100 if row1['Value'] != 0 else float('inf')
                print(f"Change: {change} {row1['Unit']} ({pct_change:.2f}%)")
    except Exception as e:
        print(f"Error comparing periods: {e}")

def query_ttm_data(db, company_name, metric_name, consolidation_type="Consolidated"):
    """Query TTM (Trailing Twelve Months) data for a specific company and metric"""
    query = f"""
    SELECT TOP 1 f.Value_ as Value, u.unitname AS Unit, t.term AS Term, c.CompanyName AS Company,
           h.SubHeadName AS Metric, con.consolidationname AS Consolidation, f.PeriodEnd as PeriodEnd
    FROM tbl_financialrawdataTTM f
    JOIN tbl_headsmaster h ON f.SubHeadID = h.SubHeadID
    JOIN tbl_unitofmeasurement u ON h.UnitID = u.UnitID
    JOIN tbl_terms t ON f.TermID = t.TermID
    JOIN tbl_companieslist c ON f.CompanyID = c.CompanyID
    JOIN tbl_industryandsectormapping im ON im.sectorid = c.SectorID
    JOIN tbl_consolidation con ON f.ConsolidationID = con.ConsolidationID
    WHERE c.CompanyName LIKE '%{company_name}%'
    AND h.SubHeadName LIKE '%{metric_name}%'
    AND con.consolidationname LIKE '%{consolidation_type}%'
    ORDER BY f.PeriodEnd DESC
    """
    
    try:
        result = db.execute_query(query)
        if not result.empty:
            print(f"\nTTM {metric_name} for {company_name} ({consolidation_type}):")
            for idx, row in result.iterrows():
                print(f"Value: {row['Value']} {row['Unit']}")
                print(f"Period End: {row['PeriodEnd']}")
        else:
            print(f"No TTM data found for {company_name} - {metric_name}")
    except Exception as e:
        print(f"Error querying TTM data: {e}")

def query_ratios_for_industry(db, industry_name):
    """Query ratios available for a specific industry"""
    query = f"""
    SELECT DISTINCT r.SubHeadName
    FROM tbl_industryandkeystatsandratiosmapping m
    JOIN tbl_industrynames i ON m.IndustryID = i.IndustryID
    JOIN tbl_ratiosheadmaster r ON m.RatioHeadID = r.SubHeadID
    WHERE i.IndustryName LIKE '%{industry_name}%'
    ORDER BY r.SubHeadName
    """
    
    try:
        result = db.execute_query(query)
        if not result.empty:
            print(f"\nRatios available for {industry_name} industry:")
            for idx, row in result.iterrows():
                print(f"- {row['SubHeadName']}")
        else:
            print(f"No ratios found for {industry_name} industry")
    except Exception as e:
        print(f"Error querying ratios for industry: {e}")

def query_companies_with_high_ratio(db, ratio_name, threshold, term):
    """List companies with a ratio higher than a threshold for a specific term"""
    query = f"""
    SELECT c.CompanyName, r.Value_, u.unitname AS Unit, t.term AS Term, r.PeriodEnd
    FROM tbl_ratiorawdata r
    JOIN tbl_ratiosheadmaster h ON r.SubHeadID = h.SubHeadID
    JOIN tbl_unitofmeasurement u ON h.UnitID = u.UnitID
    JOIN tbl_terms t ON r.TermID = t.TermID
    JOIN tbl_companieslist c ON r.CompanyID = c.CompanyID
    WHERE h.SubHeadName LIKE '%{ratio_name}%'
    AND t.term LIKE '%{term}%'
    AND r.Value_ > {threshold}
    ORDER BY r.Value_ DESC
    """
    
    try:
        result = db.execute_query(query)
        if not result.empty:
            print(f"\nCompanies with {ratio_name} > {threshold} in {term}:")
            for idx, row in result.iterrows():
                print(f"{row['CompanyName']}: {row['Value_']} {row['Unit']} (Period End: {row['PeriodEnd']})")
        else:
            print(f"No companies found with {ratio_name} > {threshold} in {term}")
    except Exception as e:
        print(f"Error querying companies with high ratio: {e}")

def query_live_keystats(db, company_name, keystat_name):
    """Query live key stats for a specific company"""
    query = f"""
    SELECT k.KeyStatsValue, u.unitname AS Unit, ks.KeyStatsName, c.CompanyName, k.UpdateDate
    FROM tbl_keystatslive k
    JOIN tbl_keystatsmaster ks ON k.KeyStatsID = ks.KeyStatsID
    JOIN tbl_unitofmeasurement u ON ks.UnitID = u.UnitID
    JOIN tbl_companieslist c ON k.CompanyID = c.CompanyID
    WHERE c.CompanyName LIKE '%{company_name}%'
    AND ks.KeyStatsName LIKE '%{keystat_name}%'
    ORDER BY k.UpdateDate DESC
    """
    
    try:
        result = db.execute_query(query)
        if not result.empty:
            print(f"\nLive {keystat_name} for {company_name}:")
            for idx, row in result.iterrows():
                print(f"Value: {row['KeyStatsValue']} {row['Unit']}")
                print(f"Last Updated: {row['UpdateDate']}")
        else:
            print(f"No live key stats found for {company_name} - {keystat_name}")
    except Exception as e:
        print(f"Error querying live key stats: {e}")

def query_historical_keystats(db, company_name, keystat_name, years=5):
    """Query historical key stats for a specific company over a number of years"""
    query = f"""
    SELECT k.KeyStatsValue, u.unitname AS Unit, ks.KeyStatsName, c.CompanyName, k.PeriodEnd
    FROM tbl_keystatshistory k
    JOIN tbl_keystatsmaster ks ON k.KeyStatsID = ks.KeyStatsID
    JOIN tbl_unitofmeasurement u ON ks.UnitID = u.UnitID
    JOIN tbl_companieslist c ON k.CompanyID = c.CompanyID
    WHERE c.CompanyName LIKE '%{company_name}%'
    AND ks.KeyStatsName LIKE '%{keystat_name}%'
    ORDER BY k.PeriodEnd DESC
    """
    
    try:
        result = db.execute_query(query)
        if not result.empty:
            print(f"\nHistorical {keystat_name} for {company_name} (past {years} years):")
            for idx, row in result.iterrows():
                if idx < years:  # Limit to specified number of years
                    print(f"{row['PeriodEnd']}: {row['KeyStatsValue']} {row['Unit']}")
        else:
            print(f"No historical key stats found for {company_name} - {keystat_name}")
    except Exception as e:
        print(f"Error querying historical key stats: {e}")

def query_available_keystats(db, company_name):
    """Query available key stats for a specific company"""
    query = f"""
    SELECT DISTINCT ks.KeyStatsName
    FROM tbl_keystatslive k
    JOIN tbl_keystatsmaster ks ON k.KeyStatsID = ks.KeyStatsID
    JOIN tbl_companieslist c ON k.CompanyID = c.CompanyID
    WHERE c.CompanyName LIKE '%{company_name}%'
    ORDER BY ks.KeyStatsName
    """
    
    try:
        result = db.execute_query(query)
        if not result.empty:
            print(f"\nAvailable Key Stats for {company_name}:")
            for idx, row in result.iterrows():
                print(f"- {row['KeyStatsName']}")
        else:
            print(f"No key stats found for {company_name}")
    except Exception as e:
        print(f"Error querying available key stats: {e}")

def query_consolidated_data(db, company_name, metric_name, term, consolidation_type="Consolidated"):
    """Query consolidated data for a specific company, metric, and term"""
    try:
        # Get financial data with specified consolidation
        result = db.get_financial_data(company_name, metric_name, term, consolidation_type)
        
        if result:
            print(f"\n{consolidation_type} {metric_name} for {company_name} ({term}):")
            print(f"Value: {result['Value']} {result['Unit']}")
            print(f"Period End: {result['PeriodEnd']}")
        else:
            print(f"No {consolidation_type} data found for {company_name} - {metric_name} ({term})")
    except Exception as e:
        print(f"Error querying consolidated data: {e}")

def query_compare_consolidation(db, company_name, metric_name, term):
    """Compare consolidated and unconsolidated data for a company, metric, and term"""
    try:
        # Get consolidated data
        consolidated = db.get_financial_data(company_name, metric_name, term, "Consolidated")
        
        # Get unconsolidated data
        unconsolidated = db.get_financial_data(company_name, metric_name, term, "Unconsolidated")
        
        print(f"\nComparing {metric_name} for {company_name} ({term}):")
        
        if consolidated:
            print(f"Consolidated: {consolidated['Value']} {consolidated['Unit']}")
        else:
            print("Consolidated data not found")
            
        if unconsolidated:
            print(f"Unconsolidated: {unconsolidated['Value']} {unconsolidated['Unit']}")
        else:
            print("Unconsolidated data not found")
            
        if consolidated and unconsolidated and consolidated['Unit'] == unconsolidated['Unit']:
            diff = consolidated['Value'] - unconsolidated['Value']
            print(f"Difference: {diff} {consolidated['Unit']}")
    except Exception as e:
        print(f"Error comparing consolidation: {e}")

def query_unit_info(db, company_name):
    """Query the unit of measurement used for a specific company's financial results"""
    query = f"""
    SELECT DISTINCT u.unitname, u.UnitDescription
    FROM tbl_financialrawdata f
    JOIN tbl_headsmaster h ON f.SubHeadID = h.SubHeadID
    JOIN tbl_unitofmeasurement u ON h.UnitID = u.UnitID
    JOIN tbl_companieslist c ON f.CompanyID = c.CompanyID
    WHERE c.CompanyName LIKE '%{company_name}%'
    """
    
    try:
        result = db.execute_query(query)
        if not result.empty:
            print(f"\nUnits used for {company_name}'s financial results:")
            for idx, row in result.iterrows():
                print(f"- {row['unitname']}: {row['UnitDescription']}")
        else:
            print(f"No unit information found for {company_name}")
    except Exception as e:
        print(f"Error querying unit info: {e}")

def query_industries_by_sector(db, sector_name):
    """Query industries belonging to a specific sector"""
    query = f"""
    SELECT DISTINCT i.IndustryName
    FROM tbl_industryandsectormapping m
    JOIN tbl_industrynames i ON m.industryid = i.IndustryID
    JOIN tbl_sectornames s ON m.sectorid = s.SectorID
    WHERE s.SectorName LIKE '%{sector_name}%'
    ORDER BY i.IndustryName
    """
    
    try:
        result = db.execute_query(query)
        if not result.empty:
            print(f"\nIndustries in {sector_name} sector:")
            for idx, row in result.iterrows():
                print(f"- {row['IndustryName']}")
        else:
            print(f"No industries found for {sector_name} sector")
    except Exception as e:
        print(f"Error querying industries by sector: {e}")

def query_heads_for_industry(db, industry_name):
    """Query heads mapped to a specific industry"""
    query = f"""
    SELECT DISTINCT h.SubHeadName
    FROM tbl_industryandheadsmastermapping m
    JOIN tbl_industrynames i ON m.IndustryID = i.IndustryID
    JOIN tbl_headsmaster h ON m.SubHeadID = h.SubHeadID
    WHERE i.IndustryName LIKE '%{industry_name}%'
    ORDER BY h.SubHeadName
    """
    
    try:
        result = db.execute_query(query)
        if not result.empty:
            print(f"\nHeads mapped to {industry_name} industry:")
            for idx, row in result.iterrows():
                print(f"- {row['SubHeadName']}")
        else:
            print(f"No heads found mapped to {industry_name} industry")
    except Exception as e:
        print(f"Error querying heads for industry: {e}")

def query_sector_for_industry(db, industry_name):
    """Query the sector for a specific industry"""
    query = f"""
    SELECT DISTINCT s.SectorName
    FROM tbl_industryandsectormapping m
    JOIN tbl_industrynames i ON m.industryid = i.IndustryID
    JOIN tbl_sectornames s ON m.sectorid = s.SectorID
    WHERE i.IndustryName LIKE '%{industry_name}%'
    """
    
    try:
        result = db.execute_query(query)
        if not result.empty:
            print(f"\nSector for {industry_name} industry:")
            for idx, row in result.iterrows():
                print(f"- {row['SectorName']}")
        else:
            print(f"No sector found for {industry_name} industry")
    except Exception as e:
        print(f"Error querying sector for industry: {e}")

def query_term_definition(db, term_name):
    """Query the definition of a specific term"""
    query = f"""
    SELECT t.term, t.TermDescription
    FROM tbl_terms t
    WHERE t.term LIKE '%{term_name}%'
    """
    
    try:
        result = db.execute_query(query)
        if not result.empty:
            print(f"\nDefinition of {term_name}:")
            for idx, row in result.iterrows():
                print(f"{row['term']}: {row['TermDescription']}")
        else:
            print(f"No definition found for {term_name}")
    except Exception as e:
        print(f"Error querying term definition: {e}")

def query_term_mapping(db, term_name):
    """Query which term is mapped to a specific term"""
    query = f"""
    SELECT t1.term AS SourceTerm, t2.term AS MappedTerm
    FROM tbl_termsmapping m
    JOIN tbl_terms t1 ON m.SourceTermID = t1.TermID
    JOIN tbl_terms t2 ON m.MappedTermID = t2.TermID
    WHERE t1.term LIKE '%{term_name}%' OR t2.term LIKE '%{term_name}%'
    """
    
    try:
        result = db.execute_query(query)
        if not result.empty:
            print(f"\nTerm mappings for {term_name}:")
            for idx, row in result.iterrows():
                print(f"{row['SourceTerm']} -> {row['MappedTerm']}")
        else:
            print(f"No term mappings found for {term_name}")
    except Exception as e:
        print(f"Error querying term mapping: {e}")

def query_profitability_definitions(db):
    """Query definitions related to profitability ratios"""
    query = """
    SELECT r.SubHeadName, r.SubHeadDescription
    FROM tbl_ratiosheadmaster r
    WHERE r.SubHeadName LIKE '%Profit%' OR r.SubHeadName LIKE '%Margin%' OR r.SubHeadName LIKE '%Return%'
    ORDER BY r.SubHeadName
    """
    
    try:
        result = db.execute_query(query)
        if not result.empty:
            print("\nDefinitions related to profitability ratios:")
            for idx, row in result.iterrows():
                print(f"{row['SubHeadName']}: {row['SubHeadDescription']}")
        else:
            print("No profitability ratio definitions found")
    except Exception as e:
        print(f"Error querying profitability definitions: {e}")

def query_latest_file(db, company_name):
    """Query the latest file containing results for a specific company"""
    query = f"""
    SELECT TOP 1 f.FileName, f.FilePath, f.UploadDate
    FROM tbl_financialfiles f
    JOIN tbl_companieslist c ON f.CompanyID = c.CompanyID
    WHERE c.CompanyName LIKE '%{company_name}%'
    ORDER BY f.UploadDate DESC
    """
    
    try:
        result = db.execute_query(query)
        if not result.empty:
            print(f"\nLatest file for {company_name}:")
            for idx, row in result.iterrows():
                print(f"File: {row['FileName']}")
                print(f"Path: {row['FilePath']}")
                print(f"Upload Date: {row['UploadDate']}")
        else:
            print(f"No files found for {company_name}")
    except Exception as e:
        print(f"Error querying latest file: {e}")

def query_data_edits(db, company_name, fiscal_year):
    """Query data edits made to a specific company's results for a fiscal year"""
    query = f"""
    SELECT e.EditDate, e.OldValue, e.NewValue, h.SubHeadName, u.username
    FROM tbl_financialdataedit e
    JOIN tbl_headsmaster h ON e.SubHeadID = h.SubHeadID
    JOIN tbl_companieslist c ON e.CompanyID = c.CompanyID
    JOIN tbl_users u ON e.UserID = u.UserID
    WHERE c.CompanyName LIKE '%{company_name}%'
    AND e.FiscalYear = {fiscal_year}
    ORDER BY e.EditDate DESC
    """
    
    try:
        result = db.execute_query(query)
        if not result.empty:
            print(f"\nData edits for {company_name} (FY {fiscal_year}):")
            for idx, row in result.iterrows():
                print(f"Metric: {row['SubHeadName']}")
                print(f"Old Value: {row['OldValue']}")
                print(f"New Value: {row['NewValue']}")
                print(f"Edit Date: {row['EditDate']}")
                print(f"User: {row['username']}")
                print("---")
        else:
            print(f"No data edits found for {company_name} (FY {fiscal_year})")
    except Exception as e:
        print(f"Error querying data edits: {e}")

def query_metric_calculation(db, metric_name):
    """Query how a specific metric is calculated in the system"""
    query = f"""
    SELECT m.CalculationFormula, c.CategoryName
    FROM tbl_financialmetriccalculationmapping m
    JOIN tbl_financialmetriccategories c ON m.CategoryID = c.CategoryID
    WHERE m.MetricName LIKE '%{metric_name}%'
    """
    
    try:
        result = db.execute_query(query)
        if not result.empty:
            print(f"\nCalculation for {metric_name}:")
            for idx, row in result.iterrows():
                print(f"Formula: {row['CalculationFormula']}")
                print(f"Category: {row['CategoryName']}")
        else:
            print(f"No calculation found for {metric_name}")
    except Exception as e:
        print(f"Error querying metric calculation: {e}")

def query_metric_category(db, metric_name):
    """Query which category a specific metric belongs to"""
    query = f"""
    SELECT c.CategoryName
    FROM tbl_financialmetriccalculationmapping m
    JOIN tbl_financialmetriccategories c ON m.CategoryID = c.CategoryID
    WHERE m.MetricName LIKE '%{metric_name}%'
    """
    
    try:
        result = db.execute_query(query)
        if not result.empty:
            print(f"\nCategory for {metric_name}:")
            for idx, row in result.iterrows():
                print(f"- {row['CategoryName']}")
        else:
            print(f"No category found for {metric_name}")
    except Exception as e:
        print(f"Error querying metric category: {e}")

def query_metrics_by_category(db, category_name):
    """Query metrics under a specific category"""
    query = f"""
    SELECT m.MetricName
    FROM tbl_financialmetriccalculationmapping m
    JOIN tbl_financialmetriccategories c ON m.CategoryID = c.CategoryID
    WHERE c.CategoryName LIKE '%{category_name}%'
    ORDER BY m.MetricName
    """
    
    try:
        result = db.execute_query(query)
        if not result.empty:
            print(f"\nMetrics under {category_name} category:")
            for idx, row in result.iterrows():
                print(f"- {row['MetricName']}")
        else:
            print(f"No metrics found under {category_name} category")
    except Exception as e:
        print(f"Error querying metrics by category: {e}")

def query_dissection_metrics(db, sector_name):
    """Query dissection metrics available for a specific sector"""
    try:
        # First, let's get the industry IDs for the sector
        industry_query = f"""
        SELECT i.IndustryID, i.IndustryName
        FROM tbl_industryandsectormapping m 
        JOIN tbl_industrynames i ON m.industryid = i.IndustryID
        JOIN tbl_sectornames s ON m.sectorid = s.SectorID
        WHERE s.SectorName LIKE '%{sector_name}%'
        """
        
        industry_result = db.execute_query(industry_query)
        
        if industry_result.empty:
            print(f"No industries found for {sector_name} sector")
            return
            
        print(f"\nIndustries in {sector_name} sector:")
        for idx, row in industry_result.iterrows():
            print(f"- {row['IndustryName']}")
            
        # Now query for common financial metrics used in this sector
        metrics_query = f"""
        SELECT DISTINCT r.HeadNames
        FROM tbl_ratiosheadmaster r
        JOIN tbl_industryandsectormapping m ON r.IndustryID = m.industryid
        JOIN tbl_sectornames s ON m.sectorid = s.SectorID
        WHERE s.SectorName LIKE '%{sector_name}%'
        ORDER BY r.HeadNames
        """
        
        try:
            metrics_result = db.execute_query(metrics_query)
            if not metrics_result.empty:
                print(f"\nCommon financial metrics for {sector_name} sector:")
                for m_idx, m_row in metrics_result.iterrows():
                    print(f"- {m_row['HeadNames']}")
            else:
                print(f"No specific metrics found for {sector_name} sector")
        except Exception as e:
            print(f"Error querying sector metrics: {e}")
    except Exception as e:
        print(f"Error querying dissection metrics: {e}")

def query_ratio_dissections(db, company_name, ratio_name, term, consolidation_type="Consolidated"):
    """Query ratio dissections for a specific company, ratio, term, and consolidation"""
    try:
        # First, get the SubHeadID for the ratio
        ratio_query = f"""
        SELECT SubHeadID, HeadNames 
        FROM tbl_ratiosheadmaster 
        WHERE HeadNames LIKE '%{ratio_name}%'
        """
        ratio_result = db.execute_query(ratio_query)
        
        if ratio_result.empty:
            print(f"No ratio found with name '{ratio_name}'")
            return
            
        subhead_id = ratio_result.iloc[0]['SubHeadID']
        
        # Now query the ratio data with the correct SubHeadID
        query = f"""
        SELECT r.Value_, r.PeriodEnd, c.CompanyName, t.term, con.consolidationname
        FROM tbl_ratiorawdata r
        JOIN tbl_ratiosheadmaster h ON r.SubHeadID = h.SubHeadID
        JOIN tbl_terms t ON r.TermID = t.TermID
        JOIN tbl_companieslist c ON r.CompanyID = c.CompanyID
        JOIN tbl_consolidation con ON r.ConsolidationID = con.ConsolidationID
        WHERE c.CompanyName LIKE '%{company_name}%'
        AND r.SubHeadID = {subhead_id}
        AND t.term LIKE '%{term}%'
        AND con.consolidationname LIKE '%{consolidation_type}%'
        ORDER BY r.PeriodEnd DESC
        """
        
        result = db.execute_query(query)
        if not result.empty:
            print(f"\nRatio data for {company_name} - {ratio_name} ({term}, {consolidation_type}):")
            for idx, row in result.iterrows():
                print(f"Value: {row['Value_']}")
                print(f"Period End: {row['PeriodEnd']}")
                print(f"Company: {row['CompanyName']}")
                print(f"Term: {row['term']}")
                print(f"Consolidation: {row['consolidationname']}")
                print("---")
                
            # Try to get the components that make up this ratio
            try:
                # Get the period end date from the result
                period_end = result.iloc[0]['PeriodEnd']
                
                # Query for financial data that might be components of this ratio
                components_query = f"""
                SELECT h.HeadName, f.Value_, c.CompanyName, t.term
                FROM tbl_financialrawdata f
                JOIN tbl_headsmaster h ON f.HeadID = h.HeadID
                JOIN tbl_companieslist c ON f.CompanyID = c.CompanyID
                JOIN tbl_terms t ON f.TermID = t.TermID
                JOIN tbl_consolidation con ON f.ConsolidationID = con.ConsolidationID
                WHERE c.CompanyName LIKE '%{company_name}%'
                AND t.term LIKE '%{term}%'
                AND con.consolidationname LIKE '%{consolidation_type}%'
                AND f.PeriodEnd = '{period_end}'
                AND (h.HeadName LIKE '%Debt%' OR h.HeadName LIKE '%Equity%')
                ORDER BY h.HeadName
                """
                
                components_result = db.execute_query(components_query)
                if not components_result.empty:
                    print(f"\nComponents that may be used in {ratio_name} calculation:")
                    for c_idx, c_row in components_result.iterrows():
                        print(f"- {c_row['HeadName']}: {c_row['Value_']}")
            except Exception as e:
                print(f"Could not retrieve ratio components: {e}")
        else:
            print(f"No ratio data found for {company_name} - {ratio_name} ({term}, {consolidation_type})")
    except Exception as e:
        print(f"Error querying ratio dissections: {e}")

def main():
    db = setup_database()
    
    # 1. Companies & Metadata
    print("\n=== Companies & Metadata ===\n")
    
    # What is the industry and sector of UBL?
    query_company_info(db, "UBL")
    
    # Which companies belong to the Banking sector?
    query_companies_by_sector(db, "Banking")
    
    # List all companies operating in Pakistan from the Oil & Gas sector
    query_companies_by_country_and_sector(db, "Pakistan", "Oil & Gas")
    
    # Which industries exist under the Financial Services sector?
    query_industries_by_sector(db, "Financial Services")
    
    # Find the country, industry, and sector of LUCK
    query_company_info(db, "LUCK")
    
    # 2. Statements & Heads
    print("\n=== Statements & Heads ===\n")
    
    # Which heads belong to the Income Statement?
    query_statement_heads(db, "Income Statement")
    
    # What are the subheads available under Debt to Equity ratio?
    query_subheads_for_ratio(db, "Debt to Equity")
    
    # In which financial statement does "Operating Profit" appear?
    query_statement_for_head(db, "Operating Profit")
    
    # Which heads are mapped to EPS?
    query_heads_for_metric(db, "EPS")
    
    # Find all ratio heads available in the database
    query_ratio_heads(db)
    
    # 3. Financial Raw Data
    print("\n=== Financial Raw Data ===\n")
    
    # What was the EPS of HBL for Q4 2023?
    query_financial_data(db, "HBL", "EPS", "Q4 2023")
    
    # Show me the Net Profit of OGDC for FY 2024
    query_financial_data(db, "OGDC", "Net Profit", "FY 2024")
    
    # Retrieve the Total Assets of LUCK for the most recent quarter
    query_most_recent_data(db, "LUCK", "Total Assets")
    
    # Compare the Revenue of ENGRO for Q1 2023 and Q1 2024
    query_compare_periods(db, "ENGRO", "Revenue", "Q1 2023", "Q1 2024")
    
    # What is the 12-month trailing EPS of MCB?
    query_ttm_data(db, "MCB", "EPS")
    
    # 4. Ratios
    print("\n=== Ratios ===\n")
    
    # What is the Debt to Equity ratio of HBL for FY 2023?
    query_ratio_data(db, "HBL", "Debt to Equity", "FY 2023")
    
    # Show me the Current Ratio of UBL for 2022
    query_ratio_data(db, "UBL", "Current Ratio", "FY 2022")
    
    # Compare ROA of ABL and MCB for Q2 2024
    query_ratio_data(db, "ABL", "ROA", "Q2 2024")
    query_ratio_data(db, "MCB", "ROA", "Q2 2024")
    
    # Which ratios are available for the Cement industry?
    query_ratios_for_industry(db, "Cement")
    
    # List all companies with a Debt to Equity ratio higher than 2 in FY 2023
    query_companies_with_high_ratio(db, "Debt to Equity", 2, "FY 2023")
    
    # 5. Key Stats
    print("\n=== Key Stats ===\n")
    
    # What is the live EPS of HBL?
    query_live_keystats(db, "HBL", "EPS")
    
    # Show me the historical P/E ratio of UBL for the past 5 years
    query_historical_keystats(db, "UBL", "P/E", 5)
    
    # Which key stats are available for LUCK?
    query_available_keystats(db, "LUCK")
    
    # Compare the Dividend Yield of ENGRO in 2020 vs 2024
    query_compare_periods(db, "ENGRO", "Dividend Yield", "FY 2020", "FY 2024")
    
    # 6. Consolidation & Units
    print("\n=== Consolidation & Units ===\n")
    
    # Show EPS of HBL in consolidated format for FY 2023
    query_consolidated_data(db, "HBL", "EPS", "FY 2023", "Consolidated")
    
    # What is the difference between standalone and consolidated results for UBL in Q2 2024?
    query_compare_consolidation(db, "UBL", "Net Profit", "Q2 2024")
    
    # In which unit are financial results reported for OGDC?
    query_unit_info(db, "OGDC")
    
    # 7. Industry & Sector Mapping
    print("\n=== Industry & Sector Mapping ===\n")
    
    # Which industries belong to the Manufacturing sector?
    query_industries_by_sector(db, "Manufacturing")
    
    # Show all heads mapped to the Banking industry
    query_heads_for_industry(db, "Banking")
    
    # Find the sector for the Fertilizer industry
    query_sector_for_industry(db, "Fertilizer")
    
    # Which ratios are applicable to the Banking industry?
    query_ratios_for_industry(db, "Banking")
    
    # 8. Terms & Definitions
    print("\n=== Terms & Definitions ===\n")
    
    # What does EPS stand for?
    query_term_definition(db, "EPS")
    
    # Define the Current Ratio
    query_term_definition(db, "Current Ratio")
    
    # Which term is mapped to Price to Book Value?
    query_term_mapping(db, "Price to Book Value")
    
    # Show all definitions related to profitability ratios
    query_profitability_definitions(db)
    
    # 9. File & Data Management
    print("\n=== File & Data Management ===\n")
    
    # Which file contains the latest results of HBL?
    query_latest_file(db, "HBL")
    
    # List all financial data edits made to ENGRO's FY 2023 results
    query_data_edits(db, "ENGRO", 2023)
    
    # 10. Calculation & Mapping Tables
    print("\n=== Calculation & Mapping Tables ===\n")
    
    # How is ROE calculated in the system?
    query_metric_calculation(db, "ROE")
    
    # Which category does EPS belong to?
    query_metric_category(db, "EPS")
    
    # What is the calculation mapping for Debt to Equity?
    query_metric_calculation(db, "Debt to Equity")
    
    # Show all metrics under the Profitability category
    query_metrics_by_category(db, "Profitability")
    
    # 11. Validation & Dissection
    print("\n=== Validation & Dissection ===\n")
    
    # What dissection metrics are available for Banking sector companies?
    query_dissection_metrics(db, "Banking")
    
    # Show the ratio dissections for UBL in FY 2023
    query_ratio_dissections(db, "UBL", "Debt to Equity", "FY 2023")

if __name__ == "__main__":
    main()