#!/usr/bin/env python3
"""
Comprehensive test suite for dissection queries in the Enhanced Financial RAG System
Tests all aspects of dissection data processing including:
- Entity extraction for dissection indicators
- Dissection group ID mapping
- Query building with DisectionGroupID filters
- Fallback mechanisms for dissection metrics
- Integration with existing RAG pipeline
"""

import sys
import os
import json
from datetime import datetime
from typing import Dict, List, Any

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from enhanced_financial_rag import EnhancedFinancialRAG
from utils import logger

def test_dissection_entity_extraction():
    """
    Test entity extraction for dissection-specific queries
    """
    print("\n" + "="*60)
    print("TESTING DISSECTION ENTITY EXTRACTION")
    print("="*60)
    
    rag = EnhancedFinancialRAG(
        server='MUHAMMADUSMAN',
        database='MGFinancials'
    )
    
    test_queries = [
        # Per share metrics
        "What is HBL's EPS per share for the latest quarter?",
        "Show me OGDC's revenue per share in Q2 2023",
        "Give me UBL's book value per share for the most recent period",
        
        # Percentage of revenue metrics
        "What is MCB's net profit as % of revenue?",
        "Show me PSO's operating expenses as percentage of revenue",
        "Give me ENGRO's cost of sales as % of revenue for Q1 2023",
        
        # Percentage of assets metrics
        "What is LUCK's cash as % of assets?",
        "Show me HBL's loans as percentage of total assets",
        "Give me UBL's equity as % of assets for the latest quarter",
        
        # Annual growth metrics
        "What is OGDC's revenue annual growth?",
        "Show me MCB's net profit annual growth rate",
        "Give me PSO's assets annual growth for 2023",
        
        # Quarterly growth metrics
        "What is ENGRO's quarterly growth in revenue?",
        "Show me LUCK's quarterly growth in net profit",
        "Give me HBL's quarterly growth rate for total assets"
    ]
    
    for i, query in enumerate(test_queries, 1):
        print(f"\nTest {i}: {query}")
        print("-" * 50)
        
        try:
            entities = rag._extract_entities(query)
            print(f"Extracted Entities:")
            for key, value in entities.items():
                print(f"  {key}: {value}")
            
            # Check for dissection indicators
            has_dissection = entities.get('has_dissection_indicator', False)
            dissection_group = entities.get('dissection_group')
            
            print(f"\nDissection Analysis:")
            print(f"  Has dissection indicator: {has_dissection}")
            print(f"  Dissection group: {dissection_group}")
            
            if has_dissection and dissection_group:
                print("✅ Dissection extraction successful")
            else:
                print("⚠️  No dissection indicators detected")
                
        except Exception as e:
            print(f"❌ Error: {e}")

def test_dissection_group_mapping():
    """
    Test the dissection group ID mapping functionality
    """
    print("\n" + "="*60)
    print("TESTING DISSECTION GROUP ID MAPPING")
    print("="*60)
    
    rag = EnhancedFinancialRAG(
        server='MUHAMMADUSMAN',
        database='MGFinancials'
    )
    
    test_groups = [
        "per share",
        "% of revenue",
        "percentage of revenue",
        "% of assets",
        "percentage of assets",
        "annual growth",
        "quarterly growth",
        "invalid group"
    ]
    
    for i, group in enumerate(test_groups, 1):
        print(f"\nTest {i}: Mapping '{group}'")
        print("-" * 40)
        
        try:
            group_id = rag.get_disection_group_id(group)
            if group_id:
                print(f"✅ Group ID: {group_id}")
            else:
                print(f"❌ No mapping found for '{group}'")
                
        except Exception as e:
            print(f"❌ Error: {e}")

def test_dissection_metric_resolution():
    """
    Test metric resolution for dissection data
    """
    print("\n" + "="*60)
    print("TESTING DISSECTION METRIC RESOLUTION")
    print("="*60)
    
    rag = EnhancedFinancialRAG(
        server='MUHAMMADUSMAN',
        database='MGFinancials'
    )
    
    # Get a sample company context
    company_context = rag._resolve_company_context("HBL")
    if not company_context:
        print("❌ Could not resolve company context for HBL")
        return
    
    test_metrics = [
        {"metric": "EPS", "group": "per share"},
        {"metric": "Revenue", "group": "per share"},
        {"metric": "Net Profit", "group": "% of revenue"},
        {"metric": "Operating Expenses", "group": "% of revenue"},
        {"metric": "Cash", "group": "% of assets"},
        {"metric": "Total Assets", "group": "annual growth"},
        {"metric": "Revenue", "group": "quarterly growth"}
    ]
    
    for i, test_case in enumerate(test_metrics, 1):
        print(f"\nTest {i}: Resolving '{test_case['metric']}' for '{test_case['group']}'")
        print("-" * 60)
        
        try:
            # Create entities dict
            entities = {
                'metric': test_case['metric'],
                'dissection_group': test_case['group'],
                'has_dissection_indicator': True
            }
            
            metric_info = rag._resolve_metric_context(entities, company_context)
            
            if metric_info:
                print(f"✅ Metric resolved:")
                print(f"  Head ID: {metric_info['head_id']}")
                print(f"  Data Type: {metric_info['data_type']}")
                print(f"  Dissection Group ID: {metric_info.get('dissection_group_id', 'N/A')}")
                if 'formula' in metric_info:
                    print(f"  Formula: {metric_info['formula']}")
            else:
                print(f"❌ Metric not found")
                
        except Exception as e:
            print(f"❌ Error: {e}")

def test_dissection_query_building():
    """
    Test SQL query building for dissection data
    """
    print("\n" + "="*60)
    print("TESTING DISSECTION QUERY BUILDING")
    print("="*60)
    
    rag = EnhancedFinancialRAG(
        server='MUHAMMADUSMAN',
        database='MGFinancials'
    )
    
    # Get sample contexts
    company_context = rag._resolve_company_context("HBL")
    if not company_context:
        print("❌ Could not resolve company context")
        return
    
    test_cases = [
        {
            "entities": {
                "company": "HBL",
                "metric": "EPS",
                "term": "latest",
                "consolidation": "consolidated",
                "dissection_group": "per share",
                "has_dissection_indicator": True,
                "is_relative_term": True,
                "relative_type": "most_recent_quarter"
            },
            "description": "Latest EPS per share for HBL"
        },
        {
            "entities": {
                "company": "OGDC",
                "metric": "Net Profit",
                "term": "Q2 2023",
                "consolidation": "consolidated",
                "dissection_group": "% of revenue",
                "has_dissection_indicator": True
            },
            "description": "Net Profit as % of revenue for OGDC Q2 2023"
        }
    ]
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\nTest {i}: {test_case['description']}")
        print("-" * 60)
        
        try:
            entities = test_case['entities']
            
            # Resolve metric context
            metric_info = rag._resolve_metric_context(entities, company_context)
            if not metric_info:
                print(f"❌ Could not resolve metric context")
                continue
            
            # Resolve term/period
            term_info = rag._resolve_term_period(entities, company_context, metric_info)
            if not term_info:
                print(f"❌ Could not resolve term/period")
                continue
            
            # Build SQL query
            sql_query = rag._build_enhanced_sql_query(entities, company_context, metric_info, term_info)
            
            print(f"✅ SQL Query generated:")
            print(f"Query: {sql_query[:200]}..." if len(sql_query) > 200 else f"Query: {sql_query}")
            
            # Check for dissection-specific elements
            if "tbl_disectionrawdata" in sql_query:
                print(f"✅ Uses dissection table")
            if "DisectionGroupID" in sql_query:
                print(f"✅ Includes DisectionGroupID filter")
                
        except Exception as e:
            print(f"❌ Error: {e}")

def test_dissection_fallback_mechanisms():
    """
    Test fallback mechanisms for dissection queries
    """
    print("\n" + "="*60)
    print("TESTING DISSECTION FALLBACK MECHANISMS")
    print("="*60)
    
    rag = EnhancedFinancialRAG(
        server='MUHAMMADUSMAN',
        database='MGFinancials'
    )
    
    # Get sample company context
    company_context = rag._resolve_company_context("HBL")
    if not company_context:
        print("❌ Could not resolve company context")
        return
    
    print("\nTest 1: Available dissection metrics")
    print("-" * 40)
    
    try:
        available_metrics = rag._get_available_metrics(company_context)
        dissection_metrics = [m for m in available_metrics if any(group in m for group in ["per share", "% of revenue", "% of assets", "growth"])]
        
        print(f"✅ Found {len(dissection_metrics)} dissection metrics:")
        for metric in dissection_metrics[:10]:  # Show first 10
            print(f"  - {metric}")
        
        if len(dissection_metrics) > 10:
            print(f"  ... and {len(dissection_metrics) - 10} more")
            
    except Exception as e:
        print(f"❌ Error: {e}")
    
    print("\nTest 2: Metric not found fallback")
    print("-" * 40)
    
    try:
        fallback_response = rag._handle_metric_not_found("Invalid Dissection Metric", company_context)
        print(f"✅ Fallback response generated:")
        print(f"Response: {fallback_response[:200]}..." if len(fallback_response) > 200 else f"Response: {fallback_response}")
        
        # Check if dissection metrics are included in suggestions
        if any(group in fallback_response for group in ["per share", "% of revenue", "% of assets", "growth"]):
            print(f"✅ Includes dissection metric suggestions")
        else:
            print(f"⚠️  No dissection metrics in suggestions")
            
    except Exception as e:
        print(f"❌ Error: {e}")

def test_full_dissection_query_processing():
    """
    Test complete dissection query processing pipeline
    """
    print("\n" + "="*60)
    print("TESTING FULL DISSECTION QUERY PROCESSING")
    print("="*60)
    
    rag = EnhancedFinancialRAG(
        server='MUHAMMADUSMAN',
        database='MGFinancials'
    )
    
    test_queries = [
        "What is HBL's latest EPS per share?",
        "Show me OGDC's revenue per share for Q2 2023",
        "What is MCB's net profit as % of revenue for the most recent quarter?",
        "Give me UBL's cash as % of assets for the latest period",
        "What is PSO's revenue annual growth?",
        "Show me ENGRO's quarterly growth in net profit",
        "What is LUCK's invalid dissection metric per share?",  # Should trigger fallback
        "Give me InvalidCompany's EPS per share"  # Should trigger company fallback
    ]
    
    for i, query in enumerate(test_queries, 1):
        print(f"\nTest {i}: {query}")
        print("-" * 60)
        
        try:
            start_time = datetime.now()
            response = rag.process_query(query)
            end_time = datetime.now()
            processing_time = (end_time - start_time).total_seconds()
            
            print(f"Response ({processing_time:.2f}s):")
            print(response)
            
            # Analyze response for dissection-specific elements
            if any(group in response for group in ["per share", "% of revenue", "% of assets", "growth"]):
                print(f"✅ Response includes dissection context")
            
            if "Available metrics include" in response:
                print(f"✅ Fallback mechanism triggered")
                
        except Exception as e:
            print(f"❌ Error processing query: {e}")

def test_dissection_data_validation():
    """
    Test data availability validation for dissection queries
    """
    print("\n" + "="*60)
    print("TESTING DISSECTION DATA VALIDATION")
    print("="*60)
    
    rag = EnhancedFinancialRAG(
        server='MUHAMMADUSMAN',
        database='MGFinancials'
    )
    
    # Test companies
    test_companies = ["HBL", "OGDC", "UBL", "MCB", "PSO"]
    
    for i, company in enumerate(test_companies, 1):
        print(f"\nTest {i}: Validating dissection data for {company}")
        print("-" * 50)
        
        try:
            company_context = rag._resolve_company_context(company)
            if not company_context:
                print(f"❌ Could not resolve company context")
                continue
            
            # Check data availability
            has_data = rag._validate_data_availability(company_context['company_id'])
            
            print(f"Data availability:")
            print(f"  Financial: {has_data.get('financial', False)}")
            print(f"  Ratio: {has_data.get('ratio', False)}")
            print(f"  Dissection: {has_data.get('dissection', False)}")
            
            if has_data.get('dissection', False):
                print(f"✅ Dissection data available")
            else:
                print(f"⚠️  No dissection data available")
                
        except Exception as e:
            print(f"❌ Error: {e}")

def run_all_dissection_tests():
    """
    Run all dissection query tests
    """
    print("\n" + "="*80)
    print("ENHANCED FINANCIAL RAG - DISSECTION QUERIES TEST SUITE")
    print("="*80)
    print(f"Test started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    tests = [
        test_dissection_entity_extraction,
        test_dissection_group_mapping,
        test_dissection_metric_resolution,
        test_dissection_query_building,
        test_dissection_fallback_mechanisms,
        test_full_dissection_query_processing,
        test_dissection_data_validation
    ]
    
    passed = 0
    failed = 0
    
    for test_func in tests:
        try:
            test_func()
            passed += 1
            print(f"\n✅ {test_func.__name__} completed successfully")
        except Exception as e:
            failed += 1
            print(f"\n❌ {test_func.__name__} failed: {e}")
    
    print("\n" + "="*80)
    print("TEST SUMMARY")
    print("="*80)
    print(f"Total tests: {len(tests)}")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    print(f"Success rate: {(passed/len(tests)*100):.1f}%")
    print(f"Test completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    run_all_dissection_tests()