#!/usr/bin/env python3
"""
Comprehensive test script for the Enhanced Financial RAG System
Tests all key capabilities specified in the project scope.
"""

import sys
import os
import json
from datetime import datetime

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from enhanced_financial_rag import EnhancedFinancialRAG
from utils import logger

def test_entity_extraction():
    """
    Test advanced entity extraction capabilities
    """
    print("\n" + "="*60)
    print("TESTING ENTITY EXTRACTION")
    print("="*60)
    
    # Initialize the RAG system
    rag = EnhancedFinancialRAG(
        server='MUHAMMADUSMAN',
        database='MGFinancials'
    )
    
    test_queries = [
        "What is the most recent EPS of HBL?",
        "Show me OGDC's revenue for Q2 2023",
        "What was UBL's ROE in the latest quarter?",
        "Give me MCB's consolidated net profit for 6M 2023",
        "What is the current year-to-date revenue of PSO?",
        "Show me ENGRO's standalone total assets for the most recent period",
        "What was LUCK's debt to equity ratio in Q1 2023?",
        "Give me the latest quarterly EPS for Habib Bank Limited"
    ]
    
    for i, query in enumerate(test_queries, 1):
        print(f"\nTest {i}: {query}")
        print("-" * 50)
        
        try:
            entities = rag._extract_entities(query)
            print(f"Extracted Entities:")
            for key, value in entities.items():
                print(f"  {key}: {value}")
            
            confidence = entities.get('confidence_score', 0)
            print(f"\nConfidence Score: {confidence:.2f}")
            
            if confidence >= 0.5:
                print("✅ Extraction successful")
            else:
                print("⚠️  Low confidence extraction")
                
        except Exception as e:
            print(f"❌ Error: {e}")

def test_company_resolution():
    """
    Test company context resolution with sector/industry mapping
    """
    print("\n" + "="*60)
    print("TESTING COMPANY RESOLUTION")
    print("="*60)
    
    rag = EnhancedFinancialRAG(
        server='MUHAMMADUSMAN',
        database='MGFinancials'
    )
    
    test_companies = [
        "HBL",
        "Habib Bank Limited",
        "OGDC",
        "UBL",
        "MCB",
        "PSO",
        "ENGRO",
        "LUCK",
        "InvalidCompany"
    ]
    
    for i, company in enumerate(test_companies, 1):
        print(f"\nTest {i}: Resolving '{company}'")
        print("-" * 40)
        
        try:
            context = rag._resolve_company_context(company)
            if context:
                print(f"✅ Company found:")
                print(f"  Company ID: {context['company_id']}")
                print(f"  Symbol: {context['symbol']}")
                print(f"  Name: {context['company_name']}")
                print(f"  Sector: {context['sector_name']} (ID: {context['sector_id']})")
                print(f"  Industry: {context['industry_name']} (ID: {context['industry_id']})")
            else:
                print(f"❌ Company not found")
                # Test similar company suggestions
                similar = rag._find_similar_companies(company)
                if similar:
                    print(f"💡 Similar companies: {', '.join(similar[:3])}")
                    
        except Exception as e:
            print(f"❌ Error: {e}")

def test_metric_resolution():
    """
    Test dynamic metric to head ID resolution
    """
    print("\n" + "="*60)
    print("TESTING METRIC RESOLUTION")
    print("="*60)
    
    rag = EnhancedFinancialRAG(
        server='MUHAMMADUSMAN',
        database='MGFinancials'
    )
    
    # Get a sample company context
    company_context = rag._resolve_company_context("HBL")
    if not company_context:
        print("❌ Could not resolve HBL for testing")
        return
    
    test_metrics = [
        "EPS",
        "Earnings Per Share",
        "Revenue",
        "Net Profit",
        "Total Assets",
        "ROE",
        "Return on Equity",
        "Debt to Equity",
        "InvalidMetric"
    ]
    
    for i, metric in enumerate(test_metrics, 1):
        print(f"\nTest {i}: Resolving '{metric}' for {company_context['company_name']}")
        print("-" * 50)
        
        try:
            metric_info = rag._resolve_metric_head_id(metric, company_context)
            if metric_info:
                print(f"✅ Metric found:")
                print(f"  Head ID: {metric_info['head_id']}")
                print(f"  Name: {metric_info['head_name']}")
                print(f"  Data Type: {metric_info['data_type']}")
                print(f"  Source: {metric_info['source_table']}")
                if 'unit_name' in metric_info:
                    print(f"  Unit: {metric_info['unit_name']}")
                if 'formula' in metric_info:
                    print(f"  Formula: {metric_info['formula']}")
            else:
                print(f"❌ Metric not found")
                # Test available metrics
                available = rag._get_available_metrics(company_context)
                if available:
                    print(f"💡 Available metrics: {', '.join(available[:5])}...")
                    
        except Exception as e:
            print(f"❌ Error: {e}")

def test_relative_period_resolution():
    """
    Test relative period query support
    """
    print("\n" + "="*60)
    print("TESTING RELATIVE PERIOD RESOLUTION")
    print("="*60)
    
    rag = EnhancedFinancialRAG(
        server='MUHAMMADUSMAN',
        database='MGFinancials'
    )
    
    # Get sample contexts
    company_context = rag._resolve_company_context("HBL")
    if not company_context:
        print("❌ Could not resolve HBL for testing")
        return
    
    metric_info = rag._resolve_metric_head_id("EPS", company_context)
    if not metric_info:
        print("❌ Could not resolve EPS metric for testing")
        return
    
    test_relative_types = [
        "most_recent_quarter",
        "ytd",
        "most_recent_annual"
    ]
    
    for i, relative_type in enumerate(test_relative_types, 1):
        print(f"\nTest {i}: Resolving '{relative_type}' for {company_context['company_name']} EPS")
        print("-" * 60)
        
        try:
            term_info = rag._resolve_relative_period(relative_type, company_context, metric_info)
            if term_info:
                print(f"✅ Period resolved:")
                print(f"  Type: {term_info['type']}")
                print(f"  Period End: {term_info['period_end']}")
                print(f"  Term ID: {term_info['term_id']}")
                print(f"  Relative Type: {term_info.get('relative_type', 'N/A')}")
            else:
                print(f"❌ Period not resolved")
                
        except Exception as e:
            print(f"❌ Error: {e}")

def test_full_query_processing():
    """
    Test complete query processing pipeline
    """
    print("\n" + "="*60)
    print("TESTING FULL QUERY PROCESSING")
    print("="*60)
    
    rag = EnhancedFinancialRAG(
        server='MUHAMMADUSMAN',
        database='MGFinancials'
    )
    
    test_queries = [
        "What is the most recent EPS of HBL?",
        "Show me OGDC's revenue for the latest quarter",
        "What was UBL's ROE in Q2 2023?",
        "Give me MCB's consolidated net profit for the most recent period",
        "What is the current total assets of PSO?",
        "Show me ENGRO's standalone revenue for the latest quarter",
        "What was LUCK's debt to equity ratio in the most recent quarter?",
        "Give me the latest EPS for an invalid company",
        "What is the invalid metric for HBL?"
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
            print()
            
        except Exception as e:
            print(f"❌ Error processing query: {e}")

def test_fallback_mechanisms():
    """
    Test fallback and disambiguation capabilities
    """
    print("\n" + "="*60)
    print("TESTING FALLBACK MECHANISMS")
    print("="*60)
    
    rag = EnhancedFinancialRAG(
        server='MUHAMMADUSMAN',
        database='MGFinancials'
    )
    
    fallback_queries = [
        "What is the EPS?",  # Missing company
        "Show me HBL's data",  # Missing metric
        "What is HBL's EPS?",  # Missing term
        "Give me data for XYZ company",  # Invalid company
        "What is HBL's invalid metric?",  # Invalid metric
        "Show me HBL's EPS for invalid period",  # Invalid period
        "incomplete query",  # Very incomplete
        "HBL",  # Just company name
        "EPS latest",  # Missing company
    ]
    
    for i, query in enumerate(fallback_queries, 1):
        print(f"\nFallback Test {i}: {query}")
        print("-" * 50)
        
        try:
            response = rag.process_query(query)
            print(f"Response: {response}")
            
            # Check if response contains helpful suggestions
            if any(keyword in response.lower() for keyword in ['suggest', 'available', 'did you mean', 'please specify']):
                print("✅ Helpful fallback response provided")
            else:
                print("⚠️  Basic response provided")
                
        except Exception as e:
            print(f"❌ Error: {e}")

def test_rag_integration():
    """
    Test integration with FinRAG server format
    """
    print("\n" + "="*60)
    print("TESTING RAG INTEGRATION")
    print("="*60)
    
    rag = EnhancedFinancialRAG(
        server='MUHAMMADUSMAN',
        database='MGFinancials'
    )
    
    # Test messages format
    test_messages = [
        [
            {"role": "user", "content": "What is the most recent EPS of HBL?"}
        ],
        [
            {"role": "user", "content": "Hello"},
            {"role": "assistant", "content": "Hi! How can I help you?"},
            {"role": "user", "content": "Show me OGDC's revenue for the latest quarter"}
        ]
    ]
    
    init_inputs = {"session_id": "test_session"}
    
    for i, messages in enumerate(test_messages, 1):
        print(f"\nIntegration Test {i}:")
        print(f"Messages: {json.dumps(messages, indent=2)}")
        print("-" * 50)
        
        try:
            response, retrieval_results = rag.get_rag_result(init_inputs, messages)
            
            print(f"Response: {response}")
            print(f"\nRetrieval Results:")
            for result in retrieval_results:
                print(f"  Query: {result['query']}")
                print(f"  Source: {result['source']}")
                print(f"  Method: {result['processing_method']}")
                print(f"  Timestamp: {result['timestamp']}")
                
        except Exception as e:
            print(f"❌ Error: {e}")

def run_all_tests():
    """
    Run all test suites
    """
    print("🚀 STARTING ENHANCED FINANCIAL RAG SYSTEM TESTS")
    print("=" * 80)
    
    try:
        test_entity_extraction()
        test_company_resolution()
        test_metric_resolution()
        test_relative_period_resolution()
        test_full_query_processing()
        test_fallback_mechanisms()
        test_rag_integration()
        
        print("\n" + "="*80)
        print("🎉 ALL TESTS COMPLETED")
        print("="*80)
        
    except Exception as e:
        print(f"\n❌ CRITICAL ERROR DURING TESTING: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    run_all_tests()