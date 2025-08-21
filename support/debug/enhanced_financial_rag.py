#!/usr/bin/env python3
"""
Enhanced Financial RAG System
Implements the comprehensive RAG system as specified in the project scope.

Key Features:
- Advanced entity extraction with sector/industry context
- Dynamic head ID resolution with fallback mechanisms
- Relative period query support
- Multi-table joins with intelligent data source selection
- Comprehensive fallback and disambiguation
"""

import os
import re
import json
from typing import Dict, List, Optional, Tuple, Any, Union
from datetime import datetime, timedelta
import sys
import os

# Add the project root to the path so we can import utils
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from utils import logger

from app.core.database.financial_db import FinancialDatabase
from app.core.chat.mistral_chat import MistralChat

class EnhancedFinancialRAG:
    def __init__(self, server: str, database: str, model_path: str = "Mistral-7B-Instruct-v0.1.Q4_K_M.gguf"):
        """
        Initialize the Enhanced Financial RAG system
        
        Args:
            server: SQL Server name
            database: Database name
            model_path: Path to the Mistral model file
        """
        self.db = FinancialDatabase(server, database)
        self.mistral = MistralChat(model_path)
        
        # Load metadata on initialization
        self.db.load_metadata()
        
        # Cache for frequently accessed data
        self._company_cache = {}
        self._metric_cache = {}
        self._sector_industry_cache = {}
        
        logger.info("Enhanced Financial RAG system initialized")
    
    def _extract_entities(self, query: str) -> Dict[str, Any]:
        """
        Advanced entity extraction with improved accuracy and context awareness
        
        Args:
            query: Natural language query
            
        Returns:
            Dictionary with extracted entities including:
            - company: Company name or ticker
            - metric: Financial metric name
            - term: Term description (e.g., 'Q2 2023', '3M', '6M', etc.)
            - consolidation: 'Consolidated' or 'Unconsolidated'
            - period_end: Optional specific period end date
            - is_relative_term: Boolean flag for relative terms
            - relative_type: Type of relative period
            - confidence_score: Confidence in extraction accuracy
        """
        # Enhanced extraction prompt with better context
        extraction_prompt = [
            {
                "role": "system",
                "content": """You are an expert financial entity extraction assistant. Extract the following information from financial queries:
                
                1. Company: Company name, ticker symbol, or stock code
                2. Metric: Financial metric (e.g., EPS, Revenue, ROE, Total Assets, Net Profit)
                3. Term: Time period (e.g., Q1 2023, 3M, 6M, latest, most recent)
                4. Consolidation: Consolidated or Unconsolidated/Standalone
                
                Respond ONLY in valid JSON format with these exact keys: company, metric, term, consolidation
                Use empty string "" for missing values, not null or N/A.
                
                Examples:
                {"company": "HBL", "metric": "EPS", "term": "Q2 2023", "consolidation": "Unconsolidated"}
                {"company": "OGDC", "metric": "Revenue", "term": "latest", "consolidation": "Consolidated"}
                """
            },
            {
                "role": "user",
                "content": query
            }
        ]
        
        # Get response from Mistral
        response = self.mistral.chat(extraction_prompt)
        
        # Parse JSON response with error handling
        entities = {}
        try:
            # Clean the response to extract JSON
            json_match = re.search(r'\{.*\}', response, re.DOTALL)
            if json_match:
                json_str = json_match.group(0)
                entities = json.loads(json_str)
            else:
                # Fallback to regex parsing
                entities = self._fallback_entity_extraction(response, query)
        except json.JSONDecodeError:
            logger.warning(f"Failed to parse JSON response: {response}")
            entities = self._fallback_entity_extraction(response, query)
        
        # Post-process and validate entities
        entities = self._post_process_entities(entities, query)
        
        return entities
    
    def _fallback_entity_extraction(self, response: str, query: str) -> Dict[str, Any]:
        """
        Fallback entity extraction using regex patterns
        """
        entities = {"company": "", "metric": "", "term": "", "consolidation": ""}
        
        # Extract company using common patterns
        company_patterns = [
            r'\b([A-Z]{2,5})\b',  # Ticker symbols
            r'\b([A-Z][a-z]+ [A-Z][a-z]+)\b',  # Company names
            r'\b(HBL|UBL|MCB|ABL|OGDC|PSO|ENGRO|LUCK|DGKC|FCCL)\b'  # Known tickers
        ]
        
        for pattern in company_patterns:
            match = re.search(pattern, query, re.IGNORECASE)
            if match:
                entities["company"] = match.group(1)
                break
        
        # Extract metric using financial terms
        metric_patterns = [
            r'\b(EPS|earnings per share)\b',
            r'\b(revenue|sales|turnover)\b',
            r'\b(net profit|net income|profit after tax|PAT)\b',
            r'\b(total assets|assets)\b',
            r'\b(ROE|return on equity)\b',
            r'\b(ROA|return on assets)\b'
        ]
        
        for pattern in metric_patterns:
            match = re.search(pattern, query, re.IGNORECASE)
            if match:
                entities["metric"] = match.group(1)
                break
        
        # Extract term patterns
        term_patterns = [
            r'\b(Q[1-4]\s*20\d{2})\b',
            r'\b(FY\s*20\d{2})\b',
            r'\b([36912]M)\b',
            r'\b(latest|most recent|last|current)\b'
        ]
        
        for pattern in term_patterns:
            match = re.search(pattern, query, re.IGNORECASE)
            if match:
                entities["term"] = match.group(1)
                break
        
        # Extract consolidation
        if re.search(r'\b(consolidated|consolidate)\b', query, re.IGNORECASE):
            entities["consolidation"] = "Consolidated"
        elif re.search(r'\b(standalone|unconsolidated|separate)\b', query, re.IGNORECASE):
            entities["consolidation"] = "Unconsolidated"
        else:
            entities["consolidation"] = "Unconsolidated"  # Default
        
        return entities
    
    def _post_process_entities(self, entities: Dict[str, Any], query: str) -> Dict[str, Any]:
        """
        Post-process and enhance extracted entities
        """
        logger.debug(f"Post-processing entities: {entities}")
        logger.debug(f"Original query: {query}")
        
        # Clean and standardize values
        for key in ["company", "metric", "term", "consolidation"]:
            if key in entities:
                value = str(entities[key]).strip()
                if value.lower() in ["null", "n/a", "none", ""]:
                    entities[key] = ""
                else:
                    entities[key] = value
            else:
                entities[key] = ""
        
        logger.debug(f"Cleaned entities: {entities}")
        
        # Detect relative terms
        query_lower = query.lower()
        relative_keywords = ['latest', 'most recent', 'last', 'current', 'ytd', 'year-to-date']
        
        entities["is_relative_term"] = any(keyword in query_lower for keyword in relative_keywords)
        
        if entities["is_relative_term"]:
            if 'ytd' in query_lower or 'year-to-date' in query_lower:
                entities["relative_type"] = "ytd"
            elif 'quarter' in query_lower:
                entities["relative_type"] = "most_recent_quarter"
            elif 'annual' in query_lower or 'year' in query_lower:
                entities["relative_type"] = "most_recent_annual"
            logger.debug(f"Detected relative term: {entities.get('relative_type')}")
        
        # Detect dissection group indicators
        dissection_keywords = {
            'per share': 1,
            'per-share': 1,
            'annual growth': 2,
            'yoy': 2,
            'year over year': 2,
            'quarterly growth': 3,
            'qoq': 3,
            'quarter over quarter': 3,
            '% of assets': 4,
            'percentage of assets': 4,
            '% of sales': 5,
            '% of revenue': 5,
            'percentage of sales': 5,
            'percentage of revenue': 5
        }
        
        entities["is_dissection"] = False
        entities["dissection_group"] = None
        entities["dissection_group_id"] = None
        
        logger.debug(f"Checking for dissection indicators in query: {query_lower}")
        
        for keyword, group_id in dissection_keywords.items():
            if keyword in query_lower:
                entities["is_dissection"] = True
                if keyword in ['per share', 'per-share']:
                    entities["dissection_group"] = "Per Share"
                elif keyword in ['annual growth', 'yoy', 'year over year']:
                    entities["dissection_group"] = "Annual Growth"
                elif keyword in ['quarterly growth', 'qoq', 'quarter over quarter']:
                    entities["dissection_group"] = "Quarterly Growth"
                elif keyword in ['% of assets', 'percentage of assets']:
                    entities["dissection_group"] = "% of Assets"
                elif keyword in ['% of sales', '% of revenue', 'percentage of sales', 'percentage of revenue']:
                    entities["dissection_group"] = "% of Sales/Revenue"
                entities["dissection_group_id"] = group_id
                logger.info(f"Detected dissection group: '{entities['dissection_group']}' (ID: {group_id}) from keyword: '{keyword}'")
                break
            else:
                entities["relative_type"] = "most_recent_quarter"
        
        # Handle date formats
        term = entities.get("term", "")
        if re.match(r'\d{1,2}-\d{1,2}-\d{4}', term):
            # Convert DD-MM-YYYY to YYYY-MM-DD
            parts = term.split('-')
            entities["period_end"] = f"{parts[2]}-{parts[1].zfill(2)}-{parts[0].zfill(2)}"
            entities["term"] = "3M"  # Default term for specific dates
        
        # Calculate confidence score
        confidence = 0.0
        if entities["company"]: confidence += 0.3
        if entities["metric"]: confidence += 0.4
        if entities["term"]: confidence += 0.2
        if entities["consolidation"]: confidence += 0.1
        
        entities["confidence_score"] = confidence
        
        return entities
    
    def _resolve_company_context(self, company_name: str) -> Optional[Dict[str, Any]]:
        """
        Resolve company context including sector and industry information
        
        Args:
            company_name: Company name or ticker
            
        Returns:
            Dictionary with company context or None if not found
        """
        if company_name in self._company_cache:
            return self._company_cache[company_name]
        
        # Get company ID
        company_id = self.db.get_company_id(company_name)
        if company_id is None:
            return None
        
        # Get sector and industry information
        context_query = """
        SELECT 
            c.CompanyID, c.Symbol, c.CompanyName, c.SectorID,
            s.SectorName,
            i.IndustryID, i.IndustryName
        FROM tbl_companieslist c
        LEFT JOIN tbl_sectornames s ON c.SectorID = s.SectorID
        LEFT JOIN tbl_industryandsectormapping m ON s.SectorID = m.sectorid
        LEFT JOIN tbl_industrynames i ON m.industryid = i.IndustryID
        WHERE c.CompanyID = ?
        """
        
        try:
            result = self.db.execute_query(context_query.replace('?', str(company_id)))
            if not result.empty:
                context = {
                    'company_id': result.iloc[0]['CompanyID'],
                    'symbol': result.iloc[0]['Symbol'],
                    'company_name': result.iloc[0]['CompanyName'],
                    'sector_id': result.iloc[0]['SectorID'],
                    'sector_name': result.iloc[0]['SectorName'],
                    'industry_id': result.iloc[0].get('IndustryID'),
                    'industry_name': result.iloc[0].get('IndustryName')
                }
                
                self._company_cache[company_name] = context
                return context
        except Exception as e:
            logger.error(f"Error resolving company context: {e}")
        
        return None
    
    def _resolve_metric_head_id(self, metric_name: str, company_context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Dynamically resolve metric to appropriate SubHeadID or RatioHeadID
        with sector/industry context and data availability validation
        
        Args:
            metric_name: Name of the financial metric
            company_context: Company context with sector/industry info
            
        Returns:
            Dictionary with head_id, data_type, and metadata or None if not found
        """
        logger.info(f"Resolving metric head ID for: '{metric_name}'")
        logger.debug(f"Company context for metric resolution: {company_context}")
        
        cache_key = f"{metric_name}_{company_context.get('industry_id', 'none')}"
        if cache_key in self._metric_cache:
            logger.debug(f"Found cached result for metric: {metric_name}")
            return self._metric_cache[cache_key]
        
        # If dissection group is specified, prioritize searching in dissection data
        if company_context.get('dissection_group_id'):
            logger.info(f"Dissection group specified ({company_context['dissection_group_id']}), prioritizing dissection data search")
            # Search in dissection data first
            dissection_result = self._search_in_dissection_master(metric_name, company_context)
            if dissection_result:
                logger.info(f"Found metric in dissection data: {dissection_result['head_name']}")
                self._metric_cache[cache_key] = dissection_result
                return dissection_result
            else:
                logger.debug(f"No dissection data found for metric with specified group ID")
        
        # Search in non-ratio metrics first
        logger.debug(f"Searching in heads master for: {metric_name}")
        head_result = self._search_in_heads_master(metric_name, company_context)
        if head_result:
            logger.info(f"Found metric in heads master: {head_result['head_name']}")
            self._metric_cache[cache_key] = head_result
            return head_result
        
        # Search in ratio metrics
        logger.debug(f"Searching in ratios master for: {metric_name}")
        ratio_result = self._search_in_ratios_master(metric_name, company_context)
        if ratio_result:
            logger.info(f"Found metric in ratios master: {ratio_result['head_name']}")
            self._metric_cache[cache_key] = ratio_result
            return ratio_result
        
        # If not already searched, search in dissection data
        if not company_context.get('dissection_group_id'):
            logger.debug(f"No specific dissection group, searching all dissection data for: {metric_name}")
            dissection_result = self._search_in_dissection_master(metric_name, company_context)
            if dissection_result:
                logger.info(f"Found metric in dissection data: {dissection_result['head_name']}")
                self._metric_cache[cache_key] = dissection_result
                return dissection_result
        
        logger.warning(f"No metric found in any data source for: {metric_name}")
        return None
    
    def _search_in_heads_master(self, metric_name: str, company_context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Search for metric in tbl_headsmaster with industry context
        """
        industry_filter = ""
        if company_context.get('industry_id'):
            industry_filter = f"AND h.IndustryID = {company_context['industry_id']}"
        
        search_query = f"""
        SELECT h.SubHeadID, h.SubHeadName, h.UnitID, u.unitname as UnitName
        FROM tbl_headsmaster h
        LEFT JOIN tbl_unitofmeasurement u ON h.UnitID = u.UnitID
        WHERE (
            h.SubHeadName LIKE '%{metric_name}%'
            OR h.SubHeadName LIKE '%{metric_name.replace(' ', '%')}%'
        )
        {industry_filter}
        ORDER BY 
            CASE WHEN h.SubHeadName = '{metric_name}' THEN 1 ELSE 2 END,
            LEN(h.SubHeadName)
        """
        
        try:
            result = self.db.execute_query(search_query)
            if not result.empty:
                # Validate data availability
                for _, row in result.iterrows():
                    if self._validate_data_availability(company_context['company_id'], row['SubHeadID'], 'financial'):
                        return {
                            'head_id': row['SubHeadID'],
                            'head_name': row['SubHeadName'],
                            'data_type': 'financial',
                            'unit_id': row['UnitID'],
                            'unit_name': row['UnitName'],
                            'source_table': 'tbl_headsmaster'
                        }
        except Exception as e:
            logger.error(f"Error searching heads master: {e}")
        
        return None
    
    def _search_in_ratios_master(self, metric_name: str, company_context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Search for metric in tbl_ratiosheadmaster
        """
        search_query = f"""
        SELECT r.SubHeadID, r.RatioName, r.Formula
        FROM tbl_ratiosheadmaster r
        WHERE (
            r.RatioName LIKE '%{metric_name}%'
            OR r.RatioName LIKE '%{metric_name.replace(' ', '%')}%'
        )
        ORDER BY 
            CASE WHEN r.RatioName = '{metric_name}' THEN 1 ELSE 2 END,
            LEN(r.RatioName)
        """
        
        try:
            result = self.db.execute_query(search_query)
            if not result.empty:
                # Validate data availability
                for _, row in result.iterrows():
                    if self._validate_data_availability(company_context['company_id'], row['SubHeadID'], 'ratio'):
                        return {
                            'head_id': row['SubHeadID'],
                            'head_name': row['RatioName'],
                            'data_type': 'ratio',
                            'formula': row['Formula'],
                            'source_table': 'tbl_ratiosheadmaster'
                        }
        except Exception as e:
            logger.error(f"Error searching ratios master: {e}")
        
        return None
    
    def _search_in_dissection_master(self, metric_name: str, company_context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Search for metric in dissection data
        """
        logger.info(f"Searching dissection master for metric: '{metric_name}'")
        logger.debug(f"Company context: {company_context}")
        
        # Load dissection master if not in cache
        if 'dissection' not in self.metadata_cache:
            logger.debug("Loading dissection master into cache")
            self.metadata_cache['dissection'] = self.db.execute_query(
                "SELECT * FROM tbl_disectionmaster"
            )
        
        # Check if we have a dissection group ID from entity extraction
        dissection_group_id = company_context.get('dissection_group_id')
        logger.debug(f"Dissection group ID from context: {dissection_group_id}")
        
        # First, find the metric in tbl_headsmaster
        industry_filter = ""
        if company_context.get('industry_id'):
            industry_filter = f"AND h.IndustryID = {company_context['industry_id']}"
            logger.debug(f"Using industry filter: {industry_filter}")
        
        search_query = f"""
        SELECT h.SubHeadID, h.SubHeadName, h.UnitID, u.unitname as UnitName
        FROM tbl_headsmaster h
        LEFT JOIN tbl_unitofmeasurement u ON h.UnitID = u.UnitID
        WHERE (
            h.SubHeadName LIKE '%{metric_name}%'
            OR h.SubHeadName LIKE '%{metric_name.replace(' ', '%')}%'
        )
        {industry_filter}
        ORDER BY 
            CASE WHEN h.SubHeadName = '{metric_name}' THEN 1 ELSE 2 END,
            LEN(h.SubHeadName)
        """
        
        logger.debug(f"Executing heads master search query: {search_query}")
        
        try:
            result = self.db.execute_query(search_query)
            if not result.empty:
                logger.debug(f"Found {len(result)} potential metric matches in heads master")
                
                # For each potential head, check if it exists in dissection data
                for idx, row in result.iterrows():
                    subhead_id = row['SubHeadID']
                    subhead_name = row['SubHeadName']
                    logger.debug(f"Checking dissection data for SubHeadID: {subhead_id} ({subhead_name})")
                    
                    # If we have a specific dissection group ID from entity extraction, use it
                    if dissection_group_id:
                        logger.debug(f"Validating data availability with specific group ID: {dissection_group_id}")
                        # Check if this head has dissection data for this company with the specific group ID
                        if self._validate_data_availability(
                            company_context['company_id'], 
                            subhead_id, 
                            'dissection',
                            dissection_group_id
                        ):
                            logger.info(f"Found dissection data for '{subhead_name}' with group ID {dissection_group_id}")
                            return {
                                'head_id': subhead_id,
                                'head_name': subhead_name,
                                'data_type': 'dissection',
                                'unit_id': row['UnitID'],
                                'unit_name': row['UnitName'],
                                'source_table': 'tbl_headsmaster',
                                'dissection_group_id': dissection_group_id
                            }
                        else:
                            logger.debug(f"No dissection data found for SubHeadID {subhead_id} with group ID {dissection_group_id}")
                    else:
                        logger.debug(f"No specific group ID provided, checking for any dissection data")
                        # Check if this head has any dissection data for this company
                        dissection_query = f"""
                        SELECT DISTINCT DisectionGroupID
                        FROM tbl_disectionrawdata
                        WHERE CompanyID = {company_context['company_id']}
                        AND SubHeadID = {subhead_id}
                        """
                        
                        logger.debug(f"Executing dissection availability query: {dissection_query}")
                        dissection_result = self.db.execute_query(dissection_query)
                        if not dissection_result.empty:
                            # Found dissection data for this head
                            found_dissection_group_id = dissection_result.iloc[0]['DisectionGroupID']
                            logger.info(f"Found dissection data for '{subhead_name}' with group ID {found_dissection_group_id}")
                            
                            return {
                                'head_id': subhead_id,
                                'head_name': subhead_name,
                                'data_type': 'dissection',
                                'unit_id': row['UnitID'],
                                'unit_name': row['UnitName'],
                                'source_table': 'tbl_headsmaster',
                                'dissection_group_id': found_dissection_group_id
                            }
                        else:
                            logger.debug(f"No dissection data found for SubHeadID {subhead_id}")
            else:
                logger.warning(f"No metric matches found in heads master for: {metric_name}")
        except Exception as e:
            logger.error(f"Error searching dissection data: {e}")
        
        logger.warning(f"No dissection data found for metric: {metric_name}")
        return None
    
    def _validate_data_availability(self, company_id: int, head_id: int, data_type: str, dissection_group_id: int = None) -> bool:
        """
        Validate that data exists for the given company and metric
        
        Args:
            company_id: Company ID
            head_id: Head ID or Ratio Head ID
            data_type: Type of data ('financial', 'ratio', 'dissection')
            dissection_group_id: Optional DisectionGroupID for dissection data
            
        Returns:
            True if data exists, False otherwise
        """
        logger.debug(f"Validating data availability - Company: {company_id}, Head: {head_id}, Type: {data_type}, Group: {dissection_group_id}")
        
        table_map = {
            'financial': 'tbl_financialrawdata_Quarter',
            'ratio': 'tbl_ratiorawdata',
            'dissection': 'tbl_disectionrawdata'
        }
        
        table_name = table_map.get(data_type)
        if not table_name:
            logger.error(f"Unknown data type: {data_type}")
            return False
        
        check_query = f"""
        SELECT TOP 1 1
        FROM {table_name}
        WHERE CompanyID = {company_id} AND SubHeadID = {head_id}
        """
        
        # Add DisectionGroupID filter for dissection data
        if data_type == 'dissection' and dissection_group_id is not None:
            check_query += f" AND DisectionGroupID = {dissection_group_id}"
            logger.debug(f"Added dissection group filter: {dissection_group_id}")
        
        logger.debug(f"Data availability query: {check_query}")
        
        try:
            result = self.db.execute_query(check_query)
            data_exists = not result.empty
            logger.debug(f"Data availability result: {data_exists}")
            return data_exists
        except Exception as e:
            logger.error(f"Error validating data availability: {e}")
            return False
    
    def process_query(self, query: str) -> str:
        """
        Process a natural language financial query with enhanced capabilities
        
        Args:
            query: Natural language query
            
        Returns:
            Comprehensive response with financial information
        """
        try:
            logger.info(f"Processing query: {query}")
            
            # Step 1: Extract entities
            entities = self._extract_entities(query)
            logger.info(f"Extracted entities: {entities}")
            
            # Check confidence score
            if entities.get("confidence_score", 0) < 0.5:
                return self._handle_low_confidence_extraction(query, entities)
            
            # Step 2: Resolve company context
            company_context = self._resolve_company_context(entities["company"])
            if not company_context:
                return self._handle_company_not_found(entities["company"])
            
            # Add dissection group ID to company context if present
            if entities.get("is_dissection") and entities.get("dissection_group_id"):
                company_context["dissection_group_id"] = entities["dissection_group_id"]
                company_context["dissection_group"] = entities["dissection_group"]
                logger.info(f"Added dissection context - Group: {entities['dissection_group']} (ID: {entities['dissection_group_id']})")
            
            # Step 3: Resolve metric to head ID
            logger.info(f"Resolving metric: {entities['metric']}")
            metric_info = self._resolve_metric_head_id(entities["metric"], company_context)
            if not metric_info:
                logger.warning(f"Metric not found: {entities['metric']}")
                return self._handle_metric_not_found(entities["metric"], company_context)
            
            logger.info(f"Resolved metric to: {metric_info['head_name']} (ID: {metric_info['head_id']}, Type: {metric_info['data_type']})")
            if metric_info.get('dissection_group_id'):
                logger.info(f"Using dissection group ID: {metric_info['dissection_group_id']}")
            
            # Step 4: Resolve term/period
            term_info = self._resolve_term_period(entities, company_context, metric_info)
            if not term_info:
                return self._handle_term_not_found(entities["term"])
            
            # Step 5: Build and execute SQL query
            sql_query = self._build_enhanced_sql_query(
                company_context, metric_info, term_info, entities
            )
            
            # Step 6: Execute query and get results
            financial_data = self.db.execute_query(sql_query)
            
            if financial_data.empty:
                return self._handle_no_data_found(entities, company_context, metric_info)
            
            # Step 7: Generate response
            response = self._generate_enhanced_response(financial_data, entities, company_context, metric_info)
            
            return response
            
        except Exception as e:
            logger.error(f"Error processing query: {e}")
            return f"I encountered an error while processing your query: {str(e)}"
    
    def _handle_low_confidence_extraction(self, query: str, entities: Dict[str, Any]) -> str:
        """
        Handle cases where entity extraction confidence is low
        """
        missing_entities = [k for k, v in entities.items() if not v and k in ["company", "metric", "term"]]
        
        suggestions = []
        if "company" in missing_entities:
            suggestions.append("Please specify a company name or ticker symbol")
        if "metric" in missing_entities:
            suggestions.append("Please specify a financial metric (e.g., EPS, Revenue, ROE)")
        if "term" in missing_entities:
            suggestions.append("Please specify a time period (e.g., Q1 2023, latest quarter)")
        
        return f"I need more information to answer your query. {' and '.join(suggestions)}."
    
    def _handle_company_not_found(self, company_name: str) -> str:
        """
        Handle cases where company is not found
        """
        # Suggest similar companies
        similar_companies = self._find_similar_companies(company_name)
        if similar_companies:
            suggestions = ", ".join(similar_companies[:5])
            return f"I couldn't find '{company_name}'. Did you mean one of these: {suggestions}?"
        else:
            return f"I couldn't find the company '{company_name}' in our database. Please check the company name or ticker symbol."
    
    def _find_similar_companies(self, company_name: str) -> List[str]:
        """
        Find companies with similar names
        """
        try:
            search_query = f"""
            SELECT TOP 10 Symbol, CompanyName
            FROM tbl_companieslist
            WHERE Symbol LIKE '%{company_name}%' OR CompanyName LIKE '%{company_name}%'
            ORDER BY LEN(Symbol), LEN(CompanyName)
            """
            
            result = self.db.execute_query(search_query)
            return [f"{row['Symbol']} ({row['CompanyName']})" for _, row in result.iterrows()]
        except Exception as e:
            logger.error(f"Error finding similar companies: {e}")
            return []
    
    def _handle_metric_not_found(self, metric_name: str, company_context: Dict[str, Any]) -> str:
        """
        Handle cases where metric is not found
        """
        # Suggest available metrics for this company/industry
        available_metrics = self._get_available_metrics(company_context)
        if available_metrics:
            suggestions = ", ".join(available_metrics[:10])
            return f"I couldn't find the metric '{metric_name}' for {company_context['company_name']}. Available metrics include: {suggestions}"
        else:
            return f"I couldn't find the metric '{metric_name}' in our database."
    
    def _get_available_metrics(self, company_context: Dict[str, Any]) -> List[str]:
        """
        Get available metrics for a company based on actual data
        """
        try:
            # Get metrics from financial data
            financial_query = f"""
            SELECT DISTINCT h.SubHeadName
            FROM tbl_financialrawdata_Quarter f
            JOIN tbl_headsmaster h ON f.SubHeadID = h.SubHeadID
            WHERE f.CompanyID = {company_context['company_id']}
            ORDER BY h.SubHeadName
            """
            
            # Get metrics from ratio data
            ratio_query = f"""
            SELECT DISTINCT r.RatioName
            FROM tbl_ratiorawdata rd
            JOIN tbl_ratiosheadmaster r ON rd.SubHeadID = r.SubHeadID
            WHERE rd.CompanyID = {company_context['company_id']}
            ORDER BY r.RatioName
            """
            
            # Get metrics from dissection data
            dissection_query = f"""
            SELECT DISTINCT h.SubHeadName, d.DisectionGroupID
            FROM tbl_disectionrawdata d
            JOIN tbl_headsmaster h ON d.SubHeadID = h.SubHeadID
            WHERE d.CompanyID = {company_context['company_id']}
            ORDER BY h.SubHeadName, d.DisectionGroupID
            """
            
            financial_result = self.db.execute_query(financial_query)
            ratio_result = self.db.execute_query(ratio_query)
            dissection_result = self.db.execute_query(dissection_query)
            
            metrics = []
            if not financial_result.empty:
                metrics.extend(financial_result['SubHeadName'].tolist())
            if not ratio_result.empty:
                metrics.extend(ratio_result['RatioName'].tolist())
            
            # Add dissection metrics with their group type
            if not dissection_result.empty:
                # Get the dissection group names for better readability
                group_names = {
                    1: "per share",
                    2: "% of revenue",
                    3: "% of assets",
                    4: "annual growth",
                    5: "quarterly growth"
                }
                
                for _, row in dissection_result.iterrows():
                    group_id = row['DisectionGroupID']
                    group_name = group_names.get(group_id, f"group {group_id}")
                    metrics.append(f"{row['SubHeadName']} ({group_name})")
            
            return sorted(list(set(metrics)))
            
        except Exception as e:
            logger.error(f"Error getting available metrics: {e}")
            return []
    
    def _resolve_term_period(self, entities: Dict[str, Any], company_context: Dict[str, Any], metric_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Resolve term/period with relative term support
        """
        term = entities.get("term", "")
        is_relative = entities.get("is_relative_term", False)
        relative_type = entities.get("relative_type")
        period_end = entities.get("period_end")
        
        if period_end:
            # Specific date provided
            return {
                'type': 'specific_date',
                'period_end': period_end,
                'term_id': None
            }
        
        if is_relative:
            # Handle relative terms
            return self._resolve_relative_period(relative_type, company_context, metric_info)
        
        # Handle specific terms
        term_id = self.db.get_term_id(term)
        if term_id:
            return {
                'type': 'specific_term',
                'term_id': term_id,
                'period_end': None
            }
        
        return None
    
    def _resolve_relative_period(self, relative_type: str, company_context: Dict[str, Any], metric_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Resolve relative periods like 'latest', 'most recent quarter', etc.
        """
        data_type = metric_info['data_type']
        table_map = {
            'financial': 'tbl_financialrawdata_Quarter',
            'ratio': 'tbl_ratiorawdata',
            'dissection': 'tbl_disectionrawdata'
        }
        
        table_name = table_map.get(data_type)
        if not table_name:
            return None
        
        try:
            # Add DisectionGroupID filter for dissection data
            dissection_filter = ""
            if data_type == 'dissection' and metric_info.get('dissection_group_id'):
                dissection_filter = f"AND DisectionGroupID = {metric_info['dissection_group_id']}"
            
            if relative_type == "most_recent_quarter":
                query = f"""
                SELECT TOP 1 PeriodEnd, TermID
                FROM {table_name}
                WHERE CompanyID = {company_context['company_id']} 
                  AND SubHeadID = {metric_info['head_id']}
                  {dissection_filter}
                ORDER BY PeriodEnd DESC
                """
            elif relative_type == "ytd":
                current_year = datetime.now().year
                query = f"""
                SELECT TOP 1 PeriodEnd, TermID
                FROM {table_name}
                WHERE CompanyID = {company_context['company_id']} 
                  AND SubHeadID = {metric_info['head_id']}
                  AND YEAR(PeriodEnd) = {current_year}
                  {dissection_filter}
                ORDER BY PeriodEnd DESC
                """
            else:
                # Default to most recent
                query = f"""
                SELECT TOP 1 PeriodEnd, TermID
                FROM {table_name}
                WHERE CompanyID = {company_context['company_id']} 
                  AND SubHeadID = {metric_info['head_id']}
                  {dissection_filter}
                ORDER BY PeriodEnd DESC
                """
            
            result = self.db.execute_query(query)
            if not result.empty:
                return {
                    'type': 'relative_resolved',
                    'period_end': result.iloc[0]['PeriodEnd'],
                    'term_id': result.iloc[0]['TermID'],
                    'relative_type': relative_type
                }
        
        except Exception as e:
            logger.error(f"Error resolving relative period: {e}")
        
        return None
    
    def _build_enhanced_sql_query(self, company_context: Dict[str, Any], metric_info: Dict[str, Any], 
                                 term_info: Dict[str, Any], entities: Dict[str, Any]) -> str:
        """
        Build comprehensive SQL query with proper joins and filters
        """
        data_type = metric_info['data_type']
        consolidation = entities.get('consolidation', 'Unconsolidated')
        
        # Get consolidation ID
        consolidation_id = self.db.get_consolidation_id(consolidation)
        
        if data_type == 'financial':
            return self._build_financial_query(company_context, metric_info, term_info, consolidation_id)
        elif data_type == 'ratio':
            return self._build_ratio_query(company_context, metric_info, term_info, consolidation_id)
        elif data_type == 'dissection':
            return self._build_dissection_query(company_context, metric_info, term_info, consolidation_id)
        
        return ""
    
    def _build_financial_query(self, company_context: Dict[str, Any], metric_info: Dict[str, Any], 
                              term_info: Dict[str, Any], consolidation_id: int) -> str:
        """
        Build SQL query for financial data
        """
        base_query = f"""
        SELECT 
            c.CompanyName,
            c.Symbol,
            h.SubHeadName as MetricName,
            f.Value_ as Value,
            f.PeriodEnd,
            t.Term,
            u.unitname as Unit,
            con.consolidationname as Consolidation,
            s.SectorName,
            ind.IndustryName
        FROM tbl_financialrawdata_Quarter f
        JOIN tbl_companieslist c ON f.CompanyID = c.CompanyID
        JOIN tbl_headsmaster h ON f.SubHeadID = h.SubHeadID
        LEFT JOIN tbl_terms t ON f.TermID = t.TermID
        LEFT JOIN tbl_unitofmeasurement u ON h.UnitID = u.UnitID
        LEFT JOIN tbl_consolidation con ON f.ConsolidationID = con.ConsolidationID
        LEFT JOIN tbl_sectornames s ON c.SectorID = s.SectorID
        LEFT JOIN tbl_industryandsectormapping m ON s.SectorID = m.sectorid
        LEFT JOIN tbl_industrynames ind ON m.industryid = ind.IndustryID
        WHERE f.CompanyID = {company_context['company_id']}
          AND f.SubHeadID = {metric_info['head_id']}
        """
        
        # Add term/period filters
        if term_info['type'] == 'specific_date':
            base_query += f" AND f.PeriodEnd = '{term_info['period_end']}'"
        elif term_info['type'] == 'specific_term':
            base_query += f" AND f.TermID = {term_info['term_id']}"
        elif term_info['type'] == 'relative_resolved':
            base_query += f" AND f.PeriodEnd = '{term_info['period_end']}'"
        
        # Add consolidation filter
        if consolidation_id:
            base_query += f" AND f.ConsolidationID = {consolidation_id}"
        
        base_query += " ORDER BY f.PeriodEnd DESC"
        
        return base_query
    
    def _build_ratio_query(self, company_context: Dict[str, Any], metric_info: Dict[str, Any], 
                          term_info: Dict[str, Any], consolidation_id: int) -> str:
        """
        Build SQL query for ratio data
        """
        base_query = f"""
        SELECT 
            c.CompanyName,
            c.Symbol,
            r.RatioName as MetricName,
            rd.Value_ as Value,
            rd.PeriodEnd,
            t.Term,
            'Ratio' as Unit,
            con.consolidationname as Consolidation,
            s.SectorName,
            ind.IndustryName
        FROM tbl_ratiorawdata rd
        JOIN tbl_companieslist c ON rd.CompanyID = c.CompanyID
        JOIN tbl_ratiosheadmaster r ON rd.SubHeadID = r.SubHeadID
        LEFT JOIN tbl_terms t ON rd.TermID = t.TermID
        LEFT JOIN tbl_consolidation con ON rd.ConsolidationID = con.ConsolidationID
        LEFT JOIN tbl_sectornames s ON c.SectorID = s.SectorID
        LEFT JOIN tbl_industryandsectormapping m ON s.SectorID = m.sectorid
        LEFT JOIN tbl_industrynames ind ON m.industryid = ind.IndustryID
        WHERE rd.CompanyID = {company_context['company_id']}
          AND rd.SubHeadID = {metric_info['head_id']}
        """
        
        # Add term/period filters (similar to financial query)
        if term_info['type'] == 'specific_date':
            base_query += f" AND rd.PeriodEnd = '{term_info['period_end']}'"
        elif term_info['type'] == 'specific_term':
            base_query += f" AND rd.TermID = {term_info['term_id']}"
        elif term_info['type'] == 'relative_resolved':
            base_query += f" AND rd.PeriodEnd = '{term_info['period_end']}'"
        
        # Add consolidation filter
        if consolidation_id:
            base_query += f" AND rd.ConsolidationID = {consolidation_id}"
        
        base_query += " ORDER BY rd.PeriodEnd DESC"
        
        return base_query
    
    def _build_dissection_query(self, company_context: Dict[str, Any], metric_info: Dict[str, Any], 
                               term_info: Dict[str, Any], consolidation_id: int) -> str:
        """
        Build SQL query for dissection data
        """
        logger.info(f"Building dissection query for company: {company_context.get('company_name')}, metric: {metric_info.get('head_name')}")
        logger.debug(f"Company context: {company_context}")
        logger.debug(f"Metric info: {metric_info}")
        logger.debug(f"Term info: {term_info}")
        logger.debug(f"Consolidation ID: {consolidation_id}")
        
        # Get the dissection group ID from metric_info or company_context
        # First check metric_info, then fall back to company_context, then default to 1 (Per Share)
        dissection_group_id = metric_info.get('dissection_group_id', 
                                           company_context.get('dissection_group_id', 1))
        logger.info(f"Using dissection group ID: {dissection_group_id}")
        
        base_query = f"""
        SELECT 
            c.CompanyName,
            c.Symbol,
            h.SubHeadName as MetricName,
            d.Value_ as Value,
            d.PeriodEnd,
            t.Term,
            u.unitname as Unit,
            con.consolidationname as Consolidation,
            s.SectorName,
            ind.IndustryName,
            d.DisectionGroupID
        FROM tbl_disectionrawdata d
        JOIN tbl_headsmaster h ON d.SubHeadID = h.SubHeadID
        JOIN tbl_companieslist c ON d.CompanyID = c.CompanyID
        LEFT JOIN tbl_terms t ON d.TermID = t.TermID
        LEFT JOIN tbl_unitofmeasurement u ON h.UnitID = u.UnitID
        LEFT JOIN tbl_consolidation con ON d.ConsolidationID = con.ConsolidationID
        LEFT JOIN tbl_sectornames s ON c.SectorID = s.SectorID
        LEFT JOIN tbl_industryandsectormapping m ON s.SectorID = m.sectorid
        LEFT JOIN tbl_industrynames ind ON m.industryid = ind.IndustryID
        WHERE d.CompanyID = {company_context['company_id']}
          AND d.SubHeadID = {metric_info['head_id']}
          AND d.DisectionGroupID = {dissection_group_id}
        """
        
        # Add term/period filters
        if term_info['type'] == 'specific_date':
            logger.info(f"Adding specific date filter: {term_info['period_end']}")
            base_query += f" AND d.PeriodEnd = '{term_info['period_end']}'"
        elif term_info['type'] == 'specific_term':
            logger.info(f"Adding specific term filter: {term_info['term_id']}")
            base_query += f" AND d.TermID = {term_info['term_id']}"
        elif term_info['type'] == 'relative_resolved':
            logger.info(f"Adding relative period filter: {term_info['period_end']}")
            base_query += f" AND d.PeriodEnd = '{term_info['period_end']}'"
        else:
            logger.debug("No specific period filter applied")
        
        # Add consolidation filter
        if consolidation_id:
            logger.info(f"Adding consolidation filter: {consolidation_id}")
            base_query += f" AND d.ConsolidationID = {consolidation_id}"
        else:
            logger.debug("No consolidation filter applied")
        
        base_query += " ORDER BY d.PeriodEnd DESC"
        
        logger.debug(f"Final dissection query: {base_query}")
        return base_query
    
    def _handle_term_not_found(self, term: str) -> str:
        """
        Handle cases where term/period is not found
        """
        available_terms = self._get_available_terms()
        if available_terms:
            suggestions = ", ".join(available_terms[:10])
            return f"I couldn't understand the time period '{term}'. Available periods include: {suggestions}"
        else:
            return f"I couldn't understand the time period '{term}'. Please use formats like 'Q1 2023', '3M', 'latest', etc."
    
    def _get_available_terms(self) -> List[str]:
        """
        Get available terms from the database
        """
        try:
            query = "SELECT DISTINCT Term FROM tbl_terms ORDER BY Term"
            result = self.db.execute_query(query)
            return result['Term'].tolist() if not result.empty else []
        except Exception as e:
            logger.error(f"Error getting available terms: {e}")
            return []
    
    def _handle_no_data_found(self, entities: Dict[str, Any], company_context: Dict[str, Any], 
                             metric_info: Dict[str, Any]) -> str:
        """
        Handle cases where no data is found
        """
        # Check if data exists for other periods
        alternative_periods = self._find_alternative_periods(company_context, metric_info)
        if alternative_periods:
            periods_str = ", ".join(alternative_periods[:5])
            return f"No data found for {entities['metric']} of {company_context['company_name']} for the requested period. Data is available for: {periods_str}"
        else:
            return f"No data found for {entities['metric']} of {company_context['company_name']}."
    
    def _find_alternative_periods(self, company_context: Dict[str, Any], metric_info: Dict[str, Any]) -> List[str]:
        """
        Find alternative periods where data is available
        """
        data_type = metric_info['data_type']
        table_map = {
            'financial': 'tbl_financialrawdata_Quarter',
            'ratio': 'tbl_ratiorawdata',
            'dissection': 'tbl_disectionrawdata'
        }
        
        table_name = table_map.get(data_type)
        if not table_name:
            return []
        
        try:
            query = f"""
            SELECT DISTINCT TOP 10 
                CONVERT(varchar, PeriodEnd, 23) as PeriodEnd,
                t.Term
            FROM {table_name} d
            LEFT JOIN tbl_terms t ON d.TermID = t.TermID
            WHERE d.CompanyID = {company_context['company_id']}
              AND d.SubHeadID = {metric_info['head_id']}
            ORDER BY PeriodEnd DESC
            """
            
            result = self.db.execute_query(query)
            if not result.empty:
                periods = []
                for _, row in result.iterrows():
                    period_str = row['PeriodEnd']
                    if row['Term']:
                        period_str += f" ({row['Term']})"
                    periods.append(period_str)
                return periods
        except Exception as e:
            logger.error(f"Error finding alternative periods: {e}")
        
        return []
    
    def _generate_enhanced_response(self, financial_data: Any, entities: Dict[str, Any], 
                                   company_context: Dict[str, Any], metric_info: Dict[str, Any]) -> str:
        """
        Generate comprehensive response with context and formatting
        """
        if financial_data.empty:
            return "No data found for the requested query."
        
        # Get the first row of results
        data_row = financial_data.iloc[0]
        
        # Format the response
        company_name = data_row.get('CompanyName', company_context['company_name'])
        symbol = data_row.get('Symbol', company_context['symbol'])
        metric_name = data_row.get('MetricName', entities['metric'])
        value = data_row.get('Value', 'N/A')
        period_end = data_row.get('PeriodEnd', 'N/A')
        term = data_row.get('Term', 'N/A')
        unit = data_row.get('Unit', '')
        consolidation = data_row.get('Consolidation', entities.get('consolidation', 'N/A'))
        sector = data_row.get('SectorName', 'N/A')
        industry = data_row.get('IndustryName', 'N/A')
        
        # Format value with proper units
        if isinstance(value, (int, float)):
            if unit and 'million' in unit.lower():
                formatted_value = f"{value:,.2f} Million"
            elif unit and 'billion' in unit.lower():
                formatted_value = f"{value:,.2f} Billion"
            elif unit and 'rupees' in unit.lower():
                formatted_value = f"Rs. {value:,.2f}"
            else:
                formatted_value = f"{value:,.2f}"
                if unit:
                    formatted_value += f" {unit}"
        else:
            formatted_value = str(value)
        
        # Create comprehensive response
        response = f"""**{company_name} ({symbol})** — {metric_name}: **{formatted_value}**

📅 **Period:** {period_end}
📊 **Term:** {term}
🏢 **Consolidation:** {consolidation}
🏭 **Sector:** {sector}
🔧 **Industry:** {industry}

Data retrieved from {metric_info['source_table']} for {company_name}."""
        
        return response
    
    def get_rag_result(self, init_inputs: Dict[str, Any], messages: List[Dict[str, str]]) -> Tuple[str, List[Dict[str, Any]]]:
        """
        Enhanced RAG result for integration with FinRAG server
        
        Args:
            init_inputs: Initial inputs from the FinRAG server
            messages: List of chat messages
            
        Returns:
            Tuple of (response, retrieval_results)
        """
        # Extract the latest user query
        user_query = ""
        for message in reversed(messages):
            if message["role"].lower() == "user":
                user_query = message["content"]
                break
        
        if not user_query:
            return "I couldn't find a user query to process.", []
        
        # Process the query with enhanced capabilities
        response = self.process_query(user_query)
        
        # Create detailed retrieval result for tracking
        retrieval_result = {
            "query": user_query,
            "response": response,
            "source": "Enhanced Financial Database",
            "timestamp": datetime.now().isoformat(),
            "processing_method": "Enhanced RAG with Dynamic Resolution"
        }
        
        return response, [retrieval_result]