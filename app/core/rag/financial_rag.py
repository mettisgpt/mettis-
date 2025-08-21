'''
Author: AI Assistant
Date: 2023-07-10
Description: Financial RAG system integrating Mistral and SQL database
'''

import os
from typing import Dict, List, Optional, Tuple, Any
from utils import logger

from app.core.database.financial_db import FinancialDatabase
from app.core.chat.mistral_chat import MistralChat

class FinancialRAG:
    def __init__(self, server: str, database: str, model_path: str = "Mistral-7B-Instruct-v0.1.Q4_K_M.gguf"):
        """
        Initialize the Financial RAG system
        
        Args:
            server: SQL Server name
            database: Database name
            model_path: Path to the Mistral model file
        """
        self.db = FinancialDatabase(server, database)
        self.mistral = MistralChat(model_path)
        
        # Load metadata on initialization
        self.db.load_metadata()
        logger.info("Financial RAG system initialized")
    
    def _extract_entities(self, query: str) -> Dict[str, str]:
        """
        Extract financial entities from a natural language query using Mistral
        
        Args:
            query: Natural language query
            
        Returns:
            Dictionary with extracted entities including:
            - company: Company name or ticker
            - metric: Financial metric name
            - term: Term description (e.g., 'Q2 2023', '3M', '6M', etc.)
            - consolidation: 'Consolidated' or 'Unconsolidated'
            - period_end: Optional specific period end date (format: 'YYYY-MM-DD') if detected in the query
        """
        # Create a prompt for entity extraction
        extraction_prompt = [
            {
                "role": "system",
                "content": "You are a financial entity extraction assistant. Extract the company name/ticker, financial metric, time period, and consolidation type (standalone/consolidated) from the query. Respond in JSON format with keys: company, metric, term, consolidation. Do not include quotes or spaces in the values."
            },
            {
                "role": "user",
                "content": query
            }
        ]
        
        # Get response from Mistral
        response = self.mistral.chat(extraction_prompt)
        
        # Parse the response (simplified - in production, use proper JSON parsing)
        entities = {}
        
        # Extract company
        if "company" in response.lower():
            company_start = response.lower().find("company") + 8
            company_end = response.find(",", company_start)
            if company_end == -1:
                company_end = response.find("}", company_start)
            company = response[company_start:company_end].strip().strip('":')
            # Clean up the extracted value
            company = company.strip().strip('"').strip()
            if company.lower() == "null" or company.lower() == "n/a":
                company = ""
            entities["company"] = company
        
        # Extract metric
        if "metric" in response.lower():
            metric_start = response.lower().find("metric") + 7
            metric_end = response.find(",", metric_start)
            if metric_end == -1:
                metric_end = response.find("}", metric_start)
            metric = response[metric_start:metric_end].strip().strip('":')
            # Clean up the extracted value
            metric = metric.strip().strip('"').strip()
            if metric.lower() == "null" or metric.lower() == "n/a":
                metric = ""
            entities["metric"] = metric
        
        # Extract term
        if "term" in response.lower():
            term_start = response.lower().find("term") + 5
            term_end = response.find(",", term_start)
            if term_end == -1:
                term_end = response.find("}", term_start)
            term = response[term_start:term_end].strip().strip('":') 
            # Clean up the extracted value
            term = term.strip().strip('"').strip()
            if term.lower() == "null" or term.lower() == "n/a":
                term = ""
            
            # Handle different term formats
            if '-' in term and len(term.split('-')) == 3:
                logger.info(f"Date format detected in term: {term}, defaulting to '3M'")
                # Check if date is in DD-MM-YYYY or DD-M-YYYY format and convert to YYYY-MM-DD
                date_parts = term.split('-')
                if len(date_parts[0]) <= 2 and len(date_parts[1]) <= 2 and len(date_parts[2]) == 4:
                    # Convert from DD-MM-YYYY or DD-M-YYYY to YYYY-MM-DD
                    day, month, year = date_parts
                    # Ensure day and month are two digits
                    day = day.zfill(2)
                    month = month.zfill(2)
                    term = f"{year}-{month}-{day}"
                    logger.info(f"Converted date format from DD-M(M)-YYYY to YYYY-MM-DD: {term}")
                
                # Store the date as period_end for direct querying
                entities["period_end"] = term
                term = "3M"
            elif term.lower().startswith('fy'):
                # Keep FY format for fiscal year extraction
                logger.info(f"Fiscal year format detected in term: {term}")
                # Make sure there's a space between FY and the year
                if not term.lower().startswith('fy '):
                    term = 'FY ' + term[2:].strip()
            elif term.lower() in ['6m', '6 m', '6-m', '6 months', 'six months']:
                # Standardize 6M term
                logger.info(f"6M term detected: {term}, standardizing to '6M'")
                term = "6M"
            
            # Add detection for relative period terms
            elif any(keyword in term.lower() for keyword in ['latest', 'last', 'recent', 'current', 'ytd', 'year-to-date']):
                # Flag as a relative term
                entities["is_relative_term"] = True
                
                # Determine the relative term type
                if 'ytd' in term.lower() or 'year-to-date' in term.lower():
                    entities["relative_type"] = "ytd"
                    logger.info(f"Detected YTD term: {term}")
                elif 'last reported quarter' in term.lower() or 'last quarter' in term.lower():
                    entities["relative_type"] = "last_quarter"
                    logger.info(f"Detected last quarter term: {term}")
                elif 'current' in term.lower() or 'current period' in term.lower():
                    entities["relative_type"] = "current"
                    logger.info(f"Detected current period term: {term}")
                elif 'most recent' in term.lower() or 'latest' in term.lower():
                    entities["relative_type"] = "most_recent_quarter"
                    logger.info(f"Detected most recent quarter term: {term}")
                # Legacy relative term type detection (for backward compatibility)
                elif any(period in term.lower() for period in ['quarter', 'q1', 'q2', 'q3', 'q4']):
                    entities["relative_term_type"] = "quarter"
                    entities["relative_type"] = "most_recent_quarter"
                elif any(period in term.lower() for period in ['month', 'm']):
                    entities["relative_term_type"] = "month"
                    entities["relative_type"] = "most_recent_quarter"
                elif any(period in term.lower() for period in ['year', 'annual', 'fy', '12m']):
                    entities["relative_term_type"] = "annual"
                    entities["relative_type"] = "ytd"
                elif any(period in term.lower() for period in ['ttm', 'trailing twelve', 'trailing 12']):
                    entities["relative_term_type"] = "ttm"
                    entities["relative_type"] = "most_recent_quarter"
                
                logger.info(f"Detected relative term: {term}, type: {entities.get('relative_type', entities.get('relative_term_type'))}")
                
                # For relative terms, set a default term value of 'TTM' if empty
                if not term.strip():
                    term = "TTM"
                    logger.info(f"Setting default term 'TTM' for empty relative term")
            
            entities["term"] = term
        
        # Check the original query for relative terms that might not be captured in the term field
        query_lower = query.lower()
        if not entities.get("is_relative_term", False):
            if any(keyword in query_lower for keyword in ['latest', 'last available', 'last reported', 'most recent', 'current', 'ytd', 'year-to-date']):
                entities["is_relative_term"] = True
                
                # Determine the relative term type from the original query
                if 'ytd' in query_lower or 'year-to-date' in query_lower:
                    entities["relative_type"] = "ytd"
                    logger.info(f"Detected YTD in query: {query}")
                elif 'last reported quarter' in query_lower or 'last quarter' in query_lower:
                    entities["relative_type"] = "last_quarter"
                    logger.info(f"Detected last quarter in query: {query}")
                elif 'current' in query_lower or 'current period' in query_lower:
                    entities["relative_type"] = "current"
                    logger.info(f"Detected current period in query: {query}")
                elif 'most recent' in query_lower or 'latest' in query_lower or 'last available' in query_lower or 'last reported' in query_lower:
                    entities["relative_type"] = "most_recent_quarter"
                    logger.info(f"Detected most recent/latest/last available/last reported in query: {query}")
                
                # Set term to TTM for relative queries if not already set
                if not entities.get("term") or entities["term"].strip() == "":
                    entities["term"] = "TTM"
                    logger.info(f"Setting default term 'TTM' for relative query")
        
        # Check for dissection indicators in the metric
        if "metric" in entities:
            metric = entities["metric"]
            # Import dissection detection function
            from app.core.database.detect_dissection_metrics import is_dissection_metric
            
            is_dissection, dissection_group_id, data_type = is_dissection_metric(metric)
            if is_dissection:
                entities["is_dissection"] = True
                entities["dissection_group_id"] = dissection_group_id
                entities["dissection_data_type"] = data_type
                logger.info(f"Detected dissection metric: {metric} (Group ID: {dissection_group_id}, Data Type: {data_type})")
            else:
                entities["is_dissection"] = False
                entities["dissection_group_id"] = None
                entities["dissection_data_type"] = None
        
        # Extract consolidation
        if "consolidation" in response.lower():
            cons_start = response.lower().find("consolidation") + 14
            cons_end = response.find(",", cons_start)
            if cons_end == -1:
                cons_end = response.find("}", cons_start)
            consolidation = response[cons_start:cons_end].strip().strip('":')
            # Clean up the extracted value
            consolidation = consolidation.strip().strip('"').strip()
            if consolidation.lower() == "null" or consolidation.lower() == "n/a":
                consolidation = "unconsolidated"  # Default to unconsolidated
            
            # Standardize consolidation values
            if consolidation.lower() in ["standalone", "unconsolidated", "unconsolidate", "un-consolidated", "not consolidated"]:
                logger.info(f"Unconsolidated detected in consolidation: {consolidation}, standardizing to 'Unconsolidated'")
                consolidation = "Unconsolidated"
            elif consolidation.lower() in ["consolidated", "consolidate", "consolidation"]:
                logger.info(f"Consolidated detected in consolidation: {consolidation}, standardizing to 'Consolidated'")
                consolidation = "Consolidated"
                
            entities["consolidation"] = consolidation
        else:
            # Default to unconsolidated if not specified
            logger.info("No consolidation specified, defaulting to 'Unconsolidated'")
            entities["consolidation"] = "unconsolidated"
        
        return entities
    
    def process_query(self, query: str) -> str:
        """
        Process a natural language financial query
        
        Args:
            query: Natural language query
            
        Returns:
            Response with financial information including values for the requested metrics
        """
        try:
            # Extract entities from the query
            entities = self._extract_entities(query)
            logger.info(f"Extracted entities: {entities}")
            
            # Check if we have the required entities
            if not all(k in entities for k in ["company", "metric", "term"]):
                missing = [k for k in ["company", "metric", "term"] if k not in entities]
                return f"I couldn't extract all the required information from your query. Missing: {', '.join(missing)}"
                
            # Ensure term is not empty
            if not entities["term"].strip():
                # If this is a relative term query, set a default term
                if entities.get("is_relative_term", False):
                    entities["term"] = "TTM"
                    logger.info("Setting default term 'TTM' for empty relative term")
                else:
                    return f"I couldn't determine the time period from your query. Please specify a time period like 'Q1 2023' or 'latest'."
            
            # Get company metadata for better context
            company_id = self.db.get_company_id(entities["company"])
            if company_id is not None:
                # Get sector and industry information
                sector_query = f"""
                SELECT c.SectorID, s.SectorName 
                FROM tbl_companieslist c
                JOIN tbl_sectornames s ON c.SectorID = s.SectorID
                WHERE c.CompanyID = {company_id}
                """
                sector_result = self.db.execute_query(sector_query)
                
                if not sector_result.empty:
                    sector_id = sector_result.iloc[0]['SectorID']
                    sector_name = sector_result.iloc[0]['SectorName']
                    logger.info(f"Company {entities['company']} is in sector: {sector_name} (ID: {sector_id})")
                    
                    # Get industry information
                    industry_query = f"""
                    SELECT i.IndustryID, i.IndustryName 
                    FROM tbl_industrynames i
                    JOIN tbl_industryandsectormapping m ON i.IndustryID = m.industryid
                    WHERE m.sectorid = {sector_id}
                    """
                    industry_result = self.db.execute_query(industry_query)
                    
                    if not industry_result.empty:
                        industry_id = industry_result.iloc[0]['IndustryID']
                        industry_name = industry_result.iloc[0]['IndustryName']
                        logger.info(f"Company {entities['company']} is in industry: {industry_name} (ID: {industry_id})")
                        
                        # Log available metrics for this industry-sector combination
                        metrics_query = f"""
                        SELECT h.SubHeadID, h.SubHeadName
                        FROM tbl_headsmaster h
                        """
                        metrics_result = self.db.execute_query(metrics_query)
                        
                        if not metrics_result.empty:
                            logger.info(f"Found {len(metrics_result)} metrics for this industry-sector combination")
                            # Check if the requested metric is similar to any available metrics
                            similar_metrics = []
                            for _, row in metrics_result.iterrows():
                                if entities["metric"].lower() in row['SubHeadName'].lower() or \
                                   any(token in row['SubHeadName'].lower() for token in entities["metric"].lower().split()):
                                    similar_metrics.append(row['SubHeadName'])
                            
                            if similar_metrics:
                                logger.info(f"Found similar metrics to '{entities['metric']}': {similar_metrics[:5]}")
            
            # Get financial data from the database using enhanced query logic
            # Get financial data with relative term handling
            is_relative_term = entities.get("is_relative_term", False)
            relative_term_type = entities.get("relative_term_type", None)
            relative_type = entities.get("relative_type", None)
            
            period_end = entities.get("period_end", None)
            
            # Get consolidation_id for dissection filtering
            consolidation_id = None
            if entities.get("consolidation", "unconsolidated") == "consolidated":
                consolidation_id = 1
            else:
                consolidation_id = 2  # unconsolidated
            
            financial_data = self.db.get_financial_data(
                entities["company"], 
                entities["metric"], 
                entities["term"],
                entities.get("consolidation", "unconsolidated"),
                period_end,
                is_relative_term,
                relative_term_type,
                relative_type,
                company_id=company_id,
                consolidation_id=consolidation_id
            )
            
            # Generate response using Mistral
            response = self.mistral.financial_rag_response(financial_data, query)
            
            return response
        except Exception as e:
            logger.error(f"Error processing query: {e}")
            return f"I'm sorry, I encountered an error while processing your query: {str(e)}"
    
    def get_rag_result(self, init_inputs: Dict[str, Any], messages: List[Dict[str, str]]) -> Tuple[str, List[Dict[str, Any]]]:
        """
        Get RAG result for integration with FinRAG server
        
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
        
        # Process the query
        response = self.process_query(user_query)
        
        # Create a retrieval result for tracking
        retrieval_result = {
            "query": user_query,
            "response": response,
            "source": "Financial Database"
        }
        
        return response, [retrieval_result]