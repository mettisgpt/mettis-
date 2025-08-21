#!/usr/bin/env python3
"""
Enhanced FinRAG Server Integration
Integrates the Enhanced Financial RAG system with the existing server architecture.
"""

import os
import json
import time
from typing import Dict, List, Any, Optional
from datetime import datetime

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# Import the enhanced RAG system
from enhanced_financial_rag import EnhancedFinancialRAG
from utils import logger

# Pydantic models for API
class Message(BaseModel):
    role: str
    content: str

class ChatRequest(BaseModel):
    messages: List[Message]
    session_id: Optional[str] = None

class ChatResponse(BaseModel):
    response: str
    processing_time: float
    query_type: str
    retrieval_results: List[Dict[str, Any]]
    suggestions: List[str]

class DirectQueryRequest(BaseModel):
    query: str

class DirectQueryResponse(BaseModel):
    query: str
    response: str
    processing_time: float
    timestamp: str

# Import existing components for fallback
try:
    from app.core.vectorstore.milvus_client import CustomerMilvusClient
    from app.oss.open_chat import OpenChat
except ImportError:
    logger.warning("Could not import existing components, running in standalone mode")
    CustomerMilvusClient = None
    OpenChat = None

class EnhancedFinRAGServer:
    """
    Enhanced FinRAG Server with improved financial query processing
    """
    
    def __init__(self):
        self.app = FastAPI(title="Enhanced FinRAG Server", version="2.0.0")
        
        # Add CORS middleware
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        
        # Initialize the enhanced financial RAG system
        self.enhanced_rag = EnhancedFinancialRAG(
            server='MUHAMMADUSMAN',
            database='MGFinancials'
        )
        
        # Initialize fallback systems if available
        self.milvus_client = None
        self.open_chat = None
        
        if CustomerMilvusClient:
            try:
                self.milvus_client = CustomerMilvusClient()
                logger.info("Milvus client initialized for fallback")
            except Exception as e:
                logger.warning(f"Could not initialize Milvus client: {e}")
        
        if OpenChat:
            try:
                self.open_chat = OpenChat()
                logger.info("OpenChat initialized for fallback")
            except Exception as e:
                logger.warning(f"Could not initialize OpenChat: {e}")
        
        # Setup routes
        self._setup_routes()
        
        logger.info("Enhanced FinRAG Server initialized successfully")
    
    def _setup_routes(self):
        """
        Setup API routes
        """
        
        @self.app.get("/")
        async def root():
            return {
                "message": "Enhanced FinRAG Server",
                "version": "2.0.0",
                "status": "active",
                "capabilities": [
                    "Advanced Entity Extraction",
                    "Dynamic Head ID Resolution",
                    "Relative Period Queries",
                    "Multi-Table Joins",
                    "Fallback & Disambiguation",
                    "Sector/Industry Context"
                ]
            }
        
        @self.app.get("/health")
        async def health_check():
            """
            Health check endpoint
            """
            try:
                # Test database connection
                test_query = "SELECT TOP 1 CompanyID FROM tbl_companieslist"
                result = self.enhanced_rag.db.execute_query(test_query)
                db_status = "healthy" if not result.empty else "unhealthy"
            except Exception as e:
                db_status = f"error: {str(e)}"
            
            return {
                "status": "healthy",
                "timestamp": datetime.now().isoformat(),
                "database": db_status,
                "enhanced_rag": "active"
            }
        
        @self.app.post("/chat")
        async def enhanced_chat(request: ChatRequest):
            """
            Enhanced chat endpoint with improved financial query processing
            """
            try:
                start_time = time.time()
                
                # Extract the latest user message
                user_message = ""
                for message in reversed(request.messages):
                    if message.role.lower() == "user":
                        user_message = message.content
                        break
                
                if not user_message:
                    raise HTTPException(status_code=400, detail="No user message found")
                
                # Determine if this is a financial query
                is_financial = self._is_financial_query(user_message)
                
                if is_financial:
                    # Process with enhanced financial RAG
                    response, retrieval_results = self.enhanced_rag.get_rag_result(
                        {"session_id": getattr(request, 'session_id', 'default')},
                        [msg.dict() for msg in request.messages]
                    )
                    
                    processing_time = time.time() - start_time
                    
                    return ChatResponse(
                        response=response,
                        processing_time=processing_time,
                        query_type="enhanced_financial",
                        retrieval_results=retrieval_results,
                        suggestions=self._generate_suggestions(user_message, response)
                    )
                else:
                    # Fallback to existing systems
                    return await self._handle_non_financial_query(request, start_time)
                    
            except Exception as e:
                logger.error(f"Error in enhanced chat: {e}")
                raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
        
        @self.app.post("/query")
        async def direct_query(request: DirectQueryRequest):
            """
            Direct query endpoint for testing and API access
            """
            try:
                start_time = time.time()
                
                response = self.enhanced_rag.process_query(request.query)
                processing_time = time.time() - start_time
                
                return DirectQueryResponse(
                    query=request.query,
                    response=response,
                    processing_time=processing_time,
                    timestamp=datetime.now().isoformat()
                )
                
            except Exception as e:
                logger.error(f"Error in direct query: {e}")
                raise HTTPException(status_code=500, detail=f"Query processing error: {str(e)}")
        
        @self.app.get("/companies")
        async def get_companies(limit: int = 50):
            """
            Get list of available companies
            """
            try:
                query = f"""
                SELECT TOP {limit} Symbol, CompanyName, SectorName
                FROM tbl_companieslist c
                LEFT JOIN tbl_sectornames s ON c.SectorID = s.SectorID
                ORDER BY Symbol
                """
                
                result = self.enhanced_rag.db.execute_query(query)
                companies = result.to_dict('records') if not result.empty else []
                
                return {
                    "companies": companies,
                    "total": len(companies)
                }
                
            except Exception as e:
                logger.error(f"Error getting companies: {e}")
                raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
        
        @self.app.get("/metrics/{company_symbol}")
        async def get_company_metrics(company_symbol: str):
            """
            Get available metrics for a specific company
            """
            try:
                company_context = self.enhanced_rag._resolve_company_context(company_symbol)
                if not company_context:
                    raise HTTPException(status_code=404, detail=f"Company '{company_symbol}' not found")
                
                available_metrics = self.enhanced_rag._get_available_metrics(company_context)
                
                return {
                    "company": company_context,
                    "available_metrics": available_metrics,
                    "total_metrics": len(available_metrics)
                }
                
            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"Error getting company metrics: {e}")
                raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
        
        @self.app.get("/test")
        async def test_endpoint():
            """
            Test endpoint for system validation
            """
            test_query = "What is the most recent EPS of HBL?"
            
            try:
                start_time = time.time()
                response = self.enhanced_rag.process_query(test_query)
                processing_time = time.time() - start_time
                
                return {
                    "test_query": test_query,
                    "response": response,
                    "processing_time": processing_time,
                    "status": "success"
                }
                
            except Exception as e:
                return {
                    "test_query": test_query,
                    "error": str(e),
                    "status": "failed"
                }
    
    def _is_financial_query(self, query: str) -> bool:
        """
        Enhanced financial query detection
        """
        financial_keywords = [
            # Financial metrics
            'eps', 'earnings per share', 'revenue', 'sales', 'turnover',
            'profit', 'net income', 'pat', 'profit after tax',
            'assets', 'total assets', 'liabilities', 'equity',
            'roe', 'return on equity', 'roa', 'return on assets',
            'debt', 'debt to equity', 'leverage', 'ratio',
            'cash flow', 'operating cash flow', 'free cash flow',
            'dividend', 'dividend yield', 'payout ratio',
            'margin', 'gross margin', 'net margin', 'operating margin',
            'ebitda', 'ebit', 'operating income',
            
            # Time periods
            'quarter', 'quarterly', 'annual', 'yearly', 'ytd',
            'q1', 'q2', 'q3', 'q4', 'fy', 'fiscal year',
            '3m', '6m', '9m', '12m', 'ttm', 'trailing twelve months',
            'latest', 'most recent', 'current', 'last',
            
            # Company identifiers (common Pakistani stocks)
            'hbl', 'ubl', 'mcb', 'abl', 'bafl', 'bank',
            'ogdc', 'pso', 'apl', 'oil', 'gas',
            'engro', 'luck', 'dgkc', 'fccl', 'cement',
            'ptcl', 'tele', 'telecom',
            
            # Financial terms
            'consolidated', 'unconsolidated', 'standalone',
            'financial', 'balance sheet', 'income statement',
            'cash flow statement', 'statement'
        ]
        
        query_lower = query.lower()
        return any(keyword in query_lower for keyword in financial_keywords)
    
    async def _handle_non_financial_query(self, request: ChatRequest, start_time: float) -> ChatResponse:
        """
        Handle non-financial queries with fallback systems
        """
        user_message = ""
        for message in reversed(request.messages):
            if message.role.lower() == "user":
                user_message = message.content
                break
        
        # Try OpenChat first if available
        if self.open_chat:
            try:
                response = await self._process_with_openchat(request)
                processing_time = time.time() - start_time
                
                return ChatResponse(
                    response=response,
                    processing_time=processing_time,
                    query_type="general_chat",
                    retrieval_results=[],
                    suggestions=[]
                )
            except Exception as e:
                logger.warning(f"OpenChat failed: {e}")
        
        # Try Milvus RAG if available
        if self.milvus_client:
            try:
                response = await self._process_with_milvus(request)
                processing_time = time.time() - start_time
                
                return ChatResponse(
                    response=response,
                    processing_time=processing_time,
                    query_type="general_rag",
                    retrieval_results=[],
                    suggestions=[]
                )
            except Exception as e:
                logger.warning(f"Milvus RAG failed: {e}")
        
        # Fallback response
        processing_time = time.time() - start_time
        return ChatResponse(
            response="I'm specialized in financial queries. Please ask about financial metrics, company data, or market information.",
            processing_time=processing_time,
            query_type="fallback",
            retrieval_results=[],
            suggestions=[
                "Try asking about company financial metrics (e.g., 'What is HBL's EPS?')",
                "Ask for revenue, profit, or asset information",
                "Query about financial ratios like ROE or debt-to-equity"
            ]
        )
    
    async def _process_with_openchat(self, request: ChatRequest) -> str:
        """
        Process query with OpenChat system
        """
        # Implementation would depend on OpenChat interface
        return "OpenChat response not implemented"
    
    async def _process_with_milvus(self, request: ChatRequest) -> str:
        """
        Process query with Milvus RAG system
        """
        # Implementation would depend on Milvus interface
        return "Milvus RAG response not implemented"
    
    def _generate_suggestions(self, query: str, response: str) -> List[str]:
        """
        Generate follow-up suggestions based on the query and response
        """
        suggestions = []
        
        # Extract company from query
        query_lower = query.lower()
        
        # Common follow-up suggestions
        if 'eps' in query_lower:
            suggestions.extend([
                "Ask about revenue for the same period",
                "Compare with previous quarter's EPS",
                "Check the company's ROE"
            ])
        elif 'revenue' in query_lower:
            suggestions.extend([
                "Ask about net profit margin",
                "Check total assets",
                "Compare with industry peers"
            ])
        elif 'roe' in query_lower or 'return on equity' in query_lower:
            suggestions.extend([
                "Check ROA (Return on Assets)",
                "Ask about debt-to-equity ratio",
                "Compare with sector average"
            ])
        
        # Add time-based suggestions
        if 'latest' in query_lower or 'recent' in query_lower:
            suggestions.append("Ask for historical trend over multiple quarters")
        
        return suggestions[:3]  # Limit to 3 suggestions

# Create the server instance
server = EnhancedFinRAGServer()
app = server.app

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)