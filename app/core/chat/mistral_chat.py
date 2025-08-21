'''
Author: AI Assistant
Date: 2023-07-10
Description: Mistral model integration for the FinRAG system
'''

import os
from typing import Dict, List, Optional, Tuple, Any
from utils import logger
import ctransformers

class MistralChat:
    def __init__(self, model_path: str = "Mistral-7B-Instruct-v0.1.Q4_K_M.gguf"):
        """
        Initialize the Mistral model for chat
        
        Args:
            model_path: Path to the Mistral model file
        """
        self.model_path = model_path
        self.llm = self._load_model()
        
    def _load_model(self):
        """
        Load the Mistral model using ctransformers
        """
        try:
            # Check if model file exists
            if not os.path.exists(self.model_path):
                logger.error(f"Model file not found: {self.model_path}")
                raise FileNotFoundError(f"Model file not found: {self.model_path}")
                
            # Load the model in CPU mode
            llm = ctransformers.AutoModelForCausalLM.from_pretrained(
                self.model_path,
                model_type="mistral",
                gpu_layers=0  # Use CPU mode
            )
            
            logger.info(f"Mistral model loaded successfully from {self.model_path}")
            return llm
        except Exception as e:
            logger.error(f"Error loading Mistral model: {e}")
            raise
    
    def _format_prompt(self, messages: List[Dict[str, str]]) -> str:
        """
        Format messages into a prompt for Mistral
        
        Args:
            messages: List of message dictionaries with 'role' and 'content'
            
        Returns:
            Formatted prompt string
        """
        prompt = ""
        
        for message in messages:
            role = message["role"].lower()
            content = message["content"]
            
            if role == "system":
                prompt += f"<s>[INST] {content} [/INST]\n\n"
            elif role == "user":
                prompt += f"<s>[INST] {content} [/INST]\n\n"
            elif role == "assistant":
                prompt += f"{content}\n\n"
        
        # Add final user instruction if the last message was not from the user
        if messages and messages[-1]["role"].lower() != "user":
            prompt += "<s>[INST] "
        
        return prompt
    
    def _format_rag_prompt(self, context: str, question: str) -> str:
        """
        Format a RAG prompt with context and question
        
        Args:
            context: Retrieved context information
            question: User's question
            
        Returns:
            Formatted RAG prompt
        """
        prompt = f"<s>[INST] \nYou are a financial analyst assistant. Answer the question based on the following financial information.\n\n"
        prompt += f"Financial Information:\n{context}\n\n"
        prompt += f"Question: {question}\n\n"
        prompt += "Provide a clear, concise answer with the specific financial figures mentioned in the context. "
        prompt += "If the information is not in the context, say you don't have that information. [/INST]\n\n"
        
        return prompt
    
    def chat(self, messages: List[Dict[str, str]]) -> str:
        """
        Generate a response using the Mistral model
        
        Args:
            messages: List of message dictionaries with 'role' and 'content'
            
        Returns:
            Model response as string
        """
        try:
            # Format the prompt
            prompt = self._format_prompt(messages)
            
            # Generate response
            response = self.llm(prompt, 
                               max_new_tokens=512,
                               temperature=0.7,
                               top_p=0.95,
                               repetition_penalty=1.1)
            
            return response.strip()
        except Exception as e:
            logger.error(f"Error generating response: {e}")
            return f"I'm sorry, I encountered an error: {str(e)}"
    
    def rag_chat(self, context: str, question: str) -> str:
        """
        Generate a response using the Mistral model with RAG context
        
        Args:
            context: Retrieved context information
            question: User's question
            
        Returns:
            Model response as string
        """
        try:
            # Format the RAG prompt
            prompt = self._format_rag_prompt(context, question)
            
            # Generate response
            response = self.llm(prompt, 
                               max_new_tokens=512,
                               temperature=0.7,
                               top_p=0.95,
                               repetition_penalty=1.1)
            
            return response.strip()
        except Exception as e:
            logger.error(f"Error generating RAG response: {e}")
            return f"I'm sorry, I encountered an error: {str(e)}"
    
    def financial_rag_response(self, financial_data: Dict[str, Any], question: str) -> str:
        """
        Generate a response for financial data using RAG
        
        Args:
            financial_data: Dictionary with financial data
            question: User's question
            
        Returns:
            Formatted response with financial data
        """
        # Check if there's an error in the financial data
        if "error" in financial_data:
            return f"I'm sorry, I couldn't retrieve the financial information: {financial_data['error']}"
        
        # Format the financial data as context
        context = f"Company: {financial_data['company']}\n"
        context += f"Metric: {financial_data['metric']}\n"
        context += f"Period: {financial_data['term']}\n"
        context += f"Type: {financial_data['consolidation']}\n"
        context += f"Value: {financial_data['value']} {financial_data['unit']}\n"
        context += f"Date: {financial_data['date']}\n"
        
        # Generate response using RAG
        return self.rag_chat(context, question)