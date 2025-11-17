import json
from typing import Dict, Any, List
from .vertex_client import VertexAIClient
from .tools import SalesforceTool, BigQueryTool
from utils import setup_logger

logger = setup_logger(__name__)

class HealthcareAgent:
    """Main AI agent for healthcare queries"""
    
    def __init__(self):
        self.vertex_client = VertexAIClient()
        self.sf_tool = SalesforceTool()
        self.bq_tool = BigQueryTool()
        logger.info("Healthcare AI Agent initialized")
    
    def answer_question(self, question: str) -> str:
        """
        Answer a healthcare question using available tools
        """
        logger.info(f"Processing question: {question}")
        
        # Determine which tool to use based on question keywords
        question_lower = question.lower()
        
        # Route to appropriate tool
        if any(word in question_lower for word in ['high risk', 'risky', 'critical patients']):
            return self._handle_high_risk_query(question)
        
        elif any(word in question_lower for word in ['abnormal', 'a1c', 'glucose', 'lab']):
            return self._handle_lab_query(question)
        
        elif 'trend' in question_lower or 'history' in question_lower:
            return self._handle_trend_query(question)
        
        elif 'patient' in question_lower and any(word in question_lower for word in ['summary', 'info', 'about']):
            return self._handle_patient_summary(question)
        
        else:
            return self._handle_general_query(question)
    
    def _handle_high_risk_query(self, question: str) -> str:
        """Handle queries about high-risk patients"""
        logger.info("Handling high-risk patient query")
        
        # Get data from Salesforce
        high_risk_patients = self.sf_tool.get_high_risk_patients()
        
        if not high_risk_patients:
            return "No high-risk patients found in the system."
        
        # Format data for AI
        data_summary = f"Found {len(high_risk_patients)} high-risk patients:\n"
        for patient in high_risk_patients[:5]:  # Top 5
            data_summary += f"- {patient['name']}: {patient['risk_level']} (Score: {patient['risk_score']})\n"
            data_summary += f"  Risk factors: {patient['risk_factors']}\n"
        
        # Generate natural language response
        prompt = f"""You are a healthcare AI assistant. Based on this data, answer the question naturally and concisely.

Question: {question}

Data:
{data_summary}

Provide a clear, actionable response for a care coordinator."""
        
        response = self.vertex_client.generate_response(prompt, temperature=0.3)
        return response
    
    def _handle_lab_query(self, question: str) -> str:
        """Handle queries about lab results"""
        logger.info("Handling lab results query")
        
        # Extract test type if mentioned
        test_type = None
        if 'a1c' in question.lower():
            test_type = 'A1C'
        elif 'glucose' in question.lower():
            test_type = 'Glucose'
        elif 'cholesterol' in question.lower():
            test_type = 'Cholesterol'
        
        # Get data from Salesforce
        abnormal_labs = self.sf_tool.get_abnormal_lab_results(test_type)
        
        if not abnormal_labs:
            return f"No abnormal {test_type or 'lab'} results found."
        
        # Format data
        data_summary = f"Found {len(abnormal_labs)} abnormal lab results:\n"
        for lab in abnormal_labs[:10]:  # Top 10
            data_summary += f"- {lab['name']}: {lab['test_type']} = {lab['value']} ({lab['status']})\n"
            data_summary += f"  Reference: {lab['reference_range']}, Date: {lab['test_datetime']}\n"
        
        # Generate response
        prompt = f"""You are a healthcare AI assistant. Based on this data, answer the question naturally.

Question: {question}

Data:
{data_summary}

Provide a clear summary and suggest any follow-up actions."""
        
        response = self.vertex_client.generate_response(prompt, temperature=0.3)
        return response
    
    def _handle_trend_query(self, question: str) -> str:
        """Handle queries about trends and analytics"""
        logger.info("Handling trend query")
        
        # Get data from BigQuery
        risk_trends = self.bq_tool.get_risk_score_trends()
        
        if not risk_trends:
            return "No trend data available."
        
        # Format data
        data_summary = "Risk score distribution:\n"
        for trend in risk_trends:
            data_summary += f"- {trend['risk_level']}: {trend['patient_count']} patients "
            data_summary += f"(Average score: {trend['avg_score']:.1f})\n"
        
        # Generate response
        prompt = f"""You are a healthcare AI assistant. Based on this data, answer the question naturally.

Question: {question}

Data:
{data_summary}

Provide insights and recommendations."""
        
        response = self.vertex_client.generate_response(prompt, temperature=0.3)
        return response
    
    def _handle_patient_summary(self, question: str) -> str:
        """Handle queries about specific patients"""
        logger.info("Handling patient summary query")
        
        # For now, get the first high-risk patient as example
        # In production, would extract patient ID from question
        high_risk = self.sf_tool.get_high_risk_patients()
        
        if not high_risk:
            return "No patient data available."
        
        patient_id = high_risk[0]['patient_id']
        summary = self.sf_tool.get_patient_summary(patient_id)
        
        if 'error' in summary:
            return f"Error retrieving patient data: {summary['error']}"
        
        # Format data
        data_summary = f"""Patient: {summary['name']}
Gender: {summary['gender']}, DOB: {summary['date_of_birth']}
Risk Level: {summary['risk_assessment'].get('level', 'N/A')}
Recent Labs: {len(summary['recent_labs'])} tests
"""
        
        # Generate response
        prompt = f"""You are a healthcare AI assistant. Based on this patient data, provide a summary.

Question: {question}

Data:
{data_summary}

Provide a concise patient overview."""
        
        response = self.vertex_client.generate_response(prompt, temperature=0.3)
        return response
    
    def _handle_general_query(self, question: str) -> str:
        """Handle general healthcare questions"""
        logger.info("Handling general query")
        
        prompt = f"""You are a healthcare AI assistant with access to patient data systems.

Question: {question}

Provide a helpful response. If you need specific data to answer, explain what information would be needed."""
        
        response = self.vertex_client.generate_response(prompt, temperature=0.7)
        return response

if __name__ == "__main__":
    # Test the agent
    agent = HealthcareAgent()
    
    questions = [
        "Which patients are high risk?",
        "Show me abnormal A1C results",
        "What are the risk trends?",
    ]
    
    for question in questions:
        print(f"\nQ: {question}")
        print(f"A: {agent.answer_question(question)}")
        print("-" * 60)

