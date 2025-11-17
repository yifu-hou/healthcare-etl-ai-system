import os
from typing import Dict, List
from dotenv import load_dotenv
import vertexai
from vertexai.preview.generative_models import GenerativeModel
from utils import setup_logger

load_dotenv(override=True)
logger = setup_logger(__name__)

class VertexAIClient:
    """Client for interacting with Vertex AI Gemini"""
    
    def __init__(self):
        self.project_id = os.getenv('GCP_PROJECT_ID')
        self.location = os.getenv('VERTEX_AI_LOCATION', 'us-central1')
        self.model_name = os.getenv('VERTEX_AI_MODEL', 'gemini-1.5-flash')
        
        # Initialize Vertex AI
        vertexai.init(project=self.project_id, location=self.location)
        
        logger.info(f"Initialized Vertex AI: {self.project_id} in {self.location}")
    
    def generate_response(self, prompt: str, temperature: float = 0.7) -> str:
        """Generate a response from Gemini"""
        try:
            model = GenerativeModel(self.model_name)
            
            response = model.generate_content(
                prompt,
                generation_config={
                    "temperature": temperature,
                    "max_output_tokens": 2048,
                }
            )
            
            return response.text
            
        except Exception as e:
            logger.error(f"Error generating response: {e}")
            return f"Error: {str(e)}"
    
    def chat(self, messages: List[Dict[str, str]]) -> str:
        """Multi-turn chat conversation"""
        try:
            model = GenerativeModel(self.model_name)
            chat = model.start_chat()
            
            # Send all messages
            for message in messages:
                if message['role'] == 'user':
                    response = chat.send_message(message['content'])
            
            return response.text
            
        except Exception as e:
            logger.error(f"Error in chat: {e}")
            return f"Error: {str(e)}"

if __name__ == "__main__":
    # Test the client
    client = VertexAIClient()
    
    response = client.generate_response("Say hello and introduce yourself as a healthcare AI assistant.")
    print(f"Response: {response}")
