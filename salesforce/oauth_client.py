import os
import requests
from dotenv import load_dotenv
from simple_salesforce import Salesforce
from utils import setup_logger

load_dotenv(override=True)
logger = setup_logger(__name__)

def get_salesforce_oauth_connection():
    """Connect to Salesforce using OAuth password flow"""
    
    sf_username = os.getenv('SALESFORCE_USERNAME')
    sf_password = os.getenv('SALESFORCE_PASSWORD')
    consumer_key = os.getenv('SALESFORCE_CONSUMER_KEY')
    consumer_secret = os.getenv('SALESFORCE_CONSUMER_SECRET')
    domain = os.getenv('SALESFORCE_DOMAIN', 'test')
    
    # Check if OAuth credentials are available
    if consumer_key and consumer_secret:
        # Use OAuth
        token_url = f'https://{domain}.salesforce.com/services/oauth2/token'
        
        payload = {
            'grant_type': 'password',
            'client_id': consumer_key,
            'client_secret': consumer_secret,
            'username': sf_username,
            'password': sf_password
        }
        
        logger.info("Requesting OAuth token...")
        response = requests.post(token_url, data=payload)
        
        if response.status_code == 200:
            oauth_data = response.json()
            
            sf = Salesforce(
                instance_url=oauth_data['instance_url'],
                session_id=oauth_data['access_token']
            )
            
            logger.info(f"✅ OAuth connection successful: {sf.sf_instance}")
            return sf
        else:
            error_msg = f"OAuth failed: {response.status_code} - {response.text}"
            logger.error(error_msg)
            # Fall back to username/password/token
    
    # Fallback: Use username/password/security token
    logger.info("Using username/password/token authentication")
    sf_token = os.getenv('SALESFORCE_SECURITY_TOKEN')
    
    sf = Salesforce(
        sf_username,
        sf_password,
        sf_token,
        domain
    )
    
    logger.info(f"✅ Connected to Salesforce: {sf.sf_instance}")
    return sf
