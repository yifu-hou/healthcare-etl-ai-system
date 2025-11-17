
import os
from dotenv import load_dotenv
from simple_salesforce import Salesforce
from google.cloud import bigquery

load_dotenv()
# env_path = Path(__file__).parent.parent / '.env'
# load_dotenv(dotenv_path=env_path, override=True)

# Test Salesforce
try:
    sf = Salesforce(
        os.getenv('SALESFORCE_USERNAME'),
        os.getenv('SALESFORCE_PASSWORD'),
        os.getenv('SALESFORCE_SECURITY_TOKEN'),
        os.getenv('SALESFORCE_DOMAIN')
    )
    print("✅ Salesforce connection successful!")
except Exception as e:
    print(f"❌ Salesforce error: {e}")

# Test BigQuery
try:
    client = bigquery.Client()
    print(f"✅ BigQuery connection successful! Project: {client.project}")
except Exception as e:
    print(f"❌ BigQuery error: {e}")