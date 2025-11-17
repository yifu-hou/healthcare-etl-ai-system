import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from ai_agent.tools import SalesforceTool, BigQueryTool
from ai_agent import VertexAIClient

print("="*60)
print("TESTING AI AGENT TOOLS")
print("="*60)

# Test Salesforce Tool
print("\n1. Testing Salesforce Tool...")
sf_tool = SalesforceTool()

high_risk = sf_tool.get_high_risk_patients()
print(f"✅ Found {len(high_risk)} high-risk patients")
if high_risk:
    print(f"   Example: {high_risk[0]['name']} - {high_risk[0]['risk_level']}")

abnormal_labs = sf_tool.get_abnormal_lab_results('A1C')
print(f"✅ Found {len(abnormal_labs)} abnormal A1C results")

# Test BigQuery Tool
print("\n2. Testing BigQuery Tool...")
bq_tool = BigQueryTool()

risk_trends = bq_tool.get_risk_score_trends()
print(f"✅ Retrieved risk trends: {len(risk_trends)} levels")
for trend in risk_trends:
    print(f"   {trend['risk_level']}: {trend['patient_count']} patients")

# Test Vertex AI Client
print("\n3. Testing Vertex AI Client...")
vertex_client = VertexAIClient()

response = vertex_client.generate_response(
    "You are a healthcare AI assistant. Introduce yourself in one sentence."
)
print(f"✅ AI Response: {response}")

print("\n" + "="*60)
print("ALL TOOLS WORKING")
print("="*60)

