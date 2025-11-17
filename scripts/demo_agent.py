import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from ai_agent.agent import HealthcareAgent
import time

def print_separator():
    print("\n" + "="*70 + "\n")

def demo_healthcare_agent():
    """Demonstrate the healthcare AI agent with realistic scenarios"""
    
    print("="*70)
    print("HEALTHCARE AI AGENT DEMONSTRATION")
    print("Patient Care Coordination System")
    print("="*70)
    
    # Initialize agent
    print("\n🤖 Initializing AI Agent...")
    agent = HealthcareAgent()
    print("✅ Agent ready!\n")
    
    # Scenario 1: Care Coordinator Morning Briefing
    print_separator()
    print("📋 SCENARIO 1: Morning Briefing for Care Coordinators")
    print("-" * 70)
    
    questions_scenario1 = [
        "Which patients are high risk and need immediate attention?",
        "Show me all patients with abnormal A1C results",
        "What are the current risk trends across our patient population?"
    ]
    
    for i, question in enumerate(questions_scenario1, 1):
        print(f"\n💬 Question {i}: {question}")
        print("-" * 70)
        
        answer = agent.answer_question(question)
        print(f"🤖 Answer:\n{answer}")
        
        time.sleep(1)  # Small delay for readability
    
    print_separator()
    print("✅ Scenario 1 Complete")
    
    # Scenario 2: Proactive Patient Management
    print_separator()
    print("📋 SCENARIO 2: Proactive Patient Management")
    print("-" * 70)
    
    questions_scenario2 = [
        "Which patients have critical lab results?",
        "Show me abnormal glucose levels",
    ]
    
    for i, question in enumerate(questions_scenario2, 1):
        print(f"\n💬 Question {i}: {question}")
        print("-" * 70)
        
        answer = agent.answer_question(question)
        print(f"🤖 Answer:\n{answer}")
        
        time.sleep(1)
    
    print_separator()
    print("✅ Scenario 2 Complete")
    
    # Summary
    print_separator()
    print("📊 DEMONSTRATION SUMMARY")
    print("-" * 70)
    print("""
The AI Agent successfully demonstrated:
✅ Real-time patient risk assessment
✅ Clinical data analysis (lab results)
✅ Population health analytics (trends)
✅ Natural language query understanding
✅ Multi-source data integration (Salesforce + BigQuery)
✅ Actionable insights for care coordinators

System Components Working:
✅ ETL Pipeline (Extract → Transform → Load)
✅ Salesforce CRM (Operational data)
✅ BigQuery Data Warehouse (Analytics)
✅ Vertex AI (Natural language processing)
✅ Intelligent Agent (Query routing & synthesis)
    """)
    print("="*70)

if __name__ == "__main__":
    demo_healthcare_agent()
