import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from ai_agent.agent import HealthcareAgent

# Test the agent
agent = HealthcareAgent()

questions = [
    "Which patients are high risk?",
    "Show me abnormal A1C results",
    "What are the risk trends?",
]

print("="*60)
print("TESTING HEALTHCARE AI AGENT")
print("="*60)

for question in questions:
    print(f"\n❓ Question: {question}")
    print("-" * 60)
    answer = agent.answer_question(question)
    print(f"🤖 Answer: {answer}")
    print("="*60)