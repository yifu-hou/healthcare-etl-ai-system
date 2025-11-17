import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from etl.extract import FHIRParser

# Read one patient file directly
patient_file = Path("data/raw/synthea_output").glob("*.json").__next__()

print(f"Reading: {patient_file}")
print("="*60)

with open(patient_file, 'r') as f:
    data = json.load(f)

print("JSON Structure:")
print(json.dumps(data, indent=2)[:500])  # First 500 chars

print("\n" + "="*60)
print("Keys in JSON:", list(data.keys()))
print("="*60)

# Test parser
parser = FHIRParser()
extracted = parser.extract_patient_info(data)

print("\nExtracted data:")
for key, value in extracted.items():
    print(f"  {key}: {value}")
