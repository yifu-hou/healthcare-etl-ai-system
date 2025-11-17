import pandas as pd
import random
import json
from datetime import datetime, timedelta
import os
from pathlib import Path

print("Starting data generation...")
print("="*60)

# Create data directory
os.makedirs('data/raw', exist_ok=True)

# Read actual patient IDs from FHIR files
patient_ids = []
fhir_dir = Path('data/raw/synthea_output')

print(f"Looking for FHIR files in: {fhir_dir.absolute()}")
print(f"Directory exists: {fhir_dir.exists()}")

if fhir_dir.exists():
    json_files = list(fhir_dir.glob('*.json'))
    print(f"Found {len(json_files)} JSON files")
    
    for file_path in json_files:
        print(f"  Processing: {file_path.name}")
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
                
                # Extract patient from bundle
                if data.get('resourceType') == 'Bundle':
                    for entry in data.get('entry', []):
                        resource = entry.get('resource', {})
                        if resource.get('resourceType') == 'Patient':
                            patient_id = resource.get('id')
                            if patient_id:
                                patient_ids.append(patient_id)
                                print(f"    Found patient ID: {patient_id}")
                                break
                elif data.get('resourceType') == 'Patient':
                    patient_id = data.get('id')
                    if patient_id:
                        patient_ids.append(patient_id)
                        print(f"    Found patient ID: {patient_id}")
        except Exception as e:
            print(f"    Error reading {file_path}: {e}")
else:
    print("⚠️  FHIR directory does not exist!")

print(f"\n✅ Found {len(patient_ids)} patient IDs total")
print(f"Patient IDs: {patient_ids[:3]}...")

if not patient_ids:
    print("\n❌ No patient IDs found! Cannot generate matching lab data.")
    print("Please check:")
    print("  1. FHIR files exist in data/raw/synthea_output/")
    print("  2. Files contain Patient resources")
    exit(1)

print("\n" + "="*60)
print("Generating lab results...")

# Generate Lab Results CSV
lab_data = []
test_types = ['A1C', 'Glucose', 'Cholesterol', 'Blood Pressure']

for patient_id in patient_ids:
    num_labs = random.randint(2, 5)
    print(f"  Generating {num_labs} labs for patient {patient_id[:20]}...")
    
    for _ in range(num_labs):
        test_type = random.choice(test_types)
        
        if test_type == 'A1C':
            value = round(random.uniform(4.5, 10.0), 1)
            ref_range = '4.0-5.6'
            status = 'Critical' if value > 6.5 else 'Abnormal' if value > 5.7 else 'Normal'
        elif test_type == 'Glucose':
            value = round(random.uniform(70, 200), 0)
            ref_range = '70-100'
            status = 'Critical' if value > 140 else 'Abnormal' if value > 100 else 'Normal'
        elif test_type == 'Cholesterol':
            value = round(random.uniform(150, 300), 0)
            ref_range = '<200'
            status = 'Critical' if value > 240 else 'Abnormal' if value > 200 else 'Normal'
        else:
            value = round(random.uniform(80, 180), 1)
            ref_range = '80-120'
            status = random.choice(['Normal', 'Normal', 'Abnormal'])
        
        lab_data.append({
            'patient_id': patient_id,
            'test_type': test_type,
            'value': value,
            'reference_range': ref_range,
            'test_date': (datetime.now() - timedelta(days=random.randint(1, 365))).strftime('%Y-%m-%d'),
            'status': status
        })

labs_df = pd.DataFrame(lab_data)

print(f"\n✅ Generated {len(labs_df)} lab results")
print(f"Sample lab result:")
print(labs_df.head(1))

# Save
labs_df.to_csv('data/raw/lab_results.csv', index=False)
print(f"\n✅ Saved to data/raw/lab_results.csv")

print("\n" + "="*60)
print("COMPLETE")
print("="*60)
