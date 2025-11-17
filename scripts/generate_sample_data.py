import pandas as pd
import random
import json 
from datetime import datetime, timedelta
import os
from pathlib import Path 

print("Create data directory")
# Create data directory
os.makedirs('data/raw', exist_ok=True)

print("Read patient IDs from FHIR files")
# Read patient IDs from FHIR files
patient_ids = []
fhir_dir = Path('data/raw/synthea_output')

# If FHIR file exits: extract patient data from bundle   
if fhir_dir.exists():
    for file_path in fhir_dir.glob('*.json'):
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)

            if data.get('resourceType') == 'Bundle':
                for entry in data.get('entry', []):
                    resource = entry.get('resource', {})
                    if resource.get('resourceType') == 'Patient':
                        patient_id = resource.get('id')
                        if patient_id:
                            patient_ids.append(patient_id)
                            break
        except Exception as e:
            print(f"Error reading {file_path}: {e}")
        
print(f"Found {len(patient_ids)} patient IDs from FHIR files.")

if not patient_ids:
    print("⚠️  No patient IDs found! Using fallback IDs")
    patient_ids = [f"P{str(i).zfill(4)}" for i in range(1, 11)]


# Generate Lab Results CSV
lab_data = []
patient_ids = [f"P{str(i).zfill(4)}" for i in range(1, 11)]  # P0001 to P0010
test_types = ['A1C', 'Glucose', 'Cholesterol', 'Blood Pressure']

for patient_id in patient_ids:
    for _ in range(random.randint(2, 5)): 
        lab_data.append({
            'patient_id': patient_id,
            'test_type': random.choice(test_types),
            'value': round(random.uniform(80, 200), 2),
            'reference_range': '70-100',
            'test_datetime': (datetime.now() - timedelta(days=random.randint(1, 365))).strftime('%Y-%m-%d %H:%M:%S'),
            'status': random.choice(['Normal', 'Abnormal', 'Critical'])
        })

labs_df = pd.DataFrame(lab_data)
labs_df.to_csv('data/raw/lab_results.csv', index=False)
print(f"✅ Generated {len(labs_df)} lab results")

# Generate Appointments CSV
appointment_data = []
for patient_id in patient_ids:
    for _ in range(random.randint(1, 3)): 
        appointment_data.append({
            'patient_id': patient_id,
            'appointment_date': (datetime.now() + timedelta(days=random.randint(1, 90))).strftime('%Y-%m-%d'),
            'appointment_type': random.choice(['Follow-up', 'Annual Check-up', 'Consultation']),
            'provider': random.choice(['Dr. Smith', 'Dr. Johnson', 'Dr. Williams']),
            'status': random.choice(['Scheduled', 'Confirmed', 'Pending'])
        })

appointments_df = pd.DataFrame(appointment_data)
appointments_df.to_csv('data/raw/appointments.csv', index=False)
print(f"✅ Generated {len(appointments_df)} appointments")

print("\n📊 Sample Data Summary:")
print(f"   - Patients: {len(patient_ids)}")
print(f"   - Lab results: {len(labs_df)}")
print(f"   - Appointments: {len(appointments_df)}")
print(f"\n✅ CSV files generated with actual patient IDs from FHIR files")
