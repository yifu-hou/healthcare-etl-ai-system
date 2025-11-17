import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from etl.extract import FHIRParser, CSVReader
from etl.transform import DataMapper, DataValidator, RiskCalculator
from etl.load import SalesforceLoader
from utils import setup_logger

logger = setup_logger(__name__)

def test_load():
    """Test data loading to Salesforce"""
    
    print("="*60)
    print("TESTING DATA LOAD TO SALESFORCE")
    print("="*60)
    
    # Step 1: Extract
    print("\n1. Extracting data...")
    fhir_parser = FHIRParser()
    patients = fhir_parser.parse_all_patients()
    
    csv_reader = CSVReader()
    lab_results = csv_reader.read_lab_results()
    conditions = csv_reader.read_conditions()
    
    print(f"   ✅ Extracted {len(patients)} patients, {len(lab_results)} labs")
    
    # Step 2: Transform
    print("\n2. Transforming data...")
    mapper = DataMapper()
    validator = DataValidator()
    risk_calc = RiskCalculator()
    
    # Map data
    mapped_patients = mapper.map_multiple_patients(patients)
    mapped_labs = mapper.map_multiple_labs(lab_results)
    
    # Validate data
    valid_patients, invalid_patients = validator.validate_patients_batch(mapped_patients)
    valid_labs, invalid_labs = validator.validate_labs_batch(mapped_labs)
    
    # Calculate risks
    risk_assessments = risk_calc.calculate_all_patient_risks(
        patients, lab_results, conditions
    )
    
    print(f"   ✅ Transformed: {len(valid_patients)} patients, {len(valid_labs)} labs, "
          f"{len(risk_assessments)} risks")
    
    # Step 3: Load
    print("\n3. Loading data to Salesforce...")
    loader = SalesforceLoader()
    
    # Load patients
    print("\n   a) Loading patients...")
    patient_results = loader.upsert_patients_batch(valid_patients)
    print(f"      ✅ Patients: {patient_results['success']} success, "
          f"{patient_results['failed']} failed")
    
    if patient_results['errors']:
        print(f"      ⚠️  Errors: {patient_results['errors'][:3]}")
    
    # Load lab results
    print("\n   b) Loading lab results...")
    lab_results_load = loader.insert_lab_results_batch(
        valid_labs, 
        patient_results['patient_id_map']
    )
    print(f"      ✅ Lab results: {lab_results_load['success']} success, "
          f"{lab_results_load['failed']} failed")
    
    if lab_results_load['errors']:
        print(f"      ⚠️  Errors: {lab_results_load['errors'][:3]}")
    
    # Load risk assessments
    print("\n   c) Loading risk assessments...")
    risk_results = loader.insert_risk_assessments_batch(
        risk_assessments,
        patient_results['patient_id_map']
    )
    print(f"      ✅ Risk assessments: {risk_results['success']} success, "
          f"{risk_results['failed']} failed")
    
    if risk_results['errors']:
        print(f"      ⚠️  Errors: {risk_results['errors'][:3]}")
    
    # Step 4: Verify
    print("\n4. Verifying data in Salesforce...")
    queried_patients = loader.query_patients(limit=5)
    print(f"   ✅ Queried {len(queried_patients)} patients from Salesforce")
    
    if queried_patients:
        print("\n   Sample patients in Salesforce:")
        for patient in queried_patients[:3]:
            print(f"      - {patient['First_Name__c']} {patient['Last_Name__c']} "
                  f"(ID: {patient['Patient_ID__c']})")
    
    print("\n" + "="*60)
    print("DATA LOAD TEST COMPLETE")
    print("="*60)
    print(f"\nSummary:")
    print(f"  Patients loaded: {patient_results['success']}/{patient_results['total']}")
    print(f"  Lab results loaded: {lab_results_load['success']}/{lab_results_load['total']}")
    print(f"  Risk assessments loaded: {risk_results['success']}/{risk_results['total']}")
    print("="*60)

if __name__ == "__main__":
    test_load()