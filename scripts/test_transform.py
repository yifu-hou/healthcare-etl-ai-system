import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from etl.extract import FHIRParser, CSVReader
from etl.transform import DataMapper, DataValidator, RiskCalculator
from utils import setup_logger

logger = setup_logger(__name__)

def test_transform():
    """Test data transformation"""
    
    print("="*50)
    print("TESTING DATA TRANSFORMATION")
    print("="*50)
    
    # Extract data first
    print("\n1. Extracting data...")
    fhir_parser = FHIRParser()
    patients = fhir_parser.parse_all_patients()
    
    csv_reader = CSVReader()
    lab_results = csv_reader.read_lab_results()
    conditions = csv_reader.read_conditions()
    
    print(f"   Extracted {len(patients)} patients, {len(lab_results)} labs")
    
    # Test Data Mapper
    print("\n2. Testing Data Mapper...")
    mapper = DataMapper()
    
    mapped_patients = mapper.map_multiple_patients(patients)
    print(f"   ✅ Mapped {len(mapped_patients)} patients")
    if mapped_patients:
        print(f"   Sample mapped patient: {mapped_patients[0]}")

    mapped_labs = mapper.map_multiple_labs(lab_results)
    print(f"   ✅ Mapped {len(mapped_labs)} lab results")
    
    # Test Validator
    print("\n3. Testing Data Validator...")
    validator = DataValidator()
    
    valid_patients, invalid_patients = validator.validate_patients_batch(mapped_patients)
    print(f"   ✅ Validated: {len(valid_patients)} valid, {len(invalid_patients)} invalid patients")
    
    valid_labs, invalid_labs = validator.validate_labs_batch(mapped_labs)
    print(f"   ✅ Validated: {len(valid_labs)} valid, {len(invalid_labs)} invalid labs")
    
    if invalid_patients:
        print(f"   ⚠️  Invalid patients: {invalid_patients}")
    
    # Test Risk Calculator
    print("\n4. Testing Risk Calculator...")
    risk_calc = RiskCalculator()
    
    risk_assessments = risk_calc.calculate_all_patient_risks(
        patients, lab_results, conditions
    )
    print(f"   ✅ Calculated {len(risk_assessments)} risk assessments")
    
    if risk_assessments:
        print("\n   Sample risk assessments:")
        for risk in risk_assessments[:3]:
            print(f"      Patient {risk['patient_id']}: {risk['Risk_Level__c']} "
                  f"(Score: {risk['Risk_Score__c']})")
            print(f"         Factors: {risk['Risk_Factors__c'][:80]}...")
    
    print("\n" + "="*50)
    print("TRANSFORMATION TEST COMPLETE")
    print("="*50)
    
    # Return data for potential use
    return {
        'patients': valid_patients,
        'labs': valid_labs,
        'risks': risk_assessments
    }

if __name__ == "__main__":
    test_transform()

