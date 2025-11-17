import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from etl.extract import FHIRParser, CSVReader
from utils import setup_logger

logger = setup_logger(__name__)

def test_extraction():
    """Test data extraction"""
    
    print("="*50)
    print("TESTING DATA EXTRACTION")
    print("="*50)
    
    # Test FHIR parser
    print("\n1. Testing FHIR Patient Parser...")
    fhir_parser = FHIRParser()
    patients = fhir_parser.parse_all_patients()
    
    print(f"✅ Extracted {len(patients)} patients")
    if patients:
        print("\nSample patient:")
        sample = patients[0]
        for key, value in sample.items():
            print(f"  {key}: {value}")
    
    # Test CSV reader
    print("\n2. Testing CSV Reader...")
    csv_reader = CSVReader()
    
    lab_results = csv_reader.read_lab_results()
    print(f"✅ Extracted {len(lab_results)} lab results")
    if lab_results:
        print(f"\nSample lab result: {lab_results[0]}")
    
    appointments = csv_reader.read_appointments()
    print(f"✅ Extracted {len(appointments)} appointments")
    if appointments:
        print(f"\nSample appointment: {appointments[0]}")
    
    conditions = csv_reader.read_conditions()
    print(f"✅ Extracted {len(conditions)} conditions")
    
    print("\n" + "="*50)
    print("EXTRACTION TEST COMPLETE")
    print("="*50)

if __name__ == "__main__":
    test_extraction()