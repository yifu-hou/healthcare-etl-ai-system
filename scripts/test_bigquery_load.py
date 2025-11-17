import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from etl.extract import FHIRParser, CSVReader
from etl.transform import DataMapper, RiskCalculator
from etl.load import BigQueryLoader
from utils import setup_logger

logger = setup_logger(__name__)

def test_bigquery():
    """Test loading data to BigQuery"""
    
    print("="*60)
    print("TESTING BIGQUERY LOAD")
    print("="*60)
    
    # Extract data
    print("\n1. Extracting data...")
    fhir_parser = FHIRParser()
    patients = fhir_parser.parse_all_patients()
    
    csv_reader = CSVReader()
    lab_results = csv_reader.read_lab_results()
    conditions = csv_reader.read_conditions()
    
    print(f"   ✅ Extracted {len(patients)} patients, {len(lab_results)} labs")
    
    # Transform
    print("\n2. Transforming data...")
    mapper = DataMapper()
    risk_calc = RiskCalculator()
    
    mapped_patients = mapper.map_multiple_patients(patients)
    
    # Calculate risks
    risk_assessments = risk_calc.calculate_all_patient_risks(
        patients, lab_results, conditions
    )
    
    print(f"   ✅ Prepared {len(mapped_patients)} patients, {len(risk_assessments)} risks")
    
    # Load to BigQuery
    print("\n3. Loading to BigQuery...")
    bq_loader = BigQueryLoader()
    
    patient_result = bq_loader.load_patients_snapshot(mapped_patients)
    print(f"   ✅ Patients: {patient_result}")
    
    events_result = bq_loader.load_clinical_events(lab_results)
    print(f"   ✅ Clinical events: {events_result}")
    
    risks_result = bq_loader.load_risk_scores(risk_assessments)
    print(f"   ✅ Risk scores: {risks_result}")
    
    # Query back
    print("\n4. Querying BigQuery...")
    queried = bq_loader.query_patients(limit=5)
    print(f"   ✅ Queried {len(queried)} patients")
    
    if queried:
        print("\n   Sample patients:")
        for p in queried[:3]:
            print(f"      - {p['first_name']} {p['last_name']} ({p['patient_id']})")
    
    print("\n" + "="*60)
    print("BIGQUERY TEST COMPLETE")
    print("="*60)

if __name__ == "__main__":
    test_bigquery()

