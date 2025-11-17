import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from etl.extract import FHIRParser, CSVReader
from etl.transform import DataMapper, DataValidator, RiskCalculator
from etl.load import SalesforceLoader, BigQueryLoader
from utils import setup_logger

logger = setup_logger(__name__)

class ETLOrchestrator:
    """Orchestrate the complete ETL pipeline"""
    
    def __init__(self):
        self.fhir_parser = FHIRParser()
        self.csv_reader = CSVReader()
        self.mapper = DataMapper()
        self.validator = DataValidator()
        self.risk_calculator = RiskCalculator()
        self.sf_loader = SalesforceLoader()
        self.bq_loader = BigQueryLoader()
    
    def run_pipeline(self):
        """Execute the complete ETL pipeline"""
        
        logger.info("="*60)
        logger.info("STARTING ETL PIPELINE")
        logger.info("="*60)
        
        # EXTRACT
        logger.info("\n[EXTRACT] Reading source data...")
        patients = self.fhir_parser.parse_all_patients()
        lab_results = self.csv_reader.read_lab_results()
        conditions = self.csv_reader.read_conditions()
        
        logger.info(f"Extracted: {len(patients)} patients, {len(lab_results)} labs, "
                   f"{len(conditions)} conditions")
        
        # TRANSFORM
        logger.info("\n[TRANSFORM] Processing data...")
        
        # Map
        mapped_patients = self.mapper.map_multiple_patients(patients)
        mapped_labs = self.mapper.map_multiple_labs(lab_results)
        
        # Validate
        valid_patients, invalid_patients = self.validator.validate_patients_batch(mapped_patients)
        valid_labs, invalid_labs = self.validator.validate_labs_batch(mapped_labs)
        
        logger.info(f"Validated: {len(valid_patients)} valid patients, "
                   f"{len(valid_labs)} valid labs")
        
        if invalid_patients:
            logger.warning(f"Invalid patients: {len(invalid_patients)}")
        if invalid_labs:
            logger.warning(f"Invalid labs: {len(invalid_labs)}")
        
        # Calculate risks
        risk_assessments = self.risk_calculator.calculate_all_patient_risks(
            patients, lab_results, conditions
        )
        logger.info(f"Calculated: {len(risk_assessments)} risk assessments")
        
        # LOAD TO SALESFORCE
        logger.info("\n[LOAD] Loading data to Salesforce...")
        
        # Load patients
        patient_results = self.sf_loader.upsert_patients_batch(valid_patients)
        logger.info(f"Loaded patients: {patient_results['success']}/{patient_results['total']}")
        
        # Load labs
        lab_load_results = self.sf_loader.insert_lab_results_batch(
            valid_labs, patient_results['patient_id_map']
        )
        logger.info(f"Loaded lab results: {lab_load_results['success']}/{lab_load_results['total']}")
        
        # Load risks
        risk_load_results = self.sf_loader.insert_risk_assessments_batch(
            risk_assessments, patient_results['patient_id_map']
        )
        logger.info(f"Loaded risk assessments: {risk_load_results['success']}/{risk_load_results['total']}")
        
        # LOAD TO BIGQUERY
        logger.info("\n[LOAD] Loading data to BigQuery...")
        
        # Add Salesforce IDs to patients for BigQuery
        for patient in valid_patients:
            patient_id = patient.get('Patient_ID__c')
            patient['sf_id'] = patient_results['patient_id_map'].get(patient_id)
        
        # Load to BigQuery
        bq_patient_results = self.bq_loader.load_patients_snapshot(valid_patients)
        logger.info(f"BigQuery patients: {bq_patient_results.get('count', 0)} loaded")
        
        bq_events_results = self.bq_loader.load_clinical_events(lab_results)
        logger.info(f"BigQuery events: {bq_events_results.get('count', 0)} loaded")
        
        bq_risks_results = self.bq_loader.load_risk_scores(risk_assessments)
        logger.info(f"BigQuery risks: {bq_risks_results.get('count', 0)} loaded")
        
        # SUMMARY
        logger.info("\n" + "="*60)
        logger.info("ETL PIPELINE COMPLETE")
        logger.info("="*60)
        logger.info("\nSalesforce:")
        logger.info(f"  Patients: {patient_results['success']}/{patient_results['total']}")
        logger.info(f"  Lab results: {lab_load_results['success']}/{lab_load_results['total']}")
        logger.info(f"  Risk assessments: {risk_load_results['success']}/{risk_load_results['total']}")
        logger.info("\nBigQuery:")
        logger.info(f"  Patients: {bq_patient_results.get('count', 0)}")
        logger.info(f"  Clinical events: {bq_events_results.get('count', 0)}")
        logger.info(f"  Risk scores: {bq_risks_results.get('count', 0)}")
        logger.info("="*60)
        
        return {
            'salesforce': {
                'patients': patient_results,
                'labs': lab_load_results,
                'risks': risk_load_results
            },
            'bigquery': {
                'patients': bq_patient_results,
                'events': bq_events_results,
                'risks': bq_risks_results
            }
        }

if __name__ == "__main__":
    orchestrator = ETLOrchestrator()
    orchestrator.run_pipeline()

