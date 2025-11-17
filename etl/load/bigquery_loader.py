import os
from typing import List, Dict
from datetime import datetime
from dotenv import load_dotenv
from google.cloud import bigquery
from utils import setup_logger

load_dotenv(override=True)
logger = setup_logger(__name__)

class BigQueryLoader:
    """Load data into BigQuery for analytics"""
    
    def __init__(self):
        self.project_id = os.getenv('GCP_PROJECT_ID')
        self.dataset_id = os.getenv('BIGQUERY_DATASET')
        self.client = bigquery.Client(project=self.project_id)
        logger.info(f"Connected to BigQuery: {self.project_id}.{self.dataset_id}")
    
    def load_patients_snapshot(self, patients: List[Dict]) -> Dict:
        """Load patient data to BigQuery"""
        table_id = f"{self.project_id}.{self.dataset_id}.patients_snapshot"
        
        # Prepare rows
        rows = []
        snapshot_time = datetime.utcnow()
        
        for patient in patients:
            row = {
                'patient_id': patient.get('Patient_ID__c', ''),
                'salesforce_id': patient.get('sf_id', ''),
                'first_name': patient.get('First_Name__c', ''),
                'last_name': patient.get('Last_Name__c', ''),
                'date_of_birth': patient.get('Date_of_Birth__c'),
                'gender': patient.get('Gender__c', ''),
                'email': patient.get('Email__c', ''),
                'phone': patient.get('Phone__c', ''),
                'address': patient.get('Address__c', ''),
                'snapshot_date': snapshot_time.isoformat()
            }
            rows.append(row)
        
        # Insert rows
        try:
            errors = self.client.insert_rows_json(table_id, rows)
            
            if errors:
                logger.error(f"Errors inserting patients: {errors}")
                return {'success': False, 'errors': errors}
            else:
                logger.info(f"Loaded {len(rows)} patients to BigQuery")
                return {'success': True, 'count': len(rows)}
                
        except Exception as e:
            logger.error(f"Error loading patients to BigQuery: {e}")
            return {'success': False, 'error': str(e)}
    
    def load_clinical_events(self, lab_results: List[Dict]) -> Dict:
        """Load lab results as clinical events"""
        table_id = f"{self.project_id}.{self.dataset_id}.clinical_events"
        
        rows = []
        timestamp = datetime.utcnow()
        
        for lab in lab_results:
            row = {
                'event_id': f"LAB_{lab.get('patient_id', '')}_{timestamp.timestamp()}",
                'patient_id': lab.get('patient_id', ''),
                'event_type': 'LAB',
                'event_date': lab.get('test_datetime'),
                'event_value': str(lab.get('value', '')),
                'event_status': lab.get('status', ''),
                'event_details': {
                    'test_type': lab.get('test_type', ''),
                    'reference_range': lab.get('reference_range', '')
                },
                'created_timestamp': timestamp.isoformat()
            }
            rows.append(row)
        
        try:
            errors = self.client.insert_rows_json(table_id, rows)
            
            if errors:
                logger.error(f"Errors inserting events: {errors}")
                return {'success': False, 'errors': errors}
            else:
                logger.info(f"Loaded {len(rows)} clinical events to BigQuery")
                return {'success': True, 'count': len(rows)}
                
        except Exception as e:
            logger.error(f"Error loading events to BigQuery: {e}")
            return {'success': False, 'error': str(e)}
    
    def load_risk_scores(self, risk_assessments: List[Dict]) -> Dict:
        """Load risk assessments to BigQuery"""
        table_id = f"{self.project_id}.{self.dataset_id}.risk_scores_history"
        
        rows = []
        timestamp = datetime.utcnow()
        
        for risk in risk_assessments:
            row = {
                'patient_id': risk.get('patient_id', ''),
                'risk_level': risk.get('Risk_Level__c', ''),
                'risk_score': int(risk.get('Risk_Score__c', 0)),
                'risk_factors': risk.get('Risk_Factors__c', ''),
                'assessment_date': risk.get('Assessment_Date__c'),
                'created_timestamp': timestamp.isoformat()
            }
            rows.append(row)
        
        try:
            errors = self.client.insert_rows_json(table_id, rows)
            
            if errors:
                logger.error(f"Errors inserting risks: {errors}")
                return {'success': False, 'errors': errors}
            else:
                logger.info(f"Loaded {len(rows)} risk assessments to BigQuery")
                return {'success': True, 'count': len(rows)}
                
        except Exception as e:
            logger.error(f"Error loading risks to BigQuery: {e}")
            return {'success': False, 'error': str(e)}
    
    def query_patients(self, limit: int = 10) -> List[Dict]:
        """Query patients from BigQuery"""
        query = f"""
            SELECT patient_id, first_name, last_name, gender, 
                   DATE(snapshot_date) as snapshot_date
            FROM `{self.project_id}.{self.dataset_id}.patients_snapshot`
            ORDER BY snapshot_date DESC
            LIMIT {limit}
        """
        
        try:
            results = self.client.query(query).result()
            rows = [dict(row) for row in results]
            logger.info(f"Queried {len(rows)} patients from BigQuery")
            return rows
        except Exception as e:
            logger.error(f"Error querying BigQuery: {e}")
            return []
