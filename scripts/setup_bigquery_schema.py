import sys
from pathlib import Path
import os
from dotenv import load_dotenv
from google.cloud import bigquery

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils import setup_logger

load_dotenv(override=True)
logger = setup_logger(__name__)

class BigQuerySetup:
    """Set up BigQuery dataset and tables"""
    
    def __init__(self):
        self.project_id = os.getenv('GCP_PROJECT_ID')
        self.dataset_id = os.getenv('BIGQUERY_DATASET')
        self.client = bigquery.Client(project=self.project_id)
        
    def create_dataset(self):
        """
        Create BigQuery dataset 
            if it doesn't exist: create dataset 
            if it exists: return the dataset 
        """
        dataset_ref = f"{self.project_id}.{self.dataset_id}"
        
        try:
            self.client.get_dataset(dataset_ref)
            logger.info(f"Dataset {dataset_ref} already exists")
            return dataset 
        except Exception as e:
            try: 
                dataset = bigquery.Dataset(dataset_ref)
                dataset.location = "US"
                dataset = self.client.create_dataset(dataset)
                logger.info(f"Created dataset {dataset_ref}")
                return dataset 
            except Exception as create_error:
                if "Already Exists" in str(create_error):
                    logger.info(f'Dataset {dataset_ref} already exists (created currently)')
                    return self.client.get_dataset(dataset_ref)
                else:
                    logger.error(f"Error creating dataset: {create_error}")
                    raise 
    
    def create_patients_table(self):
        """Create patients snapshot table"""
        table_id = f"{self.project_id}.{self.dataset_id}.patients_snapshot"
        
        schema = [
            bigquery.SchemaField("patient_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("salesforce_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("first_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("last_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("date_of_birth", "DATE", mode="NULLABLE"),
            bigquery.SchemaField("gender", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("email", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("phone", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("address", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("snapshot_date", "TIMESTAMP", mode="REQUIRED"),
        ]
        
        table = bigquery.Table(table_id, schema=schema)
        
        try:
            table = self.client.create_table(table)
            logger.info(f"Created table {table_id}")
        except Exception as e:
            if "Already Exists" in str(e):
                logger.info(f"Table {table_id} already exists")
            else:
                logger.error(f"Error creating table: {e}")
                raise
    
    def create_clinical_events_table(self):
        """Create clinical events table (labs, appointments, etc.)"""
        table_id = f"{self.project_id}.{self.dataset_id}.clinical_events"
        
        schema = [
            bigquery.SchemaField("event_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("patient_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("event_type", "STRING", mode="REQUIRED"),  # LAB, APPOINTMENT, etc.
            bigquery.SchemaField("event_date", "DATE", mode="NULLABLE"),
            bigquery.SchemaField("event_value", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("event_status", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("event_details", "JSON", mode="NULLABLE"),
            bigquery.SchemaField("created_timestamp", "TIMESTAMP", mode="REQUIRED"),
        ]
        
        table = bigquery.Table(table_id, schema=schema)
        
        try:
            table = self.client.create_table(table)
            logger.info(f"Created table {table_id}")
        except Exception as e:
            if "Already Exists" in str(e):
                logger.info(f"Table {table_id} already exists")
            else:
                logger.error(f"Error creating table: {e}")
                raise
    
    def create_risk_scores_table(self):
        """Create risk scores history table"""
        table_id = f"{self.project_id}.{self.dataset_id}.risk_scores_history"
        
        schema = [
            bigquery.SchemaField("patient_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("risk_level", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("risk_score", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("risk_factors", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("assessment_date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("created_timestamp", "TIMESTAMP", mode="REQUIRED"),
        ]
        
        table = bigquery.Table(table_id, schema=schema)
        
        try:
            table = self.client.create_table(table)
            logger.info(f"Created table {table_id}")
        except Exception as e:
            if "Already Exists" in str(e):
                logger.info(f"Table {table_id} already exists")
            else:
                logger.error(f"Error creating table: {e}")
                raise
    
    def create_all_tables(self):
        """Create all BigQuery tables"""
        logger.info("="*60)
        logger.info("SETTING UP BIGQUERY SCHEMA")
        logger.info("="*60)
        
        self.create_dataset()
        self.create_patients_table()
        self.create_clinical_events_table()
        self.create_risk_scores_table()
        
        logger.info("="*60)
        logger.info("BIGQUERY SETUP COMPLETE")
        logger.info("="*60)
        
        # List tables
        tables = list(self.client.list_tables(f"{self.project_id}.{self.dataset_id}"))
        logger.info(f"\nCreated {len(tables)} tables:")
        for table in tables:
            logger.info(f"  - {table.table_id}")

if __name__ == "__main__":
    setup = BigQuerySetup()
    setup.create_all_tables()

