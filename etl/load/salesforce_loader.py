import os
from typing import List, Dict, Tuple
from dotenv import load_dotenv
from simple_salesforce import Salesforce
from utils import setup_logger

load_dotenv(override=True)
logger = setup_logger(__name__)

class SalesforceLoader:
    """Load data into Salesforce via REST API"""
    
    def __init__(self):
        """Initialize Salesforce connection"""
        try:
            self.sf = Salesforce(
                os.getenv('SALESFORCE_USERNAME'),
                os.getenv('SALESFORCE_PASSWORD'),
                os.getenv('SALESFORCE_SECURITY_TOKEN'),
                os.getenv('SALESFORCE_DOMAIN')
            )
            logger.info(f"Connected to Salesforce: {self.sf.sf_instance}")
        except Exception as e:
            logger.error(f"Failed to connect to Salesforce: {e}")
            raise
    
    def upsert_patient(self, patient_data: Dict) -> Tuple[bool, str, str]:
        """
        Upsert a single patient record
        Returns: (success, salesforce_id, message)
        """
        try:
            # Extract Patient_ID for upsert key
            patient_id = patient_data.get('Patient_ID__c')

            if not patient_id:
                return False, None, "Missing Patient_ID__c"
            
            # Create a copy without Patient_ID__c for the upsert
            upsert_data = {k:v for k, v in patient_data.items() if k != 'Patient_ID__c'}
            
            # Upsert using Patient_ID__c as external ID
            result = self.sf.Patient_Medical_Record__c.upsert(
                f"Patient_ID__c/{patient_id}",
                upsert_data
            )

            # Handle different result formats 
            # result variants: int (200/201), dict with 'id', dict with 'created' 
            sf_id = None 

            # 1) New record created or existing updated (result is dict)
            if isinstance(result, dict):

                sf_id = result.get('id')
                if not sf_id:
                    # Try to query to get the ID 
                    query = f"SELECT Id FROM Patient_Medical_Record__c WHERE Patient_ID__c = '{patient_id}' LIMIT 1"
                    query_result = self.sf.query(query)
                    if query_result['records']:
                        sf_id = query_result['records'][0]['Id']
            
            # 2) HTTP status code returned (result is a numeric code)
            elif isinstance(result, int):
                query = f"SELECT Id FROM Patient_Medical_Record__c WHERE Patient_ID__c = '{patient_id}' LIMIT 1" 
                query_result = self.sf.query(query)
                if query_result['records']:
                    sf_id = query_result['records'][0]['Id']
                else:
                    sf_id = str(result)

            # 3) Unknown format returned (result is unrecognizable, raise error)
            if not sf_id: 
                logger.error(f"Could not extract Salesforce ID for patient {patient_id}")
                return False, None, f"Could not extract SF ID, result type: {type(result)}"
   
            logger.debug(f"Upserted patient {patient_data['Patient_ID__c']}: {sf_id}")
            return True, sf_id, "Success"
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error upserting patient {patient_data.get('Patient_ID__c')}: {error_msg}")
            return False, None, error_msg
    
    def upsert_patients_batch(self, patients: List[Dict]) -> Dict:
        """
        Upsert multiple patients
        Returns: Summary with success/failure counts
        """
        results = {
            'total': len(patients),
            'success': 0,
            'failed': 0,
            'patient_id_map': {},  # Map patient_id to Salesforce ID
            'errors': []
        }
        
        logger.info(f"Starting batch upsert of {len(patients)} patients")
        
        for patient in patients:
            patient_id = patient.get('Patient_ID__c')
            success, sf_id, message = self.upsert_patient(patient)
            
            if success:
                results['success'] += 1
                results['patient_id_map'][patient_id] = sf_id
            else:
                results['failed'] += 1
                results['errors'].append({
                    'patient_id': patient_id,
                    'error': message
                })
        
        logger.info(f"Batch upsert complete: {results['success']} success, {results['failed']} failed")
        return results
    
    def insert_lab_result(self, lab_data: Dict, patient_sf_id: str) -> Tuple[bool, str]:
        """
        Insert a single lab result
        Returns: (success, message)
        """
        try:
            # Create clean data with only valid Salesforce fields
            clean_data = {
                'Patient__c': patient_sf_id,
                'Test_Type_c': lab_data.get('Test_Type__c'),
                'Test_Value__c': lab_data.get('Test_Value__c'),
                'Reference_Range__c': lab_data.get('Reference_Range__c'),
                'Test_Datetime__c': lab_data.get('Test_Datetime__c'),
                'Status__c': lab_data.get('Status__c')
            }

            # Remove None values 
            clean_data = {k: v for k, v in clean_data.items() if k is not None}
            
            result = self.sf.Lab_Result__c.create(lab_data)
            
            logger.debug(f"Inserted lab result for patient {patient_sf_id}")
            return True, "Success"
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error inserting lab result: {error_msg}")
            return False, error_msg
    
    def insert_lab_results_batch(self, lab_results: List[Dict], 
                                  patient_id_map: Dict) -> Dict:
        """
        Insert multiple lab results
        patient_id_map: Maps patient_id to Salesforce ID
        """
        results = {
            'total': len(lab_results),
            'success': 0,
            'failed': 0,
            'errors': []
        }
        
        logger.info(f"Starting batch insert of {len(lab_results)} lab results")
        
        for lab in lab_results:
            # Get patient's Salesforce ID
            patient_id = lab.get('patient_id')
            patient_sf_id = patient_id_map.get(patient_id)
            
            if not patient_sf_id:
                results['failed'] += 1
                results['errors'].append({
                    'patient_id': patient_id,
                    'error': 'Patient Salesforce ID not found'
                })
                continue

            # Extract only the fields we need (remove patient_id and other raw fields)
            lab_clean = {
                'Test_Type__c': lab.get('Test_Type__c'),
                'Test_Value__c': lab.get('Test_Value__c'),
                'Reference_Range__c': lab.get('Reference_Range__c'),
                'Test_Datetime__c': lab.get('Test_Datetime__c'),
                'Status__c': lab.get('Status__c')
            }
            
            success, message = self.insert_lab_result(lab_clean, patient_sf_id)
            
            if success:
                results['success'] += 1
            else:
                results['failed'] += 1
                results['errors'].append({
                    'patient_id': patient_id,
                    'error': message
                })
        
        logger.info(f"Batch insert complete: {results['success']} success, {results['failed']} failed")
        return results
    
    def insert_risk_assessment(self, risk_data: Dict, patient_sf_id: str) -> Tuple[bool, str]:
        """Insert a single risk assessment"""
        try:
            clean_data = {
                'Patient__c': patient_sf_id,
                'Risk_Level__c': risk_data.get('Risk_Level__c'),
                'Risk_Score__c': risk_data.get('Risk_Score__c'),
                'Assessment_Date__c': risk_data.get('Assessment_Date__c'),
                'Risk_Factors__c': risk_data.get('Risk_Factors__c')
            }

            # Remove None Values 
            clean_data = {k: v for k, v in clean_data.items() if k is not None}

            result = self.sf.Risk_Assessment__c.create(clean_data)

            logger.debug(f"Inserted risk assessment for patient {patient_sf_id}")
            return True, "Success"
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error inserting risk assessment: {error_msg}")
            return False, error_msg
    
    def insert_risk_assessments_batch(self, risk_assessments: List[Dict],
                                      patient_id_map: Dict) -> Dict:
        """Insert multiple risk assessments"""
        results = {
            'total': len(risk_assessments),
            'success': 0,
            'failed': 0,
            'errors': []
        }
        
        logger.info(f"Starting batch insert of {len(risk_assessments)} risk assessments")
        
        for risk in risk_assessments:
            patient_id = risk.get('patient_id')
            patient_sf_id = patient_id_map.get(patient_id)
            
            if not patient_sf_id:
                results['failed'] += 1
                results['errors'].append({
                    'patient_id': patient_id,
                    'error': 'Patient Salesforce ID not found'
                })
                continue

            # Extract only the fields we need (remove patient_id)
            risk_clean = {
                'Risk_Level__c': risk.get('Risk_Level__c'),
                'Risk_Score__c': risk.get('Risk_Score__c'),
                'Assessment_Date__c': risk.get('Assessment_Date__c'),
                'Risk_Factors__c': risk.get('Risk_Factors__c')
            }
            
            success, message = self.insert_risk_assessment(risk, patient_sf_id)
            
            if success:
                results['success'] += 1
            else:
                results['failed'] += 1
                results['errors'].append({
                    'patient_id': patient_id,
                    'error': message
                })
        
        logger.info(f"Batch insert complete: {results['success']} success, {results['failed']} failed")
        return results
    
    def query_patients(self, limit: int = 100) -> List[Dict]:
        """Query patients from Salesforce"""
        try:
            query = f"""
                SELECT Id, Patient_ID__c, First_Name__c, Last_Name__c, 
                       Date_of_Birth__c, Gender__c, Email__c, Phone__c, Address__c
                FROM Patient_Medical_Record__c
                LIMIT {limit}
            """
            
            results = self.sf.query(query)
            records = results['records']
            
            logger.info(f"Queried {len(records)} patients from Salesforce")
            return records
            
        except Exception as e:
            logger.error(f"Error querying patients: {e}")
            return []
