import os
from typing import List, Dict, Any
from dotenv import load_dotenv
from salesforce.oauth_client import get_salesforce_oauth_connection
from utils import setup_logger

load_dotenv(override=True)
logger = setup_logger(__name__)

class SalesforceTool:
    """Tool for querying Salesforce data"""
    
    def __init__(self):
        self.sf = get_salesforce_oauth_connection()
        logger.info("Connected to Salesforce for AI agent queries")
        
    def get_high_risk_patients(self) -> List[Dict[str, Any]]:
        """Get patients with high or critical risk levels"""
        query = """
            SELECT 
                Id,
                Patient__c,
                Risk_Level__c,
                Risk_Score__c,
                Risk_Factors__c,
                Assessment_Date__c
            FROM Risk_Assessment__c
            WHERE Risk_Level__c IN ('High', 'Critical')
            ORDER BY Risk_Score__c DESC
            LIMIT 20
        """
        
        try:
            result = self.sf.query(query)
            
            # Get unique patient IDs
            patient_ids = list(set([r.get('Patient__c') for r in result['records'] if r.get('Patient__c')]))
            
            # Fetch patient names separately
            patient_map = {}
            if patient_ids:
                # Build query with patient IDs
                patient_query = f"""
                    SELECT Id, Patient_ID__c, First_Name__c, Last_Name__c
                    FROM Patient_Medical_Record__c
                    WHERE Id IN ({','.join(["'" + pid + "'" for pid in patient_ids])})
                """
                patient_result = self.sf.query(patient_query)
                
                for p in patient_result['records']:
                    patient_map[p['Id']] = {
                        'patient_id': p.get('Patient_ID__c'),
                        'first_name': p.get('First_Name__c', ''),
                        'last_name': p.get('Last_Name__c', '')
                    }
            
            # Combine data
            patients = []
            for record in result['records']:
                patient_sf_id = record.get('Patient_Medical_Record__c')
                patient_info = patient_map.get(patient_sf_id, {})
                
                patients.append({
                    'patient_id': patient_info.get('patient_id', 'Unknown'),
                    'name': f"{patient_info.get('first_name', '')} {patient_info.get('last_name', '')}".strip(),
                    'risk_level': record.get('Risk_Level__c'),
                    'risk_score': record.get('Risk_Score__c'),
                    'risk_factors': record.get('Risk_Factors__c'),
                    'assessment_date': record.get('Assessment_Date__c')
                })
            
            logger.info(f"Found {len(patients)} high-risk patients")
            return patients
            
        except Exception as e:
            logger.error(f"Error querying high-risk patients: {e}")
            return []
    
    def get_abnormal_lab_results(self, test_type: str = None) -> List[Dict[str, Any]]:
        """Get abnormal or critical lab results"""
        where_clause = "WHERE Status__c IN ('Abnormal', 'Critical')"
        if test_type:
            where_clause += f" AND Test_Type__c = '{test_type}'"
        
        query = f"""
            SELECT 
                Id,
                Patient_Medical_Record__c,
                Test_Type__c,
                Test_Value__c,
                Reference_Range__c,
                Status__c,
                Test_Datetime__c
            FROM Lab_Result__c
            {where_clause}
            ORDER BY Test_Datetime__c DESC
            LIMIT 50
        """
        
        try:
            result = self.sf.query(query)
            labs = []
            
            # Get unique patient IDs
            patient_ids = list(set([r.get('Patient_Medical_Record__c') for r in result['records'] if r.get('Patient_Medical_Record__c')]))
            
            # Fetch patient names
            patient_map = {}
            if patient_ids:
                patient_query = f"""
                    SELECT Id, Patient_ID__c, First_Name__c, Last_Name__c
                    FROM Patient_Medical_Record__c
                    WHERE Id IN ({','.join(["'" + pid + "'" for pid in patient_ids])})
                """
                patient_result = self.sf.query(patient_query)
                for p in patient_result['records']:
                    patient_map[p['Id']] = {
                        'patient_id': p.get('Patient_ID__c'),
                        'name': f"{p.get('First_Name__c', '')} {p.get('Last_Name__c', '')}"
                    }
            
            for record in result['records']:
                patient_sf_id = record.get('Patient_Medical_Record__c')
                patient_info = patient_map.get(patient_sf_id, {})
                
                labs.append({
                    'patient_id': patient_info.get('patient_id', 'Unknown'),
                    'name': patient_info.get('name', 'Unknown'),
                    'test_type': record.get('Test_Type__c'),
                    'value': record.get('Test_Value__c'),
                    'reference_range': record.get('Reference_Range__c'),
                    'status': record.get('Status__c'),
                    'test_datetime': record.get('Test_Datetime__c')
                })
            
            logger.info(f"Found {len(labs)} abnormal lab results")
            return labs
            
        except Exception as e:
            logger.error(f"Error querying lab results: {e}")
            return []
    
    def get_patient_summary(self, patient_id: str) -> Dict[str, Any]:
        """Get comprehensive patient information"""
        try:
            # Get patient info
            patient_query = f"""
                SELECT Id, Patient_ID__c, First_Name__c, Last_Name__c, 
                       Date_of_Birth__c, Gender__c, Email__c, Phone__c
                FROM Patient_Medical_Record__c
                WHERE Patient_ID__c = '{patient_id}'
                LIMIT 1
            """
            patient_result = self.sf.query(patient_query)
            
            if not patient_result['records']:
                return {'error': f'Patient {patient_id} not found'}
            
            patient = patient_result['records'][0]
            sf_patient_id = patient['Id']
            
            # Get recent labs
            labs_query = f"""
                SELECT Test_Type__c, Test_Value__c, Status__c, Test_Datetime__c
                FROM Lab_Result__c
                WHERE Patient_Medical_Record__c = '{sf_patient_id}'
                ORDER BY Test_Datetime__c DESC
                LIMIT 10
            """
            labs_result = self.sf.query(labs_query)
            
            # Get risk assessment
            risk_query = f"""
                SELECT Risk_Level__c, Risk_Score__c, Risk_Factors__c, Assessment_Date__c
                FROM Risk_Assessment__c
                WHERE Patient__c = '{sf_patient_id}'
                ORDER BY Assessment_Date__c DESC
                LIMIT 1
            """
            risk_result = self.sf.query(risk_query)
            
            summary = {
                'patient_id': patient.get('Patient_ID__c'),
                'name': f"{patient.get('First_Name__c', '')} {patient.get('Last_Name__c', '')}",
                'date_of_birth': patient.get('Date_of_Birth__c'),
                'gender': patient.get('Gender__c'),
                'contact': {
                    'email': patient.get('Email__c'),
                    'phone': patient.get('Phone__c')
                },
                'recent_labs': [
                    {
                        'test': lab.get('Test_Type__c'),
                        'value': lab.get('Test_Value__c'),
                        'status': lab.get('Status__c'),
                        'date': lab.get('Test_Datetime__c')
                    }
                    for lab in labs_result['records']
                ],
                'risk_assessment': {}
            }
            
            if risk_result['records']:
                risk = risk_result['records'][0]
                summary['risk_assessment'] = {
                    'level': risk.get('Risk_Level__c'),
                    'score': risk.get('Risk_Score__c'),
                    'factors': risk.get('Risk_Factors__c'),
                    'date': risk.get('Assessment_Date__c')
                }
            
            logger.info(f"Retrieved summary for patient {patient_id}")
            return summary
            
        except Exception as e:
            logger.error(f"Error getting patient summary: {e}")
            return {'error': str(e)}
    
    def search_patients(self, criteria: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Search patients by various criteria"""
        where_conditions = []
        
        if criteria.get('risk_level'):
            # This requires a join - simplified version
            pass
        
        if criteria.get('gender'):
            where_conditions.append(f"Gender__c = '{criteria['gender']}'")
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        query = f"""
            SELECT Patient_ID__c, First_Name__c, Last_Name__c, 
                   Date_of_Birth__c, Gender__c
            FROM Patient_Medical_Record__c
            {where_clause}
            LIMIT 20
        """
        
        try:
            result = self.sf.query(query)
            patients = []
            
            for record in result['records']:
                patients.append({
                    'patient_id': record.get('Patient_ID__c'),
                    'name': f"{record.get('First_Name__c', '')} {record.get('Last_Name__c', '')}",
                    'date_of_birth': record.get('Date_of_Birth__c'),
                    'gender': record.get('Gender__c')
                })
            
            logger.info(f"Found {len(patients)} patients matching criteria")
            return patients
            
        except Exception as e:
            logger.error(f"Error searching patients: {e}")
            return []

if __name__ == "__main__":
    # Test the tool
    tool = SalesforceTool()
    
    print("High-risk patients:")
    high_risk = tool.get_high_risk_patients()
    for patient in high_risk[:3]:
        print(f"  - {patient['name']}: {patient['risk_level']} (score: {patient['risk_score']})")
    
    print("\nAbnormal A1C results:")
    abnormal_a1c = tool.get_abnormal_lab_results('A1C')
    for lab in abnormal_a1c[:3]:
        print(f"  - {lab['name']}: {lab['value']} ({lab['status']})")
    