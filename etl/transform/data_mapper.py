from typing import Dict, List
from datetime import datetime
from utils import setup_logger

logger = setup_logger(__name__)

class DataMapper:
    """Map extracted data to Salesforce object schemas"""
    
    def map_patient_to_salesforce(self, patient: Dict) -> Dict:
        """Map patient data to Patient_Medical_Record__c fields"""
        try:
            mapped = {
                'Patient_ID__c': patient.get('patient_id', ''),
                'First_Name__c': patient.get('first_name', ''),
                'Last_Name__c': patient.get('last_name', ''),
                'Date_of_Birth__c': patient.get('date_of_birth', ''),
                'Gender__c': self._normalize_gender(patient.get('gender', '')),
                'Email__c': patient.get('email', ''),
                'Phone__c': patient.get('phone', ''),
                'Address__c': patient.get('address', '')
            }
            
            logger.debug(f"Mapped patient: {mapped['Patient_ID__c']}")
            return mapped
            
        except Exception as e:
            logger.error(f"Error mapping patient: {e}")
            return {}
    
    def map_lab_result_to_salesforce(self, lab: Dict, patient_sf_id: str = None) -> Dict:
        """Map lab result to Lab_Result__c fields"""
        try:
            mapped = {
                'patient_id': lab.get('patient_id', ''),
                'Test_Type__c': lab.get('test_type', ''),
                'Test_Value__c': float(lab.get('value', 0)),
                'Reference_Range__c': lab.get('reference_range', ''),
                'Test_Datetime__c': lab.get('test_datetime', ''),
                'Status__c': lab.get('status', 'Normal')
            }
            
            # Add patient lookup if provided
            if patient_sf_id:
                mapped['Patient__c'] = patient_sf_id
            
            logger.debug(f"Mapped lab result: {mapped['Test_Type__c']}")
            return mapped
            
        except Exception as e:
            logger.error(f"Error mapping lab result: {e}")
            return {}
    
    def map_multiple_patients(self, patients: List[Dict]) -> List[Dict]:
        """Map multiple patients"""
        mapped_patients = []
        
        for patient in patients:
            mapped = self.map_patient_to_salesforce(patient)
            if mapped:
                mapped_patients.append(mapped)
        
        logger.info(f"Mapped {len(mapped_patients)} patients")
        return mapped_patients
    
    def map_multiple_labs(self, labs: List[Dict]) -> List[Dict]:
        """Map multiple lab results"""
        mapped_labs = []
        
        for lab in labs:
            mapped = self.map_lab_result_to_salesforce(lab)
            if mapped and mapped.get('patient_id'):
                mapped_labs.append(mapped)
        
        logger.info(f"Mapped {len(mapped_labs)} lab results")
        return mapped_labs
    
    def _normalize_gender(self, gender: str) -> str:
        """Normalize gender values to match Salesforce picklist"""
        gender_lower = gender.lower()
        
        if gender_lower in ['male', 'm']:
            return 'Male'
        elif gender_lower in ['female', 'f']:
            return 'Female'
        else:
            return 'Other'
