from typing import Dict, List, Tuple
from datetime import datetime
from utils import setup_logger

logger = setup_logger(__name__)

class DataValidator:
    """Validate data before loading to Salesforce"""
    
    def validate_patient(self, patient: Dict) -> Tuple[bool, List[str]]:
        """Validate patient record"""
        errors = []
        
        # Required fields
        if not patient.get('Patient_ID__c'):
            errors.append("Missing Patient_ID__c")
        
        if not patient.get('First_Name__c'):
            errors.append("Missing First_Name__c")
        
        if not patient.get('Last_Name__c'):
            errors.append("Missing Last_Name__c")
        
        # Validate date format
        dob = patient.get('Date_of_Birth__c')
        if dob:
            try:
                datetime.strptime(dob, '%Y-%m-%d')
            except ValueError:
                errors.append(f"Invalid date format for Date_of_Birth__c: {dob}")
        
        # Validate gender
        gender = patient.get('Gender__c')
        if gender and gender not in ['Male', 'Female', 'Other']:
            errors.append(f"Invalid Gender__c value: {gender}")
        
        # Validate email format (basic check)
        email = patient.get('Email__c')
        if email and '@' not in email:
            errors.append(f"Invalid Email__c format: {email}")
        
        is_valid = len(errors) == 0
        
        if not is_valid:
            logger.warning(f"Patient validation failed: {errors}")
        
        return is_valid, errors
    
    def validate_lab_result(self, lab: Dict) -> Tuple[bool, List[str]]:
        """Validate lab result record"""
        errors = []
        
        # Required fields
        if not lab.get('Test_Type__c'):
            errors.append("Missing Test_Type__c")
        
        # Validate numeric value
        try:
            value = lab.get('Test_Value__c')
            if value is not None:
                float(value)
        except (ValueError, TypeError):
            errors.append(f"Invalid Test_Value__c: {lab.get('Test_Value__c')}")
        
        # Validate date
        test_date = lab.get('Test_Date__c')
        if test_date:
            try:
                datetime.strptime(test_date, '%Y-%m-%d')
            except ValueError:
                errors.append(f"Invalid date format for Test_Date__c: {test_date}")
        
        # Validate status
        status = lab.get('Status__c')
        if status and status not in ['Normal', 'Abnormal', 'Critical']:
            errors.append(f"Invalid Status__c value: {status}")
        
        is_valid = len(errors) == 0
        
        if not is_valid:
            logger.warning(f"Lab result validation failed: {errors}")
        
        return is_valid, errors
    
    def validate_patients_batch(self, patients: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """Validate batch of patients, return valid and invalid records"""
        valid = []
        invalid = []
        
        for patient in patients:
            is_valid, errors = self.validate_patient(patient)
            if is_valid:
                valid.append(patient)
            else:
                patient['_validation_errors'] = errors
                invalid.append(patient)
        
        logger.info(f"Validated patients: {len(valid)} valid, {len(invalid)} invalid")
        return valid, invalid
    
    def validate_labs_batch(self, labs: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """Validate batch of lab results"""
        valid = []
        invalid = []
        
        for lab in labs:
            is_valid, errors = self.validate_lab_result(lab)
            if is_valid:
                valid.append(lab)
            else:
                lab['_validation_errors'] = errors
                invalid.append(lab)
        
        logger.info(f"Validated lab results: {len(valid)} valid, {len(invalid)} invalid")
        return valid, invalid
    
