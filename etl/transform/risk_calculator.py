from typing import Dict, List
from datetime import datetime
from utils import setup_logger

logger = setup_logger(__name__)

class RiskCalculator:
    """Calculate patient risk scores based on lab results and conditions"""
    
    def calculate_patient_risk(self, patient_id: str, lab_results: List[Dict], 
                               conditions: List[Dict] = None) -> Dict:
        """Calculate risk assessment for a patient"""
        
        risk_score = 0
        risk_factors = []
        
        # Analyze lab results
        patient_labs = [lab for lab in lab_results if lab.get('patient_id') == patient_id]
        
        for lab in patient_labs:
            test_type = lab.get('test_type', '')
            status = lab.get('status', 'Normal')
            value = lab.get('value', 0)
            
            # A1C risk scoring
            if test_type == 'A1C':
                if value > 6.5:
                    risk_score += 20
                    risk_factors.append(f"Elevated A1C: {value}")
                elif value > 5.7:
                    risk_score += 10
                    risk_factors.append(f"Pre-diabetic A1C: {value}")
            
            # Glucose risk scoring
            elif test_type == 'Glucose':
                if value > 140:
                    risk_score += 15
                    risk_factors.append(f"High glucose: {value}")
                elif value > 100:
                    risk_score += 5
                    risk_factors.append(f"Elevated glucose: {value}")
            
            # Cholesterol risk scoring
            elif test_type == 'Cholesterol':
                if value > 240:
                    risk_score += 15
                    risk_factors.append(f"High cholesterol: {value}")
                elif value > 200:
                    risk_score += 5
                    risk_factors.append(f"Elevated cholesterol: {value}")
            
            # General critical status
            if status == 'Critical':
                risk_score += 10
                risk_factors.append(f"Critical {test_type} result")
        
        # Analyze conditions if provided
        if conditions:
            patient_conditions = [c for c in conditions if c.get('patient_id') == patient_id]
            
            high_risk_conditions = ['Type 2 Diabetes', 'Hypertension', 'Hyperlipidemia']
            
            for condition in patient_conditions:
                condition_name = condition.get('condition', '')
                if condition_name in high_risk_conditions:
                    risk_score += 15
                    risk_factors.append(f"Chronic condition: {condition_name}")
        
        # Determine risk level
        if risk_score >= 50:
            risk_level = 'Critical'
        elif risk_score >= 30:
            risk_level = 'High'
        elif risk_score >= 15:
            risk_level = 'Medium'
        else:
            risk_level = 'Low'
        
        risk_assessment = {
            'patient_id': patient_id,
            'Risk_Level__c': risk_level,
            'Risk_Score__c': min(risk_score, 100),  # Cap at 100
            'Assessment_Date__c': datetime.now().strftime('%Y-%m-%d'),
            'Risk_Factors__c': '; '.join(risk_factors) if risk_factors else 'No significant risk factors'
        }
        
        logger.debug(f"Calculated risk for {patient_id}: {risk_level} ({risk_score})")
        return risk_assessment
    
    def calculate_all_patient_risks(self, patients: List[Dict], lab_results: List[Dict],
                                    conditions: List[Dict] = None) -> List[Dict]:
        """Calculate risk assessments for all patients"""
        risk_assessments = []
        
        for patient in patients:
            patient_id = patient.get('patient_id') or patient.get('Patient_ID__c')
            
            if patient_id:
                risk = self.calculate_patient_risk(patient_id, lab_results, conditions)
                risk_assessments.append(risk)
        
        logger.info(f"Calculated {len(risk_assessments)} risk assessments")
        return risk_assessments
    
