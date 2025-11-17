import json
import os
from pathlib import Path
from typing import List, Dict
from utils import setup_logger

logger = setup_logger(__name__)

class FHIRParser:
    """Parse FHIR JSON patient files"""
    
    def __init__(self, data_dir: str = "data/raw/synthea_output"):
        self.data_dir = Path(data_dir)
        
    def read_patient_files(self) -> List[Dict]:
        """Read all patient JSON files from directory"""
        patients = []
        
        if not self.data_dir.exists():
            logger.error(f"Directory not found: {self.data_dir}")
            return patients
        
        json_files = list(self.data_dir.glob("*.json"))
        logger.info(f"Found {len(json_files)} patient files")
        
        for file_path in json_files:
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    
                    # Check if it's a FHIR Bundle
                    if data.get('resourceType') == 'Bundle':
                        # Extract patient from bundle entries
                        patient_resource = self._extract_patient_from_bundle(data)
                        if patient_resource:
                            patients.append(patient_resource)
                            logger.debug(f"Loaded patient from bundle: {file_path.name}")
                    elif data.get('resourceType') == 'Patient':
                        # Direct patient resource
                        patients.append(data)
                        logger.debug(f"Loaded patient: {file_path.name}")
                    else:
                        logger.warning(f"Unknown resource type in {file_path.name}: {data.get('resourceType')}")
                        
            except Exception as e:
                logger.error(f"Error reading {file_path}: {e}")
                
        logger.info(f"Successfully loaded {len(patients)} patients")
        return patients
    
    def _extract_patient_from_bundle(self, bundle: Dict) -> Dict:
        """Extract Patient resource from FHIR Bundle"""
        entries = bundle.get('entry', [])
        
        for entry in entries:
            resource = entry.get('resource', {})
            if resource.get('resourceType') == 'Patient':
                return resource
        
        return None
    
    def extract_patient_info(self, patient: Dict) -> Dict:
        """Extract relevant fields from FHIR patient resource"""
        try:
            # Extract name
            name_list = patient.get('name', [])
            if name_list:
                name = name_list[0]
                given = name.get('given', [])
                first_name = given[0] if given else ''
                last_name = name.get('family', '')
            else:
                first_name = ''
                last_name = ''
            
            # Extract address
            address_list = patient.get('address', [])
            if address_list:
                address = address_list[0]
                line = address.get('line', [])
                address_line = line[0] if line else ''
                city = address.get('city', '')
                state = address.get('state', '')
                postal_code = address.get('postalCode', '')
                full_address = f"{address_line}, {city}, {state} {postal_code}".strip(', ')
            else:
                full_address = ''
            
            # Extract telecom (phone/email)
            email = ''
            phone = ''
            for telecom in patient.get('telecom', []):
                system = telecom.get('system', '')
                value = telecom.get('value', '')
                
                if system == 'phone':
                    phone = value
                elif system == 'email':
                    email = value
            
            # Get patient ID
            patient_id = patient.get('id', '')
            
            # If no name found, try to extract from file or generate
            if not first_name and not last_name:
                # Try to get from text div if exists
                text = patient.get('text', {}).get('div', '')
                # For now, use patient ID as fallback
                logger.warning(f"No name found for patient {patient_id}, using ID as identifier")
            
            extracted = {
                'patient_id': patient_id,
                'first_name': first_name,
                'last_name': last_name,
                'date_of_birth': patient.get('birthDate', ''),
                'gender': patient.get('gender', '').capitalize() if patient.get('gender') else '',
                'email': email,
                'phone': phone,
                'address': full_address
            }
            
            logger.debug(f"Extracted patient: {extracted['patient_id']}")
            return extracted
            
        except Exception as e:
            logger.error(f"Error extracting patient info: {e}")
            return {}

    def parse_all_patients(self) -> List[Dict]:
        """Read and parse all patient files"""
        raw_patients = self.read_patient_files()
        parsed_patients = []
        
        for patient in raw_patients:
            extracted = self.extract_patient_info(patient)
            if extracted and extracted.get('patient_id'):  # Only add if has patient_id
                parsed_patients.append(extracted)
        
        logger.info(f"Parsed {len(parsed_patients)} patients")
        return parsed_patients

