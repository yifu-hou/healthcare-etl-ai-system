import pandas as pd
from pathlib import Path
from typing import List, Dict
from utils import setup_logger

logger = setup_logger(__name__)

class CSVReader:
    """Read CSV files (lab results, appointments, conditions)"""
    
    def __init__(self, data_dir: str = "data/raw"):
        self.data_dir = Path(data_dir)
    
    def read_lab_results(self) -> List[Dict]:
        """Read lab results CSV"""
        file_path = self.data_dir / "lab_results.csv"
        
        try:
            df = pd.read_csv(file_path)
            logger.info(f"Loaded {len(df)} lab results from {file_path}")
            
            # Convert to list of dicts
            records = df.to_dict('records')
            return records
            
        except FileNotFoundError:
            logger.error(f"File not found: {file_path}")
            return []
        except Exception as e:
            logger.error(f"Error reading lab results: {e}")
            return []
    
    def read_appointments(self) -> List[Dict]:
        """Read appointments CSV"""
        file_path = self.data_dir / "appointments.csv"
        
        try:
            df = pd.read_csv(file_path)
            logger.info(f"Loaded {len(df)} appointments from {file_path}")
            return df.to_dict('records')
            
        except FileNotFoundError:
            logger.error(f"File not found: {file_path}")
            return []
        except Exception as e:
            logger.error(f"Error reading appointments: {e}")
            return []
    
    def read_conditions(self) -> List[Dict]:
        """Read conditions CSV"""
        file_path = self.data_dir / "conditions.csv"
        
        try:
            df = pd.read_csv(file_path)
            logger.info(f"Loaded {len(df)} conditions from {file_path}")
            return df.to_dict('records')
            
        except FileNotFoundError:
            logger.warning(f"File not found: {file_path} (this is optional)")
            return []
        except Exception as e:
            logger.error(f"Error reading conditions: {e}")
            return []