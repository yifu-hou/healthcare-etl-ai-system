import os
from typing import List, Dict, Any
from dotenv import load_dotenv
from google.cloud import bigquery
from utils import setup_logger

load_dotenv(override=True)
logger = setup_logger(__name__)

class BigQueryTool:
    """Tool for querying BigQuery analytics data"""
    
    def __init__(self):
        self.project_id = os.getenv('GCP_PROJECT_ID')
        self.dataset_id = os.getenv('BIGQUERY_DATASET')
        self.client = bigquery.Client(project=self.project_id)
        logger.info("Connected to BigQuery for AI agent queries")
    
    def get_patient_trends(self, patient_id: str) -> Dict[str, Any]:
        """Get historical trends for a patient"""
        query = f"""
            SELECT 
                event_type,
                event_date,
                event_value,
                event_status
            FROM `{self.project_id}.{self.dataset_id}.clinical_events`
            WHERE patient_id = '{patient_id}'
            ORDER BY event_date DESC
            LIMIT 50
        """
        
        try:
            results = self.client.query(query).result()
            events = [dict(row) for row in results]
            
            logger.info(f"Retrieved {len(events)} events for patient {patient_id}")
            return {
                'patient_id': patient_id,
                'events': events,
                'count': len(events)
            }
            
        except Exception as e:
            logger.error(f"Error getting patient trends: {e}")
            return {'error': str(e)}
    
    def get_risk_score_trends(self) -> List[Dict[str, Any]]:
        """Get risk score trends across all patients"""
        query = f"""
            SELECT 
                risk_level,
                COUNT(*) as patient_count,
                AVG(risk_score) as avg_score
            FROM `{self.project_id}.{self.dataset_id}.risk_scores_history`
            GROUP BY risk_level
            ORDER BY avg_score DESC
        """
        
        try:
            results = self.client.query(query).result()
            trends = [dict(row) for row in results]
            
            logger.info(f"Retrieved risk trends for {len(trends)} risk levels")
            return trends
            
        except Exception as e:
            logger.error(f"Error getting risk trends: {e}")
            return []
    
    def get_abnormal_test_statistics(self) -> List[Dict[str, Any]]:
        """Get statistics on abnormal test results"""
        query = f"""
            SELECT 
                JSON_EXTRACT_SCALAR(event_details, '$.test_type') as test_type,
                event_status,
                COUNT(*) as count
            FROM `{self.project_id}.{self.dataset_id}.clinical_events`
            WHERE event_type = 'LAB' AND event_status IN ('Abnormal', 'Critical')
            GROUP BY test_type, event_status
            ORDER BY count DESC
        """
        
        try:
            results = self.client.query(query).result()
            stats = [dict(row) for row in results]
            
            logger.info(f"Retrieved statistics for {len(stats)} test types")
            return stats
            
        except Exception as e:
            logger.error(f"Error getting test statistics: {e}")
            return []

if __name__ == "__main__":
    # Test the tool
    tool = BigQueryTool()
    
    print("Risk score trends:")
    trends = tool.get_risk_score_trends()
    for trend in trends:
        print(f"  {trend['risk_level']}: {trend['patient_count']} patients (avg score: {trend['avg_score']:.1f})")
    
    print("\nAbnormal test statistics:")
    stats = tool.get_abnormal_test_statistics()
    for stat in stats[:5]:
        print(f"  {stat.get('test_type', 'Unknown')}: {stat['count']} {stat['event_status']} results")