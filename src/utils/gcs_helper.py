# src/utils/gcs_helper.py
import json
from typing import Dict, List, Union
from google.cloud import storage
import structlog

logger = structlog.get_logger()

class GCSHelper:
    """Helper class for Google Cloud Storage operations."""
    
    def __init__(self, project_id: str, bucket_name: str):
        self.project_id = project_id
        self.bucket_name = bucket_name
        self.client = storage.Client(project=project_id)
        self.bucket = self.client.bucket(bucket_name)

    def upload_json(self, data: Union[List, Dict], blob_name: str) -> str:
        """
        Upload JSON data to GCS.
        
        Args:
            data: JSON serializable data
            blob_name: Target blob name/path
            
        Returns:
            GCS URI of uploaded file
        """
        try:
            blob = self.bucket.blob(blob_name)
            blob.upload_from_string(
                json.dumps(data, ensure_ascii=False),
                content_type='application/json'
            )
            
            uri = f"gs://{self.bucket_name}/{blob_name}"
            logger.info(
                "Successfully uploaded data to GCS",
                uri=uri,
                size=blob.size
            )
            
            return uri
            
        except Exception as e:
            logger.error(
                "Failed to upload to GCS",
                error=str(e),
                blob_name=blob_name
            )
            raise

    def download_json(self, blob_name: str) -> Union[List, Dict]:
        """
        Download JSON data from GCS.
        
        Args:
            blob_name: Source blob name/path
            
        Returns:
            Parsed JSON data
        """
        try:
            blob = self.bucket.blob(blob_name)
            content = blob.download_as_string()
            data = json.loads(content)
            
            logger.info(
                "Successfully downloaded data from GCS",
                blob_name=blob_name
            )
            
            return data
            
        except Exception as e:
            logger.error(
                "Failed to download from GCS",
                error=str(e),
                blob_name=blob_name
            )
            raise