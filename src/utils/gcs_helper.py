# src/utils/gcs_helper.py
from google.cloud import storage
from google.api_core import exceptions
import json
from typing import Dict, List, Union
import structlog

logger = structlog.get_logger()

class GCSHelper:
    def __init__(self, project_id: str, bucket_name: str):
        self.project_id = project_id
        self.bucket_name = bucket_name
        self.client = storage.Client(project=project_id)
        self.bucket = self._get_or_create_bucket()

    def _get_or_create_bucket(self) -> storage.Bucket:
        try:
            bucket = self.client.get_bucket(self.bucket_name)
        except exceptions.NotFound:
            bucket = self.client.create_bucket(
                self.bucket_name,
                location="US"
            )
            logger.info(
                "Created GCS bucket",
                bucket_name=self.bucket_name
            )
        return bucket

    def upload_json(self, data: Union[List, Dict], blob_name: str) -> str:
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