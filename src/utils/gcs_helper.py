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

    def _get_or_create_bucket(self):
        try:
            return self.client.get_bucket(self.bucket_name)
        except exceptions.NotFound:
            bucket = self.client.create_bucket(self.bucket_name, location="US")
            logger.info("created GCS bucket", bucket_name=self.bucket_name)
            return bucket

    def upload_json(self, data: Union[List, Dict], blob_name: str) -> str:
        try:
            if isinstance(data, list):
                # Convert the list of dicts into NDJSON
                ndjson_str = "\n".join(json.dumps(record, ensure_ascii=False) for record in data)
            else:
                # Single dict, just dump as is (one line)
                ndjson_str = json.dumps(data, ensure_ascii=False)

            blob = self.bucket.blob(blob_name)
            blob.upload_from_string(ndjson_str, content_type='application/json')
            uri = f"gs://{self.bucket_name}/{blob_name}"
            # logger.info("Uploaded data to GCS", uri=uri, size=blob.size)
            return uri
        except Exception as e:
            logger.error("Failed to upload to GCS", error=str(e), blob_name=blob_name)
            raise