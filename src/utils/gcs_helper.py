# src/utils/gcs_helper.py

from google.cloud import storage
from google.api_core import exceptions
import json
from typing import Dict, List, Union
import structlog
import fnmatch
import glob

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
                "created GCS bucket",
                bucket_name=self.bucket_name
            )
        return bucket

    def upload_json(self, data: Union[List, Dict], blob_name: str) -> str:
        """upload JSON data to GCS in newline-delimited format."""
        try:
            # Convert each record to newline-delimited JSON
            if isinstance(data, list):
                content = '\n'.join(json.dumps(record) for record in data)
            else:
                content = json.dumps(data)
                
            blob = self.bucket.blob(blob_name)
            blob.upload_from_string(
                content,
                content_type='application/json'
            )
            
            uri = f"gs://{self.bucket_name}/{blob_name}"
            logger.info(
                "successfully uploaded data to GCS",
                uri=uri,
                size=blob.size
            )
            return uri
            
        except Exception as e:
            logger.error(
                "failed to upload to GCS",
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
                "successfully downloaded data from GCS",
                blob_name=blob_name
            )
            return data
            
        except Exception as e:
            logger.error(
                "failed to download from GCS",
                error=str(e),
                blob_name=blob_name
            )
            raise

    def list_blobs_with_prefix(self, prefix: str) -> List[str]:
        """list all blob names with given prefix."""
        try:
            blobs = self.client.list_blobs(self.bucket, prefix=prefix)
            return [blob.name for blob in blobs]
        except Exception as e:
            logger.error(
                "failed to list blobs",
                error=str(e),
                prefix=prefix
            )
            raise

    def files_exist(self, prefix: str, pattern: str) -> bool:
        """check if files exist matching pattern under prefix."""
        try:
            blobs = self.bucket.list_blobs(prefix=prefix)
            for blob in blobs:
                # Check just the filename part against the pattern
                filename = blob.name.split('/')[-1]
                if fnmatch.fnmatch(filename, pattern):
                    logger.info(
                        "Found matching file in GCS",
                        blob_name=blob.name,
                        pattern=pattern
                    )
                    return True
            
            logger.warning(
                "no matching files found",
                prefix=prefix,
                pattern=pattern
            )
            return False
        except Exception as e:
            logger.error(
                "error checking files existence",
                error=str(e),
                prefix=prefix,
                pattern=pattern
            )
            raise

    def get_all_file_uris(self, prefix: str) -> List[str]:
        """get all file URIs under a prefix."""
        try:
            blobs = self.bucket.list_blobs(prefix=prefix)
            uris = [f"gs://{self.bucket_name}/{blob.name}" for blob in blobs]
            logger.info(
                "retrieved file URIs",
                prefix=prefix,
                file_count=len(uris)
            )
            return uris
        except Exception as e:
            logger.error(
                "failed to get file URIs",
                error=str(e),
                prefix=prefix
            )
            raise