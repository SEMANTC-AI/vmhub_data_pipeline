# src/utils/gcs_helper.py

from google.cloud import storage
from google.api_core import exceptions
import json
from typing import Dict, List, Union, Optional
import structlog
import fnmatch
from datetime import datetime
import pytz

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
        except exceptions.GoogleAPIError as e:
            logger.error(
                "GCS API error when accessing bucket",
                error=str(e),
                bucket_name=self.bucket_name
            )
            raise
        return bucket

    def upload_json(self, data: Union[List, Dict], blob_name: str) -> str:
        """Upload JSON data to GCS in newline-delimited format."""
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
                "Successfully uploaded data to GCS",
                uri=uri,
                size=blob.size
            )
            return uri
            
        except exceptions.GoogleAPIError as e:
            logger.error(
                "GCS API error during upload",
                error=str(e),
                blob_name=blob_name
            )
            raise
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
            
        except exceptions.NotFound:
            logger.error(
                "Blob not found in GCS",
                blob_name=blob_name
            )
            raise
        except exceptions.GoogleAPIError as e:
            logger.error(
                "GCS API error during download",
                error=str(e),
                blob_name=blob_name
            )
            raise
        except Exception as e:
            logger.error(
                "Failed to download from GCS",
                error=str(e),
                blob_name=blob_name
            )
            raise

    def list_blobs_with_prefix(self, prefix: str) -> List[str]:
        """List all blob names with the given prefix."""
        try:
            blobs = self.client.list_blobs(self.bucket, prefix=prefix)
            blob_names = [blob.name for blob in blobs]
            logger.info(
                "Listed blobs with prefix",
                prefix=prefix,
                count=len(blob_names)
            )
            return blob_names
        except exceptions.GoogleAPIError as e:
            logger.error(
                "GCS API error during listing blobs",
                error=str(e),
                prefix=prefix
            )
            raise
        except Exception as e:
            logger.error(
                "Failed to list blobs",
                error=str(e),
                prefix=prefix
            )
            raise

    def files_exist(self, prefix: str, pattern: str) -> bool:
        """Check if files exist matching pattern under prefix."""
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
                "No matching files found",
                prefix=prefix,
                pattern=pattern
            )
            return False
        except exceptions.GoogleAPIError as e:
            logger.error(
                "GCS API error during file existence check",
                error=str(e),
                prefix=prefix,
                pattern=pattern
            )
            raise
        except Exception as e:
            logger.error(
                "Error checking files existence",
                error=str(e),
                prefix=prefix,
                pattern=pattern
            )
            raise

    def get_all_file_uris(self, prefix: str) -> List[str]:
        """Get all file URIs under a prefix."""
        try:
            blobs = self.bucket.list_blobs(prefix=prefix)
            uris = [f"gs://{self.bucket_name}/{blob.name}" for blob in blobs]
            logger.info(
                "Retrieved file URIs",
                prefix=prefix,
                file_count=len(uris)
            )
            return uris
        except exceptions.GoogleAPIError as e:
            logger.error(
                "GCS API error during getting file URIs",
                error=str(e),
                prefix=prefix
            )
            raise
        except Exception as e:
            logger.error(
                "Failed to get file URIs",
                error=str(e),
                prefix=prefix
            )
            raise

    def get_latest_processed_date(self, endpoint: str, cnpj: str) -> Optional[datetime]:
        """Retrieve the latest processed date for a given endpoint and CNPJ."""
        prefix = f"CNPJ_{cnpj}/{endpoint}/"
        try:
            blobs = self.client.list_blobs(self.bucket, prefix=prefix)
            dates = []
            for blob in blobs:
                # expecting blobs like CNPJ_48986168000144/vendas/20230717/response_pg0.json
                parts = blob.name.split('/')
                if len(parts) >= 3:
                    date_str = parts[2]
                    try:
                        date = datetime.strptime(date_str, '%Y%m%d').replace(tzinfo=pytz.UTC)
                        dates.append(date)
                    except ValueError:
                        logger.warning("invalid date format in blob name", blob_name=blob.name)
            if dates:
                latest_date = max(dates)
                logger.info("latest processed date found", latest_date=latest_date.isoformat())
                return latest_date
            else:
                logger.info("no processed dates found for endpoint", endpoint=endpoint)
                return None
        except exceptions.GoogleAPIError as e:
            logger.error(
                "GCS API error during retrieving latest processed date",
                error=str(e),
                endpoint=endpoint,
                cnpj=cnpj
            )
            raise
        except Exception as e:
            logger.error(
                "Failed to retrieve latest processed date",
                error=str(e),
                endpoint=endpoint,
                cnpj=cnpj
            )
            raise