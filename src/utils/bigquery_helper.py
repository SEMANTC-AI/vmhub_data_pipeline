# src/utils/bigquery_helper.py
import json
import io
from typing import Dict, List, Union
from google.cloud import bigquery
import structlog

logger = structlog.get_logger()

class BigQueryHelper:
    def __init__(self, project_id: str, dataset_id: str):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.client = bigquery.Client(project=project_id)
        self._create_dataset_if_not_exists()

    def _create_dataset_if_not_exists(self):
        dataset_ref = self.client.dataset(self.dataset_id)
        try:
            self.client.get_dataset(dataset_ref)
        except Exception:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            self.client.create_dataset(dataset, exists_ok=True)
            logger.info(
                "Created BigQuery dataset",
                dataset_id=self.dataset_id
            )

    def get_existing_ids(self, table_id: str) -> set:
        """Get set of existing IDs from the table."""
        try:
            query = f"""
                SELECT DISTINCT id 
                FROM `{self.project_id}.{self.dataset_id}.{table_id}`
            """
            query_job = self.client.query(query)
            results = query_job.result()
            
            existing_ids = {row.id for row in results}
            
            logger.info(
                "Retrieved existing IDs",
                table_id=table_id,
                count=len(existing_ids)
            )
            
            return existing_ids
            
        except Exception as e:
            # If table doesn't exist, return empty set
            logger.info(
                "No existing IDs found (table might not exist)",
                table_id=table_id,
                error=str(e)
            )
            return set()

    def load_json(
        self, 
        data: List[Dict], 
        table_id: str, 
        schema: List[Dict],
        write_disposition: str = 'WRITE_APPEND'
    ) -> None:
        """Load JSON data into BigQuery table with deduplication."""
        try:
            # Get existing IDs
            existing_ids = self.get_existing_ids(table_id)
            
            # Filter out records with existing IDs
            new_records = [
                record for record in data 
                if record.get('id') not in existing_ids
            ]
            
            if not new_records:
                logger.info(
                    "No new records to insert",
                    table_id=table_id
                )
                return
                
            logger.info(
                "Found new records to insert",
                table_id=table_id,
                total_records=len(data),
                new_records=len(new_records)
            )

            # Configure job
            job_config = bigquery.LoadJobConfig(
                schema=[
                    bigquery.SchemaField(
                        name=field['name'],
                        field_type=field['type'],
                        mode=field['mode']
                    ) for field in schema
                ],
                write_disposition=write_disposition,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            )

            # Convert filtered data to newline-delimited JSON
            nl_json = '\n'.join(json.dumps(record) for record in new_records)

            # Load data
            table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"
            job = self.client.load_table_from_file(
                io.StringIO(nl_json),
                table_ref,
                job_config=job_config
            )
            job.result()  # Wait for job to complete

            logger.info(
                "Successfully loaded new data into BigQuery",
                table_id=table_id,
                row_count=len(new_records)
            )

        except Exception as e:
            logger.error(
                "Failed to load data into BigQuery",
                error=str(e),
                table_id=table_id
            )
            raise