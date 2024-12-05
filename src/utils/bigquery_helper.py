# src/utils/bigquery_helper.py
from typing import Dict, List, Union
from google.cloud import bigquery
import structlog

logger = structlog.get_logger()

class BigQueryHelper:
    """Helper class for BigQuery operations."""
    
    def __init__(self, project_id: str, dataset_id: str):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.client = bigquery.Client(project=project_id)
        
        # Ensure dataset exists
        self._create_dataset_if_not_exists()

    def _create_dataset_if_not_exists(self):
        """Create dataset if it doesn't exist."""
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

    def load_json(
        self, 
        data: List[Dict], 
        table_id: str, 
        schema: List[Dict],
        write_disposition: str = 'WRITE_APPEND'
    ) -> None:
        """
        Load JSON data into BigQuery table.
        
        Args:
            data: List of dictionaries to load
            table_id: Target table ID
            schema: BigQuery table schema
            write_disposition: Write disposition (WRITE_APPEND, WRITE_TRUNCATE, WRITE_EMPTY)
        """
        try:
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

            # Convert data to newline-delimited JSON
            nl_json = '\n'.join(json.dumps(record) for record in data)

            # Load data
            table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"
            load_job = self.client.load_table_from_string(
                nl_json,
                table_ref,
                job_config=job_config
            )
            load_job.result()  # Wait for job to complete

            logger.info(
                "Successfully loaded data into BigQuery",
                table_id=table_id,
                row_count=len(data)
            )

        except Exception as e:
            logger.error(
                "Failed to load data into BigQuery",
                error=str(e),
                table_id=table_id
            )
            raise

    def execute_query(self, query: str) -> List[Dict]:
        """
        Execute BigQuery SQL query.
        
        Args:
            query: SQL query string
            
        Returns:
            List of dictionaries containing query results
        """
        try:
            query_job = self.client.query(query)
            results = query_job.result()
            
            return [dict(row.items()) for row in results]
            
        except Exception as e:
            logger.error(
                "Failed to execute BigQuery query",
                error=str(e),
                query=query
            )
            raise