# src/utils/bigquery_helper.py

from typing import Dict, List
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
            logger.info("BigQuery dataset exists", dataset_id=self.dataset_id)
        except bigquery.NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            self.client.create_dataset(dataset, exists_ok=True)
            logger.info("created BigQuery dataset", dataset_id=self.dataset_id)
        except Exception as e:
            logger.error(
                "failed to access or create BigQuery dataset",
                error=str(e),
                dataset_id=self.dataset_id
            )
            raise

    def _create_schema_field(self, field_def: Dict) -> bigquery.SchemaField:
        field_name = field_def['name']
        field_type = field_def['type']
        field_mode = field_def.get('mode', 'NULLABLE')

        if field_type == 'RECORD' and 'fields' in field_def:
            sub_fields = [self._create_schema_field(f) for f in field_def['fields']]
            return bigquery.SchemaField(name=field_name, field_type=field_type, mode=field_mode, fields=sub_fields)
        else:
            return bigquery.SchemaField(name=field_name, field_type=field_type, mode=field_mode)

    def load_data_from_gcs(self, table_id: str, schema: List[Dict], source_uris: List[str]):
        """load data from GCS files into BigQuery table."""
        try:
            full_table_id = f"{self.project_id}.{self.dataset_id}.{table_id}"
            
            # Create schema
            bq_schema = [self._create_schema_field(field) for field in schema]
            
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                schema=bq_schema,
                write_disposition="WRITE_TRUNCATE",
                ignore_unknown_values=True
            )

            # # log the URIs we're loading from
            # logger.info(
            #     "starting BigQuery load",
            #     table_id=full_table_id,
            #     source_uris=source_uris
            # )

            load_job = self.client.load_table_from_uri(
                source_uris,
                full_table_id,
                job_config=job_config
            )

            # Wait for job completion
            load_job.result()

            if load_job.errors:
                logger.error(
                    "BigQuery load job failed",
                    errors=load_job.errors,
                    table_id=full_table_id
                )
                raise Exception(f"Load job failed: {load_job.errors}")

            logger.info(
                "successfully loaded data to BigQuery",
                table_id=full_table_id,
                input_files=load_job.input_files,
                input_bytes=load_job.input_file_bytes,
                output_rows=load_job.output_rows
            )

        except bigquery.BadRequest as e:
            logger.error(
                "Bad request error during BigQuery load",
                error=str(e),
                table_id=table_id,
                source_uris=source_uris
            )
            raise
        except bigquery.NotFound as e:
            logger.error(
                "BigQuery table or dataset not found",
                error=str(e),
                table_id=table_id
            )
            raise
        except Exception as e:
            logger.error(
                "failed to load data to BigQuery",
                error=str(e),
                table_id=table_id
            )
            raise