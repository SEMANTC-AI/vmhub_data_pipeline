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
        except Exception:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            self.client.create_dataset(dataset, exists_ok=True)
            logger.info("created BigQuery dataset", dataset_id=self.dataset_id)

    def load_data_from_gcs(self, table_id: str, schema: List[Dict], source_uri: str):
        full_table_id = f"{self.project_id}.{self.dataset_id}.{table_id}"

        bq_schema = []
        for field in schema:
            bq_schema.append(self._create_schema_field(field))

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema=bq_schema,
            write_disposition="WRITE_TRUNCATE",
            ignore_unknown_values=True
        )

        load_job = self.client.load_table_from_uri(
            source_uri,
            full_table_id,
            job_config=job_config
        )

        result = load_job.result()
        if load_job.errors:
            logger.error("Failed to load data to BigQuery", errors=load_job.errors)
            raise Exception(f"Load job failed: {load_job.errors}")

        # note: input_files and input_file_bytes are integers, no need to use len()
        logger.info(
            "successfully loaded data to BigQuery",
            table_id=full_table_id,
            input_files=load_job.input_files,
            input_bytes=load_job.input_file_bytes
        )

    def _create_schema_field(self, field_def: Dict) -> bigquery.SchemaField:
        field_name = field_def['name']
        field_type = field_def['type']
        field_mode = field_def.get('mode', 'NULLABLE')

        if field_type == 'RECORD' and 'fields' in field_def:
            sub_fields = [self._create_schema_field(f) for f in field_def['fields']]
            return bigquery.SchemaField(name=field_name, field_type=field_type, mode=field_mode, fields=sub_fields)
        else:
            return bigquery.SchemaField(name=field_name, field_type=field_type, mode=field_mode)