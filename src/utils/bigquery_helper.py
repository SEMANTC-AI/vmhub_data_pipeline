# src/utils/bigquery_helper.py

from typing import Dict, List
from google.cloud import bigquery
import structlog
from google.api_core import exceptions
from datetime import datetime

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
        except exceptions.NotFound:
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

    def create_message_history_table(self, cnpj: str) -> None:
        """create the message_history table in the CAMPAIGN dataset"""
        campaign_dataset_id = self.dataset_id.replace('_RAW', '_CAMPAIGN')
        
        # create CAMPAIGN dataset if it doesn't exist
        campaign_dataset_ref = self.client.dataset(campaign_dataset_id)
        try:
            self.client.get_dataset(campaign_dataset_ref)
            logger.info("Campaign dataset exists", dataset_id=campaign_dataset_id)
        except exceptions.NotFound:
            campaign_dataset = bigquery.Dataset(campaign_dataset_ref)
            campaign_dataset.location = "US"
            self.client.create_dataset(campaign_dataset, exists_ok=True)
            logger.info("Created campaign dataset", dataset_id=campaign_dataset_id)

        # create message_history table
        table_id = f"{self.project_id}.{campaign_dataset_id}.message_history"
        
        try:
            # check if table exists
            self.client.get_table(table_id)
            logger.info("message history table already exists", table_id=table_id)
            return
        except exceptions.NotFound:
            # define schema directly
            schema = [
                bigquery.SchemaField("message_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("campaign_type", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("campaign_id", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("message_content", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("phone", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("sent_at", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("delivered_at", "TIMESTAMP", mode="NULLABLE"),
                bigquery.SchemaField("read_at", "TIMESTAMP", mode="NULLABLE"),
                bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("error_message", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("retry_count", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("template_name", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("template_language", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("template_variables", "JSON", mode="NULLABLE"),
                bigquery.SchemaField("customer_response", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("response_timestamp", "TIMESTAMP", mode="NULLABLE"),
                bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("whatsapp_message_id", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("whatsapp_status", "STRING", mode="NULLABLE")
            ]
            
            table = bigquery.Table(table_id, schema=schema)
            
            # set table options
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="sent_at"
            )
            table.clustering_fields = ["campaign_type", "status"]
            
            # create the table
            self.client.create_table(table)
            logger.info("created message history table", table_id=table_id)

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
        """load data from GCS files into BigQuery table"""
        try:
            full_table_id = f"{self.project_id}.{self.dataset_id}.{table_id}"
            
            # create schema
            bq_schema = [self._create_schema_field(field) for field in schema]
            
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                schema=bq_schema,
                write_disposition="WRITE_TRUNCATE",
                ignore_unknown_values=True
            )

            load_job = self.client.load_table_from_uri(
                source_uris,
                full_table_id,
                job_config=job_config
            )

            # wait for job completion
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

        except exceptions.BadRequest as e:
            logger.error(
                "bad request error during BigQuery load",
                error=str(e),
                table_id=table_id,
                source_uris=source_uris
            )
            raise
        except exceptions.NotFound as e:
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