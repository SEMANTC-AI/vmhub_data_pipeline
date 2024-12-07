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
            logger.info("Created BigQuery dataset", dataset_id=self.dataset_id)

    def _create_schema_field(self, field_def: Dict) -> bigquery.SchemaField:
        field_name = field_def['name']
        field_type = field_def['type']
        field_mode = field_def.get('mode', 'NULLABLE')

        if field_type == 'RECORD' and 'fields' in field_def:
            sub_fields = [self._create_schema_field(f) for f in field_def['fields']]
            return bigquery.SchemaField(name=field_name, field_type=field_type, mode=field_mode, fields=sub_fields)
        else:
            return bigquery.SchemaField(name=field_name, field_type=field_type, mode=field_mode)

    def create_or_update_external_table(
        self,
        table_config: Dict,
        external_config: Dict,
        schema: List[Dict],
        bucket_name: str,
        cnpj: str
    ) -> None:
        try:
            table_id = f"{self.project_id}.{self.dataset_id}.{table_config['table_id']}"
            
            schema_fields = [self._create_schema_field(field) for field in schema]

            ext_config = bigquery.ExternalConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            )

            source_uris = [uri.format(bucket=bucket_name, cnpj=cnpj) for uri in external_config['source_uris']]
            ext_config.source_uris = source_uris
            logger.info("Configuring external source", source_uris=source_uris)

            ext_config.ignore_unknown_values = external_config.get('ignore_unknown_values', True)
            ext_config.max_bad_records = external_config.get('max_bad_records', 0)
            ext_config.autodetect = False

            table = bigquery.Table(table_id, schema=schema_fields)
            table.external_data_configuration = ext_config

            self.client.create_table(table, exists_ok=True)
            logger.info("Successfully created/updated external table", table_id=table_id)

        except Exception as e:
            logger.error(
                "Failed to create external table",
                error=str(e),
                table_id=table_config.get('table_id')
            )
            raise