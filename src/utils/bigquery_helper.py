import json
from typing import Dict, List, Optional
from google.cloud import bigquery
import structlog
from datetime import datetime
import pytz

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

    def _create_schema_field(self, field_def: Dict) -> bigquery.SchemaField:
        """Create a schema field with proper handling of nested and repeated fields."""
        try:
            field_name = field_def['name']
            field_type = field_def['type']
            field_mode = field_def.get('mode', 'NULLABLE')
            
            logger.debug(
                "Creating schema field",
                name=field_name,
                type=field_type,
                mode=field_mode
            )

            if field_type == 'RECORD' and 'fields' in field_def:
                sub_fields = [
                    self._create_schema_field(f) 
                    for f in field_def['fields']
                ]
                return bigquery.SchemaField(
                    name=field_name,
                    field_type=field_type,
                    mode=field_mode,
                    fields=sub_fields
                )
            else:
                return bigquery.SchemaField(
                    name=field_name,
                    field_type=field_type,
                    mode=field_mode
                )
                
        except Exception as e:
            logger.error(
                "Error creating schema field",
                error=str(e),
                field=field_def
            )
            raise

    def create_or_update_external_table(
        self,
        table_config: Dict,
        external_config: Dict,
        schema: List[Dict]
    ) -> None:
        """Create or update external table configuration."""
        try:
            table_id = f"{self.project_id}.{self.dataset_id}.{table_config['table_id']}"
            
            # Create schema fields
            schema_fields = [
                self._create_schema_field(field)
                for field in schema
            ]

            # Configure external data
            ext_config = bigquery.ExternalConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            )
            
            # Set source URIs with correct wildcard pattern
            uri_template = external_config['source_uris'][0]
            uri = uri_template.format(
                bucket=external_config['bucket'],
                cnpj=external_config['cnpj']
            )
            ext_config.source_uris = [uri]
            
            logger.info(
                "Configuring external source",
                uri=uri
            )
            
            # Set additional config
            ext_config.ignore_unknown_values = external_config.get('ignore_unknown_values', True)
            ext_config.max_bad_records = external_config.get('max_bad_records', 0)
            ext_config.autodetect = False

            # Create table
            table = bigquery.Table(table_id, schema=schema_fields)
            table.external_data_configuration = ext_config

            # Create or update table
            table = self.client.create_table(table, exists_ok=True)
            
            logger.info(
                "Successfully created/updated external table",
                table_id=table_id,
                uri=uri
            )

        except Exception as e:
            logger.error(
                "Failed to create external table",
                error=str(e),
                table_id=table_config.get('table_id')
            )
            raise

    def create_materialized_view(
        self,
        view_id: str,
        source_table: str,
        query: str
    ) -> None:
        """Create a materialized view."""
        try:
            full_view_id = f"{self.project_id}.{self.dataset_id}.{view_id}"
            
            view = bigquery.Table(full_view_id)
            view.materialized_view = bigquery.MaterializedView(
                enable_refresh=True,
                refresh_interval_ms=3600000,  # 1 hour
                query=query
            )
            
            view = self.client.create_table(view, exists_ok=True)
            
            logger.info(
                "Created/Updated materialized view",
                view_id=view_id
            )
            
        except Exception as e:
            logger.error(
                "Failed to create materialized view",
                error=str(e),
                view_id=view_id
            )
            raise