import os
from datetime import datetime
import pytz
from dotenv import load_dotenv
from pathlib import Path
import structlog
import json

from src.utils.bigquery_helper import BigQueryHelper
from src.config.settings import Settings

load_dotenv()
logger = structlog.get_logger()

def format_cnpj(cnpj: str) -> str:
    """Remove special characters from CNPJ."""
    return cnpj.replace('.', '').replace('/', '').replace('-', '')

def setup_external_table(
    project_id: str,
    cnpj: str,
    endpoint: str,
    bucket_name: str
) -> None:
    """Setup external table and materialized views."""
    try:
        formatted_cnpj = format_cnpj(cnpj)
        
        # Initialize BigQuery helper
        bq_helper = BigQueryHelper(
            project_id=project_id,
            dataset_id=f"CNPJ_{formatted_cnpj}_RAW"
        )
        
        # Load external table config
        schema_path = Path(__file__).parent.parent / 'schemas' / f'{endpoint}_external.json'
        with open(schema_path) as f:
            config = json.load(f)
            
        # Update config with runtime values
        config['external_config']['bucket'] = bucket_name
        config['external_config']['cnpj'] = formatted_cnpj
        
        # Create external table
        bq_helper.create_or_update_external_table(
            table_config=config['table_config'],
            external_config=config['external_config'],
            schema=config['schema']
        )
        
        # Create materialized view for clean data
        deduplicated_view_query = f"""
        WITH RankedRecords AS (
          SELECT 
            *,
            ROW_NUMBER() OVER (
              PARTITION BY requisicao, data
              ORDER BY data DESC
            ) as rn
          FROM `{project_id}.CNPJ_{formatted_cnpj}_RAW.{endpoint}_external`
          WHERE status = 'SUCESSO'
        )
        SELECT * EXCEPT(rn)
        FROM RankedRecords
        WHERE rn = 1
        """
        
        bq_helper.create_materialized_view(
            view_id=f"{endpoint}_clean",
            source_table=f"{endpoint}_external",
            query=deduplicated_view_query
        )
        
        logger.info(
            "Completed external table and view setup",
            endpoint=endpoint
        )
            
    except Exception as e:
        logger.error(
            "Error setting up external table",
            error=str(e),
            endpoint=endpoint
        )
        raise

def main():
    """Main execution function."""
    try:
        settings = Settings()
        endpoint = "vendas"
        
        logger.info(
            "Starting external table setup",
            endpoint=endpoint
        )
        
        setup_external_table(
            project_id=settings.GCP_PROJECT_ID,
            cnpj=settings.VMHUB_CNPJ,
            endpoint=endpoint,
            bucket_name=settings.GCS_BUCKET_NAME
        )

    except Exception as e:
        logger.error("Error in main execution", error=str(e))
        raise

if __name__ == "__main__":
    main()