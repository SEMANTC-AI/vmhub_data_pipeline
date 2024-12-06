import os
from datetime import datetime
import pytz
from dotenv import load_dotenv
from pathlib import Path
import structlog
import time

from src.api.vmhub_client import VMHubClient, VMHubAPIError
from src.utils.gcs_helper import GCSHelper
from src.utils.bigquery_helper import BigQueryHelper
from src.config.settings import Settings

# Load environment variables
load_dotenv()

logger = structlog.get_logger()

def format_cnpj(cnpj: str) -> str:
    """Remove special characters from CNPJ."""
    return cnpj.replace('.', '').replace('/', '').replace('-', '')

def get_storage_path(cnpj: str, endpoint: str, page: int) -> str:
    """Generate storage path based on CNPJ, endpoint and current timestamp."""
    tz = pytz.timezone('America/Sao_Paulo')
    now = datetime.now(tz)
    formatted_date = now.strftime('%Y/%m/%d/%H')
    return f"CNPJ_{cnpj}/{endpoint}/{formatted_date}/response_pg{page}.json"

def main():
    """Main execution function."""
    try:
        # Initialize settings
        settings = Settings()
        
        # Format CNPJ
        formatted_cnpj = format_cnpj(settings.VMHUB_CNPJ)
        
        # Initialize clients
        vmhub_client = VMHubClient(
            base_url=settings.VMHUB_BASE_URL,
            api_key=settings.VMHUB_API_KEY,
            max_retries=3,
            initial_backoff=2.0
        )
        gcs_helper = GCSHelper(
            project_id=settings.GCP_PROJECT_ID,
            bucket_name=settings.GCS_BUCKET_NAME
        )
        bq_helper = BigQueryHelper(
            project_id=settings.GCP_PROJECT_ID,
            dataset_id=f"CNPJ_{formatted_cnpj}_RAW"
        )

        page = 0
        all_data = []
        consecutive_failures = 0
        max_consecutive_failures = 3

        # First phase: Load all to GCS
        while consecutive_failures < max_consecutive_failures:
            try:
                # Fetch data from VMHub
                data = vmhub_client.get_clients(
                    cnpj=settings.VMHUB_CNPJ,
                    page=page,
                    page_size=10
                )
                
                if not data:
                    logger.info("No data in page", page=page)
                    break
                    
                # Generate storage path
                storage_path = get_storage_path(
                    cnpj=formatted_cnpj,
                    endpoint='clientes',
                    page=page
                )
                
                # Upload to GCS
                gcs_helper.upload_json(
                    data=data,
                    blob_name=storage_path
                )
                
                # Add to collected data
                all_data.extend(data)
                
                # Reset failures counter and increment page
                consecutive_failures = 0
                page += 1
                
                # Add small delay between requests
                time.sleep(0.5)
                
            except VMHubAPIError as e:
                consecutive_failures += 1
                logger.warning(
                    "Failed to fetch page", 
                    error=str(e), 
                    page=page
                )
                page += 1  # Try next page on failure

        # Second phase: Single BigQuery load
        if all_data:
            logger.info(
                "Starting BigQuery load",
                total_records=len(all_data)
            )
            
            bq_helper.load_json(
                data=all_data,
                table_id='clientes',
                schema=settings.get_schema('clientes')
            )
            
            logger.info(
                "Finished processing",
                total_pages=page,
                total_records=len(all_data)
            )
        else:
            logger.warning("No data collected to load into BigQuery")

    except Exception as e:
        logger.error("Error in main execution", error=str(e))
        raise

if __name__ == "__main__":
    main()