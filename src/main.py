# src/main.py

import os
from datetime import datetime
import pytz
from dotenv import load_dotenv
from pathlib import Path
import structlog
import time
from typing import Optional

from src.api.vmhub_client import VMHubClient, VMHubAPIError
from src.utils.gcs_helper import GCSHelper
from src.utils.bigquery_helper import BigQueryHelper
from src.config.settings import Settings
from src.config.endpoints import VMHubEndpoints

# Load environment variables
load_dotenv()

logger = structlog.get_logger()

def format_cnpj(cnpj: str) -> str:
    """Remove special characters from CNPJ."""
    return cnpj.replace('.', '').replace('/', '').replace('-', '')

def get_storage_path(cnpj: str, endpoint: str, page: int, date_suffix: str = '') -> str:
    """Generate storage path based on CNPJ, endpoint and current timestamp."""
    tz = pytz.timezone('America/Sao_Paulo')
    now = datetime.now(tz)
    formatted_date = now.strftime('%Y/%m/%d/%H')
    base_path = f"CNPJ_{cnpj}/{endpoint}/{formatted_date}/response_pg{page}"
    return f"{base_path}{date_suffix}.json"

def process_date_range(
    endpoint: VMHubEndpoints,
    settings: Settings,
    vmhub_client: VMHubClient,
    gcs_helper: GCSHelper,
    bq_helper: BigQueryHelper,
    formatted_cnpj: str,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None
) -> None:
    """Process endpoint for a specific date range."""
    page = 0
    all_data = []
    consecutive_failures = 0
    max_consecutive_failures = 3

    while consecutive_failures < max_consecutive_failures:
        try:
            # Fetch data from VMHub
            data = vmhub_client.get_data(
                endpoint=endpoint.path,
                cnpj=settings.VMHUB_CNPJ,
                page=page,
                page_size=endpoint.page_size,
                date_start=start_date,
                date_end=end_date
            )
            
            if not data:
                logger.info("No data in page", page=page)
                break
                
            # Generate storage path with date suffix for date-based endpoints
            date_suffix = f"_{start_date.strftime('%Y%m%d')}" if start_date else ""
            storage_path = get_storage_path(
                cnpj=formatted_cnpj,
                endpoint=endpoint.name,
                page=page,
                date_suffix=date_suffix
            )
            
            # Upload to GCS
            gcs_uri = gcs_helper.upload_json(
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
                page=page,
                endpoint=endpoint.name,
                start_date=start_date.isoformat() if start_date else None,
                end_date=end_date.isoformat() if end_date else None
            )
            page += 1

    # Load to BigQuery
    if all_data:
        logger.info(
            "Starting BigQuery load",
            endpoint=endpoint.name,
            total_records=len(all_data),
            start_date=start_date.isoformat() if start_date else None,
            end_date=end_date.isoformat() if end_date else None
        )
        
        bq_helper.load_json(
            data=all_data,
            table_id=endpoint.name,
            schema=settings.get_schema(endpoint.name),
            gcs_uri=gcs_uri
        )

def process_endpoint(
    endpoint: VMHubEndpoints,
    settings: Settings,
    vmhub_client: VMHubClient,
    gcs_helper: GCSHelper,
    bq_helper: BigQueryHelper,
    formatted_cnpj: str
) -> None:
    """Process a single endpoint."""
    logger.info("Starting to process endpoint", endpoint=endpoint.name)
    
    # If endpoint requires date range, process each range separately
    if endpoint.requires_date_range:
        date_ranges = endpoint.get_date_ranges()
        total_ranges = len(date_ranges)
        
        logger.info(
            "Processing date ranges",
            endpoint=endpoint.name,
            total_ranges=total_ranges,
            first_date=date_ranges[0][0].isoformat(),
            last_date=date_ranges[-1][1].isoformat()
        )
        
        for idx, (start_date, end_date) in enumerate(date_ranges, 1):
            logger.info(
                "Processing date range",
                endpoint=endpoint.name,
                range_number=f"{idx}/{total_ranges}",
                start_date=start_date.isoformat(),
                end_date=end_date.isoformat()
            )
            
            try:
                process_date_range(
                    endpoint=endpoint,
                    start_date=start_date,
                    end_date=end_date,
                    settings=settings,
                    vmhub_client=vmhub_client,
                    gcs_helper=gcs_helper,
                    bq_helper=bq_helper,
                    formatted_cnpj=formatted_cnpj
                )
            except Exception as e:
                logger.error(
                    "Failed to process date range",
                    endpoint=endpoint.name,
                    start_date=start_date.isoformat(),
                    end_date=end_date.isoformat(),
                    error=str(e)
                )
    else:
        process_date_range(
            endpoint=endpoint,
            start_date=None,
            end_date=None,
            settings=settings,
            vmhub_client=vmhub_client,
            gcs_helper=gcs_helper,
            bq_helper=bq_helper,
            formatted_cnpj=formatted_cnpj
        )

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

        # Process each configured endpoint
        for endpoint in VMHubEndpoints.get_all():
            try:
                process_endpoint(
                    endpoint=endpoint,
                    settings=settings,
                    vmhub_client=vmhub_client,
                    gcs_helper=gcs_helper,
                    bq_helper=bq_helper,
                    formatted_cnpj=formatted_cnpj
                )
            except Exception as e:
                logger.error(
                    "Error processing endpoint",
                    endpoint=endpoint.name,
                    error=str(e)
                )

    except Exception as e:
        logger.error("Error in main execution", error=str(e))
        raise

if __name__ == "__main__":
    main()