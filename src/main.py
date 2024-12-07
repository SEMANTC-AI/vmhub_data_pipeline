# src/main.py

import os
from datetime import datetime
import pytz
from dotenv import load_dotenv
import structlog
import time
from typing import Optional, List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.api.vmhub_client import VMHubClient, VMHubAPIError, NoMoreDataError
from src.utils.gcs_helper import GCSHelper
from src.utils.bigquery_helper import BigQueryHelper
from src.config.settings import Settings
from src.config.endpoints import VMHubEndpoints, Endpoint

load_dotenv()
logger = structlog.get_logger()

def format_cnpj(cnpj: str) -> str:
    return cnpj.replace('.', '').replace('/', '').replace('-', '')

def get_storage_path(cnpj: str, endpoint: str, page: int, date_start: Optional[datetime] = None) -> str:
    """generate storage path for data files"""
    if endpoint == 'vendas' and date_start:
        date_str = date_start.strftime('%Y%m%d')
        return f"CNPJ_{cnpj}/{endpoint}/{date_str}/response_pg{page}.json"
    else:
        return f"CNPJ_{cnpj}/{endpoint}/response_pg{page}.json"

def enrich_data(records: List[Dict], gcs_uri: str) -> List[Dict]:
    """add metadata to records"""
    ingestion_ts = datetime.utcnow().isoformat() + "Z"
    for rec in records:
        rec['gcs_uri'] = gcs_uri
        rec['ingestion_timestamp'] = ingestion_ts
        rec['source_system'] = "VMHUB"
    return records

def process_pages_for_date_range(
    endpoint: Endpoint,
    settings: Settings,
    vmhub_client: VMHubClient,
    gcs_helper: GCSHelper,
    formatted_cnpj: str,
    start_date: Optional[datetime],
    end_date: Optional[datetime]
) -> bool:
    """process all pages for a specific date range"""
    page = 0
    consecutive_failures = 0
    max_consecutive_failures = 3
    any_data_fetched = False

    while consecutive_failures < max_consecutive_failures:
        try:
            data = vmhub_client.get_data(
                endpoint=endpoint.path,
                cnpj=settings.VMHUB_CNPJ,
                page=page,
                page_size=endpoint.page_size,
                date_start=start_date,
                date_end=end_date
            )

            if not data:
                logger.info("no more data returned", page=page, endpoint=endpoint.name)
                break

            storage_path = get_storage_path(
                cnpj=formatted_cnpj,
                endpoint=endpoint.name,
                page=page,
                date_start=start_date if endpoint.name == 'vendas' else None
            )
            gcs_uri = f"gs://{settings.GCS_BUCKET_NAME}/{storage_path}"
            enriched_data = enrich_data(data, gcs_uri=gcs_uri)
            gcs_helper.upload_json(data=enriched_data, blob_name=storage_path)

            any_data_fetched = True
            consecutive_failures = 0
            page += 1
            time.sleep(0.5)
        except NoMoreDataError:
            logger.info("no more data available", page=page, endpoint=endpoint.name)
            break
        except VMHubAPIError as e:
            consecutive_failures += 1
            logger.warning(
                "failed to fetch page",
                error=str(e),
                page=page,
                endpoint=endpoint.name,
                start_date=start_date.isoformat() if start_date else None,
                end_date=end_date.isoformat() if end_date else None
            )
            page += 1

    return any_data_fetched

def process_endpoint(
    endpoint: Endpoint,
    settings: Settings,
    vmhub_client: VMHubClient,
    gcs_helper: GCSHelper,
    bq_helper: BigQueryHelper,
    formatted_cnpj: str
) -> None:
    """process a single endpoint day by day"""
    logger.info("processing endpoint", endpoint=endpoint.name)
    any_data_processed = False

    if endpoint.requires_date_range:
        # Process each day and store in GCS
        for start_date, end_date in endpoint.get_daily_ranges():
            logger.info(
                "Processing date",
                endpoint=endpoint.name,
                date=start_date.date().isoformat()
            )
            
            try:
                data_fetched = process_pages_for_date_range(
                    endpoint=endpoint,
                    settings=settings,
                    vmhub_client=vmhub_client,
                    gcs_helper=gcs_helper,
                    formatted_cnpj=formatted_cnpj,
                    start_date=start_date,
                    end_date=end_date
                )
                
                if data_fetched:
                    any_data_processed = True
                
                # Add delay between days to avoid rate limiting
                time.sleep(1.0)
                
            except Exception as e:
                logger.error(
                    "error processing date",
                    endpoint=endpoint.name,
                    date=start_date.date().isoformat(),
                    error=str(e)
                )
                continue
        
        # after all days are processed, load everything to BigQuery
        if any_data_processed:
            # Get all files for this endpoint
            prefix = f"CNPJ_{formatted_cnpj}/{endpoint.name}/"
            source_uris = gcs_helper.get_all_file_uris(prefix)
            
            if source_uris:
                logger.info(
                    "loading all files to BigQuery",
                    endpoint=endpoint.name,
                    file_count=len(source_uris)
                )
                schema = settings.get_schema(endpoint.name)
                bq_helper.load_data_from_gcs(
                    table_id=endpoint.name,
                    schema=schema,
                    source_uris=source_uris
                )
            else:
                logger.warning(
                    "no files found for loading",
                    endpoint=endpoint.name
                )
    else:
        # non-date-range endpoint processing (like clientes)
        process_pages_for_date_range(
            endpoint=endpoint,
            settings=settings,
            vmhub_client=vmhub_client,
            gcs_helper=gcs_helper,
            formatted_cnpj=formatted_cnpj,
            start_date=None,
            end_date=None
        )

def main():
    try:
        settings = Settings()
        vmhub_client = VMHubClient(
            base_url=settings.VMHUB_BASE_URL,
            api_key=settings.VMHUB_API_KEY,
            max_retries=3,
            initial_backoff=1.0,
            max_backoff=16.0,
            backoff_factor=1.5
        )

        formatted_cnpj = format_cnpj(settings.VMHUB_CNPJ)
        gcs_helper = GCSHelper(project_id=settings.GCP_PROJECT_ID, bucket_name=settings.GCS_BUCKET_NAME)
        bq_helper = BigQueryHelper(project_id=settings.GCP_PROJECT_ID, dataset_id=f"CNPJ_{formatted_cnpj}_RAW")

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
                    "error processing endpoint",
                    endpoint=endpoint.name,
                    error=str(e)
                )

    except Exception as e:
        logger.error("error in main execution", error=str(e))
        raise

if __name__ == "__main__":
    main()