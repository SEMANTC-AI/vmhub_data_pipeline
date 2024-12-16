# src/main.py

import os
from datetime import datetime, timedelta
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
from src.utils.firestore_helper import get_customer_data  # NEW

load_dotenv()
logger = structlog.get_logger()

def format_cnpj(cnpj: str) -> str:
    return cnpj.replace('.', '').replace('/', '').replace('-', '')

def get_storage_path(cnpj: str, endpoint: str, page: int, date_start: Optional[datetime] = None) -> str:
    """Generate storage path for data files"""
    if endpoint == 'vendas' and date_start:
        date_str = date_start.strftime('%Y%m%d')
        return f"CNPJ_{cnpj}/{endpoint}/{date_str}/response_pg{page}.json"
    else:
        return f"CNPJ_{cnpj}/{endpoint}/response_pg{page}.json"

def enrich_data(records: List[Dict], gcs_uri: str) -> List[Dict]:
    """Add metadata to records"""
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
    """process all pages for a specific date range sequentially with robust error handling"""
    any_data_fetched = False
    page = 0
    max_retries = 3
    original_page_size = endpoint.page_size

    while True:
        retries = 0
        while retries < max_retries:
            try:
                data = vmhub_client.get_data(
                    endpoint=endpoint.path,
                    cnpj=settings.VMHUB_CNPJ,
                    page=page,
                    page_size=original_page_size,
                    date_start=start_date,
                    date_end=end_date
                )

                if not data:
                    logger.info("No more data returned", page=page, endpoint=endpoint.name)
                    return any_data_fetched  # Stop fetching further pages

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
                page += 1
                time.sleep(0.5)  # Optional: Adjust delay as needed
                break  # Successful fetch, move to next page

            except VMHubAPIError as e:
                retries += 1
                logger.warning(
                    "Failed to fetch page",
                    error=str(e),
                    page=page,
                    endpoint=endpoint.name,
                    attempt=retries
                )
                time.sleep(2 ** retries)  # Exponential backoff

            except Exception as e:
                retries += 1
                logger.error(
                    "Unexpected error processing page",
                    error=str(e),
                    page=page,
                    endpoint=endpoint.name,
                    attempt=retries
                )
                time.sleep(2 ** retries)  # Exponential backoff

        else:
            # After max retries for this page
            if endpoint.name == 'clientes':
                logger.info(
                    "Max retries reached. Attempting individual records.",
                    endpoint=endpoint.name,
                    page=page
                )
                start_record = (page - 1) * original_page_size + 1
                end_record = page * original_page_size

                for individual_record in range(start_record, end_record + 1):
                    try:
                        individual_page = individual_record
                        individual_data = vmhub_client.get_data(
                            endpoint=endpoint.path,
                            cnpj=settings.VMHUB_CNPJ,
                            page=individual_page,
                            page_size=1,
                            date_start=start_date,
                            date_end=end_date
                        )

                        if individual_data:
                            storage_path = get_storage_path(
                                cnpj=formatted_cnpj,
                                endpoint=endpoint.name,
                                page=individual_page,
                                date_start=start_date if endpoint.name == 'vendas' else None
                            )
                            gcs_uri = f"gs://{settings.GCS_BUCKET_NAME}/{storage_path}"
                            enriched_data = enrich_data(individual_data, gcs_uri=gcs_uri)
                            gcs_helper.upload_json(data=enriched_data, blob_name=storage_path)
                            any_data_fetched = True
                            logger.info(
                                "Fetched and uploaded individual record",
                                endpoint=endpoint.name,
                                record=individual_record
                            )
                        else:
                            logger.info(
                                "No data returned for individual record",
                                endpoint=endpoint.name,
                                record=individual_record
                            )
                    except VMHubAPIError as e:
                        logger.warning(
                            "Failed to fetch individual record",
                            error=str(e),
                            endpoint=endpoint.name,
                            record=individual_record
                        )
                        continue
                    except Exception as e:
                        logger.error(
                            "Unexpected error fetching individual record",
                            error=str(e),
                            endpoint=endpoint.name,
                            record=individual_record
                        )
                        continue
            else:
                logger.warning(
                    "Max retries reached. Skipping page.",
                    endpoint=endpoint.name,
                    page=page
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
    logger.info("Processing endpoint", endpoint=endpoint.name)
    any_data_processed = False

    if endpoint.requires_date_range:
        if endpoint.name == 'vendas':
            latest_date = gcs_helper.get_latest_processed_date(endpoint.name, settings.VMHUB_CNPJ)
            if latest_date:
                start_date = latest_date
                logger.info(
                    "Resuming from latest processed date",
                    endpoint=endpoint.name,
                    start_date=start_date.date().isoformat()
                )
            else:
                start_date = datetime.now(pytz.UTC) - timedelta(days=2*365)
                logger.info(
                    "No existing data found. Starting from 2 years ago",
                    endpoint=endpoint.name,
                    start_date=start_date.date().isoformat()
                )
            end_date = endpoint.end_date
        else:
            start_date = endpoint.start_date
            end_date = endpoint.end_date

        for current_start_date, current_end_date in Endpoint(
            name=endpoint.name,
            path=endpoint.path,
            page_size=endpoint.page_size,
            max_retries=endpoint.max_retries,
            schema_file=endpoint.schema_file,
            requires_date_range=endpoint.requires_date_range,
            start_date=start_date,
            end_date=end_date
        ).get_daily_ranges():
            logger.info(
                "Processing date",
                endpoint=endpoint.name,
                date=current_start_date.date().isoformat()
            )

            try:
                data_fetched = process_pages_for_date_range(
                    endpoint=endpoint,
                    settings=settings,
                    vmhub_client=vmhub_client,
                    gcs_helper=gcs_helper,
                    formatted_cnpj=formatted_cnpj,
                    start_date=current_start_date,
                    end_date=current_end_date
                )
                if data_fetched:
                    any_data_processed = True
                time.sleep(1.0)
            except Exception as e:
                logger.error(
                    "Error processing date",
                    endpoint=endpoint.name,
                    date=current_start_date.date().isoformat(),
                    error=str(e)
                )
                continue

        if any_data_processed:
            prefix = f"CNPJ_{formatted_cnpj}/{endpoint.name}/"
            source_uris = gcs_helper.get_all_file_uris(prefix)
            if source_uris:
                logger.info(
                    "Loading files to BigQuery",
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
                logger.warning("No files found for loading", endpoint=endpoint.name)
    else:
        data_fetched = process_pages_for_date_range(
            endpoint=endpoint,
            settings=settings,
            vmhub_client=vmhub_client,
            gcs_helper=gcs_helper,
            formatted_cnpj=formatted_cnpj,
            start_date=None,
            end_date=None
        )
        if data_fetched:
            prefix = f"CNPJ_{formatted_cnpj}/{endpoint.name}/"
            source_uris = gcs_helper.get_all_file_uris(prefix)
            if source_uris:
                logger.info(
                    "Loading files to BigQuery",
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
                logger.warning("No files found for loading", endpoint=endpoint.name)

def main():
    try:
        # Fetch the user_id from environment or another source
        user_id = os.getenv('USER_ID')
        if not user_id:
            raise ValueError("Missing USER_ID environment variable")

        # Get credentials from Firestore
        vmhub_token, cnpj = get_customer_data(user_id)

        # Set environment variables so Settings can use them
        os.environ['VMHUB_API_KEY'] = vmhub_token
        os.environ['VMHUB_CNPJ'] = cnpj

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

        endpoints = VMHubEndpoints.get_all()

        # Example: concurrency for vendas and clientes
        with ThreadPoolExecutor(max_workers=2) as executor:
            future_to_endpoint = {
                executor.submit(
                    process_endpoint,
                    endpoint=endpoint,
                    settings=settings,
                    vmhub_client=vmhub_client,
                    gcs_helper=gcs_helper,
                    bq_helper=bq_helper,
                    formatted_cnpj=formatted_cnpj
                ): endpoint for endpoint in endpoints if endpoint.name in ['vendas', 'clientes']
            }

            for future in as_completed(future_to_endpoint):
                endpoint = future_to_endpoint[future]
                try:
                    future.result()
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