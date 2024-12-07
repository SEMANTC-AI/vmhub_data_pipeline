# src/main.py

import os
from datetime import datetime
import pytz
from dotenv import load_dotenv
import structlog
import time
from typing import Optional, List, Dict

from src.api.vmhub_client import VMHubClient, VMHubAPIError, NoMoreDataError
from src.utils.gcs_helper import GCSHelper
from src.utils.bigquery_helper import BigQueryHelper
from src.config.settings import Settings
from src.config.endpoints import VMHubEndpoints, Endpoint

load_dotenv()
logger = structlog.get_logger()

def format_cnpj(cnpj: str) -> str:
    return cnpj.replace('.', '').replace('/', '').replace('-', '')

def get_storage_path(cnpj: str, endpoint: str, page: int) -> str:
    return f"CNPJ_{cnpj}/{endpoint}/response_pg{page}.json"

def enrich_data(records: List[Dict], gcs_uri: str) -> List[Dict]:
    ingestion_ts = datetime.utcnow().isoformat() + "Z"
    for rec in records:
        rec['gcs_uri'] = gcs_uri
        rec['ingestion_timestamp'] = ingestion_ts
        rec['source_system'] = "VMHUB"
    return records

def process_date_range(
    endpoint: Endpoint,
    settings: Settings,
    vmhub_client: VMHubClient,
    gcs_helper: GCSHelper,
    formatted_cnpj: str,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None
) -> None:
    page = 0
    consecutive_failures = 0
    max_consecutive_failures = 3
    fetched_any_data = False  # Track if we fetched any data successfully

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
                # no data returned from api (normal scenario)
                logger.info("no more data returned", page=page, endpoint=endpoint.name)
                break

            fetched_any_data = True
            storage_path = get_storage_path(cnpj=formatted_cnpj, endpoint=endpoint.name, page=page)
            gcs_uri = f"gs://{settings.GCS_BUCKET_NAME}/{storage_path}"
            enriched_data = enrich_data(data, gcs_uri=gcs_uri)
            gcs_helper.upload_json(data=enriched_data, blob_name=storage_path)

            consecutive_failures = 0
            page += 1
            time.sleep(0.5)

        except NoMoreDataError:
            # we got a 500 after some pages, indicating no more data
            logger.info("no more data indicated by persistent 500 errors", page=page, endpoint=endpoint.name)
            break
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

def process_endpoint(
    endpoint: Endpoint,
    settings: Settings,
    vmhub_client: VMHubClient,
    gcs_helper: GCSHelper,
    bq_helper: BigQueryHelper,
    formatted_cnpj: str
) -> None:
    # logger.info("processing endpoint", endpoint=endpoint.name)

    if endpoint.requires_date_range:
        date_ranges = endpoint.get_date_ranges()
        total_ranges = len(date_ranges)
        # logger.info("processing date ranges", endpoint=endpoint.name, total_ranges=total_ranges)

        for idx, (start_date, end_date) in enumerate(date_ranges, 1):
            # logger.info(
            #     "processing date range",
            #     endpoint=endpoint.name,
            #     range_number=f"{idx}/{total_ranges}",
            #     start_date=start_date.isoformat(),
            #     end_date=end_date.isoformat()
            # )
            try:
                process_date_range(
                    endpoint=endpoint,
                    start_date=start_date,
                    end_date=end_date,
                    settings=settings,
                    vmhub_client=vmhub_client,
                    gcs_helper=gcs_helper,
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
        # No date range required
        process_date_range(
            endpoint=endpoint,
            start_date=None,
            end_date=None,
            settings=settings,
            vmhub_client=vmhub_client,
            gcs_helper=gcs_helper,
            formatted_cnpj=formatted_cnpj
        )

    # After processing all pages, load data from GCS to BQ
    gcs_uri_pattern = f"gs://{settings.GCS_BUCKET_NAME}/CNPJ_{formatted_cnpj}/{endpoint.name}/response_pg*.json"
    schema = settings.get_schema(endpoint.name)
    bq_helper.load_data_from_gcs(table_id=endpoint.name, schema=schema, source_uri=gcs_uri_pattern)

def main():
    try:
        settings = Settings()
        formatted_cnpj = format_cnpj(settings.VMHUB_CNPJ)
        
        vmhub_client = VMHubClient(
            base_url=settings.VMHUB_BASE_URL,
            api_key=settings.VMHUB_API_KEY,
            max_retries=3,
            initial_backoff=2.0
        )
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
                logger.error("Error processing endpoint", endpoint=endpoint.name, error=str(e))
    except Exception as e:
        logger.error("Error in main execution", error=str(e))
        raise

if __name__ == "__main__":
    main()