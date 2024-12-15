# VMHub Data Fetch Pipeline

This project focuses solely on fetching data from the VMHub API, storing it in Google Cloud Storage (GCS), and loading it into BigQuery for analysis. The pipeline dynamically retrieves credentials (CNPJ and VMHub token) from Firestore rather than relying on static environment variables.

## Overview

**Key Operations:**
- Fetch `clientes` and `vendas` from the VMHub API
- Handle time-based endpoints (like `vendas`) in daily batches for historical data
- Store raw JSON responses in GCS
- Load processed data into BigQuery with schema validation
- Use incremental loading and deduplication to keep data fresh and clean

**Dynamic Credentials:**
- The pipeline uses a Firestore `users` collection to fetch the `vmhubToken` and `cnpj`
- Provide the `USER_ID` (the Firestore document ID) at runtime. The pipeline will read the `config` map from the document to get these values

## Project Structure
```
vmhub_data_pipeline/
├── schemas/
│   ├── clientes.json
│   └── vendas.json
├── src/
│   ├── api/                   # VMHub API client
│   ├── config/                # Configuration and endpoints
│   ├── utils/                 # GCS, BQ, Firestore helpers
│   └── main.py                # Main execution script
├── scripts/
│   └── build_push.sh          # Build & deploy scripts
├── credentials/
│   └── credentials.json       # GCP service account credentials (not committed)
├── Dockerfile
├── requirements.txt
└── .env
```

## Prerequisites

- **Python** 3.9+
- **Docker**
- **GCP Setup**:
  - Firestore (Native mode) in the same project as GCS & BigQuery
  - BigQuery and GCS enabled
  - A service account with **Datastore User**, **Storage Admin**, and **BigQuery Admin** roles
- The Firestore document at `users/{USER_ID}` must have a `config` map with `vmhubToken` and `cnpj`

**Example Firestore Document:**
```json
{
  "config": {
    "cnpj": "48986168000144",
    "vmhubToken": "e8d5026b-7779-42e6-b695-a208567423db",
    "status": "pending",
    "createdAt": "<timestamp>",
    "updatedAt": "<timestamp>"
  }
}
```

## Configuration

1. **.env File**:
   Provide the base URL and GCP details:
```env
VMHUB_BASE_URL=https://apps.vmhub.vmtecnologia.io/vmlav/api/externa/v1
GCP_PROJECT_ID=your-project-id
GCS_BUCKET_NAME=vmhub-data
```
No need for VMHUB_API_KEY or VMHUB_CNPJ here; they are fetched from Firestore.

2. **Credentials**:
   Place your GCP service account key as credentials.json under credentials/.

## Installation

1. **Virtual Environment:**
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. **Local Testing:**
```bash
export USER_ID=<your_firestore_user_id>
python -m src.main
```

## Running via Docker

**Build & Run Locally:**
```bash
docker build -t vmhub-pipeline:local .
docker run \
  -v $(pwd)/.env:/app/.env \
  -v $(pwd)/credentials/credentials.json:/app/credentials/credentials.json:ro \
  -e GOOGLE_APPLICATION_CREDENTIALS=/app/credentials/credentials.json \
  -e PYTHONPATH=/app \
  -e USER_ID=<your_firestore_user_id> \
  vmhub-pipeline:local
```

**Deploying to Cloud Run via Artifact Registry**

**Build & Push:**
```bash
./scripts/build_push.sh
```
Then configure the Cloud Run job with USER_ID as an environment variable and run the job.

## Data Flow

1. **Credentials from Firestore**:
   - Retrieve vmhubToken and cnpj from users/{USER_ID}/config

2. **Data Fetching**:
   - clientes: Paginated retrieval
   - vendas: Daily partitioned fetch for historical data

3. **Storage in GCS**:
   - Raw JSON data saved as gs://{GCS_BUCKET_NAME}/CNPJ_{cnpj}/{endpoint}/{date}/response_pg{page}.json
   - Each record enriched with ingestion_timestamp, gcs_uri, and source_system

4. **Loading into BigQuery**:
   - Data loaded into CNPJ_{cnpj}_RAW dataset
   - Deduplication and schema validation ensures data quality

## License

This project is proprietary and confidential.