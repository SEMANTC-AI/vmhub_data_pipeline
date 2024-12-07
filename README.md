# VMHub Data Pipeline

This project implements a data pipeline to fetch data from VMHub API, store it in Google Cloud Storage (GCS), and load it into BigQuery for analysis.

## Overview

The pipeline performs the following operations:
- Fetches data from VMHub API (clientes and vendas endpoints)
- Processes data in daily batches for time-based endpoints
- Stores raw data in GCS with appropriate partitioning
- Loads processed data into BigQuery with schema validation
- Handles incremental loading and deduplication

## Project Structure

```
vmhub_data_pipeline/
├── schemas/                    # BigQuery schema definitions
│   ├── clientes.json
│   └── vendas.json
├── src/
│   ├── api/                   # API client implementation
│   │   ├── __init__.py
│   │   └── vmhub_client.py
│   ├── config/                # Configuration management
│   │   ├── __init__.py
│   │   ├── settings.py
│   │   └── endpoints.py
│   ├── utils/                 # Utility functions
│   │   ├── __init__.py
│   │   ├── gcs_helper.py
│   │   └── bigquery_helper.py
│   ├── __init__.py
│   └── main.py               # Main execution script
├── scripts/
│   └── build_push.sh         # Docker build and deployment script
├── credentials/              # GCP credentials (not in repo)
│   └── credentials.json
├── Dockerfile
├── requirements.txt
└── .env                      # Environment variables
```

## Prerequisites

- Python 3.9+
- Docker
- Google Cloud Platform account with:
  - BigQuery enabled
  - Cloud Storage enabled
  - Service account with appropriate permissions

## Configuration

1. Create `.env` file with required variables:
```env
VMHUB_API_KEY=your-api-key
VMHUB_CNPJ=your-cnpj
VMHUB_BASE_URL=https://apps.vmhub.vmtecnologia.io/vmlav/api/externa/v1
GCP_PROJECT_ID=your-project-id
GCS_BUCKET_NAME=vmhub-data
```

2. Place GCP service account credentials in `credentials/credentials.json`

## Installation

1. Create virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
.venv\Scripts\activate    # Windows
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Local Development
```bash
python -m src.main
```

### Docker Deployment
```bash
# Build image
docker build -t vmhub-pipeline:local .

# Run container
docker run \
  -v $(pwd)/.env:/app/.env \
  -v $(pwd)/credentials/credentials.json:/app/credentials/credentials.json:ro \
  -e GOOGLE_APPLICATION_CREDENTIALS=/app/credentials/credentials.json \
  -e PYTHONPATH=/app \
  vmhub-pipeline:local
```

### Cloud Run Deployment
```bash
# Build and deploy
./scripts/build_push.sh
```

## Data Flow

1. **Data Fetch**:
   - Clients data: Simple pagination
   - Sales data: Daily batches with pagination

2. **Storage**:
   - GCS Path Format: `CNPJ_{cnpj}/{endpoint}/{yyyy/mm/dd}/response_pg{page}.json`
   - Data enriched with metadata (GCS URI, ingestion timestamp)

3. **BigQuery**:
   - Tables created in dataset: `CNPJ_{cnpj}_RAW`
   - Schema validation and enforcement
   - Deduplication based on record IDs

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit changes
4. Push to the branch
5. Create a Pull Request

## License

This project is proprietary and confidential.