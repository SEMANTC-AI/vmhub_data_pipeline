# src/config/settings.py

import os
from dataclasses import dataclass
from typing import Dict, List
from pathlib import Path
import json

@dataclass
class Settings:
    VMHUB_API_KEY: str
    VMHUB_CNPJ: str
    VMHUB_BASE_URL: str
    GCP_PROJECT_ID: str
    GCS_BUCKET_NAME: str

    def __init__(self):
        self.VMHUB_API_KEY = os.getenv('VMHUB_API_KEY')
        self.VMHUB_CNPJ = os.getenv('VMHUB_CNPJ')
        self.VMHUB_BASE_URL = os.getenv('VMHUB_BASE_URL')
        self.GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
        self.GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME')
        self._validate_settings()

    def _validate_settings(self):
        required_vars = [
            'VMHUB_API_KEY',
            'VMHUB_CNPJ',
            'VMHUB_BASE_URL',
            'GCP_PROJECT_ID',
            'GCS_BUCKET_NAME'
        ]
        missing = [var for var in required_vars if not getattr(self, var)]
        if missing:
            raise ValueError(f"missing required environment variables: {', '.join(missing)}")

    def get_schema(self, endpoint: str) -> List[Dict]:
        schema_path = Path(__file__).parent.parent.parent / 'schemas' / f'{endpoint}.json'
        if not schema_path.exists():
            raise ValueError(f"schema file not found for endpoint: {endpoint}")
        with open(schema_path) as f:
            schema_data = json.load(f)
            return schema_data['schema']