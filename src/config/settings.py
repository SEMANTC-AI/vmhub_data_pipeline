import os
import json
from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict

@dataclass
class Settings:
    """Application settings."""
    
    def __init__(self):
        # API Settings
        self.VMHUB_API_KEY = os.getenv('VMHUB_API_KEY')
        self.VMHUB_CNPJ = os.getenv('VMHUB_CNPJ')
        self.VMHUB_BASE_URL = os.getenv('VMHUB_BASE_URL')
        
        # GCP Settings
        self.GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
        self.GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME')
        
        # Validate required settings
        self._validate_settings()

    def _validate_settings(self):
        """Validate required environment variables."""
        required_vars = [
            'VMHUB_API_KEY',
            'VMHUB_CNPJ',
            'VMHUB_BASE_URL',
            'GCP_PROJECT_ID',
            'GCS_BUCKET_NAME'
        ]
        
        missing = [var for var in required_vars if not getattr(self, var)]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    def get_schema(self, endpoint: str) -> List[Dict]:
        """Load BigQuery schema from JSON file."""
        schema_path = Path(__file__).parent.parent.parent / 'schemas' / f'{endpoint}.json'
        
        try:
            with open(schema_path) as f:
                schema_data = json.load(f)
                return schema_data['schema']
        except FileNotFoundError:
            raise ValueError(f"Schema file not found for endpoint: {endpoint}")