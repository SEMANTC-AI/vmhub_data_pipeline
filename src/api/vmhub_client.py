# src/api/vmhub_client.py

import requests
from typing import Dict, List, Optional, Union
import structlog
from urllib.parse import quote
import time
from random import uniform
from datetime import datetime

logger = structlog.get_logger()

class VMHubAPIError(Exception):
    pass

class VMHubClient:
    """Client to interact with VMHub API."""
    def __init__(
        self, 
        base_url: str, 
        api_key: str,
        max_retries: int = 3,
        initial_backoff: float = 1.0,
        max_backoff: float = 32.0,
        backoff_factor: float = 2.0
    ):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.max_retries = max_retries
        self.initial_backoff = initial_backoff
        self.max_backoff = max_backoff
        self.backoff_factor = backoff_factor
        
        self.session = requests.Session()
        self.session.headers.update({
            'accept': 'application/json',
            'x-api-key': self.api_key
        })
    
    def _make_request_with_backoff(
        self, 
        method: str, 
        endpoint: str, 
        params: Optional[Dict] = None
    ) -> Union[List[Dict], Dict]:
        url = f"{self.base_url}/{endpoint}"
        current_retry = 0
        current_backoff = self.initial_backoff

        while current_retry <= self.max_retries:
            try:
                response = self.session.request(method=method, url=url, params=params)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                status_code = e.response.status_code if hasattr(e, 'response') and e.response else None
                current_retry += 1
                
                if current_retry > self.max_retries:
                    logger.error("VMHub API request failed after all retries", error=str(e))
                    raise VMHubAPIError(f"API request failed after {self.max_retries} retries: {str(e)}")

                if status_code == 500 and params and params.get('pagina', 0) > 0:
                    logger.warning("500 error possibly indicating no more data", page=params.get('pagina'), endpoint=endpoint)
                    raise VMHubAPIError(f"API request failed: {str(e)}")

                jitter = uniform(0, 0.1 * current_backoff)
                sleep_time = min(current_backoff + jitter, self.max_backoff)
                logger.warning("Request failed, retrying", error=str(e), retry_number=current_retry, backoff_time=sleep_time)
                time.sleep(sleep_time)
                current_backoff *= self.backoff_factor

    def get_data(
        self, 
        endpoint: str,
        cnpj: str, 
        page: int = 0, 
        page_size: int = 10,
        date_start: Optional[datetime] = None,
        date_end: Optional[datetime] = None,
        somente_sucesso: bool = True
    ) -> List[Dict]:
        if endpoint == 'clientes' and page_size > 10:
            raise ValueError("page_size cannot exceed 10 for clientes endpoint")
        if endpoint == 'vendas' and page_size > 1000:
            raise ValueError("page_size cannot exceed 1000 for vendas endpoint")

        encoded_cnpj = quote(cnpj)
        params = {
            'CNPJ': encoded_cnpj,
            'pagina': page,
            'quantidade': page_size
        }
        if date_start and date_end:
            params.update({
                'dataInicio': date_start.strftime('%Y-%m-%dT%H:%M:%SZ'),
                'dataTermino': date_end.strftime('%Y-%m-%dT%H:%M:%SZ'),
                'somenteSucesso': str(somente_sucesso).lower()
            })

        response_data = self._make_request_with_backoff('GET', endpoint, params=params)
        
        if not isinstance(response_data, list):
            raise VMHubAPIError("Unexpected response format")

        logger.info("Fetched data", endpoint=endpoint, cnpj=cnpj, page=page, record_count=len(response_data))
        return response_data

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()