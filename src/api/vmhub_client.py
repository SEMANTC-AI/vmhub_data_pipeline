# src/api/vmhub_client.py
import requests
from typing import Dict, List, Optional, Union
import structlog
from urllib.parse import quote

logger = structlog.get_logger()

class VMHubAPIError(Exception):
    """Custom exception for VMHub API errors."""
    pass

class VMHubClient:
    """Client for interacting with VMHub API."""
    
    def __init__(self, base_url: str, api_key: str):
        """
        Initialize VMHub client.
        
        Args:
            base_url: Base URL for VMHub API
            api_key: API key for authentication
        """
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({
            'accept': 'application/json',
            'x-api-key': self.api_key
        })
    
    def _make_request(
        self, 
        method: str, 
        endpoint: str, 
        params: Optional[Dict] = None
    ) -> Union[List[Dict], Dict]:
        """
        Make HTTP request to VMHub API.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint
            params: Query parameters
            
        Returns:
            API response data
            
        Raises:
            VMHubAPIError: If API request fails
        """
        url = f"{self.base_url}/{endpoint}"
        
        try:
            response = self.session.request(
                method=method,
                url=url,
                params=params
            )
            
            # Raise for HTTP errors
            response.raise_for_status()
            
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(
                "VMHub API request failed",
                error=str(e),
                url=url,
                method=method,
                params=params
            )
            raise VMHubAPIError(f"API request failed: {str(e)}")
        
        except ValueError as e:
            logger.error(
                "Failed to parse VMHub API response",
                error=str(e),
                url=url,
                method=method,
                params=params
            )
            raise VMHubAPIError(f"Failed to parse API response: {str(e)}")
    
    def get_clients(
        self, 
        cnpj: str, 
        page: int = 0, 
        page_size: int = 10
    ) -> List[Dict]:
        """
        Fetch clients from VMHub API.
        
        Args:
            cnpj: Company CNPJ
            page: Page number (0-based)
            page_size: Number of records per page (max 1000)
            
        Returns:
            List of client records
            
        Raises:
            VMHubAPIError: If API request fails
            ValueError: If page_size > 1000
        """
        if page_size > 1000:
            raise ValueError("page_size cannot exceed 1000")
        
        # URL encode CNPJ
        encoded_cnpj = quote(cnpj)
        
        params = {
            'CNPJ': encoded_cnpj,
            'pagina': page,
            'quantidade': page_size
        }
        
        try:
            response_data = self._make_request(
                method='GET',
                endpoint='clientes',
                params=params
            )
            
            if not isinstance(response_data, list):
                logger.error(
                    "Unexpected response format",
                    response_data=response_data
                )
                raise VMHubAPIError("Unexpected response format")
                
            logger.info(
                "Successfully fetched clients",
                cnpj=cnpj,
                page=page,
                record_count=len(response_data)
            )
            
            return response_data
            
        except VMHubAPIError:
            logger.error(
                "Failed to fetch clients",
                cnpj=cnpj,
                page=page,
                page_size=page_size
            )
            raise

    def __enter__(self):
        """Context manager enter."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.session.close()