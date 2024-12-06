# src/config/endpoints.py

from dataclasses import dataclass
from typing import List

@dataclass
class Endpoint:
    name: str
    path: str
    page_size: int = 10
    max_retries: int = 3
    schema_file: str = None

class VMHubEndpoints:
    CLIENTES = Endpoint(
        name='clientes',
        path='clientes',
        page_size=10,
        schema_file='clientes.json'
    )
    
    @classmethod
    def get_all(cls) -> List[Endpoint]:
        return [value for name, value in vars(cls).items() 
                if isinstance(value, Endpoint)]