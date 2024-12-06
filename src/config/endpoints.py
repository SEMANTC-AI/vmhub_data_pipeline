# src/config/endpoints.py

from dataclasses import dataclass
from typing import List, Optional
from datetime import datetime, timedelta
import pytz

@dataclass
class Endpoint:
    name: str
    path: str
    page_size: int = 10
    max_retries: int = 3
    schema_file: str = None
    requires_date_range: bool = False
    max_date_range_days: Optional[int] = None
    historical_days: Optional[int] = None

    def get_date_ranges(self) -> List[tuple]:
        """Get list of date ranges from historical days until now."""
        if not self.requires_date_range or not self.max_date_range_days:
            return []
            
        ranges = []
        end_date = datetime.now(pytz.UTC)
        start_date = end_date - timedelta(days=self.historical_days or 1000)
        
        current_start = start_date
        while current_start < end_date:
            range_start = current_start
            range_end = min(current_start + timedelta(days=self.max_date_range_days), end_date)
            
            ranges.append((range_start, range_end))
            current_start = range_end
            
        return ranges

class VMHubEndpoints:
    CLIENTES = Endpoint(
        name='clientes',
        path='clientes',
        page_size=10,
        schema_file='clientes.json'
    )
    
    VENDAS = Endpoint(
        name='vendas',
        path='vendas',
        page_size=1000,  # Changed from 10 to 1000
        schema_file='vendas.json',
        requires_date_range=True,
        max_date_range_days=90,
        historical_days=1000
    )
    
    @classmethod
    def get_all(cls) -> List[Endpoint]:
        return [value for name, value in vars(cls).items() 
                if isinstance(value, Endpoint)]