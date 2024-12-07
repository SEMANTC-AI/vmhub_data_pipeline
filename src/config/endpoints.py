# src/config/endpoints.py

from dataclasses import dataclass
from typing import List, Optional, Generator
from datetime import datetime, timedelta, time
import pytz

@dataclass
class Endpoint:
    name: str
    path: str
    page_size: int = 10
    max_retries: int = 3
    schema_file: str = None
    requires_date_range: bool = False
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None

    def get_daily_ranges(self) -> Generator[tuple[datetime, datetime], None, None]:
        """generate daily date ranges from start_date to end_date"""
        if not self.requires_date_range or not self.start_date:
            return

        current_date = self.start_date
        end_date = self.end_date or datetime.now(pytz.UTC)

        while current_date <= end_date:
            # set time to start of day
            day_start = datetime.combine(current_date.date(), time.min, tzinfo=pytz.UTC)
            # set time to end of day
            day_end = datetime.combine(current_date.date(), time.max, tzinfo=pytz.UTC)
            
            yield (day_start, day_end)
            current_date += timedelta(days=1)

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
        page_size=100,
        schema_file='vendas.json',
        requires_date_range=True,
        start_date=datetime(2023, 3, 1, tzinfo=pytz.UTC),
        end_date=datetime.now(pytz.UTC)
    )
    
    @classmethod
    def get_all(cls) -> List[Endpoint]:
        return [value for name, value in vars(cls).items() 
                if isinstance(value, Endpoint)]