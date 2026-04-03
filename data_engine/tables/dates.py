from datetime import datetime, timedelta
from typing import List
import polars as pl
from utils.database import DB


class DateCalculator:
    def __init__(self, client: DB):
        self.client = client

    def get_holidays(self, query: str) -> List[datetime.date]:
        df = self.client.query_to_df(query=query)

        holidays_list = (
            df["holiday_date"]
            .to_list()
        )

        return holidays_list
    
    @staticmethod
    def busday_calculator(date, bus_day, holidays) -> datetime.date:
        if isinstance(date, str):
            date = datetime.strptime(date, "%Y-%m-%dT%H:%M:%S%z").date()
        
        start_search = date - timedelta(days=bus_day * 4 + 10)

        bus_dates = (
            pl.date_range(start_search, date, interval="1d", eager=True)
            .to_frame("date")
            .filter(
                (pl.col("date").dt.weekday() < 6) &
                (~pl.col("date").is_in(holidays))
            )
            .sort("date", descending=True)
        )
        
        if date in holidays or date.weekday() >= 5:
            bus_day -= 1

        try:
            bus_date = bus_dates["date"][bus_day]
        except IndexError:
            return None
        
        return bus_date
            