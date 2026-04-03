from datetime import datetime, date, timedelta
import yaml
import polars as pl
from utils.api import API
from pprint import pprint


class Fixings:
    def __init__(self, client: API):
        self.client = client
        self.bcb_fixings = self._load_fixings_config('./config/support_files/yml/bcb_fixings.yml')

    def _load_fixings_config(self, config_path: str) -> dict:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        
        return config['fixings']
    
    def get_di(self, date: date, range: int = 7) -> float | None:
        endpoint = f"dados/serie/bcdata.sgs.{self.bcb_fixings['cdi']}/dados"
        params = {
            "formato": "json",
            "dataInicial": (date - timedelta(days=range)).strftime("%d/%m/%Y"),
            "dataFinal": date.strftime("%d/%m/%Y")
        }
        text = self.client.request("GET", endpoint, params=params)
        
        df = pl.DataFrame(text)
        df = df.with_columns(
            pl.col('data')
            .str.to_datetime(format="%d/%m/%Y")
            .dt.date()
        )

        df = df.with_columns(
            pl.col('valor')
            .cast(pl.Float64)
        )

        try:
            di = df.filter(
                pl.col('data') == date
            ).select(
                pl.col('valor')
            ).item()/100

            return di
        
        except Exception as e:
            print(str(e))

            return None
        
