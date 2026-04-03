import os
import polars as pl
import duckdb
from datetime import date
from utils.database import DB

class DataManager:
    def __init__(self, client: DB):
        self.bd_client = client

    def get_or_update_parquet(self, file_path: str, query: str, ref_date: date, date_col: str = 'position_date') -> pl.DataFrame:
        must_update = True
        if os.path.exists(file_path):
            df_lazy = pl.scan_parquet(file_path)

            try:
                exists = df_lazy.filter(pl.col(date_col) == ref_date).collect().height > 0
            except Exception as e:
                print(f"Erro ao verificar a data no parquet: {e}")
                return
            
            if exists:
                must_update = False

        if must_update:
            self.bd_client.query_to_file(query=query, path=file_path)
        
        return pl.read_parquet(file_path)
    
    def save_duckdb(self, df: pl.DataFrame, table_name: str, ref_date: date, db_path: str = 'config/db/duckdb/balances_payout.duckdb'):
        with duckdb.connect(db_path) as con:
            con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df LIMIT 0")
            
            con.execute(f"DELETE FROM {table_name} WHERE position_date = '{ref_date}'")
            
            con.execute(f"INSERT INTO {table_name} SELECT * FROM df")
    
