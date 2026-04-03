import os
import polars as pl
from utils.db_config import DatabaseConfig

class DB(DatabaseConfig):
    def __init__(self, config_path: str = "./config/support_files/yml/postgresql.yml"):
        super().__init__(config_path)
        self.connection_uri = self.get_connection_uri()

    def query_to_df(self, query: str) -> pl.DataFrame:
        try:
            return pl.read_database_uri(query=query, uri=self.connection_uri)
        except Exception as e:
            raise RuntimeError(f"Erro ao executar a query: {e}")

    def df_to_file(self, df: pl.DataFrame, path: str, **kwargs) -> None:
        ext = os.path.splitext(path)[1].lower()

        match ext:
            case '.parquet':
                df.write_parquet(path, **kwargs)
            case '.csv':
                df.write_csv(path, **kwargs)
            case '.xlsx' | '.xls':
                df.write_excel(path, **kwargs)
            case '.json':
                df.write_json(path, **kwargs)
            case _:
                raise ValueError(f"Unsupported file format: {ext}")
            
    def query_to_file(self, query: str, path: str, **kwargs) -> None:
        df = self.query_to_df(query)
        self.df_to_file(df, path, **kwargs)