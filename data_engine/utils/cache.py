import redis
from utils.db_config import DatabaseConfig

class Cache(DatabaseConfig):
    def __init__(self):
        super().__init__(config_path="./config/support_files/yml/redis.yml")
        self.client = redis.Redis.from_url(self.get_connection_uri(), decode_responses=True)

    def push_balances_batch(self, balances_dict: dict, ttl_seconds: int =3600) -> None:
        if not balances_dict:
            print("Nenhum dado para armazenar no cache.")
            return
        
        try:
            pipeline = self.client.pipeline()
            for key, value in balances_dict.items():
                pipeline.set(key, value, ex=ttl_seconds)

            pipeline.execute()
        
        except Exception as e:
            print(f"Erro ao armazenar dados no cache Redis: {e}")
            return None
        
        