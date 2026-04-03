import yaml
import os

class DatabaseConfig:
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.config = self._load_config()

    def _load_config(self) -> dict:
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Arquivo de configuração não encontrado: {self.config_path}")
        
        with open(self.config_path, 'r') as file:
            return yaml.safe_load(file)['database']

    def get_connection_uri(self) -> str:
        engine = self.config.get('engine', '').lower()
        user = self.config.get('user', '')
        password = self.config.get('password', '')
        host = self.config.get('host', '')
        port = self.config.get('port', '')
        db_name = self.config.get('db_name', '')

        if engine not in ['sqlite', 'redis'] and not all([user, host, port, db_name]):
            raise ValueError(f"Faltam credenciais no YAML para a engine: {engine}")

        match engine:
            case 'postgresql' | 'postgres':
                return f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
            
            case 'mysql' | 'mariadb':
                return f"mysql://{user}:{password}@{host}:{port}/{db_name}"
            
            case 'sqlserver' | 'mssql':
                return f"mssql://{user}:{password}@{host}:{port}/{db_name}"
            
            case 'oracle':
                return f"oracle://{user}:{password}@{host}:{port}/{db_name}"
            
            case 'redshift':
                return f"redshift://{user}:{password}@{host}:{port}/{db_name}"
            
            case 'sqlite':
                return f"sqlite:///{db_name}"
            
            case 'redis':
                if user == "" and password == "":
                    return f"redis://{host}:{port}/{db_name}"
                
                return f"redis://{user}:{password}@{host}:{port}/{db_name}"
            
            case _:
                raise ValueError(f"Engine de banco de dados não suportada: {engine}")