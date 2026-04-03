import os
import random
from datetime import date, timedelta
import polars as pl
from faker import Faker

def generate_mock_clients(num_clients: int = 20):
    fake = Faker('pt_BR')
    
    clients_data = []
    account_ids = []
    
    for _ in range(num_clients):
        acc_id = fake.lexify(text="????????????????", letters="0123456789abcdef")
        account_ids.append(acc_id)
        
        cnpj = fake.cnpj()
        
        rate_type = "%DI"
        
        rate_value = random.randrange(80, 100, 5)
        
        clients_data.append({
            "account_id": acc_id,
            "cnpj": cnpj,
            "rate_type": rate_type,
            "rate_value": rate_value
        })

    df_clients = pl.DataFrame(clients_data)
    
    output_dir = "config/support_files"
    os.makedirs(output_dir, exist_ok=True)
    clients_path = os.path.join(output_dir, "clients.csv")

    if os.path.exists(clients_path):
        df_clients_existing = pl.read_csv(clients_path)
        df_clients = pl.concat([df_clients_existing, df_clients], how="vertical")
    
    df_clients.write_csv(clients_path)


if __name__ == "__main__":
    generate_mock_clients(45)