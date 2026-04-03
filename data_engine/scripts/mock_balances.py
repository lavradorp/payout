import os
import random
from datetime import date, timedelta
import polars as pl

def generate_balances(ref_date: date):
    balance_ref_date = ref_date - timedelta(days=1)
    balances_data = []

    csv_dir = "./config/support_files/csv"
    os.makedirs(csv_dir, exist_ok=True)

    clients_path = os.path.join(csv_dir, "clients.csv")
    if not os.path.exists(clients_path):
        raise FileNotFoundError(f"Arquivo de clientes não encontrado em {clients_path}. Por favor, crie o arquivo com os dados dos clientes antes de gerar os saldos.")
    
    df_clients = pl.read_csv(clients_path)

    account_ids = df_clients.select(pl.col("account_id")).to_series().to_list()
    
    for acc_id in account_ids:
        balance = round(random.uniform(1000000.0, 10000000.0), 2)
        
        balances_data.append({
            "position_date": balance_ref_date,
            "account_id": acc_id,
            "balance": balance
        })

    df_balances = pl.DataFrame(balances_data)
 
    balances_path = os.path.join(csv_dir, "initial_balances.csv")

    if os.path.exists(balances_path):
        df_balances_existing = pl.read_csv(balances_path)

        if isinstance(df_balances_existing['position_date'][0], str):
            df_balances_existing = df_balances_existing.with_columns(
                pl.col('position_date')
                .str.to_datetime("%Y-%m-%d")
                .dt.date()
            )
            
        if balance_ref_date in df_balances_existing['position_date'].unique():
            return
        else:
            df_balances = pl.concat([df_balances_existing, df_balances], how="vertical")
            df_balances = df_balances.sort("position_date")

    df_balances.write_csv(balances_path)
