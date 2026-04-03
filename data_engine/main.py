from datetime import date
import warnings
import os
import duckdb
from pathlib import Path
import polars as pl
from endpoints.fixings import Fixings
from scripts.mock_balances import generate_balances
from services.data_manager import DataManager
from tables.dates import DateCalculator
from utils.api import API
from utils.cache import Cache
from utils.cl_and_form import CleaningFormating as cf
from utils.database import DB
from utils.financial import Financial as fin
from utils.queries import holidays_date_query, balances_query, clients_query


warnings.filterwarnings("ignore")

ref_date = date.today()
generate_balances(ref_date=ref_date)

path_support_files = './config/support_files'

db_client = DB()

dc = DateCalculator(client=db_client)
holidays = dc.get_holidays(query=holidays_date_query())
lbday_ref_date = dc.busday_calculator(date=ref_date, bus_day=1, holidays=holidays)

api_client = API(base_url="https://api.bcb.gov.br")
fixings = Fixings(client=api_client)

parquet_name = 'daily_payout.parquet'
path_parquet_payout = os.path.join(path_support_files, 'parquet', parquet_name)

data_manager = DataManager(client=db_client)

df = data_manager.get_or_update_parquet(
    file_path=path_parquet_payout,
    query=balances_query(),
    ref_date=lbday_ref_date
)

di = fixings.get_di(dc.busday_calculator(ref_date, 2, holidays))
factor_di = 1 + di
annualized_di = round(((factor_di ** 252) - 1)*100, 2)

df_clients = pl.LazyFrame(db_client.query_to_df(query=clients_query()))

lf = pl.scan_parquet(path_parquet_payout).filter(pl.col('position_date') == lbday_ref_date)
df_teste = pl.read_parquet(path_parquet_payout).filter(pl.col('position_date') == lbday_ref_date)

tax_columns = [col for col in df_clients.columns if col.endswith('tax')]

query = (
    lf
    .join(df_clients, left_on='account_id', right_on='account_id', how='left')
    .with_columns([
        pl.lit(annualized_di).alias('di'),
        pl.lit(factor_di).alias('factor_di'),
        pl.lit(0.0465).alias('pis'),
        pl.lit(0.225).alias('ir')
    ])
    .with_columns(
        gross_yield = pl.col('balance') * pl.col('factor_di') - pl.col('balance')
    )
    .with_columns(
        pis_value = pl.col('gross_yield')*pl.col('pis')
    )
    .with_columns(
        net_yield = pl.col('gross_yield') - pl.col('pis_value')
    )
    .with_columns([
        fin.calc_tax_factor(type_col='tax_type', di_factor_col='factor_di', spread_col=col).alias(f"{col.lower().replace(' ', '_')}_factor")
        for col in tax_columns
    ])
)

for col in tax_columns:
    preffix = col.lower().replace(' ', '_')
    f_name = f"{preffix}_factor"

    preffix_without_tax = preffix.replace('_tax', '')

    query = query.with_columns([
        (pl.col(f_name) * pl.col('balance')).alias(f"{preffix_without_tax}_balance"),
        ((pl.col(f_name)-1) * pl.col('balance')).alias(f"{preffix_without_tax}_gross_payout")
    ]).with_columns([
        (pl.col(f"{preffix_without_tax}_gross_payout") * pl.col('ir')).alias(f"{preffix_without_tax}_ir_payout")
    ]).with_columns([
        (pl.col(f"{preffix_without_tax}_gross_payout") - pl.col(f"{preffix_without_tax}_ir_payout")).alias(f"{preffix_without_tax}_net_payout")
    ])

query = query.with_columns(
    (pl.col('net_yield') - pl.col('client_gross_payout')).alias('cash_buffer')
)

df_final = query.collect()

df_final = df_final.rename({col: cf.columns_cleanings(col) for col in df_final.columns})

data_manager.save_duckdb(df=df_final, table_name='daily_payout', ref_date=lbday_ref_date)

redis_cache = Cache()

account_ids = df_final['account_id'].to_list()
net_yields = df_final['net_yield'].to_list()

redis_data = {
    f"payout:client:{account_id}:net_yield": round(float(net_yield), 2)
    for account_id, net_yield in zip(account_ids, net_yields)
}

redis_cache.push_balances_batch(balances_dict=redis_data)

