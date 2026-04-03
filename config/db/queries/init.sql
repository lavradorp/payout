
CREATE TABLE IF NOT EXISTS tb_holidays (
holiday_date DATE PRIMARY KEY,
holiday_label VARCHAR(100)
);

COPY tb_holidays(holiday_date, holiday_label) 
FROM '/support_files/csv/nacional_holidays.csv' WITH (FORMAT CSV, HEADER TRUE);

CREATE TABLE IF NOT EXISTS tb_clients (
account_id VARCHAR(16) PRIMARY KEY,
cnpj VARCHAR(18),
rate_type VARCHAR(5),
rate_value FLOAT
);

COPY tb_clients(account_id, cnpj, rate_type, rate_value) 
FROM '/support_files/csv/clients.csv' WITH (FORMAT CSV, HEADER TRUE);

CREATE TABLE IF NOT EXISTS tb_balances (
position_date DATE,
account_id VARCHAR(16),
balance FLOAT
);

COPY tb_balances(position_date, account_id, balance) 
FROM '/support_files/csv/initial_balances.csv' WITH (FORMAT CSV, HEADER TRUE);
