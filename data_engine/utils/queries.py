def holidays_date_query() -> str:
    return """
        SELECT holiday_date
        FROM tb_holidays
    """

def balances_query() -> str:
    return """
        SELECT *
        FROM tb_balances
        ORDER BY position_date DESC
    """

def clients_query() -> str:
    return """
        SELECT
            account_id,
            cnpj,
            rate_type as tax_type,
            rate_value/100 AS client_tax
        FROM tb_clients
    """