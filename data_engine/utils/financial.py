import polars as pl

class Financial:
    @staticmethod
    def calc_di_factor(di_col: str = 'di', base: int = 252) -> pl.Expr:
        return (1 + pl.col(di_col)) ** (1 / base)
    
    @staticmethod
    def calc_tax_factor(type_col: str, di_factor_col: str, spread_col: str, base: int = 252) -> pl.Expr:

        expr_porcent_di = ((pl.col(di_factor_col) - 1) * pl.col(spread_col)) + 1
        expr_cdi = pl.col(di_factor_col) * (pl.col(spread_col) + 1) ** (1 / base)

        return (
            pl.when(pl.col(type_col) == '%DI')
            .then(expr_porcent_di)
            .when(pl.col(type_col) == 'CDI+')
            .then(expr_cdi)
            .otherwise(None)
        )