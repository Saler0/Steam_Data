import pandas as pd
import numpy as np

def to_monthly(df, date_col, value_cols, how="sum"):
    """Agrega a nivel mensual.

    Si `how` es un string ("sum", "mean", ...), aplica a todas las columnas de `value_cols`.
    """
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col])
    df['year_month'] = df[date_col].dt.to_period('M').dt.to_timestamp()
    if isinstance(how, dict):
        agg = {c: how.get(c, 'sum') for c in value_cols}
        g = df.groupby('year_month').agg(agg)
    else:
        g = df.groupby('year_month')[value_cols].agg(how)
    return g.reset_index()

def dlog(x):
    x = pd.Series(x).astype(float)
    return np.log(x.replace(0, np.nan)).diff()

def monthly_or_quarterly(df, min_points=12):
    if len(df) < min_points:
        df = df.copy()
        df['quarter'] = pd.to_datetime(df['year_month']).dt.to_period('Q').dt.to_timestamp(how='end')
        return df.groupby('quarter').sum(numeric_only=True).reset_index().rename(columns={'quarter':'year_month'})
    return df

def rolling_zscore(s, win=6):
    s = pd.Series(s, dtype=float)
    mu = s.rolling(win, min_periods=3).mean()
    sd = s.rolling(win, min_periods=3).std()
    return (s - mu) / sd
