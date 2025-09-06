from pathlib import Path
import pandas as pd

def load_youtube_monthly(appid: str, cfg: dict, **kwargs) -> pd.DataFrame | None:
    mode = (cfg or {}).get('mode', 'file')
    if mode == 'file':
        path = Path(cfg.get('file', f"data/external/youtube/monthly_{appid}.csv"))
        if not path.exists(): return None
        df = pd.read_csv(path)
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df = df.dropna(subset=['date'])
        df = df.rename(columns={'date':'year_month'})
        df['year_month'] = df['year_month'].dt.to_period('M').dt.to_timestamp()
        return df
    return None
