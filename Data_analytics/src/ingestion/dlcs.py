import pandas as pd
from pymongo import MongoClient

def load_dlcs_for_game(appid: str, mongo_cfg: dict) -> pd.DataFrame:
    client = MongoClient(mongo_cfg['uri'])
    col = client[mongo_cfg['database']][mongo_cfg['collection']]
    doc = col.find_one({"appid": {"$in":[appid, int(appid)]}}, {"_id":0,"dlc":1})
    client.close()
    dlc = (doc or {}).get('dlc') or []
    if not dlc: return pd.DataFrame(columns=['appid','dlc_appid','dlc_name','release_date','year_month'])
    df = pd.DataFrame(dlc)
    if 'release_date' in df.columns:
        df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
        df['year_month'] = df['release_date'].dt.to_period('M').dt.to_timestamp()
    df = df.rename(columns={'appid':'dlc_appid','name':'dlc_name'})
    df['appid'] = str(appid)
    return df[['appid','dlc_appid','dlc_name','release_date','year_month']]
