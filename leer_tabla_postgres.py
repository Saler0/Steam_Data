import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()

# Configuración de la base de datos PostgreSQL desde variables de entorno
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "steam_data_db")
TABLE_NAME = 'player_counts'

# URL de conexión
DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def read_data_from_postgres():
    """
    Lee y muestra los datos de la tabla player_counts desde PostgreSQL.
    """
    print(f"Conectando a la base de datos '{DB_NAME}' en '{DB_HOST}'...")
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            # Comprobar si la tabla existe
            query = f"SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename  = '{TABLE_NAME}');"
            table_exists = pd.read_sql(query, conn).iloc[0,0]

            if not table_exists:
                print(f"Error: La tabla '{TABLE_NAME}' no existe en la base de datos.")
                return

            print(f"Leyendo datos de la tabla '{TABLE_NAME}'...")
            df = pd.read_sql(f"SELECT * FROM {TABLE_NAME} ORDER BY appid, month_date DESC", conn)
            
            if df.empty:
                print("La tabla está vacía.")
            else:
                print("Datos encontrados:")
                print(df.to_string())

    except Exception as e:
        print(f"Ocurrió un error al conectar o leer la base de datos: {e}")

if __name__ == '__main__':
    read_data_from_postgres()
