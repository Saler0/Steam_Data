import sqlite3
import os
import logging

# --- Configuración ---
DB_PATH = 'data/steam_data.db'
TABLE_NAME = 'player_counts'

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def initialize_database():
    """
    Crea el archivo de la base de datos y la tabla 'player_counts' si no existen.
    Esta función está diseñada para ser ejecutada una sola vez al configurar el entorno.
    """
    try:
        logging.info(f"Asegurando que el directorio para la base de datos exista en '{os.path.dirname(DB_PATH)}'...")
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        
        logging.info(f"Conectando y creando la base de datos en: {DB_PATH}")
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        logging.info(f"Creando la tabla '{TABLE_NAME}' si no existe...")
        # La clave primaria (appid, month) asegura que no haya datos duplicados para el mismo juego en el mismo mes.
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                appid INTEGER,
                name TEXT,
                month TEXT,
                avg_players REAL,
                gain REAL,
                gain_percent REAL,
                peak_players REAL,
                PRIMARY KEY (appid, month)
            )
        ''')
        
        conn.commit()
        conn.close()
        logging.info("✅ Base de datos inicializada correctamente.")
        
    except Exception as e:
        logging.critical(f"❌ Error fatal al inicializar la base de datos: {e}")

if __name__ == "__main__":
    initialize_database()