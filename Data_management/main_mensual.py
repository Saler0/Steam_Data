import argparse
from web_scraping_steambase import Steamcharts  # Importamos nuestro módulo de scraping

def main():
    # --- Configuración del Parser Principal ---
    parser = argparse.ArgumentParser(
        description="Herramienta para recopilar y analizar datos de jugadores de Steam.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    # Ya no necesitamos subparsers, los argumentos van directamente al parser principal
    parser.add_argument(
        "-i", "--input",
        default="data/demo.ndjson",
        help="Ruta al archivo NDJSON de entrada con la lista de appids.\n(default: %(default)s)"
    )
    parser.add_argument(
        "-d", "--database",
        default="data/steam_data.db",
        help="Ruta a la base de datos SQLite donde se guardarán los datos.\n(default: %(default)s)"
    )

    args = parser.parse_args()

    print("Iniciando tarea de scraping...")
    Steamcharts.main(args.input, args.database)
    print("Tarea de scraping finalizada.")

if __name__ == "__main__":
    main()
