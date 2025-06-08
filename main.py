import logging
import os
from data_ingestion.api_steam import ApiSteam


def setup_logging(log_dir="logs", log_file="pipeline.log"):
    os.makedirs(log_dir, exist_ok=True)
    full_path = os.path.join(log_dir, log_file)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(full_path, mode="a", encoding="utf-8"),  # Guarda en archivo
            logging.StreamHandler()  # Muestra en consola
        ]
    )

class PipelineIngest:
    def __init__(self, appids):
        self.appids = appids

    def run(self):
        logging.info(f"Iniciando ingesta de {len(self.appids)} juegos en ApiSteam…")
        steam = ApiSteam(appids_to_process=self.appids)
        steam.run()


def main():


    # para hacerlo realista se deberia hacer la parte de obtener todos los juegos de steam 
    # luego con eso hacer un clustering y tomar appids de juegos parecidos y obtener sus reviews
    # esta lista de appids serian una lista de juegos ya parecidos

    # get_all_games_steam() -> una funcion que obtendria todos los juegos de steam actualmente. luego en el cronjob del batch deberia obtener el datos de los juegos ya aun no han sido guardados en nuestra base de datos 

    # la logica de obtener todo el historico debe ser un poco diferente que la de obtener los ultimos cambios (esto es mas notorio en las reseñas)


    # Lista de APPIDs a procesar
    appids_to_process = [
        570940,
        374320,
        335300,
        1245620,
        1627720,
        1903340,
        814380,
        2680010,
        1501750,
        236430,
    ]

    setup_logging()
    pipeline = PipelineIngest(appids_to_process)
    pipeline.run()


if __name__ == "__main__":
    main()
