import logging
import os
from data_ingestion.api_steam import ApiSteam


def setup_logging():
    LOG_DIR = "logs"
    os.makedirs(LOG_DIR, exist_ok=True)

    LOG_FILE = os.path.join(LOG_DIR, "etl_process.log")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8"),  # Guarda en archivo
            logging.StreamHandler()  # Muestra en consola
        ]
    )

class PipelineIngest:
    def __init__(self):
        pass
    def run():
        ApiSteam.run()


def main():
    setup_logging()
    pipeline = PipelineIngest()
    pipeline.run()


if __name__ == "__main__":
    main()
