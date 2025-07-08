#################################################################################
# GLOBALS                                                                       #
#################################################################################

PROJECT_NAME = modelos_steam_data
PYTHON_VERSION = 3.10
PYTHON_INTERPRETER = python

#################################################################################
# COMMANDS                                                                      #
#################################################################################

## Install Python dependencies
.PHONY: requirements
requirements:
	$(PYTHON_INTERPRETER) -m pip install -U pip
	$(PYTHON_INTERPRETER) -m pip install -r requirements.txt

## Set up Python interpreter environment
.PHONY: create_environment
create_environment:
	@bash -c "if [ ! -z `which virtualenvwrapper.sh` ]; then source `which virtualenvwrapper.sh`; mkvirtualenv $(PROJECT_NAME) --python=$(PYTHON_INTERPRETER); else mkvirtualenv.bat $(PROJECT_NAME) --python=$(PYTHON_INTERPRETER); fi"
	@echo ">>> New virtualenv created. Activate with:\nworkon $(PROJECT_NAME)"

## Delete all compiled Python files
.PHONY: clean
clean:
	find . -type f -name "*.py[co]" -delete
	find . -type d -name "__pycache__" -delete

## Lint using ruff
.PHONY: lint
lint:
	ruff format --check
	ruff check

## Format source code with ruff
.PHONY: format
format:
	ruff check --fix
	ruff format

#################################################################################
# PIPELINE COMMANDS                                                             #
#################################################################################

## Run the full DVC pipeline (features -> clustering -> similitud)
.PHONY: pipeline
pipeline:
	dvc repro

## Run the clustering script (Spark + FAISS + MLflow)
.PHONY: clustering
clustering:
	$(PYTHON_INTERPRETER) train_clustering_faiss_spark.py

## Run the similitud script con FAISS + MLflow
.PHONY: similitud
similitud:
	$(PYTHON_INTERPRETER) predict_faiss_mlflow.py

## Launch the MLflow UI
.PHONY: mlflow
mlflow:
	mlflow ui

## Push versioned data and models to DVC remote
.PHONY: push
push:
	dvc push

## Pull versioned data from DVC remote
.PHONY: pull
pull:
	dvc pull

#################################################################################
# Self Documenting Commands                                                     #
#################################################################################

.DEFAULT_GOAL := help

define PRINT_HELP_PYSCRIPT
import re, sys; \
lines = '\n'.join([line for line in sys.stdin]); \
matches = re.findall(r'\n## (.*)\n[\s\S]+?\n([a-zA-Z_-]+):', lines); \
print('Available rules:\n'); \
print('\n'.join(['{:25}{}'.format(*reversed(match)) for match in matches]))
endef
export PRINT_HELP_PYSCRIPT

help:
	@$(PYTHON_INTERPRETER) -c "${PRINT_HELP_PYSCRIPT}" < $(MAKEFILE_LIST)
