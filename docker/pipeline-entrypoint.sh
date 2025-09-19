#!/usr/bin/env bash
set -euo pipefail

# Permite cargar variables desde archivos .env si existen
load_env_file() {
  local env_path="$1"
  if [[ -f "$env_path" ]]; then
    echo "[entrypoint] Cargando variables desde ${env_path}"
    set -a
    # shellcheck disable=SC1090
    source "$env_path"
    set +a
  fi
}

PYTHON_BIN="${PYTHON_BIN:-python}"
MODE="${PIPELINE_MODE:-full}"
ROOT_DIR="/app"
DM_DIR="${ROOT_DIR}/Data_management"
DA_DIR="${ROOT_DIR}/Data_analytics"

EMBED_CFG="${EMBEDDINGS_CONFIG:-configs/embeddings.yaml}"
CLUSTER_CFG="${CLUSTERING_CONFIG:-configs/clustering.yaml}"
CCF_CFG="${CCF_CONFIG:-configs/ccf_analysis.yaml}"
EVENTS_CFG="${EVENTS_CONFIG:-configs/events.yaml}"

SKIP_EMBEDDINGS="${SKIP_EMBEDDINGS:-0}"
ANALYTICS_STEPS_DEFAULT="generate_embeddings run_clustering ccf events topics news enrich"
USER_STEPS="${ANALYTICS_STEPS:-}"

if [[ -n "${USER_STEPS}" ]]; then
  IFS=' ' read -r -a ANALYTICS_STEPS <<< "${USER_STEPS}"
else
  IFS=' ' read -r -a ANALYTICS_STEPS <<< "${ANALYTICS_STEPS_DEFAULT}"
fi

export PYTHONPATH="${PYTHONPATH:-}:${DM_DIR}:${DA_DIR}"

ensure_embeddings_dependency() {
  local has_cluster=0
  local has_embeddings=0
  for step in "${ANALYTICS_STEPS[@]}"; do
    case "$step" in
      generate_embeddings) has_embeddings=1 ;;
      run_clustering|clustering) has_cluster=1 ;;
    esac
  done

  if [[ ${has_cluster} -eq 1 && ${has_embeddings} -eq 0 && "${SKIP_EMBEDDINGS}" != "1" ]]; then
    ANALYTICS_STEPS=("generate_embeddings" "${ANALYTICS_STEPS[@]}")
    echo "[entrypoint] Agregando paso 'generate_embeddings' porque clustering lo requiere"
  fi
}

run_dm_pipeline() {
  load_env_file "${DM_DIR}/.env"
  echo "[entrypoint] Ejecutando pipeline de Data_management"
  pushd "${DM_DIR}" >/dev/null
  ${PYTHON_BIN} main.py
  popd >/dev/null
}

run_step_generate_embeddings() {
  if [[ "${SKIP_EMBEDDINGS}" == "1" ]]; then
    echo "[entrypoint] SKIP_EMBEDDINGS=1, saltando generacion de embeddings"
    return
  fi
  echo "[entrypoint] Generando embeddings"
  pushd "${DA_DIR}" >/dev/null
  ${PYTHON_BIN} src/pipelines/generate_embeddings.py --config "${EMBED_CFG}"
  popd >/dev/null
}

run_step_run_clustering() {
  echo "[entrypoint] Ejecutando clustering"
  pushd "${DA_DIR}" >/dev/null
  ${PYTHON_BIN} src/pipelines/run_clustering.py --config "${CLUSTER_CFG}"
  popd >/dev/null
}

run_step_ccf() {
  echo "[entrypoint] Ejecutando analisis CCF/Granger"
  pushd "${DA_DIR}" >/dev/null
  ${PYTHON_BIN} src/pipelines/ccf_analysis/analyze_competitors_ccf.py --config "${CCF_CFG}"
  popd >/dev/null
}

run_step_events() {
  echo "[entrypoint] Detectando eventos"
  pushd "${DA_DIR}" >/dev/null
  ${PYTHON_BIN} src/pipelines/event_detection/detect_events.py --config "${EVENTS_CFG}"
  popd >/dev/null
}

run_step_topics() {
  echo "[entrypoint] Modelando topicos"
  pushd "${DA_DIR}" >/dev/null
  ${PYTHON_BIN} src/insights/topic_motives.py --config "${EVENTS_CFG}"
  popd >/dev/null
}

run_step_news() {
  echo "[entrypoint] Clasificando noticias"
  pushd "${DA_DIR}" >/dev/null
  ${PYTHON_BIN} src/insights/news_classifier.py --config "${EVENTS_CFG}"
  popd >/dev/null
}

run_step_enrich() {
  echo "[entrypoint] Enriqueciendo eventos"
  pushd "${DA_DIR}" >/dev/null
  ${PYTHON_BIN} src/pipelines/event_detection/enrich_events.py --config "${EVENTS_CFG}"
  popd >/dev/null
}

run_analytics_steps() {
  load_env_file "${DA_DIR}/.env"
  ensure_embeddings_dependency
  for step in "${ANALYTICS_STEPS[@]}"; do
    case "$step" in
      generate_embeddings) run_step_generate_embeddings ;;
      run_clustering|clustering) run_step_run_clustering ;;
      ccf) run_step_ccf ;;
      events) run_step_events ;;
      topics) run_step_topics ;;
      news) run_step_news ;;
      enrich) run_step_enrich ;;
      *)
        echo "[entrypoint] Advertencia: paso '${step}' no reconocido, se omite" >&2
        ;;
    esac
  done
}

case "${MODE}" in
  full)
    run_dm_pipeline
    run_analytics_steps
    ;;
  analytics)
    run_analytics_steps
    ;;
  clustering)
    ANALYTICS_STEPS=("generate_embeddings" "run_clustering")
    run_analytics_steps
    ;;
  embeddings)
    ANALYTICS_STEPS=("generate_embeddings")
    run_analytics_steps
    ;;
  shell)
    echo "[entrypoint] Modo shell solicitado, abriendo bash"
    exec bash
    ;;
  *)
    echo "[entrypoint] Modo '${MODE}' no soportado" >&2
    exit 1
    ;;
esac

