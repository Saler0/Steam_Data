#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Clasifica noticias y etiqueta tópicos de forma paralela con un LLM OSS,
registrando cada ejecución en MLflow.
"""
import argparse
import json
import yaml
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from pathlib import Path
from typing import Dict, List

import mlflow
import pandas as pd
import requests
from pymongo import MongoClient

from src.utils.io import read_parquet_any, write_parquet_any

def query_llm(prompt: str, llm_cfg: Dict) -> str:
    """
    Envía un prompt al endpoint del LLM y devuelve la respuesta.

    Args:
        prompt (str): El prompt a enviar al modelo.
        llm_cfg (Dict): Configuración del LLM (URL del servidor, modelo, etc.).

    Returns:
        str: La respuesta de texto generada por el LLM.
    """
    headers = {"Content-Type": "application/json"}
    payload = {
        "model": llm_cfg.get("model_id", "local-model"),
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": llm_cfg.get("max_new_tokens", 128),
        "temperature": llm_cfg.get("temperature", 0.1)
    }
    try:
        response = requests.post(llm_cfg["server_url"], headers=headers, data=json.dumps(payload))
        response.raise_for_status()
        return response.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        print(f"  -> LLM query failed: {e}")
        return ""

def load_news_from_mongo(appid: int, mongo_cfg: Dict) -> pd.DataFrame:
    """
    Carga noticias para un appid específico desde la colección de explotación de MongoDB.

    Args:
        appid (int): El AppID de Steam del juego.
        mongo_cfg (Dict): Configuración de la conexión a MongoDB.

    Returns:
        pd.DataFrame: Un DataFrame con las noticias encontradas.
    """
    try:
        client = MongoClient(mongo_cfg["uri"])
        db = client[mongo_cfg.get("db_name")]
        collection = db[mongo_cfg.get("collection_name")]
        
        query = {"appid": appid}
        projection = {"_id": 0, "gid": 1, "title": 1, "contents": 1, "date": 1, "appid": 1}
        
        news_cursor = collection.find(query, projection)
        news_list = list(news_cursor)
        client.close()
        
        return pd.DataFrame(news_list) if news_list else pd.DataFrame()
    except Exception as e:
        print(f"Error al conectar o consultar MongoDB: {e}")
        return pd.DataFrame()


def distinct_appids(mongo_cfg: Dict) -> List[int]:
    """Devuelve la lista de appids distintos presentes en la colección de noticias."""
    try:
        client = MongoClient(mongo_cfg["uri"])
        db = client[mongo_cfg.get("db_name")]
        collection = db[mongo_cfg.get("collection_name")]
        values = collection.distinct("appid")
        client.close()
        out = []
        for v in values:
            try:
                if v is None:
                    continue
                out.append(int(v))
            except Exception:
                continue
        return sorted(set(out))
    except Exception as e:
        print(f"No se pudieron obtener appids distintos de Mongo: {e}")
        return []

def classify_single_news(title: str, llm_cfg: Dict) -> str | None:
    """Clasifica un título de noticia. Normaliza y valida contra etiquetas permitidas."""
    labels_cfg = llm_cfg.get("news_labels", []) or []
    allowed = {str(x).strip().lower() for x in labels_cfg}
    labels_str = ", ".join(labels_cfg)
    prompt = (
        f"Clasifica la siguiente noticia en una de estas categorías: [{labels_str}]. "
        f"Responde solo con una categoría exacta.\n\nNoticia: '{title}'\n\nCategoría:"
    )
    raw = query_llm(prompt, llm_cfg)
    label = (raw or "").strip().lower()
    # Correcciones simples de alias
    aliases = {"patches": "patch", "marketing/ads": "marketing", "community update": "community"}
    label = aliases.get(label, label)
    return label if label in allowed else None

def classify_news_parallel(news_df: pd.DataFrame, llm_cfg: Dict) -> pd.DataFrame:
    """Clasifica noticias en paralelo usando un ThreadPoolExecutor."""
    titles = news_df['title'].tolist()
    max_workers = llm_cfg.get("max_workers", 8)
    
    worker_fn = partial(classify_single_news, llm_cfg=llm_cfg)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        labels = list(executor.map(worker_fn, titles))
    
    news_df_copy = news_df.copy()
    news_df_copy['label'] = labels
    
    return news_df_copy.dropna(subset=['label']).reset_index(drop=True)

def _merge_parquet_safely(df_new: pd.DataFrame, path: Path, key_cols: list[str]) -> pd.DataFrame:
    """Anexa a un único parquet consolidado deduplicando por claves.
    Si no existe, crea; si existe, concat y drop_duplicates.
    """
    try:
        if path.exists():
            df_old = read_parquet_any(path)
            df_all = pd.concat([df_old, df_new], ignore_index=True)
            df_all = df_all.drop_duplicates(subset=key_cols)
        else:
            df_all = df_new.copy()
        write_parquet_any(df_all, path)
        return df_all
    except Exception as e:
        print(f"No se pudo consolidar parquet {path}: {e}")
        # fallback: guardar solo lo nuevo
        write_parquet_any(df_new, path)
        return df_new

def label_topics(topics_df: pd.DataFrame, llm_cfg: Dict) -> pd.DataFrame:
    """
    Etiqueta cada tópico de BERTopic en un DataFrame usando un LLM.
    Esta función también podría paralelizarse si el número de tópicos es muy grande.
    """
    topics_df_copy = topics_df.copy()
    for _, row in topics_df_copy.iterrows():
        topics_list = row.get('topics', [])
        if not isinstance(topics_list, list):
            continue
        for topic_data in topics_list:
            rep = topic_data.get('Representation', [])
            if isinstance(rep, list):
                words = rep
            else:
                words = str(rep).split(',') if rep is not None else []
            keywords = ", ".join([str(w).strip() for w in words if str(w).strip()])
            if not keywords:
                continue
            prompt = (
                "Resume los siguientes keywords en una etiqueta corta y descriptiva de 2-4 palabras "
                f"que represente el tema principal.\n\nKeywords: {keywords}\n\nEtiqueta:"
            )
            label = query_llm(prompt, llm_cfg)
            topic_data['llm_label'] = label
    return topics_df_copy

def _select_mlflow_experiment(cfg: Dict) -> str:
    """Elige el experimento de MLflow priorizando llm.mlflow_experiment,
    luego cfg.mlflow.experiment, y por último 'Steam_Events_Classification'."""
    llm = cfg.get("llm", {})
    if llm.get("mlflow_experiment"):
        return llm["mlflow_experiment"]
    mlf = cfg.get("mlflow", {})
    if mlf.get("experiment"):
        return mlf["experiment"]
    return cfg.get("mlflow_experiment_name", "Steam_Events_Classification")


def main():
    """Punto de entrada principal del script."""
    ap = argparse.ArgumentParser(description="Clasifica noticias y etiqueta tópicos con un LLM.")
    ap.add_argument("--config", required=True, help="Ruta al fichero de configuración YAML.")
    ap.add_argument("--appid", type=int, help="AppID para cargar y clasificar sus noticias desde MongoDB.")
    args = ap.parse_args()
    
    with open(args.config, 'r', encoding='utf-8') as f:
        cfg = yaml.safe_load(f)
    
    outdir = Path(cfg.get('output_dir', 'outputs/events'))
    outdir.mkdir(parents=True, exist_ok=True)
    
    llm_cfg = cfg.get('llm', {})
    if not llm_cfg.get('enabled', False):
        print("El clasificador LLM está deshabilitado en la configuración.")
        return

    mlflow.set_experiment(_select_mlflow_experiment(cfg))

    with mlflow.start_run():
        print(f"MLflow Run started. Experiment: '{mlflow.get_experiment(mlflow.active_run().info.experiment_id).name}'")
        mlflow.log_param("config_file", args.config)

        mongo_cfg = cfg.get("mongodb", {})

        if args.appid:
            mlflow.log_param("appid", args.appid)
            mlflow.log_params(llm_cfg)
            
            news_df = load_news_from_mongo(args.appid, mongo_cfg)
            
            if not news_df.empty:
                print(f"Clasificando {len(news_df)} noticias para el appid {args.appid} en paralelo...")
                classified_news = classify_news_parallel(news_df, llm_cfg)
                
                mlflow.log_metric("total_news_found", len(news_df))
                mlflow.log_metric("news_classified_count", len(classified_news))
                
                if not classified_news.empty:
                    # Consolidar en un único parquet como define DVC
                    output_path = outdir / 'news_classified.parquet'
                    all_df = _merge_parquet_safely(classified_news, output_path, key_cols=['appid','gid','title'])
                    mlflow.log_artifact(str(output_path))
                    print(f"{len(classified_news)} noticias clasificadas; consolidado total: {len(all_df)}. Registrado en MLflow.")
                else:
                    print("Ninguna noticia pudo ser clasificada.")
            else:
                print(f"No se encontraron noticias para el appid {args.appid}.")
        else:
            # Modo batch: clasificar para todos los appids presentes en Mongo
            appids = distinct_appids(mongo_cfg)
            if not appids:
                print("No se encontraron appids en la colección de noticias. Fin.")
            total_found = 0
            total_classified = 0
            output_path = outdir / 'news_classified.parquet'
            for i, appid in enumerate(appids, start=1):
                news_df = load_news_from_mongo(appid, mongo_cfg)
                if news_df.empty:
                    continue
                total_found += len(news_df)
                print(f"[{i}/{len(appids)}] Clasificando {len(news_df)} noticias para appid {appid}...")
                classified_news = classify_news_parallel(news_df, llm_cfg)
                total_classified += len(classified_news)
                if not classified_news.empty:
                    _merge_parquet_safely(classified_news, output_path, key_cols=['appid','gid','title'])
            mlflow.log_metric("total_news_found", total_found)
            mlflow.log_metric("news_classified_count", total_classified)
            if (outdir / 'news_classified.parquet').exists():
                mlflow.log_artifact(str(outdir / 'news_classified.parquet'))

        topics_input_path = cfg.get("topics_input_path")
        if topics_input_path and Path(topics_input_path).exists():
            topics_df = read_parquet_any(Path(topics_input_path))
            if not topics_df.empty:
                print(f"\nEtiquetando tópicos desde '{topics_input_path}'...")
                labeled_topics = label_topics(topics_df, llm_cfg)
                output_path = outdir / 'topics_labeled.parquet'
                write_parquet_any(labeled_topics, output_path)
                mlflow.log_artifact(str(output_path), artifact_path="topics")
                print(f"Tópicos etiquetados guardados en -> {output_path}")
        else:
             print(f"\nNo se encontró el fichero de tópicos o no fue especificado. Se omite este paso.")

if __name__ == "__main__":
    main()
