#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Entrena un clasificador local (SVM lineal con TF-IDF) para noticias,
usando como etiquetas las generadas por el LLM en outputs/events/news_classified.parquet.

Guarda un Pipeline de scikit-learn (vectorizer + clasificador) en models/news_svm.joblib
para inferencia posterior sin coste de tokens.
"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import List

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.svm import LinearSVC
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, accuracy_score
import joblib


def _compose_text(df: pd.DataFrame, cols: List[str]) -> pd.Series:
    parts: List[pd.Series] = []
    for c in cols:
        if c in df.columns:
            parts.append(df[c].fillna('').astype(str))
    if not parts:
        raise SystemExit("No hay columnas de texto disponibles para entrenar (intenta --text-cols title,contents)")
    s = parts[0]
    for p in parts[1:]:
        s = s + ' ' + p
    return s


def main() -> None:
    ap = argparse.ArgumentParser(description="Entrena SVM para clasificar noticias a partir de etiquetas LLM")
    ap.add_argument('--input', default='outputs/events/news_classified.parquet', help='Parquet con columnas title[, contents], label')
    ap.add_argument('--text-cols', default='title', help='Columnas de texto separadas por coma (ej. title,contents)')
    ap.add_argument('--label-col', default='label', help='Nombre de la columna de etiqueta')
    ap.add_argument('--model-out', default='models/news_svm.joblib', help='Ruta de salida del modelo')
    ap.add_argument('--test-size', type=float, default=0.2, help='Proporción de test para validar (0 desactiva split)')
    args = ap.parse_args()

    in_path = Path(args.input)
    if not in_path.exists():
        raise SystemExit(f"No existe el dataset de entrenamiento: {in_path}")
    df = pd.read_parquet(in_path)
    if df.empty:
        raise SystemExit("Dataset vacío: no hay ejemplos para entrenar")

    text_cols = [c.strip() for c in args.text_cols.split(',') if c.strip()]
    if args.label_col not in df.columns:
        raise SystemExit(f"No se encuentra la columna de etiqueta '{args.label_col}' en el dataset")

    df = df.dropna(subset=[args.label_col])
    X = _compose_text(df, text_cols)
    y = df[args.label_col].astype(str)

    pipe = Pipeline([
        ('tfidf', TfidfVectorizer(lowercase=True, ngram_range=(1,2), max_features=200000)),
        ('clf', LinearSVC()),
    ])

    if args.test_size and 0 < args.test_size < 0.9:
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=args.test_size, random_state=42, stratify=y)
        pipe.fit(X_train, y_train)
        y_pred = pipe.predict(X_test)
        acc = accuracy_score(y_test, y_pred)
        print(f"[OK] Accuracy test: {acc:.3f}")
        print(classification_report(y_test, y_pred))
    else:
        pipe.fit(X, y)

    out_path = Path(args.model_out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(pipe, out_path)
    print(f"[OK] Modelo guardado en -> {out_path}")


if __name__ == '__main__':
    main()

