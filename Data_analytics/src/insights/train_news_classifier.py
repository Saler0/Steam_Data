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
from sklearn.metrics import (
    classification_report,
    accuracy_score,
    precision_recall_fscore_support,
    confusion_matrix,
)
import joblib
import mlflow


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
    ap.add_argument('--mlflow-experiment', default='news_classifier_svm', help='Nombre de experimento en MLflow')
    ap.add_argument('--mlflow-enabled', action='store_true', default=True, help='Habilitar logging en MLflow')
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
    metrics: dict = {}
    report_txt: str | None = None
    if args.test_size and 0 < args.test_size < 0.9:
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=args.test_size, random_state=42, stratify=y)
        pipe.fit(X_train, y_train)
        y_pred = pipe.predict(X_test)
        acc = accuracy_score(y_test, y_pred)
        metrics['accuracy'] = float(acc)
        print(f"[OK] Accuracy test: {acc:.3f}")
        # Detailed metrics
        pr_macro, rc_macro, f1_macro, _ = precision_recall_fscore_support(y_test, y_pred, average='macro', zero_division=0)
        pr_micro, rc_micro, f1_micro, _ = precision_recall_fscore_support(y_test, y_pred, average='micro', zero_division=0)
        pr_weighted, rc_weighted, f1_weighted, _ = precision_recall_fscore_support(y_test, y_pred, average='weighted', zero_division=0)
        metrics.update({
            'precision_macro': float(pr_macro),
            'recall_macro': float(rc_macro),
            'f1_macro': float(f1_macro),
            'precision_micro': float(pr_micro),
            'recall_micro': float(rc_micro),
            'f1_micro': float(f1_micro),
            'precision_weighted': float(pr_weighted),
            'recall_weighted': float(rc_weighted),
            'f1_weighted': float(f1_weighted),
        })
        # Text report
        report_txt = classification_report(y_test, y_pred)
        print(report_txt)
        # Confusion matrix artifact
        labels_sorted = sorted(set(y_test) | set(y_pred))
        cm = confusion_matrix(y_test, y_pred, labels=labels_sorted)
        try:
            import matplotlib.pyplot as plt  # type: ignore
            import numpy as np  # noqa: F401
            fig, ax = plt.subplots(figsize=(6, 5))
            im = ax.imshow(cm, cmap='Blues')
            ax.set_title('Confusion Matrix (SVM)')
            ax.set_xlabel('Predicted')
            ax.set_ylabel('True')
            ax.set_xticks(range(len(labels_sorted)))
            ax.set_yticks(range(len(labels_sorted)))
            ax.set_xticklabels(labels_sorted, rotation=45, ha='right')
            ax.set_yticklabels(labels_sorted)
            for i in range(cm.shape[0]):
                for j in range(cm.shape[1]):
                    ax.text(j, i, str(cm[i, j]), ha='center', va='center', color='black')
            fig.tight_layout()
            cm_png = Path('outputs/events') / 'news_svm_confusion_matrix.png'
            cm_png.parent.mkdir(parents=True, exist_ok=True)
            fig.savefig(cm_png, dpi=150)
            plt.close(fig)
            cm_artifact_path = str(cm_png)
        except Exception:
            # Fallback to CSV artifact
            import csv
            cm_csv = Path('outputs/events') / 'news_svm_confusion_matrix.csv'
            cm_csv.parent.mkdir(parents=True, exist_ok=True)
            with cm_csv.open('w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([''] + labels_sorted)
                for i, row in enumerate(cm):
                    writer.writerow([labels_sorted[i]] + list(map(int, row)))
            cm_artifact_path = str(cm_csv)
    else:
        pipe.fit(X, y)

    out_path = Path(args.model_out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(pipe, out_path)
    print(f"[OK] Modelo guardado en -> {out_path}")

    if args.mlflow_enabled:
        try:
            mlflow.set_experiment(args.mlflow_experiment)
            with mlflow.start_run(run_name='svm_train'):
                # Params
                mlflow.log_param('text_cols', ','.join(text_cols))
                mlflow.log_param('label_col', args.label_col)
                mlflow.log_param('test_size', args.test_size)
                mlflow.log_param('vectorizer', 'tfidf')
                mlflow.log_param('ngram_range', '1-2')
                mlflow.log_param('max_features', 200000)
                mlflow.log_param('classifier', 'LinearSVC')
                mlflow.log_param('dataset_size', len(df))
                mlflow.log_param('n_classes', len(sorted(set(y))))
                # Metrics
                for k, v in metrics.items():
                    mlflow.log_metric(k, v)
                # Artifacts
                # Save classification report if present
                if report_txt:
                    rpt_path = Path('outputs/events') / 'news_svm_classification_report.txt'
                    rpt_path.parent.mkdir(parents=True, exist_ok=True)
                    rpt_path.write_text(report_txt, encoding='utf-8')
                    mlflow.log_artifact(str(rpt_path))
                # Confusion matrix artifact if computed
                if 'accuracy' in metrics and 'f1_macro' in metrics:
                    try:
                        mlflow.log_artifact(cm_artifact_path)
                    except Exception:
                        pass
                # Log model
                mlflow.log_artifact(str(out_path))
        except Exception as e:
            print(f"[WARN] No se pudo registrar en MLflow: {e}")


if __name__ == '__main__':
    main()
