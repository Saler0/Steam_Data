#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Entrena mÃºltiples clasificadores locales para noticias a partir de
outputs/events/news_classified.parquet, registra mÃ©tricas en MLflow,
y guarda el mejor modelo como models/news_best.joblib.

Modelos soportados:
  - svm        (TFIDF -> LinearSVC)
  - logreg     (TFIDF -> LogisticRegression)
  - nb         (TFIDF -> MultinomialNB)
  - dt         (TFIDF -> SVD -> DecisionTree)
  - rf         (TFIDF -> SVD -> RandomForest)
  - lda        (TFIDF -> SVD -> StandardScaler -> LinearDiscriminantAnalysis)
  - mlp        (TFIDF -> SVD -> StandardScaler -> MLPClassifier)
"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import List

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.svm import LinearSVC
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from sklearn.naive_bayes import MultinomialNB
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.decomposition import TruncatedSVD
from sklearn.preprocessing import StandardScaler
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
from sklearn.model_selection import train_test_split, StratifiedKFold
from sklearn.metrics import (
    classification_report,
    accuracy_score,
    precision_recall_fscore_support,
    confusion_matrix,
)
import joblib
import mlflow
from typing import Tuple

try:
    from sentence_transformers import SentenceTransformer  # type: ignore
except Exception:
    SentenceTransformer = None  # type: ignore


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
    ap = argparse.ArgumentParser(description="Auto-entrenamiento multimodelo para clasificador de noticias")
    ap.add_argument('--input', default='outputs/events/news_classified.parquet', help='Parquet con columnas title[, contents], label')
    ap.add_argument('--text-cols', default='title,contents', help='Columnas de texto separadas por coma (ej. title,contents)')
    ap.add_argument('--label-col', default='label', help='Nombre de la columna de etiqueta')
    ap.add_argument('--models', default='all', help='Modelos a entrenar: coma-sep o "all" (svm,logreg,nb,dt,rf,lda,mlp)')
    ap.add_argument('--featurizer', default='tfidf', choices=['tfidf','sbert'], help='Tipo de caracterÃ­sticas: tfidf o sbert')
    ap.add_argument('--embedding-model', default='all-MiniLM-L6-v2', help='Modelo SBERT para --featurizer sbert')
    ap.add_argument('--embed-batch-size', type=int, default=64, help='Batch size para SBERT')
    ap.add_argument('--svd-dim', type=int, default=300, help='DimensiÃ³n SVD para modelos que requieren entrada densa')
    ap.add_argument('--test-size', type=float, default=0.2, help='ProporciÃ³n de test para validar (0 desactiva split)')
    ap.add_argument('--scoring', default='f1_macro', choices=['f1_macro','accuracy','f1_weighted'], help='MÃ©trica para elegir el mejor')
    ap.add_argument('--mlflow-experiment', default='news_classifier', help='Experimento MLflow')
    ap.add_argument('--mlflow-enabled', action='store_true', default=True, help='Habilitar logging en MLflow')
    ap.add_argument('--cv', type=int, default=0, help='K-fold CV sobre el split de entrenamiento (0 desactiva)')
    args = ap.parse_args()

    in_path = Path(args.input)
    if not in_path.exists():
        raise SystemExit(f"No existe el dataset de entrenamiento: {in_path}")
    df = pd.read_parquet(in_path)
    if df.empty:
        raise SystemExit("Dataset vacÃ­o: no hay ejemplos para entrenar")

    text_cols = [c.strip() for c in args.text_cols.split(',') if c.strip()]
    if args.label_col not in df.columns:
        raise SystemExit(f"No se encuentra la columna de etiqueta '{args.label_col}' en el dataset")

    df = df.dropna(subset=[args.label_col])
    X_text = _compose_text(df, text_cols)
    y = df[args.label_col].astype(str)

    models_str = args.models.strip().lower()
    if models_str == 'all':
        model_names = ['svm','logreg','nb','dt','rf','lda','mlp']
    else:
        model_names = [m.strip() for m in models_str.split(',') if m.strip()]

    def make_pipeline(name: str, featurizer: str) -> Pipeline:
        if featurizer == 'tfidf':
            tfidf = ('tfidf', TfidfVectorizer(lowercase=True, ngram_range=(1,2), max_features=200000))
            if name == 'svm':
                return Pipeline([tfidf, ('clf', LinearSVC())])
            if name == 'logreg':
                return Pipeline([tfidf, ('clf', LogisticRegression(max_iter=1000))])
            if name == 'nb':
                return Pipeline([tfidf, ('clf', MultinomialNB())])
            svd = ('svd', TruncatedSVD(n_components=max(50, args.svd_dim), random_state=42))
            scaler = ('scaler', StandardScaler(with_mean=True))
            if name == 'dt':
                return Pipeline([tfidf, svd, ('clf', DecisionTreeClassifier(random_state=42))])
            if name == 'rf':
                return Pipeline([tfidf, svd, ('clf', RandomForestClassifier(n_estimators=300, random_state=42, n_jobs=-1))])
            if name == 'lda':
                return Pipeline([tfidf, svd, scaler, ('clf', LinearDiscriminantAnalysis())])
            if name == 'mlp':
                return Pipeline([tfidf, svd, scaler, ('clf', MLPClassifier(hidden_layer_sizes=(200,), max_iter=200, random_state=42))])
            raise SystemExit(f"Modelo desconocido: {name}")
        else:  # sbert
            # Para sbert, las features estÃ¡n precomputadas; usamos solo el clasificador y (opcional) scaler
            scaler = ('scaler', StandardScaler(with_mean=True))
            if name == 'svm':
                return Pipeline([scaler, ('clf', LinearSVC())])
            if name == 'logreg':
                return Pipeline([scaler, ('clf', LogisticRegression(max_iter=1000))])
            if name == 'nb':
                raise SystemExit("Naive Bayes no es compatible con embeddings densos; usa --models sin 'nb' o --featurizer tfidf")
            if name == 'dt':
                return Pipeline([('clf', DecisionTreeClassifier(random_state=42))])
            if name == 'rf':
                return Pipeline([('clf', RandomForestClassifier(n_estimators=300, random_state=42, n_jobs=-1))])
            if name == 'lda':
                return Pipeline([scaler, ('clf', LinearDiscriminantAnalysis())])
            if name == 'mlp':
                return Pipeline([scaler, ('clf', MLPClassifier(hidden_layer_sizes=(200,), max_iter=200, random_state=42))])
            raise SystemExit(f"Modelo desconocido: {name}")

    # Preparar features segÃºn featurizer
    X_feat = X_text
    if args.featurizer == 'sbert':
        if SentenceTransformer is None:
            raise SystemExit("sentence_transformers no disponible. InstÃ¡lalo o usa --featurizer tfidf.")
        print(f"[FEAT] Generando embeddings SBERT con '{args.embedding_model}'...")
        st_model = SentenceTransformer(args.embedding_model)
        X_feat = st_model.encode(X_text.tolist(), normalize_embeddings=False, show_progress_bar=True, batch_size=args.embed_batch_size)

    # MLflow parent run
    parent_run = None
    if args.mlflow_enabled:
        try:
            mlflow.set_experiment(args.mlflow_experiment)
            parent_run = mlflow.start_run(run_name='news_train_multimodel')
        except Exception as e:
            print(f"[WARN] No se pudo iniciar MLflow run: {e}")
            parent_run = None

    results = []
    try:
        for name in model_names:
            print(f"\n[TRAIN] Modelo: {name}")
            # Skip NB si featurizer es sbert
            if args.featurizer == 'sbert' and name == 'nb':
                print("[SKIP] Naive Bayes no soportado con embeddings sbert.")
                continue
            pipe = make_pipeline(name, args.featurizer)
            metrics: dict = {}
            report_txt: str | None = None
            cm_artifact_path: str | None = None

            # Holdout split
            if args.test_size and 0 < args.test_size < 0.9:
                X_train, X_test, y_train, y_test = train_test_split(
                    X_feat, y, test_size=args.test_size, random_state=42, stratify=y
                )
            else:
                X_train, X_test, y_train, y_test = X_feat, None, y, None

            # CV en train
            metrics_cv: dict = {}
            if args.cv and args.cv >= 2:
                skf = StratifiedKFold(n_splits=args.cv, shuffle=True, random_state=42)
                cv_list = []
                for tr_idx, va_idx in skf.split(X_train, y_train):
                    X_tr = X_train[tr_idx] if hasattr(X_train, 'shape') else [X_train[i] for i in tr_idx]
                    X_va = X_train[va_idx] if hasattr(X_train, 'shape') else [X_train[i] for i in va_idx]
                    y_tr = y_train.iloc[tr_idx] if hasattr(y_train, 'iloc') else [y_train[i] for i in tr_idx]
                    y_va = y_train.iloc[va_idx] if hasattr(y_train, 'iloc') else [y_train[i] for i in va_idx]
                    pipe_cv = make_pipeline(name, args.featurizer)
                    pipe_cv.fit(X_tr, y_tr)
                    y_pred = pipe_cv.predict(X_va)
                    acc = accuracy_score(y_va, y_pred)
                    pr_macro, rc_macro, f1_macro, _ = precision_recall_fscore_support(y_va, y_pred, average='macro', zero_division=0)
                    pr_weighted, rc_weighted, f1_weighted, _ = precision_recall_fscore_support(y_va, y_pred, average='weighted', zero_division=0)
                    cv_list.append({
                        'cv_accuracy': float(acc),
                        'cv_precision_macro': float(pr_macro),
                        'cv_recall_macro': float(rc_macro),
                        'cv_f1_macro': float(f1_macro),
                        'cv_precision_weighted': float(pr_weighted),
                        'cv_recall_weighted': float(rc_weighted),
                        'cv_f1_weighted': float(f1_weighted),
                    })
                if cv_list:
                    metrics_cv = {k: float(sum(d[k] for d in cv_list) / len(cv_list)) for k in cv_list[0].keys()}
                    metrics.update(metrics_cv)
            else:
                # No CV; entrenar para poder evaluar test
                pipe.fit(X_train, y_train)

            # Evaluación test
            if y_test is not None:
                if args.cv and args.cv >= 2:
                    # Reentrenar en todo el train para test
                    pipe = make_pipeline(name, args.featurizer)
                    pipe.fit(X_train, y_train)
                y_pred = pipe.predict(X_test)
                acc = accuracy_score(y_test, y_pred)
                pr_macro, rc_macro, f1_macro, _ = precision_recall_fscore_support(y_test, y_pred, average='macro', zero_division=0)
                pr_weighted, rc_weighted, f1_weighted, _ = precision_recall_fscore_support(y_test, y_pred, average='weighted', zero_division=0)
                metrics.update({
                    'test_accuracy': float(acc),
                    'test_precision_macro': float(pr_macro),
                    'test_recall_macro': float(rc_macro),
                    'test_f1_macro': float(f1_macro),
                    'test_precision_weighted': float(pr_weighted),
                    'test_recall_weighted': float(rc_weighted),
                    'test_f1_weighted': float(f1_weighted),
                })
                try:
                    # Matriz de confusión
                    labels_sorted = sorted(set(y_test) | set(y_pred))
                    cm = confusion_matrix(y_test, y_pred, labels=labels_sorted)
                    import matplotlib.pyplot as plt  # type: ignore
                    fig, ax = plt.subplots(figsize=(6, 5))
                    ax.imshow(cm, cmap='Blues')
                    ax.set_title(f'Confusion Matrix ({name}) [test]')
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
                    cm_png = Path('outputs/events') / f'news_{name}_confusion_matrix.png'
                    cm_png.parent.mkdir(parents=True, exist_ok=True)
                    fig.savefig(cm_png, dpi=150)
                    plt.close(fig)
                    cm_artifact_path = str(cm_png)
                except Exception:
                    pass

            model_path = Path('models') / f'news_{name}.joblib'
            model_path.parent.mkdir(parents=True, exist_ok=True)
            # Entrenar final en TODO el dataset etiquetado
            pipe_final = make_pipeline(name, args.featurizer)
            pipe_final.fit(X_feat, y)
            joblib.dump(pipe_final, model_path)
            print(f"[OK] Modelo '{name}' guardado en -> {model_path}")

            if parent_run is not None:
                try:
                    with mlflow.start_run(run_name=f'{name}_train', nested=True):
                        mlflow.log_param('model', name)
                        mlflow.log_param('text_cols', ','.join(text_cols))
                        mlflow.log_param('label_col', args.label_col)
                        mlflow.log_param('test_size', args.test_size)
                        mlflow.log_param('cv', args.cv)
                        mlflow.log_param('vectorizer', 'tfidf')
                        mlflow.log_param('ngram_range', '1-2')
                        mlflow.log_param('max_features', 200000)
                        if name in {'dt','rf','lda','mlp'}:
                            mlflow.log_param('svd_dim', args.svd_dim)
                        for k, v in metrics.items():
                            mlflow.log_metric(k, v)
                        if report_txt:
                            rpt_path = Path('outputs/events') / f'news_{name}_classification_report.txt'
                            rpt_path.parent.mkdir(parents=True, exist_ok=True)
                            rpt_path.write_text(report_txt, encoding='utf-8')
                            mlflow.log_artifact(str(rpt_path))
                        if cm_artifact_path:
                            try:
                                mlflow.log_artifact(cm_artifact_path)
                            except Exception:
                                pass
                        mlflow.log_artifact(str(model_path))
                except Exception as e:
                    print(f"[WARN] MLflow logging fallo para {name}: {e}")

            # Selección: preferir CV si existe; si no, test
            score_metric = 'cv_' + args.scoring if 'cv_' + args.scoring in metrics else 'test_' + args.scoring
            score = metrics.get(score_metric) if metrics else None
            results.append({'name': name, 'metrics': metrics, 'score': float(score) if score is not None else -1.0, 'path': str(model_path)})

    finally:
        if parent_run is not None:
            try:
                mlflow.end_run()
            except Exception:
                pass

    # Elegir el mejor modelo
    if not results:
        raise SystemExit("No se entrenaron modelos (sin resultados)")
    results_sorted = sorted(results, key=lambda d: d['score'], reverse=True)
    best = results_sorted[0]
    best_path = Path('models') / 'news_best.joblib'
    try:
        import shutil
        shutil.copyfile(best['path'], best_path)
    except Exception:
        joblib.dump(joblib.load(best['path']), best_path)
    meta = {
        'best_model': best['name'],
        'score_metric': args.scoring,
        'score': best['score'],
        'path': best['path'],
        'candidates': results,
    }
    meta_path = Path('models') / 'news_best_meta.json'
    meta_path.write_text(__import__('json').dumps(meta, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f"\n[OK] Mejor modelo: {best['name']} ({args.scoring}={best['score']:.3f}). Guardado como -> {best_path}")


if __name__ == '__main__':
    main()
