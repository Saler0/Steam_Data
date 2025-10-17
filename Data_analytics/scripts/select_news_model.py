#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Escribe configs/llm_override.yaml para forzar el clasificador local (SVM)
si existe models/news_best.joblib. En caso contrario, deja un YAML vacío
para mantener el provider del config original.
"""
from __future__ import annotations

import argparse
from pathlib import Path
import yaml


def main() -> None:
    ap = argparse.ArgumentParser(description="Selecciona modelo local de noticias si existe")
    ap.add_argument("--out", default="configs/llm_override.yaml")
    ap.add_argument("--best-model", default="models/news_best.joblib")
    args = ap.parse_args()

    best = Path(args.best_model)
    outp = Path(args.out)
    outp.parent.mkdir(parents=True, exist_ok=True)

    if best.exists():
        data = {"provider": "svm", "model_path": str(best)}
        print(f"[OK] Override a modelo local: {best}")
    else:
        data = {}
        print("[INFO] Sin modelo local; override vacío (se mantiene provider del YAML)")
    outp.write_text(yaml.safe_dump(data, sort_keys=False, allow_unicode=True), encoding="utf-8")


if __name__ == "__main__":
    main()

