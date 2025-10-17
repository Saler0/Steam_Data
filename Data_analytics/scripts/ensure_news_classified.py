#!/usr/bin/env python
from __future__ import annotations

import os
import sys
from pathlib import Path
from subprocess import run


def load_dotenv_simple(path: str = '.env') -> None:
    p = Path(path)
    if not p.exists():
        return
    for line in p.read_text(encoding='utf-8', errors='ignore').splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        if '=' not in line:
            continue
        k, v = line.split('=', 1)
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        if k and v is not None:
            os.environ.setdefault(k, v)


def main() -> int:
    target = Path('outputs/events/news_classified.parquet')
    if target.exists():
        print('[OK] Dataset de entrenamiento existente: outputs/events/news_classified.parquet')
        return 0
    print('[INFO] news_classified.parquet no existe. Ejecutando news_classifier via DVC...')
    load_dotenv_simple('.env')
    rc = run(['dvc', 'repro', '-q', '--single-item', 'news_classifier']).returncode
    if rc != 0:
        print('[ERROR] No se pudo generar outputs/events/news_classified.parquet (DVC news_classifier).')
    return rc


if __name__ == '__main__':
    sys.exit(main())

