#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Utilidades para configuración: expansión de variables de entorno en dicts cargados de YAML.
Soporta sintaxis ${VAR} y ${VAR:-default} dentro de strings.
"""
from __future__ import annotations
import os
import re
from typing import Any

_ENV_PATTERN = re.compile(r"\$\{([^}:]+)(?::-(.*?))?\}")


def _expand_string(s: str) -> str:
    def repl(m: re.Match):
        var = m.group(1)
        default = m.group(2)
        val = os.environ.get(var)
        return val if val is not None else (default if default is not None else m.group(0))
    return _ENV_PATTERN.sub(repl, s)


def expand_env_in_obj(obj: Any) -> Any:
    """Expande variables de entorno recursivamente en dict/list/str."""
    if isinstance(obj, dict):
        return {k: expand_env_in_obj(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [expand_env_in_obj(x) for x in obj]
    if isinstance(obj, str):
        return _expand_string(obj)
    return obj

