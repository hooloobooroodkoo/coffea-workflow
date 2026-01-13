from __future__ import annotations

import importlib
from typing import Any, Callable
from pathlib import Path
import cloudpickle

def import_from_string(path: str) -> Any:
    """
    Import 'pkg.module:obj' or 'pkg.module.obj'.
    """
    if ":" in path:
        module_name, attr = path.split(":", 1)
    else:
        module_name, attr = path.rsplit(".", 1)
    module = importlib.import_module(module_name)
    return getattr(module, attr)


def load_callable(path: str) -> Callable[..., Any]:
    obj = import_from_string(path)
    if not callable(obj):
        raise TypeError(f"Imported object is not callable: {path}")
    return obj

def load_merged(merged_path: str | Path) -> tuple[dict, object | None]:
    merged_path = Path(merged_path)
    with merged_path.open("rb") as f:
        out = cloudpickle.load(f)

    # tuple(payload_dict, meta?)
    if isinstance(out, tuple) and len(out) >= 1 and isinstance(out[0], dict):
        payload = out[0]
        meta = out[1] if len(out) > 1 else None
        return payload, meta

    raise TypeError(f"Unexpected merged output type: {type(out)} (expected tuple or dict)")
