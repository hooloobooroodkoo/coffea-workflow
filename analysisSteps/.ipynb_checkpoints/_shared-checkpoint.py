from __future__ import annotations

from pathlib import Path
from typing import Any, Union
import gzip
import os

import cloudpickle


PathLike = Union[str, os.PathLike]


def load_merged(path: PathLike, *, filename: str = "merged.pkl") -> Any:
    """
    Load the merged output produced by the workflow merge step.

   inputs:
    path,
    filename

   outputs:
     unpickled object (dict or coffea accumulator-like object)
    """
    p = Path(path).expanduser().resolve()


    if p.is_dir():
        candidates = [
            p / filename,
            p / (filename + ".gz"),
            p / "merged.pkl",
            p / "merged.pkl.gz",
        ]
        merged_path = next((c for c in candidates if c.exists()), None)
        if merged_path is None:
            raise FileNotFoundError(
                f"No merged file found in directory: {p}\n"
                f"Tried: {', '.join(str(c.name) for c in candidates)}\n"
                f"Dir contains: {[x.name for x in sorted(p.iterdir())]}"
            )
        p = merged_path

    if not p.exists():
        raise FileNotFoundError(f"Merged file does not exist: {p}")
    if not p.is_file():
        raise IsADirectoryError(f"Expected a file, got: {p}")

    if p.suffix == ".gz":
        with gzip.open(p, "rb") as f:
            loaded = cloudpickle.load(f)
    else:
        with open(p, "rb") as f:
            loaded = cloudpickle.load(f)
            
    if isinstance(loaded, tuple):
        payload = loaded[0]
        meta = loaded[1] if len(loaded) > 1 else None
        return payload, meta

    return loaded, None

