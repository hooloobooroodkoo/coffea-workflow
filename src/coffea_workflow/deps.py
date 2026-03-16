from __future__ import annotations
from pathlib import Path
from .artifacts import Artifact

class Deps:
    """
    Dependencies are initialized in the executor that knows how to materilize the artifact(it knows config and cache directory).
    The producer of the specific artifact then trigers the same executor to produce the previous dependency (artifact).
    """
    def __init__(self, executor):
        self._executor = executor

    def need(self, art: Artifact) -> Path:
        # triggers dependency marelization
        return self._executor.materialize(art)