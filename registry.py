"""
Maps a step "kind" to the Python function that knows how to execute it.
"""
from __future__ import annotations
from typing import Callable, Dict, Protocol
from .ir import NodeIR

class StepContext(Protocol):
    workspace: str
    artifacts: Dict[str, str] 

StepHandler = Callable[[NodeIR, StepContext], None]

_REGISTRY: Dict[str, StepHandler] = {}

def register(kind: str):
    def deco(fn: StepHandler):
        _REGISTRY[kind] = fn
        return fn
    return deco

def get_handler(kind: str) -> StepHandler:
    if kind not in _REGISTRY:
        raise KeyError(f"No handler registered for step kind '{kind}'")
    return _REGISTRY[kind]
