"""
This module defines the Intermediate Representation of a workflow.
It contains:
- Artifacts - what flows between steps
- Nodes - steps that transform artifacts
- Graph - how nodes depend on each other
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

@dataclass(frozen=True)
class ArtifactRef:
    name: str
    uri: str
    type: Optional[str] = None

@dataclass
class NodeIR:
    id: str
    kind: str
    deps: List[str] = field(default_factory=list)
    inputs: List[str] = field(default_factory=list)   
    outputs: List[str] = field(default_factory=list)
    params: Dict[str, Any] = field(default_factory=dict) # for example partition rules, processor name, chunksize, schema...
    resources: Dict[str, Any] = field(default_factory=dict) # executor backend, scheduler address...
    meta: Dict[str, Any] = field(default_factory=dict)

@dataclass
class GraphIR:
    name: str
    nodes: Dict[str, NodeIR]
    artifacts: Dict[str, ArtifactRef]
