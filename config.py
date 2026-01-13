"""
Configuration of the workflow that the user creates.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

@dataclass
class StepConfig:
    kind: str
    depends_on: List[str] = field(default_factory=list)
    inputs: List[str] = field(default_factory=list)
    outputs: List[str] = field(default_factory=list)
    params: Dict[str, Any] = field(default_factory=dict)
    resources: Dict[str, Any] = field(default_factory=dict)

@dataclass
class WorkflowConfig:
    name: str
    steps: Dict[str, StepConfig]
    artifacts: Dict[str, Dict[str, Any]] = field(default_factory=dict)  # name -> {uri,type}

def load_workflow_config(d: Dict[str, Any]) -> WorkflowConfig:
    wf = d["workflow"]
    steps = {}
    for step_id, s in wf["steps"].items():
        steps[step_id] = StepConfig(
            kind=s["kind"],
            depends_on=s.get("depends_on", []),
            inputs=s.get("inputs", []),
            outputs=s.get("outputs", []),
            params=s.get("params", {}),
            resources=s.get("resources", {}),
        )
    return WorkflowConfig(
        name=wf["name"],
        steps=steps,
        artifacts=wf.get("artifacts", {}),
    )
