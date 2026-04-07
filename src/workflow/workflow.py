# TODO: add automatic code version tracking for reproducibility.
# from:
# - git commit hash ?
# - package version ?
# - container/image tag ?
# - hash of builder source ?
# This should be perhaps Artifacts identity part
    
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Callable, List, Sequence, Tuple, Type
from .artifacts import _builder_key

@dataclass(frozen=True)
class Step:
    """
    Defines how the analysis step should be executed. 
    It requires:
        - name: name of the step given by a user;
        - step_type: preimplemented external Artifact type (Fileset, Analysis, Plotting)
        - builder: the location of the user's code for producing an Artifact
    """
    name: str
    step_type: Type
    builder: str | Callable

    def to_dict(self) -> dict:
        return {"name": self.name, "step_type": self.step_type.__name__, "builder": _builder_key(self.builder)}

@dataclass
class Workflow:
    """
    Represents workflow DAG (analysis steps and their dependencies)
    """
    steps: List[Step] = field(default_factory=list)
    edges: List[Tuple[int, int]] = field(default_factory=list)

    def add(self, step: Step, depends_on: Sequence[Step] = ()) -> Step:
        self.steps.append(step)
        step_idx = len(self.steps) - 1
        dep_idxs = [self.steps.index(d) for d in depends_on]
        for di in dep_idxs:
            self.edges.append((di, step_idx))
        return step
