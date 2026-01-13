"""
Executes the workflow DAG locally by topologically sorting steps and calling the registered handler for each step.
- creates working directory(to save artifacts, step outputs, etc)
- resolves URIs
- determines execution order (topological sort)
- executes each step by calling the handler
"""
from __future__ import annotations
import os
from dataclasses import dataclass
from typing import Dict, List, Set
from ..ir import GraphIR
from ..registry import get_handler
import time

@dataclass
class LocalContext:
    workspace: str
    artifacts: Dict[str, str]

def _toposort(nodes: Dict[str, List[str]]) -> List[str]:
    remaining = set(nodes.keys())
    done: Set[str] = set()
    order: List[str] = []

    while remaining:
        progressed = False
        for nid in list(remaining):
            if all(dep in done for dep in nodes[nid]):
                order.append(nid)
                done.add(nid)
                remaining.remove(nid)
                progressed = True
        if not progressed:
            raise RuntimeError("Cycle/unresolved deps in workflow DAG")
    return order

class LocalBackend:
    def run(self, graph: GraphIR, workspace: str) -> Dict[str, str]:
        os.makedirs(workspace, exist_ok=True)

        artifact_paths: Dict[str, str] = {}
        for name, aref in graph.artifacts.items():
            uri = aref.uri
            if "://" not in uri and not os.path.isabs(uri):
                uri = os.path.join(workspace, uri)
            artifact_paths[name] = uri

        ctx = LocalContext(workspace=workspace, artifacts=artifact_paths)

        deps_map = {nid: n.deps for nid, n in graph.nodes.items()}
        for nid in _toposort(deps_map):
            node = graph.nodes[nid]

            # print additional information
            t0 = time.time()
            print(f"\n==> STEP {node.id}  kind={node.kind}")
            if getattr(node, "depends_on", None):
                print(f"    depends_on: {node.depends_on}")
            if getattr(node, "inputs", None):
                ins = [f"{n} -> {ctx.artifacts[n]}" for n in node.inputs]
                print(f"    inputs:  {', '.join(ins)}")
            if getattr(node, "outputs", None):
                outs = [f"{n} -> {ctx.artifacts[n]}" for n in node.outputs]
                print(f"    outputs: {', '.join(outs)}")
                
            
            handler = get_handler(node.kind)
            handler(node, ctx)

            
            dt = time.time() - t0
            print(f"    done in {dt:.2f}s")

        return artifact_paths
        
