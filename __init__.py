from .ir import GraphIR, NodeIR, ArtifactRef
from .config import WorkflowConfig

def to_ir(cfg: WorkflowConfig) -> GraphIR:
    artifacts = {}
    for name, a in cfg.artifacts.items():
        artifacts[name] = ArtifactRef(name=name, uri=a["uri"], type=a.get("type"))

    nodes = {}
    for step_id, s in cfg.steps.items():
        nodes[step_id] = NodeIR(
            id=step_id,
            kind=s.kind,
            deps=s.depends_on,
            inputs=s.inputs,
            outputs=s.outputs,
            params=s.params,
            resources=s.resources,
        )
        
        for out in s.outputs:
            artifacts.setdefault(out, ArtifactRef(name=out, uri=f"workdir/{out}", type=None))

    return GraphIR(name=cfg.name, nodes=nodes, artifacts=artifacts)
