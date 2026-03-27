import typing
from .config import RunConfig
from .workflow import Workflow
from .artifacts import ArtifactBase
from pathlib import Path
from .executor import Executor


def _topo_order(num_steps, edges):
    outgoing = {i: [] for i in range(num_steps)}
    in_deg = {i: 0 for i in range(num_steps)}
    for src, dst in edges:
        outgoing[src].append(dst)
        in_deg[dst] += 1

    queue = [i for i in range(num_steps) if in_deg[i] == 0]
    order = []
    while queue:
        idx = queue.pop(0)
        order.append(idx)
        for nxt in outgoing[idx]:
            in_deg[nxt] -= 1
            if in_deg[nxt] == 0:
                queue.append(nxt)

    if len(order) != num_steps:
        raise ValueError("Workflow has a cycle or disconnected dependency graph")
    return order


def _build_artifact(step_type, name, builder, upstream):
    """
    As I wanted user to only have to define name and builder(from the Step values), 
    but artifacts can require some specific parameter which are results of execution
    of the previous dependencies. These parameters will be filled by finding the
    matching artifact in upstream by type.
    
    Example of work:
    
    _build_artifact(Analysis, "SingleMuonAnalysis", "analysis:run_analysis", upstream=[<Fileset artifact from step 1>])

    get_type_hints(Analysis) returns:
        {"name": str, "fileset": Fileset, "builder": str}
    name -> skip
    fileset -> Fileset is a subclass of ArtifactBase → scan upstream → finds the Fileset artifact → kwargs["fileset"] = <that artifact>
    builder -> skip

    """
    hints = typing.get_type_hints(step_type)
    kwargs = {"name": name, "builder": builder}
    for field_name, field_type in hints.items():
        if field_name in ("name", "builder"):
            continue
        if isinstance(field_type, type) and issubclass(field_type, ArtifactBase):
            match = next((a for a in upstream if isinstance(a, field_type)), None)
            if match is None:
                raise RuntimeError(
                    f"{step_type.__name__} requires a '{field_name}' dependency of type "
                    f"{field_type.__name__}, but none was found in upstream steps."
                )
            kwargs[field_name] = match
    return step_type(**kwargs)


def _print_dag(workflow: Workflow) -> None:
    print("Workflow DAG:")
    if not workflow.steps:
        print("  (no steps)")
        return
    for idx, step in enumerate(workflow.steps):
        print(
            f"  [{idx}] {step.name} -> {step.step_type.__name__} builder={step.builder}"
        )
    if workflow.edges:
        print("Edges:")
        for src, dst in workflow.edges:
            print(f"  {workflow.steps[src].name} -> {workflow.steps[dst].name}")
    else:
        print("Edges: (none)")


def render(workflow: Workflow, config: RunConfig):
    """
    Executes DAG, sorts the steps to begin with the last one
    (the last will trigger all the dependencies and will materialize
    the artifacts starting from the first one - Fileset).
    """
    cache_dir = Path(config.cache_dir)
    executor = Executor(cache_dir=cache_dir, config=config)
    _print_dag(workflow)
    num_steps = len(workflow.steps)
    if num_steps == 0:
        return {"paths": {}, "artifacts": {}, "order": []}

    order = _topo_order(num_steps, workflow.edges)

    artifact_by_idx = {}
    artifacts_by_name = {}
    paths_by_name = {}

    for idx in order:
        step = workflow.steps[idx]
        step_name = step.name

        upstream = [artifact_by_idx[src] for src, dst in workflow.edges if dst == idx]
        artifact = _build_artifact(step.step_type, step_name, step.builder, upstream)

        print(
            f"Executing step '{step_name}' of type '{step.step_type.__name__}' with the user code {step.builder}"
        )
        path = executor.materialize(artifact)
        print(f"  -> materialized at {path}")

        artifact_by_idx[idx] = artifact
        artifacts_by_name[step.name] = step_name
        paths_by_name[step.name] = path

    return {"paths": paths_by_name, "artifacts": artifacts_by_name, "order": [workflow.steps[i].name for i in order]}
