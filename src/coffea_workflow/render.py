from .config import RunConfig
from .workflow import Workflow
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

def _print_dag(workflow: Workflow) -> None:
    print("Workflow DAG:")
    if not workflow.steps:
        print("  (no steps)")
        return
    for idx, step in enumerate(workflow.steps):
        print(
            f"  [{idx}] {step.name} -> {step.step_type.__name__} params={step.params}"
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
        
	# how to execute DAG step-by-step
    order = _topo_order(num_steps, workflow.edges) 
    
    artifacts_by_name = {}
    paths_by_name = {}
    

    for idx in order:
        step = workflow.steps[idx]
        step_name = step.name
        # render only trigger materialisation of user's Artifacts here like Fileset, Analysis, Plotting
        artifact = step.step_type(step_name,builder=step.builder)
        print(
            f"Executing step '{step_name}' of type '{step.step_type}' with the user code {step.builder}"
        )
        path = executor.materialize(artifact)
        print(f"  -> materialized at {path}")
        artifacts_by_name[step.name] = step_name
        paths_by_name[step.name] = path

    return {"paths": paths_by_name, "artifacts": artifacts_by_name, "order": [workflow.steps[i].name for i in order]}