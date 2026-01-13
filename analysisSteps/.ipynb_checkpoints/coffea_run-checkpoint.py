from __future__ import annotations

import json
import os
import pickle

from ..registry import register
from ..util import load_callable, import_from_string


@register("coffea_run")
def run(node, ctx):
    # Load manifest
    manifest_path = ctx.artifacts[node.inputs[0]]
    with open(manifest_path) as f:
        manifest = json.load(f)

    partition_id = node.params["partition_id"]  # "A" or "B"
    part = next(p for p in manifest if p["partition_id"] == partition_id)
    fileset = part["fileset"]
    n_files = sum(len(v.get("files", [])) for v in fileset.values())
    if n_files == 0:
        raise RuntimeError(f"Partition {partition_id} has zero files. Check partitioning.")


    # Output
    out_name = node.outputs[0]
    out_path = ctx.artifacts[out_name]
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    # Resolve schema string -> class (or allow passing class directly)
    schema_spec = node.params.get("schema")
    schema = import_from_string(schema_spec) if isinstance(schema_spec, str) else schema_spec
    if schema is None:
        raise ValueError("coffea_run requires 'schema' (e.g. 'coffea.nanoevents:NanoAODSchema')")

    treename = node.params.get("treename", "Events")

    # Build processor instance from factory
    proc_factory = load_callable(node.params["processor_factory"])
    proc_kwargs = node.params.get("processor_kwargs", {})
    processor_instance = proc_factory(**proc_kwargs)

    # Executor selection
    from coffea import processor as coffea_processor

    exec_cfg = node.resources.get("executor", {})
    backend = exec_cfg.get("backend", "futures")

    if backend != "futures":
        raise ValueError(f"Currently only backend='futures' supported, got {backend!r}")

    workers = int(exec_cfg.get("workers", 1))
    try:
        executor = coffea_processor.FuturesExecutor(workers=workers)
    except TypeError:
        executor = coffea_processor.FuturesExecutor(max_workers=workers)

    runner_kwargs = node.params.get("runner_kwargs", {})
    chunksize = runner_kwargs.get("chunksize", 200_000)


    runner = coffea_processor.Runner(
        executor=executor,
        schema=schema,
        chunksize=runner_kwargs.get("chunksize", 200_000),
        savemetrics=runner_kwargs.get("savemetrics", True),
        metadata_cache=runner_kwargs.get("metadata_cache", {}),
    )

    result = runner(
        fileset=fileset,
        treename=treename,
        processor_instance=processor_instance,
    )

    with open(out_path, "wb") as f:
        pickle.dump(result, f)
