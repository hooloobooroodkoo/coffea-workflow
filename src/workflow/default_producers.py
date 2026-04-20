from __future__ import annotations
import json
import inspect
from pathlib import Path
from typing import Any
import cloudpickle
from .artifacts import Fileset, Analysis, Chunking, ChunkAnalysis, Plotting, _builder_key
from .deps import Deps
from .producers import producer
from .config import RunConfig
import importlib
import math
from coffea.processor import accumulate

def _call_builder(fn, *args, config: RunConfig | None = None) -> Any:
    """
    Call fn(*args), injecting config as a kwarg if the function accepts it.
    For example, user uses client histserv in analysis function.
    """
    if config is not None and "config" in inspect.signature(fn).parameters:
        return fn(*args, config=config)
    return fn(*args)

def _extract_acc(result) -> Any:
    """
    Depending on the processor implementation, the user can return the accumulator or something else.
    Unwrap a Result, handling both savemetrics=True (tuple) and False (bare acc).
    """
    value = result.unwrap()
    if isinstance(value, tuple):
        acc, _metrics = value
        return acc, _metrics
    return value, {}
    
def _load_object(path: str | Any) -> Any:
    """
    Finds the function implemented by a user and returns it.
    Accepts either a 'module:function' string or a callable directly.
    """
    if callable(path):
        return path
    if ":" in path:
        mod_name, attr = path.split(":", 1)
    else:
        mod_name, attr = path.rsplit(".", 1)
    module = importlib.import_module(mod_name)
    try:
        return getattr(module, attr)
    except AttributeError as e:
        raise AttributeError(f"Object '{attr}' not found in module '{mod_name}'") from e

@producer(Fileset)
def make_fileset(*, art: Fileset, deps: Deps, out: Path, config: RunConfig) -> None:
    # finds and calls the function that user specified in builder
    fn = _load_object(art.builder)
    fileset_dict = fn()

    if not isinstance(fileset_dict, dict):
        raise TypeError("Fileset builder must return a dict")

    out.mkdir(parents=True, exist_ok=True) # a folder for the Artifact (name is identity())
    (out / "fileset.json").write_text(json.dumps(fileset_dict, indent=2, sort_keys=True))


def _split_fileset(fileset, strategy=None, datasets=None, percentage=None):
    """
    Split the fileset into chunks to enable getting a partial result if one or several
    of the chunks failed to produce a result while being processed.
    One chunk is one partial fileset(unique combination of files), these are not usual coffea chunks.


    Input
    fileset:    {dataset: {"files": {path: treename, ...}}}
    strategy:   "by_dataset" — one dataset is one chunk; None — all datasets together
    percentage: integer that divides 100 evenly (20, 25, 50...).
                Each chunk gets this percentage of each dataset's files.
    datasets: list, callable or tuple of datasets' names

    Output
    List of fileset dicts
    If strategy only:
        chunks = _split_fileset(fileset, "by_dataset") - one chunk per dataset
    If percentage only:
        chunks = _split_fileset(fileset, percentage=50) - 2 chunks (50 of each dataset in 1st chunk and 2nd, mixed chunks
    If strategy and percentage:
        chunks = _split_fileset(fileset, "by_dataset", percentage=50) - N_datasets * 2 chunks, not mixed chunks
    If datasets + any/nothing:
        strategies are only applied to chosen datasets
    """
    if strategy is not None and strategy != "by_dataset":
        raise ValueError(f"Unknown strategy '{strategy}'. Use 'by_dataset' or None.")
    if percentage is not None:
        if (
            not isinstance(percentage, int)
            or not (1 <= percentage <= 100)
            or 100 % percentage != 0
        ):
            raise ValueError(
                "'percentage' must be an int that divides 100 evenly (e.g. 10, 20, 25, 50)."
            )

    if datasets is None:
        pass
    elif callable(datasets):
        fileset = {k: v for k, v in fileset.items() if datasets(k)}
    else:
        fileset = {k: fileset[k] for k in datasets if k in fileset}

    if strategy == "by_dataset":
        groups = [{name: data} for name, data in fileset.items()]
    else:
        groups = [fileset]

    if percentage is None:
        return groups

    n_chunks = 100 // percentage
    result = []
    for group in groups:
        for bin_idx in range(n_chunks):
            chunk = {}
            for dataset, data in group.items():
                files = data.get("files", {})
                if not files:
                    continue
                
                if isinstance(files, dict):
                    file_items = list(files.items())
                    as_dict = True
                else:
                    file_items = list(files)
                    as_dict = False

                n = len(file_items)
                chunk_size = max(1, math.ceil(n / n_chunks))
                start = bin_idx * chunk_size
                end = min(start + chunk_size, n)
                if start >= n:
                    continue    
                sliced = file_items[start:end]
                chunk[dataset] = {**data, "files": dict(sliced) if as_dict else list(sliced)}
                
            if chunk:
                result.append(chunk)
    return result

@producer(Chunking)
def split_fileset(*, art: Chunking, deps: Deps, out: Path, config: RunConfig) -> None:
    out.mkdir(parents=True, exist_ok=True)
    fileset = json.loads((deps.need(art.fileset) / "fileset.json").read_text())

    chunks = _split_fileset(
        fileset,
        strategy=config.strategy,
        datasets=list(config.datasets) if config.datasets else None,
        percentage=config.percentage,
    )

    manifest_files = {}
    for i, chunk in enumerate(chunks):
        file_name = f"fileset_chunk_{i}.json"
        (out / file_name).write_text(json.dumps(chunk, indent=2, sort_keys=True))
        manifest_files[str(i)] = file_name

    (out / "manifest.json").write_text(json.dumps({
        "output_files": manifest_files,
        "n_chunks": len(chunks),
    }, indent=2, sort_keys=True))

@producer(ChunkAnalysis)
def run_analysis(*, art: ChunkAnalysis, deps: Deps, out: Path, config: RunConfig) -> None:
    """
    Apply user's analysis to one chunk.
    """
    out.mkdir(parents=True, exist_ok=True)

    # create chunks applying splitting strategy
    # TODO: do I need chunking initialisation again? is it enough to just have it in execute_analysis()?
    chunking_dir = deps.need(art.chunking)  # directory with chunk jsons
    chunk_path = chunking_dir / art.chunk_file
    chunk_fileset = json.loads(chunk_path.read_text())

    fn = _load_object(art.analysis_builder)  # user's function
    result = _call_builder(fn, chunk_fileset, config=config)

    (out / "payload.pkl").write_bytes(cloudpickle.dumps(result))
    if result.is_ok():
        (out / ".success").touch()

@producer(Analysis)
def execute_analysis(*, art: Analysis, deps: Deps, out: Path, config: RunConfig) -> None:
    """
    This should execute Chunking and run the analysis per chunk + merging
    """
    # create chunks applying splitting strategy
    # it's an artifact that user is not using - internal
    chunking = Chunking(
        fileset=art.fileset,
        split_strategy=config.strategy,
        percentage=config.percentage,
        datasets=config.datasets,
    )
    chunk_dir = deps.need(chunking) # self._executor.materialize(Chunking); returns path to .cache_dir / Chunking / hash where all .json chunks are
    manifest_path = chunk_dir / "manifest.json" # manifest contains info about our fileset.json or its chunks .json

    manifest = json.loads(manifest_path.read_text())
    chunks_files = list(manifest["output_files"].values())
        
    chunks_files_num = manifest["n_chunks"]
    if chunks_files_num > 1:
        print(f"\nSplit strategy applied, starting independent processing of {chunks_files_num} fileset subsets...\n")
    else:
        print(f"\nNo split strategy was specified, proceed with processing the whole fileset...")

    if config.chunk_fraction is not None:
        n = max(1, round(len(chunks_files) * config.chunk_fraction))
        chunks_files = chunks_files[:n]
        print(f"chunk_fraction={config.chunk_fraction}: processing {n} of {manifest['n_chunks']} chunks")

    merged_acc = None
    metrics_merged = None
    failures = []

    for chunk_file in chunks_files:
        print("------------------------------------")
        print(f"Processing {chunk_file}")
        chunk_art = ChunkAnalysis(
            chunk_file=chunk_file,
            chunking=chunking,
            analysis_builder=art.builder,
        )
        # process chunk
        chunk_out_dir = deps.need(chunk_art)
        result = cloudpickle.loads((chunk_out_dir / "payload.pkl").read_bytes())

        #TODO: if config contains histserv_connection_info, then use the connection and add to the hist server, otherwise 
        if result.is_ok():
            print("Successfully processed!")
            acc, metrics = _extract_acc(result)
            if config.hist_client is not None:
                # acc is already connection_info (returned directly from run_analysis)
                # passing remote_hist directly is not possible because it holds a live gRPC connection, which is not picklable
                merged_acc = config.histserv_connection_info # connection info to histserv
            else:
                merged_acc = accumulate([acc], accum=merged_acc) # accumulatable
            metrics_merged = accumulate([metrics], accum=metrics_merged)
        else:
            print("Failure caught!")
            failures.append({"chunk_file": chunk_file, "error": str(result)})
            continue

    payload = {
        "builder": _builder_key(art.builder),
        "n_chunks_total": len(chunks_files),
        "n_chunks_ok": 0 if merged_acc is None else (len(chunks_files) - len(failures)),
        "failures": failures,
        "processor_result": (merged_acc, metrics_merged),
    }
    out.mkdir(parents=True, exist_ok=True)
    (out / "payload.pkl").write_bytes(cloudpickle.dumps(payload))
    if failures:
        (out / ".has_failures").touch()
    else:
        (out / ".has_failures").unlink(missing_ok=True)

@producer(Plotting)
def make_plot(*, art: Plotting, deps: Deps, out: Path, config: RunConfig) -> None:
    out.mkdir(parents=True, exist_ok=True)
    analysis_dir = deps.need(art.analysis)
    payload = cloudpickle.loads((analysis_dir / "payload.pkl").read_bytes())
    fn = _load_object(art.builder)
    if config.histserv_connection_info is not None:
        plot_result = _call_builder(fn, config=config)
    else:
        plot_result = _call_builder(fn, payload)
    (out / "payload.pkl").write_bytes(cloudpickle.dumps(plot_result))