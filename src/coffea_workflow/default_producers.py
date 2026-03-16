from __future__ import annotations
import json
from pathlib import Path
from typing import Dict, List, Any
import cloudpickle
from .artifacts import Fileset, Analysis,Chunking, ChunkAnalysis
from .deps import Deps
from .producers import producer
from .config import RunConfig
import importlib
import math

def _load_object(path: str) -> Any:
    """
    Finds the function implemented by a user and returns it.
    """
    if ":" in path:
        mod_name, attr = path.split(":", 1)
    else:
        mod_name, attr = path.rsplit(".", 1)
    module = importlib.import_module(mod_name)
    try:
        return getattr(module, attr)
    except AttributeError as e:
        raise AttributeError(f"Object '{attr}' not found in module '{mod_name}'") from e

def _split_fileset():
    pass

@producer(Fileset)
def make_fileset(*, art: Fileset, deps: Deps, out: Path, config: RunConfig) -> None:
    # finds and calls the function that user specified in builder
    fn = _load_object(art.builder)
    fileset_dict = fn()

    if not isinstance(fileset_dict, dict):
        raise TypeError("Fileset builder must return a dict")

    out.mkdir(parents=True, exist_ok=True) # a folder for the Artifact (name is identity())
    (out / "fileset.json").write_text(json.dumps(fileset_dict, indent=2, sort_keys=True))


@producer(Chunking)
def split_fileset(*, art: Chunking, deps: Deps, out: Path, config: RunConfig) -> None:
    """
    Chunks a Fileset based on the splitting strategy. 
    Warning: In this way I duplicate the fileset.json saving its chunks... Is it crucial?
    It returns fileset chunks based on splitting strategy(as described in Chunking artifact description)
    Manifest output  example:
        - None: "fileset.json"
        - "by_dataset": {'output_files': {'SingleMu_0': 'fileset_SingleMu_0.json', 'SingleMu_1': 'fileset_SingleMu_1.json'}, 'n_chunks': 2}
        - "percentage_per_file": {'output_files': {'0_20': 'fileset_0_20_percent.json', '20_40': 'fileset_20_40_percent.json', '40_60': 'fileset_40_60_percent.json', '60_80': 'fileset_60_80_percent.json', '80_100': 'fileset_80_100_percent.json'}, 'n_chunks': 5, 'split_strategy': 'percentage_per_file', 'percentage': 20}
    """
    # executor creates fileset and materialize() function called by deps.need() returns the path to fileset.json
    out.mkdir(parents=True, exist_ok=True)

    fileset_dir = deps.need(art.fileset) 
    fileset_path = fileset_dir / "fileset.json"
    
    splitting_strategy = config.split_strategy

    # read fileset.json
    fileset = json.loads(fileset_path.read_text())

    if splitting_strategy == "by_dataset":
        datasets = list(fileset.keys())
        datasets_files = {}
        for dataset in datasets:
            file = f"fileset_{dataset}.json"
            out_path = out / file   # where out=self.cache_dir / art.type_name / art.identity() 
            datasets_files[dataset] = file
            out_path.write_text(json.dumps({dataset: fileset[dataset]}, indent=2))
        manifest = {
            "output_files": datasets_files, 
            "n_chunks": len(datasets),

        }

    elif splitting_strategy == "percentage_per_file":
        p = art.percentage  # default is 20, we want in one chunk(fileset) 20% of each dataset and we end up having 5 bins
        
        # how many bins with step p
        bins = [(start, start + p) for start in range(0, 100, p)]

        combined_by_bin = {b: {} for b in bins}  # {(0,20) - {"SingleMu_0": {"files": {file_0, file_1}}, "SingleMu_1": {"files": {file_0, file_1}}}
                                                # {(20,40) - {"SingleMu_0": {"files": {file_2, file_3}}, "SingleMu_1": {"files": {file_2, file_3}}}....

        for dataset, files in fileset.items():
            files = files.get("files", {})
            if not isinstance(files, dict):
                raise TypeError(f"fileset[{dataset!r}]['files'] must be a dict")

            file_items = list(files.items())
            n = len(file_items)
            if n == 0:
                continue

            # split the files into bins
            n_bins = len(bins)
            chunk_size = max(1, math.ceil(n / n_bins))

            for i, (start_pct, end_pct) in enumerate(bins):
                start = i * chunk_size
                end = min((i + 1) * chunk_size, n)

                # build the same structure of dataset but reduced and put into a bin
                reduced_ds = dict(files)
                reduced_ds["files"] = dict(file_items[start:end])
                combined_by_bin[(start_pct, end_pct)][dataset] = reduced_ds

        fileset_bins_files = {}
        for (start_pct, end_pct), combined_fileset in combined_by_bin.items():
            file_name = f"fileset_{start_pct}_{end_pct}_percent.json"
            (out / file_name).write_text(json.dumps(combined_fileset, indent=2, sort_keys=True))
            fileset_bins_files[f"{start_pct}_{end_pct}"] = file_name

        manifest = {
            "output_files": fileset_bins_files,  
            "n_chunks": len(fileset_bins_files),     
            "split_strategy": "percentage_per_file",
            "percentage": p,
        }

    else:
        file_name = "fileset.json"
        (out / file_name).write_text(json.dumps(fileset, indent=2, sort_keys=True))

        manifest = {
            "output_files": {"all": file_name},
            "n_chunks": 1,
            "split_strategy": None,
            "percentage": None,
        }

    (out / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True))

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
    acc = fn(chunk_fileset)    

    (out / "payload.pkl").write_bytes(cloudpickle.dumps(acc))

@producer(Analysis)
def execute_analysis(*, art: Analysis, deps: Deps, out: Path, config: RunConfig) -> None:
    """
    This should execute Chunking and run the analysis per chunk + merging
    """
    # create chunks applying splitting strategy
    # it's an artifact that user is not using - internal
    chunking = Chunking(
        fileset=art.fileset,
        split_strategy=config.split_strategy,
        percentage=config.percentage,
    )
    chunk_dir = deps.need(chunking) # self._executor.materialize(Chunking); returns path to .cache_dir / Chunking / hash where all .json chunks are
    manifest_path = chunk_dir / "manifest.json" # manifest contains info about our fileset.json or its chunks .json

    manifest = json.loads(manifest_path.read_text())
    chunks_files = list(manifest["output_files"].values())

    merged = None
    failures = []

    for chunk_file in chunks_files:
        chunk_art = ChunkAnalysis(
            chunk_file=chunk_file,
            chunking=chunking,
            analysis_builder=art.builder,
        )
        try:
            # process chunk
            #TODO it's a folder, I need .pkl
            chunk_out_dir = deps.need(chunk_art)
            acc = cloudpickle.loads((chunk_out_dir / "payload.pkl").read_bytes())
            merged = acc if merged is None else (merged + acc)

        except Exception as e:
            failures.append({"chunk_file": chunk_file, "error": repr(e)})

    payload = {
        "builder": art.builder,
        "n_chunks_total": len(chunks_files),
        "n_chunks_ok": 0 if merged is None else (len(chunks_files) - len(failures)),
        "failures": failures,
        "merged": merged,
    }
    out.mkdir(parents=True, exist_ok=True)
    (out / "payload.pkl").write_bytes(cloudpickle.dumps(payload))