## Pseudocode
```python	 
####################################
#    Reimagined idea
####################################

"""
The user writes his analysis and can choose how to execute it:
a) run analysis per dataset
b) run analysis per certain percentage of each dataset

Benefits:
a) if one dataset was processed, and another failed, user still has half of the analysis
saved and reruns only a failed part
b) processed percentage is preserved user reruns unprocessed parts

Below you can see the main concepts. The workflow analysis example is at the end of the snippet: search for 'run_workflow.py'.

"""

###################################################################
# workflow.py
class Step:
"""
Defines how the analysis step should be executed. 
"""
	name: str
	step_type: Artifact
	params: dict
	
	def to_dict(self) -> dict:
        return {"name": self.name, "params": self.params}


class Workflow:
"""
Represents workflow DAG (analysis steps and their dependencies)
"""
	steps: [Step]
	edges: [(i, i)] # step dependencies
	version: str
	
	
	def add(self, step: Step, depends_on: Step) -> Step:
	        self.steps.append(step)
	        step_idx = len(self.steps) - 1
	        dep_idxs = [self.steps.index(d) for d in depends_on]
	        for dep_i in dep_idxs:
	            self.edges.append((dep_i, step_idx))
	        return step



###################################################################
# artifacts.py

ARTIFACT_REGISTRY = {}

def register_artifact(cls: type[Artifact]):
    ARTIFACT_REGISTRY[cls.__name__] = cls
    return cls
 
class ArtifactBase:
    def keys(self):
        raise NotImplementedError

    @property
    def type_name(self) -> str:
        return type(self).__name__

    def identity(self) -> str:
        return hash_identity(self.to_dict())

    def to_dict(self) -> dict:
        return {"type": self.__class__.__name__, "keys": self.keys()}

@register_artifact
class Fileset(ArtifactBase):
    """
    External artifact that user uses and defines the builder (his/her cutom script that returns fileset.json)
    """
    name: str
    builder: str = "" # uses function that user provides

    def keys(self) -> Mapping[str, Any]:
        return {
            "builder": self.builder,
        }

@register_artifact
class Chunking(ArtifactBase):
    """
    Internal artifact. User doesn't use this artifact, it's a helping artifact that is produced by Analysis artifact(external) to execute splitting strategy.
    Returns fileset chunks based on splitting strategy.
    Output example (depends on splitting strategy):
        - None: "fileset.json"
        - "by_dataset": "fileset_dataset1.json, fileset_dataset2.json"
        - "percentage_per_file": "fileset_0_10_percent.json", "fileset_10_20_percent.json", ... "fileset_90_100_percent.json"
    """
    fileset: Fileset
    split_strategy: str
    percentage: int

    def keys(self):
        return {
            "fileset": self.fileset,
            "split_strategy": self.split_strategy,
            "percentage": self.percentage,
        }

@register_artifact
class ChunkAnalysis(ArtifactBase):
    """
    Internal artifact. User doesn't use this artifact, it's a helping artifact that is produced by Analysis artifact to process one chunk.
    Output example:
        - coffea accumulator
    If analysis breaks then it doesn't create an output for that chunk. But Analysis artifact continues the processing of the rest.
    And will identify the missing one when rerunning analysis again.
    """
    chunk_file: str
    chunking: Chunking
    analysis_builder: str


   def keys(self):
        return {
            "chunk_file": self.chunk_file,
            "chunking": self.chunking,
            "analysis_builder": self.analysis_builder,
        }

# @register_artifact
# class Merging(ArtifactBase):
#     """
#     User doesn't use this artifact, it's a helping artifact that is produced by Analysis artifact to merge succesfully processed chunks.
#     ?????????????????
#     """
#     analysis_chunks: str[Path] # path to the folder with chunks to merge
#     parameter: str # how to merge? what to merge?

#     def keys(self):
#         return {
#             "analysis_chunks": self.analysis_chunks,
#             "parameter": self.parameter,
#         }

@register_artifact
class Analysis(ArtifactBase):
    """
    External artifact. Triggers internal Artifact materialization of Chunking artifact first. Iterates through gotten chunks and perfoms ChunkAnalysis.
    Ignores failers per chunk but tracks failed chunks.
    Merges whatever was successfully processed in ChunkAnalysis into one output. Returns merged output and report failed chunks.
    """
    name: str
    builder: str

    def keys(self):
        return {
            "builder": self.builder
        }

@register_artifact
class Plotting(ArtifactBase):
    """
    Takes merge output from Analysis and plot it.
    """
    pass

###################################################################
# deps.py
class Deps:
"""
Dependencies are initialized in the executor that knows how to materilize the artifact(it knows config and cache directory).
The producer of the specific artifact then trigers the same executor to produce the previous dependency (artifact).
"""
    def __init__(self, executor):
        self._executor = executor

    def need(self, art: Arifact) -> Path:
        # triggers dependency marelization
        return self._executor.materialize(art)

###################################################################
# producers.py

# just a function that takes (artifact, deps) and returns something
ProducerFn = Callable[[Artifact, "Deps"], Any]

_PRODUCERS = {}

def producer(artifact_type: Artifact):
    """
    @producer(Fileset) registers the function for Fileset.
    """
    def deco(fn: ProducerFn) -> ProducerFn:
        _PRODUCERS[artifact_type] = fn
        return fn
    return deco

def get_producer(artifact_type: Artifact) -> ProducerFn:
    if artifact_type not in _PRODUCERS:
        raise KeyError(f"No producer registered for {artifact_type.__name__}.\n\
                         Available artifacts: {_PRODUCERS.keys()}")
    return _PRODUCERS[artifact_type]


###################################################################
# default_producers.py
import cloudpickle

from .artifacts import Fileset, Analysis, Plotting, Chunking, ChunkAnalysis
from .deps import Deps
# find registered producers
from .producers import producer

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

@producer(Fileset)
def make_fileset(*, art: Fileset, deps: Deps, out: Path, config: RunConfig) -> None:
    # find and call the function that user created specified in builder
    fn = _load_object(art.builder)
    fileset_dict = fn()

    if not isinstance(fileset_dict, dict):
        raise TypeError("Fileset builder must return a dict")

    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(fileset_dict, indent=2, sort_keys=True))


@producer(Chunking)
def split_fileset(*, art: Chunking, deps: Deps, out: Path, config: RunConfig) -> None:
    """
    Chunks a Fileset based on the splitting strategy. 
    Warning: In this way I duplicate the fileset.json saving it's chunks... Is it crucial?
    It returns fileset chunks based on splitting strategy(as I described in Chunking artifact description)
    Manifest output  example:
        - None: "fileset.json"
        - "by_dataset": {'output_files': {'SingleMu_0': 'fileset_SingleMu_0.json', 'SingleMu_1': 'fileset_SingleMu_1.json'}, 'n_chunks': 2}
        - "percentage_per_file": {'output_files': {'0_20': 'fileset_0_20_percent.json', '20_40': 'fileset_20_40_percent.json', '40_60': 'fileset_40_60_percent.json', '60_80': 'fileset_60_80_percent.json', '80_100': 'fileset_80_100_percent.json'}, 'n_chunks': 5, 'split_strategy': 'percentage_per_file', 'percentage': 20}
    """
    # executor creates fileset and materialize() function called by deps.need() returns the path to fileset.json
    out.mkdir(parents=True, exist_ok=True)

    fileset_path = deps.need(art.fileset) 

    splitting_strategy = config.splitting_strategy

    # read fileset.json
    fileset = json.loads(fileset_path.read_text())

    if splitting_strategy == "by_dataset":
        datasets = list(fileset.keys())
        datasets_files = {}
        for dataset in datasets:
            file = f"fileset_{dataset}.json"
            out_path = out / file   # where out=self.cache_dir / f"version_{self.version}"/ art.type_name / art.identity() 
            datasets_files[dataset] = file
            out_path.write_text(json.dumps({dataset: fileset[dataset]}, indent=2))
        manifest = {
            "output_files": datasets_files, 
            "n_chunks": len(datasets),

        }
        manifest_path = out / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2))

    elif splitting_strategy == "percentage_per_file":
        p = self.percentage  # default is 20, we want in one chunk(fileset) 20% of each dataset
        if 100 % p != 0:
            raise ValueError("For this scheme, percentage must divide 100 evenly (20, 25, 10, etc).")

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
        (out / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True))

    else:
        (out / "fileset.json").write_text(json.dumps(fileset, indent=2, sort_keys=True))

@producer(ChunkAnalysis)
"""
This should apply user's analysis per chunk.
"""
def run_analysis(*, art: ChunkAnalysis, deps: Deps, out: Path, config: RunConfig) -> None:
    
    out.parent.mkdir(parents=True, exist_ok=True)

    # create chunks applying splitting strategy
    # TO-DO: do I need chunking initialisation again? is it enough to just have it in execute_analysis()?
    chunking_dir = deps.need(art.chunking)  # directory with chunk jsons
    chunk_path = chunking_dir / art.chunk_file
    chunk_fileset = json.loads(chunk_path.read_text())

    fn = _load_object(art.analysis_builder)  # user's function
    acc = fn(chunk_fileset)    

    out.write_bytes(cloudpickle.dumps(acc))

@producer(Analysis)
"""
This should execute Chunking and run the analysis per chunk + merging
"""
def execute_analysis(*, art: Analysis, deps: Deps, out: Path, config: RunConfig) -> None:
    # create chunks applying splitting strategy
    # it's an artifact that user is not using - internal
    chunking = Chunking(
        fileset=art.fileset,
        split_strategy=config.split_strategy,
        percentage=config.percentage,
    )
    chunk_dir = deps.need(chunking)# self._executor.materialize(Chunking); returns path to .cache_dir / version / Chunking / hash where all .json chunks are
    manifest_path = chunk_dir / "manifest.json" # manifest contains info about our fileset.json or its chunks .json

    #TO-DO: how to make it pretty?
    try:
        manifest = json.loads(manifest_path.read_text())
        chunks_files = list(manifest["output_files"].values()) # ['fileset_0_20_percent.json', 'fileset_20_40_percent.json', 'fileset_40_60_percent.json', 'fileset_60_80_percent.json', 'fileset_80_100_percent.json']
    except FileNotFoundError:
        chunks_files = ["fileset.json"]

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
            chunk_out_path = deps.need(chunk_art)
            acc = cloudpickle.loads(chunk_out_path.read_bytes())
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
    out.write_bytes(cloudpickle.dumps(payload))

    
###################################################################
# executor.py

"""
Executor must materialise the artifacts applying same configurations. It knows code version, config and cache directory.
"""
class Executor:
		    
	def __init__(self, cache_dir: Path, version: str, config: RunConfig):
        self.cache_dir = cache_dir
        self.version = version
        self.config = config
    

    def path_for(self, art: Artifact) -> Path:
        return self.cache_dir / f"version_{self.version}"/ art.type_name / art.identity() 

    def exists(self, art: Artifact) -> bool:
        return self.path_for(art).exists()


    def materialize(self, art: Artifact) -> Path:
        out = self.path_for(art)
        if out.exists():
            return out

        out.parent.mkdir(parents=True, exist_ok=True)
        fn = get_producer(type(art))
        deps = Deps(self)             
        fn(art=art, deps=deps, out=out, config=self.config)

        if not out.exists():
            raise RuntimeError(
                f"Producer for {art.type_name} finished but did not create output at {out}"
            )
        return out 

    
   
###################################################################
# config.py
SplitStrategy = Literal["by_dataset", "percentage_per_file", None]

_ALLOWED_SPLIT_STRATEGIES = {"by_dataset", "percentage_per_file", None}

class RunConfig:
    """
    Defines how to run the analysis, where to put cache
    """
    split_strategy: SplitStrategy = None
    cache_dir: Path = Path(".cache")
    percentage: int = 20

    def __post_init__(self):
		    # we only allow these two strategies
        if self.split_strategy not in _ALLOWED_SPLIT_STRATEGIES:
            raise ValueError(
                f"Invalid split_strategy={self.split_strategy!}\n"
                f"Allowed values: {_ALLOWED_SPLIT_STRATEGIES}."
            )
				# check the percentage if "percentage_per_file" strategy
        if self.split_strategy == "percentage_per_file":
            if not (1 <= self.percentage <= 100):
                raise ValueError("percentage must be an int between 1 and 100")
            else:
	            print(f"Every {self.percentage}% of each dataset will be processed.")
            
        else:
            pass


###################################################################
# render.py 
 
def render(workflow: Workflow, config: RunConfig):
    """
    Executes DAG, sorts the steps to begin with the last one(the last will trigger all the dependencies
    and will materialize the artifacts starting from the Fileset)
    """
    cache_dir = Path(config.cache_dir)
    executor = Executor(cache_dir=cache_dir, version=version, config=config)

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
            f"Executing step '{step_name}' of type '{step_type}' with the user code {step.builder}"
        )
        path = executor.materialize(artifact)
        print(f"  -> materialized at {path}")
        steps_by_name[step.name] = step_name
        paths_by_name[step.name] = path

    return {"paths": paths_by_name, "artifacts": artifacts_by_name, "order": [workflow.steps[i].name for i in order]}



###################################################################
# run_workflow.py

"""
Assuming user split the analysis into get_fileset, run_analysis and plot_results functions,
that all additional code that he/she would have to add.
PS: version is optional, I'm not sure about it.
"""

step_fileset = Step(
							        name="Fileset",
							        step_type = Fileset,
							        builder = "analysis:get_fileset",
							    )
							    
step_analysis = Step(
						        name="SingleMuonAnalysis",
						        step_type = Analysis,
						        builder = "analysis:run_analysis",
						    )

step_plotting = Step(
										name="PlottingMuonAnalysis",
										step_type = Plotting,
										builder = "analysis:plot_results"
								)		
								   
workflow = Workflow(version='1.0')
workflow.add(step_fileset)
workflow.add(step_analysis, depends_on=[step_fileset])
workflow.add(step_plotting, depends_on=[step_analysis])

config = RunConfig(split_strategy="dataset" | "percentage_per_file", cache_dir=".cache", percentage=20)

result = render(workflow, config)	

```
