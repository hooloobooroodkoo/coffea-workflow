# coffea-workflow

A workflow manager and HEP-specific extension for [coffea](https://github.com/scikit-hep/coffea) analyses. It does not replace existing workflow managers (Snakemake, LAW, …) — instead it focuses on three things coffea alone does not provide out of the box:

- **Partial results** — split your fileset into independently cached chunks; if some fail you keep the rest, and only the failed chunks are retried on the next run
- **Facility factories** — one-line switching between local execution, [coffea-casa](https://coffea-casa.readthedocs.io), and CERN lxplus (HTCondor) without changing your analysis code
- **Execution control** — choose between sequential and parallel chunk submission, tune executor type and worker count per facility

Your analysis code stays unchanged and fully separate from the execution logic. The only shift in thinking is structural: instead of one monolithic script, you organise the code around the natural stages of a HEP pipeline — fileset discovery, running the processor, plotting, and so on — and hand each stage to the workflow as a step. How you write each function is up to you.

---

## Why coffea-workflow
 
Some HEP analyses share 
1) a similar structure (multiple sequential steps: discovering input files, splitting them into manageable chunks, running a coffea processor over each chunk, merging partial results, and producing final plots),
2) as well as similar practices that are manually re-implemented from scratch each time (splitting the fileset to test on a smaller subset of files, implementing local caching of partial results, and so on). 

Without a pre-defined workflow layer, coffea users tend to write ad-hoc scripts that are sometimes difficult to reproduce, cannot skip already-completed work, mix workflow execution logic with analysis logic, and lose partial progress when, for example, a remote file server is temporarily unreachable.
 
`coffea-workflow` addresses this by:
 
- Defining each stage as a typed, hashable **Artifact** with a deterministic identity derived from its inputs.
- Storing every produced artifact in a **content-addressable cache** (`.cache/`), so any step whose inputs have not changed is loaded from disk on the next run.
- Providing **chunk-level fault tolerance**: if 4 out of 5 chunks succeed and one fails (e.g. a broken XRootD endpoint), the successful chunks are preserved and only the failed chunk is retried on the next run.
- Keeping **framework logic cleanly separated** from analysis code — no decorators on your functions, no YAML.

---

## Installation

`coffea-workflow` requires a fork of coffea that exposes `use_result_type=True` on `processor.Runner`, enabling the `Ok`/`Err` result-type pattern used by the fault-tolerance mechanism. This is available on the `processor_result_type` branch of this [fork](https://github.com/hooloobooroodkoo/coffea/tree/processor_result_type) (it will be added to the main coffea repository soon).

<!-- TODO: once the project is transferred to the coffea team and both packages are on PyPI,
     replace both blocks below with a single: pip install coffea-workflow -->

### Install the forked coffea

```bash
git clone https://github.com/hooloobooroodkoo/coffea.git
cd coffea
git checkout processor_result_type
python -m pip install .
cd ..
```

### Install coffea-workflow

```bash
git clone https://github.com/hooloobooroodkoo/coffea-workflow-engine.git
cd coffea-workflow-engine
python -m pip install .
```

### Optional — histserv

```bash
pip install histserv
```
---

## Quick Start

Separate your analysis into stand-alone functions, one per workflow stage:

```python
# analysis.py

def get_fileset():
    return {
        "SingleMuon_2018A": {
            "files": {"root://cmsxrootd.fnal.gov//store/...": "Events"},
        }
    }

def run_analysis(fileset, executor):
    # your existing coffea processor call here — return Ok(output) or Err(exception)
    result = processor.Runner(executor=executor, ...)(fileset, ...)
    return result

def plot_results(analysis_output):
    ...
```

Then wire them together in a notebook or script:

```python
from coffea_workflow import Step, Workflow, Fileset, Analysis, Plotting, RunConfig, ExecutorConfig, run
from coffea_workflow import facilities
from analysis import get_fileset, run_analysis, plot_results

# 1. Define steps — map artifact types to your functions
step_fileset  = Step(name="Fileset",  step_type=Fileset,  builder=get_fileset,
                     output="fileset")
step_analysis = Step(name="Analysis", step_type=Analysis, builder=run_analysis,
                     input="fileset",  output="histograms")
step_plotting = Step(name="Plotting", step_type=Plotting, builder=plot_results,
                     input="histograms")

# 2. Build the DAG
workflow = Workflow()
workflow.add(step_fileset)
workflow.add(step_analysis, depends_on=[step_fileset])
workflow.add(step_plotting, depends_on=[step_analysis])

# 3. Configure and run
config = RunConfig(
    strategy="by_dataset",  # datasets are processed independently
    facility=facilities.coffea_casa,
    executor_config=ExecutorConfig(executor_type="DaskExecutor", workers=4),
    cache_dir=".cache",
)
run(workflow, config)
```

That is the whole API surface. `coffea-workflow` handles caching, splitting, fault-tolerance, and client setup for the environment automatically.

---

## Key Features

### Split Strategies

`coffea-workflow` breaks the fileset into independent *chunks* (subsets of the fileset) before running. Each chunk is a sub-fileset processed and cached on its own, so partial results are preserved even when some chunks fail.

> **Important distinction:** these are *workflow-level* chunks (sub-filesets), not coffea's internal 50k-event chunks. A single workflow chunk may still contain many coffea-level event batches.

| Strategy | Chunks | Best for |
|---|---|---|
| `strategy=None` (default) | 1 whole fileset | small tests, single-dataset runs |
| `strategy="by_dataset"` | 1 per dataset | multi-dataset runs, dataset-level fault isolation |
| `strategy=None, percentage=20` | 5 mixed across all datasets | quick sanity checks on a representative slice |
| `strategy="by_dataset", percentage=20` | 5 per dataset (15 total for 3 datasets) | large filesets, maximum fault tolerance |

**Smaller chunks preserve more work on failure** — only the failed chunk is retried, not the whole analysis. However, very small chunks add scheduling overhead on batch systems (more HTCondor job submissions). See [examples/showcase/split_strategy/](examples/showcase/split_strategy/) for a worked notebook of each strategy.

```python
# One chunk per dataset — if one dataset's storage fails, the others succeed
RunConfig(strategy="by_dataset")

# Split each dataset into 5 chunks of 20% each
RunConfig(strategy="by_dataset", percentage=20)

# Only run over specific datasets (e.g. for a quick test)
RunConfig(datasets=["SingleMuon_2018A"])
```

---

### Facility Factories

Switching execution environments is a one-line change in `RunConfig`. Your analysis code is untouched.

```python
from coffea_workflow import facilities

# Local — FuturesExecutor with N workers (default)
config = RunConfig(facility=facilities.local)

# coffea-casa — DaskExecutor connecting to the pre-configured Dask scheduler
config = RunConfig(facility=facilities.coffea_casa)

# CERN lxplus — HTCondor cluster, workers running inside an Apptainer image
config = RunConfig(facility=facilities.LxplusFactory(
    worker_image="~/worker.sif",
    queue="longlunch",
    workers=10,
))
```

Each factory also accepts an `ExecutorConfig` that overrides the default executor type:
```python
config = RunConfig(
    facility=facilities.local,
    executor_config=ExecutorConfig(executor_type="IterativeExecutor"),  # single-threaded
)
```

#### lxplus deployment

If you have no `worker.sif` yet, run your script locally first — `LxplusFactory` generates `worker.def` and `run_on_lxplus.sh` with exact build and run instructions. You can also generate the Apptainer definition file manually:

```python
from coffea_workflow.facilities import generate_apptainer_def
generate_apptainer_def(extra_packages=("correctionlib==2.1.0",))
```

See [examples/showcase/facilities/](examples/showcase/facilities/) for a full worked example.

---

### Sequential vs Parallel Chunk Execution

By default, `coffea-workflow` processes workflow chunks **sequentially**: one chunk is submitted to the executor, runs to completion, its result is cached, then the next chunk starts. This is the safer default because all N workers collaborate on a single chunk's event-level tasks.

**When to prefer sequential (default):**
- You have more workers than chunks — all workers collaborate per chunk and self-balance across its tasks
- Chunks have unequal file counts — avoids the slowest-chunk bottleneck that parallel dispatch creates

**When to consider parallel:**
- You have many more chunks than workers and they are roughly equal in size
- You want to minimise scheduler round-trip overhead on coffea-casa (Dask cluster is persistent)

```python
# Sequential (default) — one chunk at a time, all workers per chunk
config = RunConfig(
    facility=facilities.coffea_casa,
    executor_config=ExecutorConfig(executor_type="DaskExecutor"),
)

# Parallel — all chunks submitted simultaneously, one worker per chunk
config = RunConfig(
    facility=facilities.coffea_casa,
    executor_config=ExecutorConfig(executor_type="DaskExecutor", parallel_chunks=True),
)
```

A worked analysis of the trade-offs is in [examples/showcase/optimisation/](examples/showcase/optimisation/).

---
 
## Repository structure

```
coffea-workflow/
├── src/
│   └── coffea_workflow/
│       ├── __init__.py            # public API: Step, Workflow, run, RunConfig,
│       │                          #   Fileset, Analysis, Plotting, ExecutorConfig, facilities
│       ├── artifacts.py           # Artifact classes (Fileset, Analysis, Plotting,
│       │                          #   Chunking, ChunkAnalysis, CustomArtifact)
│       ├── config.py              # RunConfig, ExecutorConfig, FacilityBase
│       ├── facilities.py          # LocalFactory, CoffeaCasaFactory, LxplusFactory
│       ├── default_producers.py   # Built-in producers for each artifact type
│       ├── snakemake_producers.py # Standalone producers for Snakemake backend
│       ├── executor.py            # Cache lookup and materialization
│       ├── render.py              # run() — topological sort + DAG execution
│       └── workflow.py            # Step dataclass, Workflow DAG container
├── examples/
│   ├── showcase/                  # Minimal MET analysis demonstrating all features
│   │   ├── split_strategy/        # One notebook per split strategy
│   │   ├── facilities/            # coffea-casa and lxplus worked examples
│   │   └── optimisation/          # Sequential vs parallel benchmarks (in progress)
│   ├── agc_ttbar/                 # Full AGC ttbar analysis with coffea-workflow
│   ├── coffea_workflow/           # Simple accumulator example (no histserv)
│   ├── coffea_workflow_histserv/  # Same analysis with histserv backend
│   └── coffea_workflow_snakemake/ # Snakemake backend example (in progress)
└── README.md
```
---
 
## Concepts
 
### Workflow & Step
 
A **`Workflow`** is a container for a directed acyclic graph. It holds a list of **`Step`** objects and a list of directed edges `(src_index, dst_index)` expressing dependencies.
 
```python
@dataclass(frozen=True)
class Step:
    name: str          # human-readable label; also used as a cache-path component
    step_type: Type    # one of the external Artifact classes (Fileset, Analysis, Plotting)
    builder: str | Callable  # pointer to user-provided function
```
 
`builder` can be:
 
- A **module:attribute string**, e.g. `"analysis:create_fileset()"`
- A **callable** (function or class) passed directly
 
```python
workflow = Workflow()
workflow.add(step_fileset)
workflow.add(step_analysis, depends_on=[step_fileset])
workflow.add(step_plotting, depends_on=[step_analysis])
```
 
`Workflow.add()` appends the step and records the dependency edges.
 
---

### Example 1 — Standard coffea accumulator
 
**Location:** `examples/coffea_workflow/`
 
**Files:**
- `workflow.ipynb` — the main notebook that defines and executes the workflow.
- `analysis.py` — user-written functions: `get_fileset`, `run_analysis`, `plot_results`.

User have to structure the analysis by function, separating Fileset, Analysis and Plotting logics (as in `analysis.py`). Then struucture the workflow the following way:
```python
# workflow.ipynb

from workflow import Step, Workflow, Fileset, Analysis, Plotting, RunConfig, render
from analysis import get_fileset, run_analysis, plot_results

# 1) decide on a name for caching, 2) specify the step type(Fileset, Analysis or Plotting artifacts), 3) map artifacts' builders to your functions
step_fileset = Step(
							        name="Fileset",
							        step_type = Fileset,
							        builder = get_fileset,
							    )
							    
step_analysis = Step(
						        name="SingleMuonAnalysis",
						        step_type = Analysis,
						        builder = run_analysis,
						    )

step_plotting = Step(
										name="PlottingMuonAnalysis",
										step_type = Plotting,
										builder = "analysis:plot_results"
								)		

# add all steps to the workflow and specify dependencies							   
workflow = Workflow()
workflow.add(step_fileset)
workflow.add(step_analysis, depends_on=[step_fileset])
workflow.add(step_plotting, depends_on=[step_analysis])

# decide on a split strategy for a fileset, where to store the partial result, optionaly histserv information
config = RunConfig(percentage=20, cache_dir="cache")

# execute the workflow with configurations
result = render(workflow, config)
```
---

### Artifacts
 
An **Artifact** is the typed, hashable representation of one unit of work and its output.
The `type_name` property returns the class name (e.g. `"Analysis"`). Combined with `identity()`, the executor stores every artifact at:
 
```
<cache_dir>/<type_name>/<identity>/
```
 
Artifacts are divided into **external** (user-visible, declared in a `Step`, such as `Fileset`, `Analysis` and `Plotting`) and **internal** (created automatically by the framework as execution details). Users never interact with the internal `Chunking` or `ChunkAnalysis` directly, which handle splitting the fileset and processing the fileset per subset, but control their behaviour through `RunConfig`.
 
#### ★ Artifact Fileset (external)
 
```python
@dataclass(frozen=True)
class Fileset(ArtifactBase):
    name: str
    builder: str | Callable
```
 
The entry point of every workflow. Its **producer** (`make_fileset`) resolves `builder` to a Python callable, calls it with no arguments, and expects a dict in the standard coffea fileset format:
 
```python
{
    "SingleMuon_2018": {
        "files": {
            "root://some-server.example.org//store/data/SingleMuon.root": "Events",
            ...
        },
        "metadata": {...}
    },
    ...
}
```
 
The result is written to `.cache/Fileset/<identity>/fileset.json`.

**The `builder` field** is the sole piece of user-supplied code at this stage. The builder function must:
- Accept no arguments. (TODO: enable params for builder)
- Return a dict matching the coffea fileset schema.
Example:
 
```python
def get_fileset():
    return {
        "SingleMuon_2018A": {
            "files": {"root://cmsxrootd.fnal.gov//store/...": "Events"},
        }
    }
```
---
 
#### ★ Artifact Analysis (external)
 
```python
@dataclass(frozen=True)
class Analysis(ArtifactBase):
    name: str
    fileset: Fileset
    builder: str | Callable
```
 
The central artifact. Its **producer** (`execute_analysis`) orchestrates the complete chunk-level fan-out calling split strategy function, then applying the user's analysis function per sub-fileset, then merging all the successful chunk results together(if local histogramming; if `histserv` is used, it returns the histserv connection information). It returns `payload.pkl` containing `{"merged": ..., "failures": [...], "n_chunks_total": N, "n_chunks_ok": M, "histserv_connection":{},}`. If any failures occurred, writes a `.has_failures` — this causes `executor.exists()` to return `False` on the next run, ensuring the Analysis is re-executed to attempt to repair missing chunks.
 
---
 
#### ★ Artifact Plotting (external)
 
```python
@dataclass(frozen=True)
class Plotting(ArtifactBase):
    always_rerun = True  # class-level flag
 
    name: str
    analysis: Analysis
    builder: str | Callable
```
 
Consumes the merged output of an `Analysis` artifact and produces visualisations. Its producer calls the user's plotting function. Unlike all other artifacts, `Plotting` carries `always_rerun = True` at the class level, so it always produces the latest plot. User's builder should accept `config` if `histserv` is being used to reconnect to the remote histogram server and retrieve a snapshot.
 
---
 
#### ★ Artifact Chunking (internal)
 
```python
@dataclass(frozen=True)
class Chunking(ArtifactBase):
    fileset: Fileset
    split_strategy: str | None
    percentage: int | None
    datasets: tuple[str, ...] | None
```
 
It is created automatically by the `Analysis` producer before it loops over chunks. Its **producer** (`split_fileset`) reads `fileset.json` from the upstream `Fileset` cache entry and produces a directory containing:
 
- One or more `fileset_chunk_N.json` files — each a sub-fileset covering a specific slice of the full input fileset.
- A `manifest.json` listing all chunk files and their count.
The splitting behaviour can be specified in `RunConfig`.
 
---
 
#### ★ Artifact ChunkAnalysis (internal)
 
```python
@dataclass(frozen=True)
class ChunkAnalysis(ArtifactBase):
    chunk_file: str
    chunking: Chunking
    analysis_builder: str | Callable
```
 
Represents the processing of a single chunk, users never create this; it is instantiated in a loop inside the `Analysis` producer. It reads the chunk's sub-fileset JSON and applies user's analysis function to it. It calls `fn(chunk_fileset)` or `fn(chunk_fileset, config=config)` if the function declares a `config` parameter (required for histserv integration, for example). It expects `Ok(accumulator)` or `Err(exception)` result type from a coffea processor. On success: writes `payload.pkl` and touches `.success`. On failure: writes only `payload.pkl` (containing the `Err`); the absence of `.success` is what tells subsequent runs that this chunk needs to be retried.
 
---
## RunConfig
 
`RunConfig` is a dataclass that carries all workflow-level configuration(split strategy, cache, histserv client. It is passed through the full call chain (render → executor → producer) so that every producer has access to the same global parameters. `RunConfig` is the single configuration object passed to `render()`.
 
```python
@dataclass(frozen=True)
class RunConfig:
    strategy: Literal["by_dataset"] | None = None
    percentage: int | None = None
    datasets: tuple[str, ...] | None = None
    cache_dir: Path = Path(".cache")
    hist_client: Any | None = None
    histserv_connection_info: dict | None = None
```
 
| Field | Type | Default | Description |
|---|---|---|---|
| `strategy` | `"by_dataset"` or `None` | `None` | Chunk splitting strategy. `"by_dataset"` creates one chunk per dataset. `None` keeps all datasets in a single chunk or mixed in chunks if percentage is specified. |
| `percentage` | `int` or `None` | `None` | When set, each chunk covers this percentage of each dataset's files. Must divide 100 evenly (e.g. 20, 25, 50). Combined with `strategy`: `strategy="by_dataset"` + `percentage=20` creates `n_datasets × 5` chunks. |
| `datasets` | `tuple[str, ...]` or `None` | `None` | If set, only the named datasets are processed. Accepts a list (auto-converted to tuple for hashability). Useful for testing on a single dataset before running the full analysis. |
| `cache_dir` | `Path` | `Path(".cache")` | Root directory for the content-addressable store. Each artifact type has a subdirectory here. |
| `hist_client` | `histserv.Client` or `None` | `None` | A live histserv client object. When specified, the `Analysis` producer routes accumulated histograms to the remote server rather than pickling them locally. |
| `histserv_connection_info` | `dict` or `None` | `None` | The serialisable connection info dict (`{"address": ..., "hist_id": ..., "token": ...}`) returned by `hist_client.init(...).get_connection_info()`. This is what gets stored in `Analysis`'s `payload.pkl` in histserv mode, providing a durable pointer to the histogram without requiring a live gRPC connection in the pickle file. |

---
 
## Splitting Strategies
 
The splitting mechanism is the key to `coffea-workflow`'s fault tolerance. Rather than running all input files as one monolithic coffea job (which either fully succeeds or fully fails), the `Chunking` layer breaks the fileset into independently processable sub-filesets called *chunks*. Each chunk corresponds to exactly one `ChunkAnalysis` artifact with its own cache entry.
 
> **Important distinction**: these are *workflow-level* chunks (sub-filesets), not the usual coffea chunks of 50k events. A single workflow chunk may still contain many coffea-level chunks.
 
### `strategy=None` (default)
 
All datasets are kept together in a single chunk. This is equivalent to running a standard coffea job. Useful for small analyses or when debugging.
 
```
manifest.json
fileset_chunk_0.json   ← all datasets
```
 
### `strategy="by_dataset"`
 
One chunk per dataset. If the fileset has three datasets, three `ChunkAnalysis` artifacts are created. If one dataset's files are on a temporarily unreachable storage element, the other two still succeed and their results are preserved.
 
```
manifest.json
fileset_chunk_0.json   ← SingleMuon_2018A
fileset_chunk_1.json   ← SingleMuon_2018B
fileset_chunk_2.json   ← SingleMuon_2018C
```
 
### `strategy=None` + `percentage=20`
 
The fileset is split into `100 / 20 = 5` chunks, each containing 20% of every dataset's files. 
 
```
manifest.json
fileset_chunk_0.json   ← files 0–20% of each dataset
fileset_chunk_1.json   ← files 20–40% of each dataset
...
fileset_chunk_4.json   ← files 80–100% of each dataset
```
 
### `strategy="by_dataset"` + `percentage=20`
 
Each dataset is first isolated into its own group, then each group is split at 20% intervals. For three datasets this yields `3 × 5 = 15` independent chunks.
 
---

## Producers
 
A **producer** is a Python function registered for producing a certain Artifact type:
 
```python
@producer(Fileset)
def make_fileset(*, art: Fileset, deps: Deps, out: Path, config: RunConfig) -> None:
    ...
```
Users never write producers themselves. They only write the **builder functions** that are *called by* the producers.
 
---
 
## Executor
 
`Executor` is instantiated once per `render()` call and holds cache_dir, checks the existence of the artifact in the current session cache, and either materializes the non-existing one or the one that contains `.has_failers` or just returns the artifact from cache:
 
| Artifact | Expected Output |
|---|---|
| `Fileset` | `fileset.json` |
| `Chunking` | `manifest.json` |
| `ChunkAnalysis` | `.success` |
| `Analysis` | `payload.pkl` **and** no `.has_failures` otherwise rerun|
| `Plotting` | `payload.pkl` |
 
`Plotting` sets `always_rerun = True` on the class so plots are regenerated every run (they are typically fast and users expect to see fresh output).
 
---
 
## render
 
`render(workflow: Workflow, config: RunConfig) -> dict` is the single entry point that the user specifies to execute the entire workflow:
 
```python
result = render(workflow, config)
```

Runs a **topological sort**  over the step graph, materializes needed artifacts and prints the summary.

---
 
## histserv
 
[histserv](https://github.com/pfackeldey/histserv) is a histogram accumulation server. `coffea-workflow` integrates with `histserv` through `RunConfig`.
 
### How it works
 
1. Before calling `render()`, the user creates a `histserv.Client`, initialises a histogram template on the server, and preserves the *connection info*.
2. The client and the connection info are passed into `RunConfig`.
3. The `Analysis` producer, upon seeing a successful chunk result, does **not** merge into a local accumulator.
4. The `Plotting` producer passes to a user's builder function the connection information to reconnect and extract the histogram.
   
### Reconnecting across sessions
 
If you interrupt and resume a workflow that was using histserv, you can reconnect to the already-filled histogram by reading the connection info stored in the previous Analysis result and passing it back into `RunConfig`:
 
```python
# On subsequent run, re-use the existing histogram
conn = previous_result["results"]["AnalysisStepName"]["merged"]
config = RunConfig(
    hist_client=hist_client,
    histserv_connection_info=conn,
    ...
)
```
 
---
 
## Example 2 — With histserv
 
**Location:** `examples/coffea_workflow_histserv/`
 
**Files:**
- `workflow_hist.ipynb` — the main notebook, structurally identical to Example 1 but with histserv wiring.
- `analysis_hist.py` — user-supplied functions: `get_fileset`, `run_analysis`, `plot_results`, `hist_template`.
This example demonstrates the full histserv integration path. The `run_analysis` function accepts a `config` keyword argument, extracts `config.hist_client` and `config.histserv_connection_info`, reconnects to the remote histogram, fills it via the normal coffea processor flow, and returns the connection info (not the histogram itself) wrapped in `Ok(...)`.
 
Key differences from Example 1:
 
```python
# workflow_hist.ipynb
import histserv
 
# 1. Create a client and initialise a histogram on the server
hist_client = histserv.Client(address="histserv.cmsaf-dev.flatiron.hollandhpc.org:8788")
histserv_connection_info = hist_client.init(hist=hist_template(), token="test").get_connection_info()
 
# 2. Thread both into RunConfig
config = RunConfig(
    hist_client=hist_client,
    histserv_connection_info=histserv_connection_info,
    strategy="by_dataset",
    percentage=20,
    cache_dir="cache_hist",
)



# analysis_hist.py

# 3. Changes in user-written function, Analysis builder(run_analysis) and Plotting builder(plot_results) must accept `config` with the histserv information and reconnect to it
def run_analysis(fileset, config):
    hist_client = config.hist_client
    conn = config.histserv_connection_info
    remote_hist = hist_client.connect(hist_id=conn["hist_id"], token=conn["token"])

# 4. The `hist_template()` function defines the histogram axes:
def hist_template():
    dataset_axis = hist.axis.StrCategory(name="dataset", label="", categories=[], growth=True)
    MET_axis = hist.axis.Regular(name="MET", label="MET [GeV]", bins=50, start=0, stop=100)
    return hist.Hist(dataset_axis, MET_axis)
```

---
 
## Mapping to Workflow Languages
 
### Snakemake and law backend (planned)
 
Integration with Snakemake and law as alternative execution backends is a planned feature. The design intent is to allow the same `Workflow` + `RunConfig` definition to be *compiled* to a `Snakefile` or a set of law `Task` classes, enabling users who already operate Snakemake or law pipelines on their HPC clusters to plug `coffea-workflow` analyses into their existing infrastructure without rewriting anything.
 
