# Simple accumulator workflow

The basic end-to-end example: a MET analysis over two CMS Open Data muon datasets, wired into a fileset → analysis → plotting DAG. Results are merged as plain coffea accumulators — no histogram server involved.

- [analysis.py](analysis.py) — the builder functions (`get_fileset`, `run_analysis`, `plot_results`) plus a custom filtering helper
- [workflow.ipynb](workflow.ipynb) — builds the DAG and runs it

Beyond the basics, the notebook demonstrates:

- **`builder_params`** — passing extra keyword arguments to a builder from the `Step` definition
- **`CustomArtifact`** — defining your own intermediate step (`custom_function_remove_last_file`) alongside the predefined artifact types
- **String builder references** — `builder="analysis:plot_results"` instead of importing the callable
- **Facility switching** — `RunConfig` variants for local `FuturesExecutor`, coffea-casa `FuturesExecutor`, and coffea-casa `DaskExecutor` with `worker_packages`/`worker_files`

For the same workflow streaming histograms to a histserv server instead, see [../coffea_workflow_histserv/](../coffea_workflow_histserv/).
