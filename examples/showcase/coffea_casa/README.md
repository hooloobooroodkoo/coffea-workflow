# CoffeaCasa + Dask

Run the showcase analysis on [coffea-casa](https://coffea-casa.readthedocs.io), which provides a pre-configured Dask cluster at `tls://localhost:8786`. No cluster setup is needed — `CoffeaCasaFactory` connects to the existing cluster, and can install extra packages or upload local files to the workers via `worker_packages` / `worker_files`.

## Notebooks

- [workflow_coffea_casa.ipynb](workflow_coffea_casa.ipynb) — **sequential** chunk processing with the split strategy, including a variant with an intentionally broken file to show chunk-level fault tolerance.
- [../optimisation/parallel_vs_sequential.ipynb](../optimisation/parallel_vs_sequential.ipynb) — **parallel** chunk processing: `parallel_chunks=True` submits all uncached chunks to Dask workers at once via `client.submit`, with a timing comparison against the sequential mode.

## Current problems with DaskExecutor usage

Using `DaskExecutor` with `coffea-workflow`'s split strategy is currently unavailable on CoffeaCasa due to a hard dependency on the exact coffea version installed in the worker image.

**Issues:**

1. `coffea-workflow`'s split strategy uses `coffea.dataset_tools.splitting.split_fileset`, which wasn't introduced in the earlier coffea version. coffea-casa doesn't support this one yet.
   
2. The natural fix is to install a consistent coffea version on workers at runtime via `worker_packages`. However, any runtime package installation requires restarting workers so the new version is actually loaded.

3. Worker restarts break the Dask scheduler: `FutureCancelledError: scheduler-restart`.

4. Without a restart, not all workers use the newly pip-installed version of coffea, so some chunks break with an error that the `use_result_type` field wasn't found in the coffea Runner.

5. The alternative — installing with `client.run()` then calling `client.restart()` explicitly. But `client.restart()` is a scheduler-level restart that also cancels any futures submitted after the install, making it impossible to pipeline setup and execution.

**In short:** everything about running `coffea-workflow` with split strategy on CoffeaCasa Dask workers depends on having a single consistent coffea version across the notebook server, the scheduler, and every worker pod — something that cannot be reliably achieved without controlling the worker container image.

## What works

Until coffea-casa ships a recent enough coffea version in its worker image:

- `FuturesExecutor` (the default) runs on the notebook server using Python multiprocessing and works reliably with all split strategies.
- For genuine distributed `DaskExecutor` runs, use [lxplus](../lxplus/) instead, where you control the worker image.
