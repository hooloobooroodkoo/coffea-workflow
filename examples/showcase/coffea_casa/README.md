# CoffeaCasa + Dask

Run the showcase analysis on [coffea-casa](https://coffea-casa.readthedocs.io), which provides a pre-configured Dask cluster at `tls://localhost:8786`.

Describes examples when no setup is needed and `CoffeaCasaFactory` connects to the existing cluster. And also describes the cases where the 
workers are required too install additional packages.

## Options shown

- **Sequential** (`workflow_coffea_casa.ipynb`): fileset chunks are processed one after another using the split strategy. Simple, cache-friendly.
- **Parallel** *(TODO)*: use `client.submit` + `IterativeExecutor` to dispatch multiple fileset subsets to the Dask cluster simultaneously, bypassing coffea-workflow's
sequential chunk loop. *How to learn Dask to see one batch as one job? Bigger coffea-chunks?*
- discuss the use case where you want to install some other packages on Dask workers and that
 some workers result in not having the environment updated while others do
