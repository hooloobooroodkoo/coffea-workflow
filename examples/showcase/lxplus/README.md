# lxplus + Dask

Run the showcase analysis on CERN lxplus, submitting HTCondor batch jobs as Dask workers via `LxplusFactory`.

Files in this example:

- [run_workflow.py](run_workflow.py) — the workflow script (fileset → analysis → plotting with `LxplusFactory`)
- [run_on_lxplus.sh](run_on_lxplus.sh) — wrapper that creates the VOMS proxy and runs the workflow inside the Apptainer image
- [worker.def](worker.def) — Apptainer definition file for the worker image
- [generate_def.ipynb](generate_def.ipynb) — optional: regenerate `worker.def` with custom package versions

## Step 1. Run locally to generate the deployment files

```bash
python run_workflow.py
```

When run anywhere other than lxplus, `LxplusFactory.preflight()` writes `worker.def` and `run_on_lxplus.sh`, then prints the exact scp, build, and run commands for deployment.

The default `worker.def` installs coffea (the version providing the `use_result_type` Runner parameter) and coffea-workflow on top of the CERN batch team's lxplus EL9 base image, which already ships XRootD and HTCondor. To pin versions or add extra packages, use [generate_def.ipynb](generate_def.ipynb), which calls `coffea_workflow.facilities.generate_apptainer_def(...)` with custom sources.

## Step 2. Build the image on lxplus

```bash
scp worker.def run_on_lxplus.sh run_workflow.py <username>@lxplus.cern.ch:~/
ssh <username>@lxplus.cern.ch
condor_submit -interactive        # get a batch node, wait for the shell
cp ~/worker.def .  &&  apptainer build --fakeroot worker.sif worker.def
cp worker.sif ~/worker.sif        # save to AFS home (slow but persistent)
```

## Step 3. Run the workflow

Back on a regular lxplus node, from the directory containing `worker.sif`:

```bash
bash run_on_lxplus.sh
```

The script creates the VOMS proxy, binds the HTCondor and Kerberos configuration into the container, and runs `run_workflow.py` inside `worker.sif`. On lxplus, `preflight()` verifies that HTCondor and `dask_jobqueue` are available and that the proxy is valid; if `worker_image` is not set, it picks up `worker.sif` from the current directory.

## How the Dask cluster is created

Unlike CoffeaCasa, lxplus has **no pre-configured Dask cluster**. Workers do not exist until you request them. 
`LxplusFactory` creates the cluster on demand at runtime:

1. `HTCondorCluster` is configured with the job spec (memory, cores, queue flavour, Apptainer image).
2. `cluster.scale(N)` submits N HTCondor batch jobs — each job becomes one Dask worker running inside your `.sif`.
3. A Dask `Client` connects to the cluster once the workers are up.
4. Coffea's `Runner` distributes event-level tasks across those workers.
5. When the analysis finishes, `LxplusFactory.close()` cancels the HTCondor jobs.

Because workers run inside your Apptainer image, they have exactly the packages you installed — this is why the image must be built before you can use `LxplusFactory`.

A valid **VOMS proxy** is also required: the workers need it to access files over XRootD (`root://eospublic.cern.ch/...`). The proxy file on the host (`/tmp/x509up_uXXXX`) is forwarded into the container automatically.


## What you can configure in coffea-workflow

The cluster is set up entirely through `LxplusFactory` passed as `facility` in `RunConfig`:

```python
from coffea_workflow import RunConfig, ExecutorConfig
from coffea_workflow.facilities import LxplusFactory

config = RunConfig(
    facility=LxplusFactory(
        worker_image="~/worker.sif",  # path to your built Apptainer image
        queue="longlunch",            # HTCondor flavour: espresso (20min), longlunch (2h), workday (8h)
        workers=10,                   # number of HTCondor jobs to submit
        cores=1,                      # CPU cores per worker
        memory="2GB",                 # RAM per worker
        disk="1GB",                   # disk per worker
        extra_pythonpath=(),          # inject local source paths into workers (for development)
    ),
    executor_config=ExecutorConfig(executor_type="DaskExecutor"),
)
```
