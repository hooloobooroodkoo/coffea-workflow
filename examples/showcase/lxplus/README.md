# lxplus + Dask

Run the showcase analysis on CERN lxplus, submitting HTCondor batch jobs as Dask workers via `LxplusFactory`.

## Step 1. Generate the Apptainer definition file
If you need some custom or not provided by the lxplus base image packages for your analysis, you have to build your own apptainer. 
Helping function `coffea_workflow.facilities.generate_apptainer_def(...)` creates this `worker.def` file that will be the base for the apptainer
that contains user-specified packages, chosen base image, etc.

Run [generate_def.ipynb](generate_def.ipynb) locally to write `worker.def`.
It installs the needed coffea version for coffea-workflow (the one with the Runner parameter `use_result_type`) 
and coffea-workflow itself from git into the CERN batch team's lxplus EL9 base image by default. But those parameters can be changed to those
prefered by the user. It also provides an instruction on how to run the analysis on lxplus with your apptainer.

## Step 2. Build the image on lxplus

```bash
scp worker.def <username>@lxplus.cern.ch:~/worker.def
ssh <username>@lxplus.cern.ch
condor_submit -interactive        # get a batch node, wait for the shell
cp ~/worker.def .  &&  apptainer build --fakeroot worker.sif worker.def
cp worker.sif ~/worker.sif        # save to AFS home (slow but persistent)
```

## Step 3. Run the workflow

```bash
voms-proxy-init --voms cms --valid 192:00
apptainer exec ~/worker.sif python ~/analysis/workflow_lxplus.py # run your workflow file for your analysis
```

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

In `coffea-workflow` `LxplusFactory.preflight()` is called automatically before the cluster is created. It checks whether:
- VOMS proxy is valid
- `condor_q` is available on PATH (confirms you are on an lxplus node)
- `worker_image` path exists (raises with a `generate_apptainer_def()` suggestion if not)

