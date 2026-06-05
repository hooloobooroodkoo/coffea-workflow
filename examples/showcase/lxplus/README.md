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
