
This prototype introduces a **workflow layer** on top of Coffea implemented as a submodule:
- workflows are described declaratively,
- execution is separated from analysis logic,
- the same analysis can be split, scheduled, and merged in different ways.

It is an intermediate abstraction intended to integrate later with tools like LAW, Luigi, Airflow, etc.

---

## What was added

coffea.workflow:
- a workflow **configuration format**
- an **intermediate representation (IR)** of the workflow
- a **registry-based execution model**
- a simple **local backend** for execution

---
```
coffea/workflow/ 
├── config.py # parse YAML → workflow config 
├── ir.py # intermediate representation (nodes, graph) 
├── registry.py # maps step "kind" → Python implementation 
├── util.py # helpers (dynamic imports, callables) 
├── backends/ 
        └────── local.py # local execution backend 
└── analysisSteps/
        ├────── _shared.py 
        ├────── dataset_creation.py 
        ├────── partition.py 
        ├────── coffea_run.py
        ├────── plot_histograms.py
        ├────── validate_histograms.py 
        └────── merge_outputs.py 
```
### Key ideas

- **Config**: what the user writes (YAML)
- **IR**: internal graph of steps + dependencies (backend-agnostic)
- **Registry**: how step kinds are resolved to executable code
- **Backend**: how the IR is executed (local for now)

## Example (AGC ttbar)

A working example using this workflow layer is implemented in a separate repository:

- **AGC example repo:** https://github.com/hooloobooroodkoo/agc-coffea-workflow

It demonstrates how an existing Coffea analysis (processor + helper code) can be executed via a declarative workflow configuration (YAML), including dataset creation, partitioning, running multiple partitions, and merging outputs.

### The user:
Keeps their existing analysis code (processor, utils, etc.)
1. Adds a workflow configuration (YAML):
```python
# agc_ttba.yaml
workflow:
  name: "cms_open_data_ttbar_two_schedulers_iterative"

  artifacts:
    fileset:    { uri: "fileset.json",           type: "json" }
    manifest:   { uri: "manifest.json",          type: "json" }
    partial_a:  { uri: "partials/part_A.pkl",    type: "pickle" }
    partial_b:  { uri: "partials/part_B.pkl",    type: "pickle" }
    merged:     { uri: "merged.pkl",             type: "pickle" }

  steps:
    # step A: dataset_creation
    datasets:
      kind: dataset_creation
      outputs: [fileset]
      params:
        fileset_factory: "agc_ttbar.wrappers:make_fileset"
        fileset_kwargs:
          n_files_max_per_sample: 5    
          use_xcache: false
          af_name: ""
          input_from_eos: false      
          xcache_atlas_prefix: "" 
          
    # split into two partitions
    partition:
      kind: partition_fileset
      depends_on: [datasets]
      inputs: [fileset]
      outputs: [manifest]
      params:
        strategy: "split_half"
        partition_ids: ["A", "B"]

    # partition A
    run_sched_a:
      kind: coffea_run
      depends_on: [partition]
      inputs: [manifest]
      outputs: [partial_a]
      params:
        partition_id: "A"
        treename: "Events"
        schema: "coffea.nanoevents:NanoAODSchema"

        processor_factory: "agc_ttbar.wrappers:make_processor"
        processor_kwargs:
          use_inference: true
          use_triton: false

        runner_kwargs:
          chunksize: 100000
          savemetrics: true
          metadata_cache: {}

      resources:
        executor:
          backend: "futures"
          workers: 1
          scheduler_label: "SCHED_A"

    # partition B
    run_sched_b:
      kind: coffea_run
      depends_on: [partition]
      inputs: [manifest]
      outputs: [partial_b]
      params:
        partition_id: "B"
        treename: "Events"
        schema: "coffea.nanoevents:NanoAODSchema"

        processor_factory: "agc_ttbar.wrappers:make_processor"
        processor_kwargs:
          use_inference: true
          use_triton: false

        runner_kwargs:
          chunksize: 100000
          savemetrics: true
          metadata_cache: {}

      resources:
        executor:
          backend: "futures"
          workers: 1
          scheduler_label: "SCHED_B"


    merge:
      kind: merge_outputs
      depends_on: [run_sched_a, run_sched_b]
      inputs: [partial_a, partial_b]
      outputs: [merged]
      params:
        merge_strategy: "coffea_accumulator"
```

2. Adds small wrapper functions `agc_ttbar\wrappers.py`to expose:
```python
# agc_ttbar/wrappers.py

def make_fileset(...):
    return construct_fileset(...)

def make_processor(use_inference, use_triton):
    return TtbarAnalysis(use_inference, use_triton)
```
3. Runs the workflow via a driver `run_workflow.py`

Currently the example can be run:
```python
python run_workflow.py
```

Execution example:
```
cms-jovyan@jupyter-yana-2eholoborodko-40cern-2ech agc-dev]$ python run_workflow.py

==> STEP datasets  kind=dataset_creation
    outputs: fileset -> /home/cms-jovyan/yana_coffea/agc-dev/workdir/fileset.json
    done in 0.26s

==> STEP partition  kind=partition_fileset
    inputs:  fileset -> /home/cms-jovyan/yana_coffea/agc-dev/workdir/fileset.json
    outputs: manifest -> /home/cms-jovyan/yana_coffea/agc-dev/workdir/manifest.json
    [partition_fileset] created partitions:
      - A: datasets=9, files=17
      - B: datasets=9, files=26
    done in 0.00s

==> STEP run_sched_b  kind=coffea_run
    inputs:  manifest -> /home/cms-jovyan/yana_coffea/agc-dev/workdir/manifest.json
    outputs: partial_b -> /home/cms-jovyan/yana_coffea/agc-dev/workdir/partials/part_B.pkl
  Preprocessing 100% ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 26/26 [ 0:00:08 < 0:00:00 | 3.5   file/s ]
Merging (local)   4% ━━━━╺━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  1/26 [ 0:00:08 < -:--:-- | ?   merges/s ]
     Processing   0% ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 0/286 [ 0:00:02 < -:--:-- | ?  chunk/s ]
Merging (local)   0% ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━   0/0 [ 0:00:02 < -:--:-- | ? merges/s ]/home/cms-jovyan/yana_coffea/agc-dev/coffea-workflow/src/coffea/nanoevents/schemas/nanoaod.py:283: RuntimeWarning: Missing cross-reference index for LowPtElectron_electronIdx => Electron
  warnings.warn(
/home/cms-jovyan/yana_coffea/agc-dev/coffea-workflow/src/coffea/nanoevents/schemas/nanoaod.py:283: RuntimeWarning: Missing cross-reference index for LowPtElectron_genPartIdx => GenPart
  warnings.warn(
/home/cms-jovyan/yana_coffea/agc-dev/coffea-workflow/src/coffea/nanoevents/schemas/nanoaod.py:283: RuntimeWarning: Missing cross-reference index for LowPtElectron_photonIdx => Photon
  warnings.warn(
/home/cms-jovyan/yana_coffea/agc-dev/coffea-workflow/src/coffea/nanoevents/schemas/nanoaod.py:283: RuntimeWarning: Missing cross-reference index for FatJet_subJetIdx1 => SubJet
  warnings.warn(
/home/cms-jovyan/yana_coffea/agc-dev/coffea-workflow/src/coffea/nanoevents/schemas/nanoaod.py:283: RuntimeWarning: Missing cross-reference index for FatJet_subJetIdx2 => SubJet
  warnings.warn(
/home/cms-jovyan/yana_coffea/agc-dev/coffea-workflow/src/coffea/nanoevents/schemas/nanoaod.py:322: RuntimeWarning: Branch Photon_mass already exists but its values will be replaced with 0.0
  warnings.warn(
/home/cms-jovyan/yana_coffea/agc-dev/coffea-workflow/src/coffea/nanoevents/schemas/nanoaod.py:322: RuntimeWarning: Branch Photon_charge already exists but its values will be replaced with 0.0
     Processing 100% ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 286/286 [ 0:16:04 < 0:00:00 | 0.3  chunk/s ]
Merging (local)   0% ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━   1/286 [ 0:16:04 < -:--:-- | ?   merges/s ]
    done in 975.33s

==> STEP run_sched_a  kind=coffea_run
    inputs:  manifest -> /home/cms-jovyan/yana_coffea/agc-dev/workdir/manifest.json
    outputs: partial_a -> /home/cms-jovyan/yana_coffea/agc-dev/workdir/partials/part_A.pkl
  Preprocessing 100% ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 17/17 [ 0:00:06 < 0:00:00 | 3.1   file/s ]
Merging (local)   6% ━━━━━━╸━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  1/17 [ 0:00:06 < -:--:-- | ?   merges/s ]
     Processing   0% ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 0/189 [ 0:00:02 < -:--:-- | ?  chunk/s ]
Merging (local)   0% ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━   0/0 [ 0:00:02 < -:--:-- | ? merges/s ]/home/cms-jovyan/yana_coffea/agc-dev/coffea-workflow/src/coffea/nanoevents/schemas/nanoaod.py:283: RuntimeWarning: Missing cross-reference index for LowPtElectron_electronIdx => Electron
  warnings.warn(
/home/cms-jovyan/yana_coffea/agc-dev/coffea-workflow/src/coffea/nanoevents/schemas/nanoaod.py:283: RuntimeWarning: Missing cross-reference index for LowPtElectron_genPartIdx => GenPart
  warnings.warn(
/home/cms-jovyan/yana_coffea/agc-dev/coffea-workflow/src/coffea/nanoevents/schemas/nanoaod.py:283: RuntimeWarning: Missing cross-reference index for LowPtElectron_photonIdx => Photon
  warnings.warn(
/home/cms-jovyan/yana_coffea/agc-dev/coffea-workflow/src/coffea/nanoevents/schemas/nanoaod.py:283: RuntimeWarning: Missing cross-reference index for FatJet_subJetIdx1 => SubJet
  warnings.warn(
/home/cms-jovyan/yana_coffea/agc-dev/coffea-workflow/src/coffea/nanoevents/schemas/nanoaod.py:283: RuntimeWarning: Missing cross-reference index for FatJet_subJetIdx2 => SubJet
  warnings.warn(
/home/cms-jovyan/yana_coffea/agc-dev/coffea-workflow/src/coffea/nanoevents/schemas/nanoaod.py:322: RuntimeWarning: Branch Photon_mass already exists but its values will be replaced with 0.0
  warnings.warn(
/home/cms-jovyan/yana_coffea/agc-dev/coffea-workflow/src/coffea/nanoevents/schemas/nanoaod.py:322: RuntimeWarning: Branch Photon_charge already exists but its values will be replaced with 0.0
     Processing   4% ━━━━╺━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 7/189 [ 0:00:48 < 0:19:44 | 0.2  chunk/s ]
Merging (local)  14% ━━━━━━━━━━━━━━━╸━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━   1/7 [ 0:00:48 < -:--:-- | ?   merges/s ]/home/cms-jovyan/yana_coffea/agc-dev/coffea-workflow/src/coffea/nanoevents/schemas/nanoaod.py:283: RuntimeWarning: Missing cross-reference index for FatJet_subJetIdx1 => SubJet
  warnings.warn(
/home/cms-jovyan/yana_coffea/agc-dev/coffea-workflow/src/coffea/nanoevents/schemas/nanoaod.py:283: RuntimeWarning: Missing cross-reference index for FatJet_subJetIdx2 => SubJet
  warnings.warn(
/home/cms-jovyan/yana_coffea/agc-dev/coffea-workflow/src/coffea/nanoevents/schemas/nanoaod.py:322: RuntimeWarning: Branch Photon_mass already exists but its values will be replaced with 0.0
  warnings.warn(
/home/cms-jovyan/yana_coffea/agc-dev/coffea-workflow/src/coffea/nanoevents/schemas/nanoaod.py:322: RuntimeWarning: Branch Photon_charge already exists but its values will be replaced with 0.0
     Processing 100% ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 286/286 [ 0:16:04 < 0:00:00 | 0.3  chunk/s ]
Merging (local)   0% ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━   1/286 [ 0:16:04 < -:--:-- | ?   merges/s ]
    done in 975.33s

==> STEP run_sched_a  kind=coffea_run
    inputs:  manifest -> /home/cms-jovyan/yana_coffea/agc-dev/workdir/manifest.json
    outputs: partial_a -> /home/cms-jovyan/yana_coffea/agc-dev/workdir/partials/part_A.pkl
  Preprocessing 100% ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 17/17 [ 0:00:06 < 0:00:00 | 3.1   file/s ]
Merging (local)   6% ━━━━━━╸━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  1/17 [ 0:00:06 < -:--:-- | ?   merges/s ]
     Processing   0% ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 0/189 [ 0:00:02 < -:--:-- | ?  chunk/s ]
Merging (local)   0% ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━   0/0 [ 0:00:02 < -:--:-- | ? merges/s ]/home/cms-jovyan/yana_coffea/agc-dev/coffea-workflow/src/coffea/nanoevents/schemas/nanoaod.py:283: RuntimeWarning: Missing cross-reference index for LowPtElectron_electronIdx => Electron
  warnings.warn(
/home/cms-jovyan/yana_coffea/agc-dev/coffea-workflow/src/coffea/nanoevents/schemas/nanoaod.py:283: RuntimeWarning: Missing cross-reference index for LowPtElectron_genPartIdx => GenPart
  warnings.warn(
/home/cms-jovyan/yana_coffea/agc-dev/coffea-workflow/src/coffea/nanoevents/schemas/nanoaod.py:283: RuntimeWarning: Missing cross-reference index for LowPtElectron_photonIdx => Photon
  warnings.warn(
/home/cms-jovyan/yana_coffea/agc-dev/coffea-workflow/src/coffea/nanoevents/schemas/nanoaod.py:283: RuntimeWarning: Missing cross-reference index for FatJet_subJetIdx1 => SubJet
  warnings.warn(
/home/cms-jovyan/yana_coffea/agc-dev/coffea-workflow/src/coffea/nanoevents/schemas/nanoaod.py:283: RuntimeWarning: Missing cross-reference index for FatJet_subJetIdx2 => SubJet
  warnings.warn(
/home/cms-jovyan/yana_coffea/agc-dev/coffea-workflow/src/coffea/nanoevents/schemas/nanoaod.py:322: RuntimeWarning: Branch Photon_mass already exists but its values will be replaced with 0.0
  warnings.warn(
/home/cms-jovyan/yana_coffea/agc-dev/coffea-workflow/src/coffea/nanoevents/schemas/nanoaod.py:322: RuntimeWarning: Branch Photon_charge already exists but its values will be replaced with 0.0
     Processing 100% ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 286/286 [ 0:16:04 < 0:00:00 | 0.3  chunk/s ]
Merging (local)   0% ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━   1/286 [ 0:16:04 < -:--:-- | ?   merges/s ]
    done in 975.33s

==> STEP run_sched_a  kind=coffea_run
    inputs:  manifest -> /home/cms-jovyan/yana_coffea/agc-dev/workdir/manifest.json
    outputs: partial_a -> /home/cms-jovyan/yana_coffea/agc-dev/workdir/partials/part_A.pkl
  Preprocessing 100% ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 17/17 [ 0:00:06 < 0:00:00 | 3.1   file/s ]
Merging (local)   6% ━━━━━━╸━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  1/17 [ 0:00:06 < -:--:-- | ?   merges/s ]
     Processing   0% ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 0/189 [ 0:00:02 < -:--:-- | ?  chunk/s ]
Merging (local)   0% ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━   0/0 [ 0:00:02 < -:--:-- | ? merges/s ]/home/cms-jovyan/yana_coffea/agc-dev/coffea-workflow/src/coffea/nanoevents/schemas/nanoaod.py:283: RuntimeWarning: Missing cross-reference index for LowPtElectron_electronIdx => Electron
  warnings.warn(
/home/cms-jovyan/yana_coffea/agc-dev/coffea-workflow/src/coffea/nanoevents/schemas/nanoaod.py:283: RuntimeWarning: Missing cross-reference index for LowPtElectron_genPartIdx => GenPart
  warnings.warn(
/home/cms-jovyan/yana_coffea/agc-dev/coffea-workflow/src/coffea/nanoevents/schemas/nanoaod.py:283: RuntimeWarning: Missing cross-reference index for LowPtElectron_photonIdx => Photon
  warnings.warn(
/home/cms-jovyan/yana_coffea/agc-dev/coffea-workflow/src/coffea/nanoevents/schemas/nanoaod.py:283: RuntimeWarning: Missing cross-reference index for FatJet_subJetIdx1 => SubJet
  warnings.warn(
/home/cms-jovyan/yana_coffea/agc-dev/coffea-workflow/src/coffea/nanoevents/schemas/nanoaod.py:283: RuntimeWarning: Missing cross-reference index for FatJet_subJetIdx2 => SubJet
  warnings.warn(
/home/cms-jovyan/yana_coffea/agc-dev/coffea-workflow/src/coffea/nanoevents/schemas/nanoaod.py:322: RuntimeWarning: Branch Photon_mass already exists but its values will be replaced with 0.0
  warnings.warn(
/home/cms-jovyan/yana_coffea/agc-dev/coffea-workflow/src/coffea/nanoevents/schemas/nanoaod.py:322: RuntimeWarning: Branch Photon_charge already exists but its values will be replaced with 0.0
     Processing 100% ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 189/189 [ 0:10:56 < 0:00:00 | 0.3  chunk/s ]
Merging (local)   1% ╸━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━   1/189 [ 0:10:56 < -:--:-- | ?   merges/s ]
    done in 663.96s

==> STEP merge  kind=merge_outputs
    inputs:  partial_a -> /home/cms-jovyan/yana_coffea/agc-dev/workdir/partials/part_A.pkl, partial_b -> /home/cms-jovyan/yana_coffea/agc-dev/workdir/partials/part_B.pkl
    outputs: merged -> /home/cms-jovyan/yana_coffea/agc-dev/workdir/merged.pkl
    done in 0.00s

Workflow finished. Artifacts:
  fileset: /home/cms-jovyan/yana_coffea/agc-dev/workdir/fileset.json
  manifest: /home/cms-jovyan/yana_coffea/agc-dev/workdir/manifest.json
  partial_a: /home/cms-jovyan/yana_coffea/agc-dev/workdir/partials/part_A.pkl
  partial_b: /home/cms-jovyan/yana_coffea/agc-dev/workdir/partials/part_B.pkl
  merged: /home/cms-jovyan/yana_coffea/agc-dev/workdir/merged.pkl
[cms-jovyan@jupyter-yana-2eholoborodko-40cern-2ech agc-dev]$ 
```


