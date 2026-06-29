"""
Showcase analysis on CERN lxplus with HTCondor Dask workers.

Run from the lxplus login node:
    voms-proxy-init --voms cms --valid 192:00
    python run.py

If no worker_image is configured, LxplusFactory will prompt you to build
an Apptainer image on a Condor batch node before starting the analysis.
On subsequent runs, pass the built image directly to skip the wizard:
    LxplusFactory(worker_image="./worker.sif", ...)
"""

import hist
import coffea.processor as processor
from coffea.nanoevents import schemas

from coffea_workflow import Step, Workflow, Fileset, Analysis, Plotting, RunConfig, ExecutorConfig, run
from coffea_workflow.facilities import LxplusFactory


def get_fileset():
    return {
        "SingleMu": {
            "files": {
                "root://eospublic.cern.ch//eos/opendata/cms/mc/RunIISummer20UL16NanoAODv9/GluGluHToWWTo2L2Nu_M125_TuneCP5_13TeV_powheg2_JHUGenV714_pythia8/NANOAODSIM/106X_mcRun2_asymptotic_v17-v2/260000/A7FEFB1C-387F-2B4D-A111-C53CC9371EC7.root": "Events",
                "root://eospublic.cern.ch//eos/opendata/cms/mc/RunIISummer20UL16NanoAODv9/GluGluHToWWTo2L2Nu_M125_TuneCP5_13TeV_powheg2_JHUGenV714_pythia8/NANOAODSIM/106X_mcRun2_asymptotic_v17-v2/260000/AB10FBAB-92C0-C043-933D-117FCC5704BA.root": "Events",
            }
        }
    }


class METProcessor(processor.ProcessorABC):
    def __init__(self):
        dataset_axis = hist.axis.StrCategory(name="dataset", label="", categories=[], growth=True)
        MET_axis = hist.axis.Regular(name="MET", label="MET [GeV]", bins=50, start=0, stop=100)
        self.output = processor.dict_accumulator({
            "MET": hist.Hist(dataset_axis, MET_axis),
            "cutflow": processor.defaultdict_accumulator(int),
        })

    def process(self, events):
        dataset = events.metadata["dataset"]
        MET = events.MET.pt
        self.output["cutflow"]["all events"] += len(MET)
        self.output["cutflow"]["chunks"] += 1
        self.output["MET"].fill(dataset=dataset, MET=MET)
        return self.output

    def postprocess(self, accumulator):
        pass


def run_analysis(fileset, executor=None):
    runner = processor.Runner(
        executor=executor,
        schema=schemas.NanoAODSchema,
        savemetrics=False,
        skipbadfiles=True,
        use_result_type=True,
    )
    return runner(fileset, METProcessor())


def print_results(result):
    hist_acc, _ = result["processor_result"]
    print("Cutflow:")
    for key, val in hist_acc["cutflow"].items():
        print(f"  {key}: {val}")


step_fileset = Step(name="Fileset", step_type=Fileset, builder=get_fileset)
step_analysis = Step(name="Analysis", step_type=Analysis, builder=run_analysis)
step_results = Step(name="Results", step_type=Plotting, builder=print_results)

workflow = Workflow()
workflow.add(step_fileset)
workflow.add(step_analysis, depends_on=[step_fileset])
workflow.add(step_results, depends_on=[step_analysis])

config = RunConfig(
    strategy="by_dataset",
    cache_dir=".cache_lxplus",
    facility=LxplusFactory(
        # worker_image="./worker.sif",  # uncomment on subsequent runs to skip the wizard
        queue="espresso",
        workers=2,
    ),
    executor_config=ExecutorConfig(executor_type="DaskExecutor"),
)

run(workflow, config)
