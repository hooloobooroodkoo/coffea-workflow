"""
Tests for render._resolve_step_config and render.run preflight wiring.
"""
import pytest
from coffea_workflow.config import RunConfig, ExecutorConfig
from coffea_workflow.facilities import LocalFactory, CoffeaCasaFactory
from coffea_workflow.workflow import Step, Workflow
from coffea_workflow.artifacts import Fileset
from coffea_workflow.render import _resolve_step_config, run


@pytest.fixture
def workflow_config():
    return RunConfig(
        strategy="by_dataset",
        percentage=50,
        cache_dir=".cache",
        executor_config=ExecutorConfig(executor_type="FuturesExecutor", workers=2),
        facility=LocalFactory(),
    )


@pytest.fixture
def bare_step():
    return Step(name="S", step_type=Fileset, builder="m:fn")


class TestResolveStepConfig:
    def test_no_overrides_returns_workflow_config(self, workflow_config, bare_step):
        result = _resolve_step_config(workflow_config, bare_step)
        assert result is workflow_config

    def test_step_facility_overrides_workflow_facility(self, workflow_config):
        step = Step(
            name="S", step_type=Fileset, builder="m:fn",
            facility=CoffeaCasaFactory(scheduler_address="tcp://x:8786"),
        )
        result = _resolve_step_config(workflow_config, step)
        assert isinstance(result.facility, CoffeaCasaFactory)

    def test_step_facility_does_not_affect_analysis_params(self, workflow_config):
        step = Step(
            name="S", step_type=Fileset, builder="m:fn",
            facility=CoffeaCasaFactory(scheduler_address="tcp://x:8786"),
        )
        result = _resolve_step_config(workflow_config, step)
        assert result.strategy == workflow_config.strategy
        assert result.percentage == workflow_config.percentage
        assert result.cache_dir == workflow_config.cache_dir

    def test_step_executor_config_overrides_workflow_executor(self, workflow_config):
        ec = ExecutorConfig(executor_type="IterativeExecutor")
        step = Step(name="S", step_type=Fileset, builder="m:fn", executor_config=ec)
        result = _resolve_step_config(workflow_config, step)
        assert result.executor_config.executor_type == "IterativeExecutor"

    def test_step_executor_does_not_affect_analysis_params(self, workflow_config):
        ec = ExecutorConfig(executor_type="IterativeExecutor")
        step = Step(name="S", step_type=Fileset, builder="m:fn", executor_config=ec)
        result = _resolve_step_config(workflow_config, step)
        assert result.strategy == workflow_config.strategy
        assert result.cache_dir == workflow_config.cache_dir

    def test_both_overrides_applied_independently(self, workflow_config):
        fc = CoffeaCasaFactory(scheduler_address="tcp://x:8786")
        ec = ExecutorConfig(executor_type="DaskExecutor")
        step = Step(name="S", step_type=Fileset, builder="m:fn", facility=fc, executor_config=ec)
        result = _resolve_step_config(workflow_config, step)
        assert isinstance(result.facility, CoffeaCasaFactory)
        assert result.executor_config.executor_type == "DaskExecutor"
        assert result.strategy == workflow_config.strategy

    def test_workflow_facility_used_when_step_has_none(self, workflow_config, bare_step):
        result = _resolve_step_config(workflow_config, bare_step)
        assert result.facility is workflow_config.facility

    def test_workflow_executor_used_when_step_has_none(self, workflow_config, bare_step):
        result = _resolve_step_config(workflow_config, bare_step)
        assert result.executor_config is workflow_config.executor_config


class TestRunPreflight:
    def test_misconfigured_facility_fails_before_any_step(self, tmp_path):
        # LocalFactory + DaskExecutor with no scheduler address must fail upfront
        # in run()'s preflight, not 30 minutes into execution.
        config = RunConfig(
            cache_dir=str(tmp_path),
            facility=LocalFactory(),
            executor_config=ExecutorConfig(executor_type="DaskExecutor"),
        )
        wf = Workflow()
        wf.add(Step(name="S", step_type=Fileset, builder="m:fn"))
        with pytest.raises(ValueError, match="scheduler address"):
            run(wf, config)

    def test_valid_facility_passes_preflight(self, tmp_path):
        # A Dask address present -> preflight passes. An empty workflow returns
        # before the executor is ever built, so no real connection is attempted;
        # reaching the early return proves preflight did not raise.
        config = RunConfig(
            cache_dir=str(tmp_path),
            facility=LocalFactory(scheduler_address="tcp://host:8786"),
            executor_config=ExecutorConfig(executor_type="DaskExecutor"),
        )
        result = run(Workflow(), config)
        assert result["order"] == []
