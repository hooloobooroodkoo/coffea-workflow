"""
Tests for workflow/artifacts.py
 
Covers:
  - _builder_key() stable serialisation of callable vs string builders
  - All five artifact types: keys(), type_name, identity(), to_dict(), always_rerun
  - Identity determinism: same inputs -> same hash; different inputs -> different hash
  - ARTIFACT_REGISTRY contains all registered types
"""
import pytest
from workflow.artifacts import (
    _builder_key,
    Fileset,
    Chunking,
    ChunkAnalysis,
    Analysis,
    Plotting,
    ARTIFACT_REGISTRY,
    ArtifactBase,
)
 
 
def _some_fn():
    pass
 
 
# ---------------------------------------------------------------------------
# _builder_key
# ---------------------------------------------------------------------------
 
class TestBuilderKey:
    def test_string_passthrough(self):
        assert _builder_key("mymod:my_fn") == "mymod:my_fn"
 
    def test_callable_returns_module_colon_qualname(self):
        key = _builder_key(_some_fn)
        assert ":" in key
        assert key.endswith(":_some_fn")
 
    def test_lambda_contains_lambda_marker(self):
        fn = lambda: None
        assert "<lambda>" in _builder_key(fn)
 
    def test_same_callable_same_key(self):
        assert _builder_key(_some_fn) == _builder_key(_some_fn)
 
    def test_different_strings_differ(self):
        assert _builder_key("mod:a") != _builder_key("mod:b")
 
 
# ---------------------------------------------------------------------------
# Fileset
# ---------------------------------------------------------------------------
 
class TestFileset:
    def test_keys_contains_name_and_builder(self):
        fs = Fileset(name="my_fs", builder="mod:fn")
        k = fs.keys()
        assert k["name"] == "my_fs"
        assert k["builder"] == "mod:fn"
 
    def test_type_name(self):
        assert Fileset(name="x", builder="mod:fn").type_name == "Fileset"
 
    def test_to_dict_structure(self):
        fs = Fileset(name="x", builder="mod:fn")
        d = fs.to_dict()
        assert d["type"] == "Fileset"
        assert "keys" in d
 
    def test_identity_deterministic(self):
        assert (
            Fileset(name="x", builder="mod:fn").identity()
            == Fileset(name="x", builder="mod:fn").identity()
        )
 
    def test_identity_changes_with_name(self):
        assert (
            Fileset(name="a", builder="mod:fn").identity()
            != Fileset(name="b", builder="mod:fn").identity()
        )
 
    def test_identity_changes_with_builder(self):
        assert (
            Fileset(name="x", builder="mod:fn1").identity()
            != Fileset(name="x", builder="mod:fn2").identity()
        )
 
    def test_frozen(self):
        fs = Fileset(name="x", builder="mod:fn")
        with pytest.raises(Exception):
            fs.name = "y"
 
 
# ---------------------------------------------------------------------------
# Chunking
# ---------------------------------------------------------------------------
 
class TestChunking:
    @pytest.fixture
    def fs(self):
        return Fileset(name="fs", builder="mod:fn")
 
    def test_keys_fields(self, fs):
        ch = Chunking(fileset=fs, split_strategy="by_dataset", percentage=25, datasets=("A",))
        k = ch.keys()
        assert k["fileset"] is fs
        assert k["split_strategy"] == "by_dataset"
        assert k["percentage"] == 25
        assert k["datasets"] == ("A",)
 
    def test_type_name(self, fs):
        assert Chunking(fileset=fs, split_strategy=None, percentage=None).type_name == "Chunking"
 
    def test_identity_deterministic(self, fs):
        ch1 = Chunking(fileset=fs, split_strategy=None, percentage=None)
        ch2 = Chunking(fileset=fs, split_strategy=None, percentage=None)
        assert ch1.identity() == ch2.identity()
 
    def test_identity_changes_with_strategy(self, fs):
        ch1 = Chunking(fileset=fs, split_strategy=None, percentage=None)
        ch2 = Chunking(fileset=fs, split_strategy="by_dataset", percentage=None)
        assert ch1.identity() != ch2.identity()
 
    def test_identity_changes_with_percentage(self, fs):
        ch1 = Chunking(fileset=fs, split_strategy=None, percentage=None)
        ch2 = Chunking(fileset=fs, split_strategy=None, percentage=50)
        assert ch1.identity() != ch2.identity()
 
    def test_identity_changes_with_datasets(self, fs):
        ch1 = Chunking(fileset=fs, split_strategy=None, percentage=None, datasets=None)
        ch2 = Chunking(fileset=fs, split_strategy=None, percentage=None, datasets=("A",))
        assert ch1.identity() != ch2.identity()
 
 
# ---------------------------------------------------------------------------
# ChunkAnalysis
# ---------------------------------------------------------------------------
 
class TestChunkAnalysis:
    @pytest.fixture
    def chunking(self):
        fs = Fileset(name="fs", builder="mod:fn")
        return Chunking(fileset=fs, split_strategy=None, percentage=None)
 
    def test_keys_fields(self, chunking):
        ca = ChunkAnalysis(chunk_file="chunk_0.json", chunking=chunking, analysis_builder="mod:run")
        k = ca.keys()
        assert k["chunk_file"] == "chunk_0.json"
        assert k["chunking"] is chunking
        assert k["analysis_builder"] == "mod:run"
 
    def test_type_name(self, chunking):
        ca = ChunkAnalysis(chunk_file="c.json", chunking=chunking, analysis_builder="mod:run")
        assert ca.type_name == "ChunkAnalysis"
 
    def test_identity_changes_with_chunk_file(self, chunking):
        ca1 = ChunkAnalysis(chunk_file="chunk_0.json", chunking=chunking, analysis_builder="mod:run")
        ca2 = ChunkAnalysis(chunk_file="chunk_1.json", chunking=chunking, analysis_builder="mod:run")
        assert ca1.identity() != ca2.identity()
 
    def test_identity_changes_with_builder(self, chunking):
        ca1 = ChunkAnalysis(chunk_file="c.json", chunking=chunking, analysis_builder="mod:run_v1")
        ca2 = ChunkAnalysis(chunk_file="c.json", chunking=chunking, analysis_builder="mod:run_v2")
        assert ca1.identity() != ca2.identity()
 
 
# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------
 
class TestAnalysis:
    @pytest.fixture
    def fs(self):
        return Fileset(name="fs", builder="mod:fn")
 
    def test_keys_fields(self, fs):
        an = Analysis(name="my_analysis", fileset=fs, builder="mod:run")
        k = an.keys()
        assert k["name"] == "my_analysis"
        assert k["fileset"] is fs
        assert k["builder"] == "mod:run"
 
    def test_type_name(self, fs):
        assert Analysis(name="an", fileset=fs, builder="mod:run").type_name == "Analysis"
 
    def test_always_rerun_false(self):
        assert Analysis.always_rerun is False
 
    def test_identity_deterministic(self, fs):
        an1 = Analysis(name="an", fileset=fs, builder="mod:run")
        an2 = Analysis(name="an", fileset=fs, builder="mod:run")
        assert an1.identity() == an2.identity()
 
    def test_identity_changes_with_name(self, fs):
        an1 = Analysis(name="v1", fileset=fs, builder="mod:run")
        an2 = Analysis(name="v2", fileset=fs, builder="mod:run")
        assert an1.identity() != an2.identity()
 
    def test_identity_changes_when_fileset_changes(self):
        fs1 = Fileset(name="fs1", builder="mod:fn")
        fs2 = Fileset(name="fs2", builder="mod:fn")
        an1 = Analysis(name="an", fileset=fs1, builder="mod:run")
        an2 = Analysis(name="an", fileset=fs2, builder="mod:run")
        assert an1.identity() != an2.identity()
 
 
# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------
 
class TestPlotting:
    @pytest.fixture
    def analysis(self):
        fs = Fileset(name="fs", builder="mod:fn")
        return Analysis(name="an", fileset=fs, builder="mod:run")
 
    def test_always_rerun_true(self):
        assert Plotting.always_rerun is True
 
    def test_type_name(self, analysis):
        pl = Plotting(name="p", analysis=analysis, builder="mod:plot")
        assert pl.type_name == "Plotting"
 
    def test_keys_fields(self, analysis):
        pl = Plotting(name="my_plot", analysis=analysis, builder="mod:plot")
        k = pl.keys()
        assert k["name"] == "my_plot"
        assert k["analysis"] is analysis
        assert k["builder"] == "mod:plot"
 
    def test_identity_deterministic(self, analysis):
        pl1 = Plotting(name="p", analysis=analysis, builder="mod:plot")
        pl2 = Plotting(name="p", analysis=analysis, builder="mod:plot")
        assert pl1.identity() == pl2.identity()
 
    def test_identity_changes_with_name(self, analysis):
        pl1 = Plotting(name="p1", analysis=analysis, builder="mod:plot")
        pl2 = Plotting(name="p2", analysis=analysis, builder="mod:plot")
        assert pl1.identity() != pl2.identity()
 
 
# ---------------------------------------------------------------------------
# ARTIFACT_REGISTRY
# ---------------------------------------------------------------------------
 
class TestArtifactRegistry:
    @pytest.mark.parametrize("name", ["Fileset", "Chunking", "ChunkAnalysis", "Analysis", "Plotting"])
    def test_type_is_registered(self, name):
        assert name in ARTIFACT_REGISTRY
 
    def test_registry_values_are_artifact_base_subclasses(self):
        for cls in ARTIFACT_REGISTRY.values():
            assert issubclass(cls, ArtifactBase)