"""Snakemake script: call the fileset builder and write fileset.json."""
import sys
sys.path.insert(0, snakemake.scriptdir + "/..")  # make analysis.py importable

from workflow import make_fileset_standalone

make_fileset_standalone(
    builder=snakemake.params.builder,
    out_path=snakemake.output[0],
    builder_params=snakemake.params.get("builder_params", {}),
)
