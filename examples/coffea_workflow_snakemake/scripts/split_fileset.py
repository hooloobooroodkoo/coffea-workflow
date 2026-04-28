"""Snakemake script: split fileset.json into per-chunk JSON files."""
from workflow import split_fileset_standalone

split_fileset_standalone(
    fileset_path=snakemake.input[0],
    out_dir=snakemake.output[0],
    strategy=snakemake.params.get("strategy"),
    percentage=snakemake.params.get("percentage"),
    datasets=snakemake.params.get("datasets"),
)
