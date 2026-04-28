"""Snakemake script: merge all chunk payloads into a single Analysis payload."""
from workflow import merge_chunk_results_standalone

merge_chunk_results_standalone(
    chunk_payloads=snakemake.input.payloads,
    chunk_statuses=snakemake.input.statuses,
    builder_key=snakemake.params.builder_key,
    out_path=snakemake.output[0],
)
