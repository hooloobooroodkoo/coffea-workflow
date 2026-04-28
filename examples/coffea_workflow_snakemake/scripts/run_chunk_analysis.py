"""
Snakemake script: run analysis on a single chunk.

Always writes both output files so the rule never fails at the Snakemake
level — failed chunks are recorded in the .status file and skipped during
merging (Option A partial-result strategy).
"""
import sys
sys.path.insert(0, snakemake.scriptdir + "/..")

from workflow import run_chunk_analysis_standalone
print("STARTING CHUNK:", snakemake.input.chunk)
run_chunk_analysis_standalone(
    chunk_path=snakemake.input.chunk,
    builder=snakemake.params.builder,
    out_payload=snakemake.output.payload,
    out_status=snakemake.output.status,
    builder_params=snakemake.params.get("builder_params", {}),
)
