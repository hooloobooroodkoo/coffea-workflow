"""Snakemake script: call the plot builder with the merged Analysis payload."""
import sys
sys.path.insert(0, snakemake.scriptdir + "/..")

from workflow import make_plot_standalone
import matplotlib.pyplot as plt

result = snakemake.input[0]

make_plot_standalone(
    merged_payload_path=result,
    builder=snakemake.params.builder,
    out_path=snakemake.output[0],
    builder_params=snakemake.params.get("builder_params", {}),
)
