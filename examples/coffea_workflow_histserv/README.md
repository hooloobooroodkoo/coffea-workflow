# Workflow with histserv

The same accumulator workflow as [../coffea_workflow/](../coffea_workflow/), but histograms are streamed to a histserv histogram server during processing instead of being merged locally.

- [analysis_hist.py](analysis_hist.py) — the builder functions plus `hist_template()`, which defines the histogram registered with the server
- [workflow_hist.ipynb](workflow_hist.ipynb) — connects a `histserv.Client`, registers the template, and runs the workflow on coffea-casa

Key differences from the plain accumulator version:

- A `histserv.Client` is created up front, and `hist_client` + `histserv_connection_info` are passed in `RunConfig`
- Builders that declare a `config` parameter receive the `RunConfig` automatically, so `run_analysis(fileset, config)` can read the connection info and `plot_results(config)` can fetch the final histogram from the server
- To reconnect to an existing server-side histogram on a later run, pass the `connection_info` from the previous result (see the comment in the notebook)

Requires `pip install histserv`. See the histserv section of the [main README](../../README.md) for details.
