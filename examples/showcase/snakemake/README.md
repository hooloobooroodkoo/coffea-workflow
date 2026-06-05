# Snakemake

*(TODO)*

This section will demonstrate using the Snakemake backend with `coffea-workflow.render.snakemake_render`.
Unlike `LxplusFactory` + `DaskExecutor` or `CoffeaCasaFactory` + `DaskExecutor` (which parallelises within a chunk),
Snakemake enables **fileset subset-level parallelism** — each fileset subset becomes an
independent batch job running simultaneously.
