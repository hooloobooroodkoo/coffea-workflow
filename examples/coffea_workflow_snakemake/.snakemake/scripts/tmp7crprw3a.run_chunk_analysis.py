######## snakemake preamble start (automatically inserted, do not edit) ########
import sys;sys.path.extend(['/usr/local/lib/python3.12/site-packages', '/home/cms-jovyan/coffea-workflow/examples/coffea_workflow_snakemake', '/usr/local/bin', '/usr/local/lib/python3.12', '/usr/local/lib/python3.12/lib-dynload', '/usr/local/lib/python3.12/site-packages', '/home/cms-jovyan/coffea-workflow/src', '/home/cms-jovyan/coffea/src', '/home/cms-jovyan/.cache/snakemake/snakemake/source-cache/snakemake-runtime-cache/tmpo383o115/file/home/cms-jovyan/coffea-workflow/examples/coffea_workflow_snakemake/scripts', '/home/cms-jovyan/coffea-workflow/examples/coffea_workflow_snakemake/scripts']);import pickle;snakemake = pickle.loads(b'\x80\x04\x95E\x05\x00\x00\x00\x00\x00\x00\x8c\x16snakemake.iocontainers\x94\x8c\tSnakemake\x94\x93\x94)\x81\x94}\x94(\x8c\x05input\x94h\x00\x8c\nInputFiles\x94\x93\x94)\x81\x94\x8c\x1bresults/chunks/chunk_1.json\x94a}\x94(\x8c\x06_names\x94}\x94\x8c\x05chunk\x94K\x00N\x86\x94s\x8c\x12_allowed_overrides\x94]\x94(\x8c\x05index\x94\x8c\x04sort\x94eh\x11h\x00\x8c\x0eAttributeGuard\x94\x93\x94)\x81\x94}\x94\x8c\x04name\x94h\x11sbh\x12h\x14)\x81\x94}\x94h\x17h\x12sbh\rh\tub\x8c\x06output\x94h\x00\x8c\x0bOutputFiles\x94\x93\x94)\x81\x94(\x8c!results/chunk_results/chunk_1.pkl\x94\x8c$results/chunk_results/chunk_1.status\x94e}\x94(h\x0b}\x94(\x8c\x07payload\x94K\x00N\x86\x94\x8c\x06status\x94K\x01N\x86\x94uh\x0f]\x94(h\x11h\x12eh\x11h\x14)\x81\x94}\x94h\x17h\x11sbh\x12h\x14)\x81\x94}\x94h\x17h\x12sbh"h\x1eh$h\x1fub\x8c\r_params_store\x94h\x00\x8c\x06Params\x94\x93\x94)\x81\x94(\x8c\x15analysis:run_analysis\x94}\x94e}\x94(h\x0b}\x94(\x8c\x07builder\x94K\x00N\x86\x94\x8c\x0ebuilder_params\x94K\x01N\x86\x94uh\x0f]\x94(h\x11h\x12eh\x11h\x14)\x81\x94}\x94h\x17h\x11sbh\x12h\x14)\x81\x94}\x94h\x17h\x12sbh3h/h5h0ub\x8c\r_params_types\x94}\x94\x8c\twildcards\x94h\x00\x8c\tWildcards\x94\x93\x94)\x81\x94\x8c\x07chunk_1\x94a}\x94(h\x0b}\x94\x8c\x08chunk_id\x94K\x00N\x86\x94sh\x0f]\x94(h\x11h\x12eh\x11h\x14)\x81\x94}\x94h\x17h\x11sbh\x12h\x14)\x81\x94}\x94h\x17h\x12sbhEhBub\x8c\x07threads\x94K\x01\x8c\tresources\x94h\x00\x8c\x0cResourceList\x94\x93\x94)\x81\x94(\x8c\x04/tmp\x94K\x01KxM@\x1f\x8c\x048 GB\x94M\xce\x1dK\x01e}\x94(h\x0b}\x94(\x8c\x06tmpdir\x94K\x00N\x86\x94\x8c\x06_nodes\x94K\x01N\x86\x94\x8c\x07runtime\x94K\x02N\x86\x94\x8c\x06mem_mb\x94K\x03N\x86\x94\x8c\x03mem\x94K\x04N\x86\x94\x8c\x07mem_mib\x94K\x05N\x86\x94\x8c\x06_cores\x94K\x06N\x86\x94uh\x0f]\x94(h\x11h\x12eh\x11h\x14)\x81\x94}\x94h\x17h\x11sbh\x12h\x14)\x81\x94}\x94h\x17h\x12sbhUhQhWK\x01hYKxh[M@\x1f\x8c\x03mem\x94hR\x8c\x07mem_mib\x94M\xce\x1dhaK\x01ub\x8c\x03log\x94h\x00\x8c\x03Log\x94\x93\x94)\x81\x94}\x94(h\x0b}\x94h\x0f]\x94(h\x11h\x12eh\x11h\x14)\x81\x94}\x94h\x17h\x11sbh\x12h\x14)\x81\x94}\x94h\x17h\x12sbub\x8c\x06config\x94}\x94(\x8c\x0ffileset_builder\x94\x8c\x14analysis:get_fileset\x94\x8c\x10analysis_builder\x94h/\x8c\x0cplot_builder\x94\x8c\x15analysis:plot_results\x94\x8c\x08strategy\x94\x8c\nby_dataset\x94\x8c\npercentage\x94N\x8c\x08datasets\x94Nu\x8c\x04rule\x94\x8c\x12run_chunk_analysis\x94\x8c\x0fbench_iteration\x94N\x8c\tscriptdir\x94\x8cK/home/cms-jovyan/coffea-workflow/examples/coffea_workflow_snakemake/scripts\x94ub.');from snakemake.logging import logger;from snakemake.iocontainers import Snakemake;__real_file__ = __file__; __file__ = '/home/cms-jovyan/coffea-workflow/examples/coffea_workflow_snakemake/scripts/run_chunk_analysis.py';
######## snakemake preamble end #########
"""Snakemake script: run analysis on a single chunk.

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
