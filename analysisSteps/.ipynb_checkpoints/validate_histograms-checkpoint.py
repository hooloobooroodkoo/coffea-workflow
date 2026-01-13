"""
This module provides a simple validation for workflow outputs stored in
a merged.pkl file. It checks whether:
- `merged.pkl` can be loaded
- top-level payload contains a key `hist_dict`
- `payload["hist_dict"]` is a Python `dict`
- `parameters["required_hist_keys"]` is provided, the validator checks that those histogram names exist in `hist_dict`
- Non-empty checks (optional, default enabled): for each `hist.Hist` object in `hist_dict`, the validator computes the total
sum of bin values; if `parameters["require_nonempty"]` is True (default), histograms with total sum == 0 are treated as failures.

inputs:
  - merged: path to merged.pkl (or merged.pkl.gz)

outputs:
  - report: path to validation.json report
"""


from __future__ import annotations
from pathlib import Path
import json

import hist

from coffea.workflow.registry import register
from ._shared import load_merged

@register("validate_histograms")
def run(node, ctx):
    """
    inputs:
      merged: path/to/merged.pkl
    outputs:
      report: path/to/validation.json
    parameters (optional):
      required_hist_keys: list[str]
      require_nonempty: bool
    """
    merged_path = Path(ctx.artifacts["merged"])
    report_path = Path(ctx.artifacts["validation_report"])
    parameters = getattr(node, "params", None) or getattr(node, "parameters", None) or {}


    report_path.parent.mkdir(parents=True, exist_ok=True)

    payload, _ = load_merged(merged_path)

    if "hist_dict" not in payload:
        raise KeyError(f"'hist_dict' not found in merged payload keys: {list(payload.keys())}")

    hist_dict = payload["hist_dict"]
    if not isinstance(hist_dict, dict):
        raise TypeError(f"payload['hist_dict'] is {type(hist_dict)}, expected dict")

    required = parameters.get("required_hist_keys") or []
    missing = [k for k in required if k not in hist_dict]


    hist_summaries: dict[str, dict] = {}
    empty_hists: list[str] = []

    for name, h in hist_dict.items():
        if not isinstance(h, hist.Hist):
            hist_summaries[name] = {"type": str(type(h))}
            continue

        axis_names = [ax.name for ax in h.axes]
        total = float(h.values(flow=True).sum())
        if total == 0.0:
            empty_hists.append(name)

        hist_summaries[name] = {
            "type": "hist.Hist",
            "axes": axis_names,
            "total": total,
        }

    require_nonempty = bool(parameters.get("require_nonempty", True))

    status = "ok"
    problems: list[dict] = []

    if missing:
        status = "fail"
        problems.append({"type": "missing_hist_keys", "missing": missing})

    if require_nonempty and empty_hists:
        status = "fail"
        problems.append({"type": "empty_histograms", "empty": empty_hists[:50]})

    result = {
        "status": status,
        "merged": str(merged_path),
        "n_hists": len(hist_dict),
        "payload_keys": list(payload.keys()),
        "problems": problems,
        "histograms": hist_summaries,
        "how_to_proceed": {
            "load_snippet": (
                "import cloudpickle; "
                "out=cloudpickle.load(open('merged.pkl','rb')); "
                "hist_dict=out[0]['hist_dict']"
            )
        },
    }

    report_path.write_text(json.dumps(result, indent=2))

    if status != "ok":
        raise RuntimeError(f"Histogram validation failed. See: {report_path}")

    return {"report": str(report_path)}

