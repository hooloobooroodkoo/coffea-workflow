from __future__ import annotations

import glob
import os
import pickle
from typing import Any, Dict, List

from ..registry import register


def _merge_dict_of_numbers(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(a)
    for k, v in b.items():
        out[k] = out.get(k, 0) + v
    return out


def _merge_dict_of_addables(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge two dicts whose values support '+' (e.g. hist.Hist).
    """
    out = dict(a)
    for k, v in b.items():
        out[k] = out[k] + v if k in out else v
    return out


def _merge_agc_outputs(acc_a: Dict[str, Any], acc_b: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}

    # Merge nevents
    if "nevents" in acc_a or "nevents" in acc_b:
        out["nevents"] = _merge_dict_of_numbers(acc_a.get("nevents", {}), acc_b.get("nevents", {}))

    # Merge hist_dict
    if "hist_dict" in acc_a or "hist_dict" in acc_b:
        out["hist_dict"] = _merge_dict_of_addables(acc_a.get("hist_dict", {}), acc_b.get("hist_dict", {}))

    # Merge ml_hist_dict if present
    if "ml_hist_dict" in acc_a or "ml_hist_dict" in acc_b:
        out["ml_hist_dict"] = _merge_dict_of_addables(acc_a.get("ml_hist_dict", {}), acc_b.get("ml_hist_dict", {}))

    # Carry through any other keys (best-effort)
    # Prefer explicit merged keys above; for extras, keep from left then fill missing from right.
    for k, v in acc_a.items():
        if k not in out:
            out[k] = v
    for k, v in acc_b.items():
        if k not in out:
            out[k] = v

    return out


@register("merge_outputs")
def run(node, ctx):
    inputs = node.inputs
    if not inputs and "glob" in node.params:
        pattern = os.path.join(ctx.workspace, node.params["glob"])
        paths: List[str] = glob.glob(pattern)
    else:
        paths = [ctx.artifacts[name] for name in inputs]

    if not paths:
        raise RuntimeError("No inputs found for merge_outputs")

    merged: Dict[str, Any] | None = None
    for p in paths:
        with open(p, "rb") as f:
            acc = pickle.load(f)

        if merged is None:
            merged = acc
        else:
            # AGC output is a dict, so merge with AGC-aware logic
            if isinstance(merged, dict) and isinstance(acc, dict):
                merged = _merge_agc_outputs(merged, acc)
            else:
                # fallback for accumulator-like objects
                merged = merged + acc  # type: ignore[operator]
    out_path = ctx.artifacts[node.outputs[0]]
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "wb") as f:
        pickle.dump(merged, f)
