from __future__ import annotations

from pathlib import Path
from typing import Any

from ..registry import register
from ..util import load_callable
from ._shared import load_merged


def _get_payload(merged_path: Path) -> Any:
    loaded = load_merged(merged_path)
    # load_merged may return payload OR (payload, meta, ...)
    return loaded[0] if isinstance(loaded, tuple) else loaded


@register("plot_histograms")
def plot_histograms(node, ctx):
    """
    Generic plotting step.
    """
    params = node.params or {}

    merged_path = Path(ctx.artifacts["merged"])
    plot_dir = Path(ctx.artifacts["plot_dir"])
    plot_index = Path(ctx.artifacts["plot_index"])

    plot_dir.mkdir(parents=True, exist_ok=True)
    plot_index.parent.mkdir(parents=True, exist_ok=True)

    payload = _get_payload(merged_path)

    plot_factory_spec = params.get("plot_factory", "agc_ttbar.wrappers:make_plots")
    plot_factory = load_callable(plot_factory_spec)

    plot_kwargs = params.get("plot_kwargs", {}) or {}

    result = plot_factory(
        payload=payload,
        merged_path=str(merged_path),
        outputs={
            "plot_dir": str(plot_dir),
            "plot_index": str(plot_index),
        },
        parameters=params,
        **plot_kwargs,
    )

    return {
        "plot_dir": str(plot_dir),
        "plot_index": str(plot_index),
        "plot_factory": plot_factory_spec,
        "result": result,
    }
