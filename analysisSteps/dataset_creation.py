from __future__ import annotations

import json
import os
from ..registry import register
from ..util import load_callable


@register("dataset_creation")
def run(node, ctx):
    out_name = node.outputs[0]
    out_path = ctx.artifacts[out_name]
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    if "fileset" in node.params:
        fileset = node.params["fileset"]
    else:
        factory_path = node.params["fileset_factory"]
        factory_kwargs = node.params.get("fileset_kwargs", {})
        factory = load_callable(factory_path)
        fileset = factory(**factory_kwargs)

    with open(out_path, "w") as f:
        json.dump(fileset, f, indent=2)
