from __future__ import annotations

import json
import os
from typing import Any, Dict, List

from ..registry import register


@register("partition_fileset")
def run(node, ctx):
    in_fileset_path = ctx.artifacts[node.inputs[0]]
    out_manifest_path = ctx.artifacts[node.outputs[0]]
    os.makedirs(os.path.dirname(out_manifest_path), exist_ok=True)

    with open(in_fileset_path) as f:
        fileset: Dict[str, Dict[str, Any]] = json.load(f)

    partition_ids: List[str] = node.params.get("partition_ids", ["A", "B"])
    if len(partition_ids) != 2:
        raise ValueError("v1 partition_fileset supports exactly 2 partitions (e.g. ['A','B']).")

    a_id, b_id = partition_ids
    partitions: Dict[str, Dict[str, Any]] = {a_id: {}, b_id: {}}

    for dataset, payload in fileset.items():
        if not isinstance(payload, dict) or "files" not in payload:
            raise ValueError(
                f"Expected fileset entries like {{'files': [...], 'metadata': {{...}}}}, "
                f"but got dataset={dataset!r} payload keys={list(payload) if isinstance(payload, dict) else type(payload)}"
            )

        file_list = payload["files"]
        metadata = payload.get("metadata", {})

        mid = len(file_list) // 2
        a_files = file_list[:mid]
        b_files = file_list[mid:]

        # Keep same metadata in both partitions
        partitions[a_id][dataset] = {"files": a_files, "metadata": metadata}
        partitions[b_id][dataset] = {"files": b_files, "metadata": metadata}

    manifest = [
        {"partition_id": a_id, "fileset": partitions[a_id]},
        {"partition_id": b_id, "fileset": partitions[b_id]},
    ]
    def _count_files(fs):
        return sum(len(v.get("files", [])) for v in fs.values())
    
    print("    [partition_fileset] created partitions:")
    for entry in manifest:
        pid = entry["partition_id"]
        n_ds = len(entry["fileset"])
        n_files = _count_files(entry["fileset"])
        print(f"      - {pid}: datasets={n_ds}, files={n_files}")
        
    with open(out_manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
