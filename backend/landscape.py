"""Landscape scanner — builds a cross-pipeline reference graph.

Recursively scans all .slp files under the data root, finds Pipeline Execute
snaps, resolves their references, and returns a graph of which pipelines call
which pipelines across all projects.
"""

from __future__ import annotations

import json
from pathlib import Path


def _val(obj, *keys, default=None):
    cur = obj
    for k in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
    if isinstance(cur, dict) and "value" in cur:
        return cur["value"]
    return cur if cur is not None else default


def scan_landscape(data_root: Path) -> dict:
    """Scan all .slp files under *data_root* and build a pipeline reference graph.

    Returns::

        {
            "projects": [ { "id": ..., "path": ..., "pipelineCount": ... } ],
            "nodes":    [ { "id": ..., "name": ..., "project": ..., "type": ..., "filePath": ... } ],
            "edges":    [ { "source": ..., "target": ..., "label": ..., "params": [...] } ],
            "externalRefs": [ { "source": ..., "rawRef": ... } ]
        }
    """
    if not data_root.exists():
        return {"projects": [], "nodes": [], "edges": [], "externalRefs": []}

    # 1. Discover all .slp and .slt files
    slp_files: list[Path] = sorted(data_root.rglob("*.slp"))
    slt_files: list[Path] = sorted(data_root.rglob("*.slt"))

    # 2. Index pipelines by name and by project
    #    project = first two path components under data_root  (e.g. "snaplogic-resource/snaplogic-Resource")
    pipeline_index: dict[str, dict] = {}  # stem -> node info
    project_set: dict[str, int] = {}  # project_path -> count

    for f in slp_files:
        rel = f.relative_to(data_root)
        project_path = str(rel.parent).replace("\\", "/")
        stem = f.stem

        project_set[project_path] = project_set.get(project_path, 0) + 1

        node_id = f"{project_path}/{stem}"
        pipeline_index[node_id] = {
            "id": node_id,
            "name": stem,
            "project": project_path,
            "type": "pipeline",
            "filePath": str(rel).replace("\\", "/"),
        }

    # Also register tasks
    for f in slt_files:
        rel = f.relative_to(data_root)
        project_path = str(rel.parent).replace("\\", "/")
        stem = f.stem
        node_id = f"{project_path}/{stem}"
        if node_id not in pipeline_index:
            project_set[project_path] = project_set.get(project_path, 0) + 1
            pipeline_index[node_id] = {
                "id": node_id,
                "name": stem,
                "project": project_path,
                "type": "task",
                "filePath": str(rel).replace("\\", "/"),
            }

    # 3. Build edges by parsing Pipeline Execute snaps and task -> pipeline refs
    edges: list[dict] = []
    external_refs: list[dict] = []

    for f in slp_files:
        rel = f.relative_to(data_root)
        project_path = str(rel.parent).replace("\\", "/")
        source_id = f"{project_path}/{f.stem}"

        try:
            with open(f, encoding="utf-8") as fh:
                data = json.load(fh)
        except (json.JSONDecodeError, OSError):
            continue

        snap_map = data.get("snap_map", {})
        for snap in snap_map.values():
            class_id = snap.get("class_id", "")
            if class_id != "com-snaplogic-snaps-flow-pipeexec":
                continue

            settings = snap.get("property_map", {}).get("settings", {})
            pipeline_ref = _val(settings, "pipeline")
            if not pipeline_ref:
                continue

            label = _val(snap.get("property_map", {}).get("info", {}), "label") or ""

            # Extract params for the edge
            params_raw = _val(settings, "params")
            params = []
            if isinstance(params_raw, list):
                params = [
                    {"name": _val(p, "paramName"), "value": _val(p, "paramValue")}
                    for p in params_raw
                ]

            # Resolve the reference
            target_id = _resolve_pipeline_ref(pipeline_ref, project_path, pipeline_index)

            if target_id:
                edges.append({
                    "source": source_id,
                    "target": target_id,
                    "label": label,
                    "rawRef": pipeline_ref,
                    "params": params,
                })
            else:
                # External / unresolved reference — create a virtual node
                virtual_id = _create_virtual_node(pipeline_ref, project_path, pipeline_index)
                edges.append({
                    "source": source_id,
                    "target": virtual_id,
                    "label": label,
                    "rawRef": pipeline_ref,
                    "params": params,
                })
                external_refs.append({
                    "source": source_id,
                    "rawRef": pipeline_ref,
                    "resolvedAs": virtual_id,
                })

    # Task -> pipeline references
    for f in slt_files:
        rel = f.relative_to(data_root)
        project_path = str(rel.parent).replace("\\", "/")
        source_id = f"{project_path}/{f.stem}"

        try:
            with open(f, encoding="utf-8") as fh:
                data = json.load(fh)
        except (json.JSONDecodeError, OSError):
            continue

        pipeline_path = data.get("parameters", {}).get("pipeline_path", "")
        if not pipeline_path:
            continue

        target_id = _resolve_pipeline_ref(pipeline_path, project_path, pipeline_index)
        if target_id:
            edges.append({
                "source": source_id,
                "target": target_id,
                "label": f"Task → {pipeline_path}",
                "rawRef": pipeline_path,
                "params": [],
            })
        else:
            virtual_id = _create_virtual_node(pipeline_path, project_path, pipeline_index)
            edges.append({
                "source": source_id,
                "target": virtual_id,
                "label": f"Task → {pipeline_path}",
                "rawRef": pipeline_path,
                "params": [],
            })

    # 4. Build project list
    projects = [
        {"id": p, "path": p, "pipelineCount": c}
        for p, c in sorted(project_set.items())
    ]

    return {
        "projects": projects,
        "nodes": list(pipeline_index.values()),
        "edges": edges,
        "externalRefs": external_refs,
    }


def _resolve_pipeline_ref(
    ref: str,
    current_project: str,
    index: dict[str, dict],
) -> str | None:
    """Try to resolve a pipeline reference to a known node id.

    References can be:
    - Simple name: "HLD-RE-019_PCD_ExcessiveUsage_Child"
    - Relative path: "../../shared/SHD_WriteS3CustomLog"
    """
    # 1. Try same-directory match (just a name)
    candidate = f"{current_project}/{ref}"
    if candidate in index:
        return candidate

    # 2. Try resolving relative path against current_project
    parts = current_project.split("/")
    ref_parts = ref.replace("\\", "/").split("/")

    # Walk up for ".." components
    base = list(parts)
    remaining = list(ref_parts)
    while remaining and remaining[0] == "..":
        remaining.pop(0)
        if base:
            base.pop()

    resolved_project = "/".join(base + remaining[:-1]) if len(remaining) > 1 else "/".join(base)
    resolved_name = remaining[-1] if remaining else ref
    resolved_id = f"{resolved_project}/{resolved_name}"

    if resolved_id in index:
        return resolved_id

    # 3. Try fuzzy match — search by pipeline name across all projects
    for node_id, node in index.items():
        if node["name"] == ref or node["name"] == resolved_name:
            return node_id

    return None


def _create_virtual_node(
    ref: str,
    current_project: str,
    index: dict[str, dict],
) -> str:
    """Create a virtual (external) node for an unresolved reference."""
    # Derive a reasonable name
    ref_clean = ref.replace("\\", "/")
    name = ref_clean.split("/")[-1] if "/" in ref_clean else ref_clean

    # Determine project hint from relative path
    parts = current_project.split("/")
    ref_parts = ref_clean.split("/")
    base = list(parts)
    remaining = list(ref_parts)
    while remaining and remaining[0] == "..":
        remaining.pop(0)
        if base:
            base.pop()

    project = "/".join(base + remaining[:-1]) if len(remaining) > 1 else "external"

    virtual_id = f"{project}/{name}"
    if virtual_id not in index:
        index[virtual_id] = {
            "id": virtual_id,
            "name": name,
            "project": project,
            "type": "external",
            "filePath": None,
        }
    return virtual_id
