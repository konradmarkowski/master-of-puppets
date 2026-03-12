"""SnapLogic pipeline / task JSON parser.

Extracts structured information from .slp (pipeline) and .slt (task) files
so the frontend can render an interactive graph.
"""

from __future__ import annotations

# Well-known snap class_id -> human-friendly category & short type name
SNAP_CATEGORIES: dict[str, tuple[str, str]] = {
    # Transform
    "com-snaplogic-snaps-transform-jsongenerator": ("Transform", "JSON Generator"),
    "com-snaplogic-snaps-transform-datatransform": ("Transform", "Mapper"),
    "com-snaplogic-snaps-transform-conditional": ("Transform", "Conditional"),
    "com-snaplogic-snaps-transform-xmlgenerator": ("Transform", "XML Generator"),
    "com-snaplogic-snaps-transform-jsonformatter": ("Transform", "JSON Formatter"),
    "com-snaplogic-snaps-transform-xmlformatter": ("Transform", "XML Formatter"),
    "com-snaplogic-snaps-transform-xmlparser": ("Transform", "XML Parser"),
    "com-snaplogic-snaps-transform-jsonparser": ("Transform", "JSON Parser"),
    "com-snaplogic-snaps-transform-csvformatter": ("Transform", "CSV Formatter"),
    "com-snaplogic-snaps-transform-csvparser": ("Transform", "CSV Parser"),
    "com-snaplogic-snaps-transform-jsonpath": ("Transform", "JSON Path"),
    "com-snaplogic-snaps-transform-base64decode": ("Transform", "Base64 Decode"),
    "com-snaplogic-snaps-transform-base64encode": ("Transform", "Base64 Encode"),
    "com-snaplogic-snaps-transform-typeconverter": ("Transform", "Type Converter"),
    "com-snaplogic-snaps-transform-aggregate": ("Transform", "Aggregate"),
    "com-snaplogic-snaps-transform-groupbyfields": ("Transform", "Group By Fields"),
    "com-snaplogic-snaps-transform-groupbyn": ("Transform", "Group By N"),
    "com-snaplogic-snaps-transform-unstructured2structured": ("Transform", "Unstructured to Structured"),

    # Flow
    "com-snaplogic-snaps-flow-copy": ("Flow", "Copy"),
    "com-snaplogic-snaps-flow-pipeexec": ("Flow", "Pipeline Execute"),
    "com-snaplogic-snaps-flow-datavalidator": ("Flow", "Data Validator"),
    "com-snaplogic-snaps-flow-union": ("Flow", "Union"),
    "com-snaplogic-snaps-flow-router": ("Flow", "Router"),
    "com-snaplogic-snaps-flow-gate": ("Flow", "Gate"),
    "com-snaplogic-snaps-flow-filter": ("Flow", "Filter"),
    "com-snaplogic-snaps-flow-sorter": ("Flow", "Sorter"),
    "com-snaplogic-snaps-flow-head": ("Flow", "Head"),
    "com-snaplogic-snaps-flow-tail": ("Flow", "Tail"),
    "com-snaplogic-snaps-flow-sequence": ("Flow", "Sequence"),
    "com-snaplogic-snaps-flow-crossjoin": ("Flow", "Cross Join"),
    "com-snaplogic-snaps-flow-join": ("Flow", "Join"),
    "com-snaplogic-snaps-flow-except": ("Flow", "Except"),
    "com-snaplogic-snaps-flow-intersect": ("Flow", "Intersect"),

    # Read / Write
    "com-snaplogic-snaps-binary-read": ("Read", "File Reader"),
    "com-snaplogic-snaps-binary-write": ("Write", "File Writer"),
    "com-snaplogic-snaps-binary-directorybrowse": ("Read", "Directory Browser"),
    "com-snaplogic-snaps-binary-filedelete": ("Write", "File Delete"),
    "com-snaplogic-snaps-binary-filemove": ("Write", "File Move"),
    "com-snaplogic-snaps-binary-filecopy": ("Write", "File Copy"),

    # REST / HTTP
    "com-snaplogic-snaps-rest-get": ("REST", "REST GET"),
    "com-snaplogic-snaps-rest-post": ("REST", "REST POST"),
    "com-snaplogic-snaps-rest-put": ("REST", "REST PUT"),
    "com-snaplogic-snaps-rest-patch": ("REST", "REST PATCH"),
    "com-snaplogic-snaps-rest-delete": ("REST", "REST DELETE"),
    "com-snaplogic-snaps-rest-head": ("REST", "REST HEAD"),

    # DynamoDB
    "com-snaplogic-snaps-dynamodb-query": ("Database", "DynamoDB Query"),
    "com-snaplogic-snaps-dynamodb-put": ("Database", "DynamoDB Put"),
    "com-snaplogic-snaps-dynamodb-get": ("Database", "DynamoDB Get"),
    "com-snaplogic-snaps-dynamodb-delete": ("Database", "DynamoDB Delete"),
    "com-snaplogic-snaps-dynamodb-scan": ("Database", "DynamoDB Scan"),

    # S3
    "com-snaplogic-snaps-s3-read": ("Cloud", "S3 Read"),
    "com-snaplogic-snaps-s3-write": ("Cloud", "S3 Write"),

    # Script
    "com-snaplogic-snaps-script-script": ("Script", "Script"),
    "com-snaplogic-snaps-script-python": ("Script", "Python Script"),
}


def _val(obj: dict | None, *keys: str, default=None):
    """Safely traverse nested dict following .value convention."""
    cur = obj
    for k in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
    if isinstance(cur, dict) and "value" in cur:
        return cur["value"]
    return cur if cur is not None else default


def _classify_snap(class_id: str) -> tuple[str, str]:
    if class_id in SNAP_CATEGORIES:
        return SNAP_CATEGORIES[class_id]
    # Fallback: derive from class_id
    parts = class_id.replace("com-snaplogic-snaps-", "").split("-")
    category = parts[0].capitalize() if parts else "Unknown"
    short = " ".join(p.capitalize() for p in parts) if parts else class_id
    return category, short


def _extract_settings_summary(snap: dict) -> dict:
    """Extract the most interesting configuration from snap settings."""
    settings = snap.get("property_map", {}).get("settings", {})
    summary: dict = {}

    # Transformations / mappings
    transformations = _val(settings, "transformations")
    if isinstance(transformations, dict):
        table = _val(transformations, "mappingTable")
        if isinstance(table, list):
            summary["mappings"] = [
                {
                    "expression": _val(m, "expression"),
                    "target": _val(m, "targetPath"),
                }
                for m in table
            ]

    # Conditional table
    cond_table = _val(settings, "conditionalTable")
    if isinstance(cond_table, list):
        summary["conditions"] = [
            {
                "if": _val(c, "conditionalExpression"),
                "then": _val(c, "returnValue"),
                "target": _val(c, "targetPath"),
            }
            for c in cond_table
        ]

    # Pipeline Execute
    pipeline_ref = _val(settings, "pipeline")
    if pipeline_ref:
        summary["pipeline"] = pipeline_ref
    params = _val(settings, "params")
    if isinstance(params, list):
        summary["params"] = [
            {"name": _val(p, "paramName"), "value": _val(p, "paramValue")}
            for p in params
        ]

    # REST / HTTP
    for key in ("url", "URL", "ServiceURL"):
        url = _val(settings, key)
        if url:
            summary["url"] = url
            break

    http_method = _val(settings, "HttpMethod")
    if http_method:
        summary["httpMethod"] = http_method

    # DynamoDB
    table_name = _val(settings, "TableName")
    if table_name:
        summary["tableName"] = table_name
    key_cond = _val(settings, "KeyConditionExpression")
    if key_cond:
        summary["keyCondition"] = key_cond

    # Validation constraints
    constraints = _val(settings, "constraintMappings")
    if isinstance(constraints, list):
        summary["constraints"] = [
            {
                "path": _val(c, "sourcePath"),
                "constraint": _val(c, "constraint"),
                "value": _val(c, "constraintValue"),
            }
            for c in constraints
        ]

    # JSON generator content
    content = _val(settings, "editable_content")
    if content:
        summary["content"] = content

    # Error behavior
    error_section = snap.get("property_map", {}).get("error", {})
    error_behavior = _val(error_section, "error_behavior")
    if error_behavior:
        summary["errorBehavior"] = error_behavior

    # Account reference
    account = snap.get("property_map", {}).get("account", {})
    account_ref = account.get("account_ref", {})
    if isinstance(account_ref, dict):
        ref_val = account_ref.get("value", {})
        if isinstance(ref_val, dict):
            label = ref_val.get("label", {})
            if isinstance(label, dict):
                acct_label = label.get("label") or label.get("value")
                if acct_label:
                    summary["account"] = acct_label

    # Pass-through
    pass_through = _val(settings, "passThrough")
    if pass_through is not None:
        summary["passThrough"] = pass_through

    return summary


def _extract_views(section: dict) -> list[str]:
    """Extract view names from input/output section."""
    return [k for k in section if k not in ("error_behavior",)]


def parse_pipeline(data: dict) -> dict:
    """Parse a .slp pipeline JSON into a structured description."""
    snap_map = data.get("snap_map", {})
    link_map = data.get("link_map", {})
    render_map = data.get("render_map", {})
    detail_map = render_map.get("detail_map", {})
    prop_map = data.get("property_map", {})

    # -- Pipeline metadata --
    info = prop_map.get("info", {})
    metadata = {
        "author": _val(info, "author"),
        "notes": _val(info, "notes"),
        "purpose": _val(info, "purpose"),
    }

    # Pipeline parameters
    params_raw = _val(prop_map.get("settings", {}), "param_table")
    parameters = []
    if isinstance(params_raw, list):
        parameters = [
            {
                "key": _val(p, "key"),
                "type": _val(p, "data_type"),
                "required": _val(p, "required"),
                "description": _val(p, "description"),
                "default": _val(p, "value"),
            }
            for p in params_raw
        ]

    # -- Nodes (snaps) --
    nodes = []
    for snap_id, snap in snap_map.items():
        class_id = snap.get("class_id", "")
        category, snap_type = _classify_snap(class_id)
        pm = snap.get("property_map", {})
        label = _val(pm.get("info", {}), "label") or snap_id
        notes = _val(pm.get("info", {}), "notes")

        # Grid position from render_map
        pos = detail_map.get(snap_id, {})
        grid_x = pos.get("grid_x_int", 0)
        grid_y = pos.get("grid_y_int", 0)

        # Input/output views
        inputs = _extract_views(pm.get("input", {}))
        outputs = _extract_views(pm.get("output", {}))
        error_views = _extract_views(pm.get("error", {}))

        settings_summary = _extract_settings_summary(snap)

        nodes.append(
            {
                "id": snap_id,
                "label": label,
                "classId": class_id,
                "category": category,
                "type": snap_type,
                "notes": notes,
                "gridX": grid_x,
                "gridY": grid_y,
                "inputs": inputs,
                "outputs": outputs,
                "errorViews": [v for v in error_views if v.startswith("error")],
                "settings": settings_summary,
            }
        )

    # -- Edges (links) --
    edges = []
    for link_id, link in link_map.items():
        is_error = link.get("src_view_id", "").startswith("error")
        edges.append(
            {
                "id": link_id,
                "source": link["src_id"],
                "sourceView": link.get("src_view_id", ""),
                "target": link["dst_id"],
                "targetView": link.get("dst_view_id", ""),
                "isError": is_error,
            }
        )

    return {
        "type": "pipeline",
        "metadata": metadata,
        "parameters": parameters,
        "nodes": nodes,
        "edges": edges,
    }


def parse_task(data: dict) -> dict:
    """Parse a .slt task JSON into a structured description."""
    return {
        "type": "task",
        "jobName": data.get("job_name"),
        "jobClass": data.get("job_class"),
        "note": data.get("note"),
        "parameters": data.get("parameters", {}),
        "schedule": data.get("schedule"),
        "trigger": data.get("trigger"),
        "triggerType": data.get("trigger_type"),
    }
