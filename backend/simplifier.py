"""Simplify a parsed SnapLogic pipeline into developer-oriented logical steps.

Collapses SnapLogic-specific plumbing (error mappers, unions for error aggregation,
copy-for-logging, pass-through filters, S3 logging helpers) into higher-level
operations that a Python developer would actually need to implement.
"""

from __future__ import annotations

# ── Labels / snap-types that represent "plumbing" rather than business logic ──
_ERROR_PREFIXES = ("e1.", "e2.", "e3.", "e4.", "e5.", "e6.", "e7.", "e8.", "e9.")

# Snap types that are pure SnapLogic plumbing when used in error/logging paths
_PLUMBING_TYPES = {"Union", "Copy", "Filter"}

# Labels that indicate S3 message-logging side-effects
_S3_LOG_LABELS = ("write message to s3", "write s3", "map s3 response", "copy to s3")

# Common "config bootstrap" pattern: JSON Generator -> Mapper (org) -> Conditional (env) -> DynamoDB Query -> Mapper (config) -> Mapper (restore)
_CONFIG_SNAP_KEYWORDS = (
    "process starter", "map in org", "conditional", "dynamodb query",
    "map config", "restore original",
)


def _is_error_handler(node: dict) -> bool:
    label = (node.get("label") or "").lower().strip()
    return label.startswith(_ERROR_PREFIXES) or "error" in label


def _is_s3_logging(node: dict) -> bool:
    label = (node.get("label") or "").lower().strip()
    return any(kw in label for kw in _S3_LOG_LABELS)


def _is_config_bootstrap(node: dict) -> bool:
    label = (node.get("label") or "").lower().strip()
    return any(kw in label for kw in _CONFIG_SNAP_KEYWORDS)


def _is_plumbing(node: dict) -> bool:
    """Nodes that exist only because of SnapLogic's document-flow model."""
    if _is_error_handler(node):
        return True
    if _is_s3_logging(node):
        return True
    typ = node.get("type", "")
    label = (node.get("label") or "").lower()
    # Copy snaps used for logging forks
    if typ == "Copy" and ("s3" in label or "log" in label):
        return True
    # Filter snaps named "pass errors through"
    if typ == "Filter" and "pass" in label and "error" in label:
        return True
    return False


def _describe_snap(node: dict) -> dict:
    """Generate a developer-friendly description of what this snap does."""
    s = node.get("settings", {})
    typ = node.get("type", "")
    label = node.get("label", "")
    notes = node.get("notes") or ""

    desc = {
        "title": label,
        "snapType": typ,
        "category": node.get("category", ""),
        "summary": "",
        "details": [],
        "pseudocode": "",
    }

    if typ == "JSON Generator":
        content = s.get("content", "")
        desc["summary"] = "Generate initial data document"
        desc["details"].append(f"Creates a JSON document to start the pipeline flow.")
        if content:
            desc["details"].append(f"Template: {content}")
        desc["pseudocode"] = f"data = {content or '{}'}"

    elif typ == "Mapper":
        mappings = s.get("mappings", [])
        desc["summary"] = notes or f"Transform/map data ({len(mappings)} field{'s' if len(mappings) != 1 else ''})"
        desc["details"].append("Transforms the data document by mapping expressions to target fields.")
        for m in mappings:
            desc["details"].append(f"  {m.get('target', '?')} = {m.get('expression', '?')}")
        if mappings:
            lines = [f"  result['{m.get('target','')}'] = {m.get('expression','')}" for m in mappings]
            desc["pseudocode"] = "result = {}\n" + "\n".join(lines)

    elif typ == "Conditional":
        conditions = s.get("conditions", [])
        desc["summary"] = notes or f"Route data based on {len(conditions)} condition{'s' if len(conditions) != 1 else ''}"
        desc["details"].append("Evaluates conditions and sets target fields accordingly.")
        branches = []
        for i, c in enumerate(conditions):
            kw = "if" if i == 0 else "elif"
            branches.append(f"{kw} {c.get('if', '?')}:\n    {c.get('target', '?')} = {c.get('then', '?')}")
        desc["pseudocode"] = "\n".join(branches)
        for c in conditions:
            desc["details"].append(f"  if {c.get('if', '?')} → {c.get('target', '?')} = {c.get('then', '?')}")

    elif typ == "DynamoDB Query":
        table = s.get("tableName", "?")
        key_cond = s.get("keyCondition", "")
        desc["summary"] = notes or f"Query DynamoDB table"
        desc["details"].append(f"Table: {table}")
        if key_cond:
            desc["details"].append(f"Key condition: {key_cond}")
        acct = s.get("account", "")
        if acct:
            desc["details"].append(f"Account/connection: {acct}")
        desc["pseudocode"] = f"config = dynamodb.query(\n  table={table},\n  condition='{key_cond}'\n)"

    elif typ in ("DynamoDB Put", "DynamoDB Get", "DynamoDB Delete", "DynamoDB Scan"):
        table = s.get("tableName", "?")
        desc["summary"] = notes or f"{typ} on DynamoDB"
        desc["details"].append(f"Table: {table}")
        desc["pseudocode"] = f"dynamodb.{typ.split()[-1].lower()}(table={table})"

    elif typ == "Pipeline Execute":
        pipeline = s.get("pipeline", "?")
        params = s.get("params", [])
        desc["summary"] = notes or f"Execute child pipeline: {pipeline}"
        desc["details"].append(f"Calls pipeline: {pipeline}")
        for p in params:
            desc["details"].append(f"  param {p.get('name', '?')} = {p.get('value', '?')}")
        param_str = ", ".join(f"{p.get('name', '?')}={p.get('value', '?')}" for p in params)
        desc["pseudocode"] = f"result = execute_pipeline(\n  '{pipeline}',\n  {param_str}\n)"

    elif typ == "Data Validator":
        constraints = s.get("constraints", [])
        desc["summary"] = notes or f"Validate data ({len(constraints)} constraint{'s' if len(constraints) != 1 else ''})"
        desc["details"].append("Validates input data against defined constraints.")
        checks = []
        for c in constraints:
            desc["details"].append(f"  {c.get('path', '?')}: {c.get('constraint', '?')} = {c.get('value', '?')}")
            checks.append(f"assert {c.get('constraint', 'check')}(data['{c.get('path', '')}'], {c.get('value', '')})")
        desc["pseudocode"] = "\n".join(checks) if checks else "validate(data)"

    elif typ == "Copy":
        desc["summary"] = notes or "Duplicate document stream (fan-out)"
        desc["details"].append("Copies the input document to multiple output streams.")
        desc["pseudocode"] = "stream_a, stream_b = copy(data)"

    elif typ == "Router":
        desc["summary"] = notes or "Route documents to different paths based on conditions"
        desc["details"].append("Routes each document to the matching output based on expressions.")
        desc["pseudocode"] = "if condition:\n  route_a(data)\nelse:\n  route_b(data)"

    elif typ == "Union":
        desc["summary"] = notes or "Merge multiple document streams into one"
        desc["pseudocode"] = "merged = stream_a + stream_b"

    elif typ == "Filter":
        desc["summary"] = notes or "Filter documents based on expression"
        desc["pseudocode"] = "filtered = [d for d in data if expression(d)]"

    elif typ in ("REST GET", "REST POST", "REST PUT", "REST PATCH", "REST DELETE"):
        url = s.get("url", "?")
        method = typ.split()[-1]
        desc["summary"] = notes or f"HTTP {method} request"
        desc["details"].append(f"URL: {url}")
        desc["pseudocode"] = f"response = requests.{method.lower()}('{url}')"

    elif typ in ("File Reader", "File Writer", "File Delete", "File Move", "File Copy"):
        desc["summary"] = notes or typ
        desc["pseudocode"] = f"# {typ}"

    elif typ in ("XML Parser", "JSON Parser", "CSV Parser"):
        desc["summary"] = notes or f"Parse {typ.split()[0]} data"
        desc["pseudocode"] = f"parsed = {typ.split()[0].lower()}.parse(data)"

    elif typ in ("XML Generator", "JSON Formatter", "XML Formatter", "CSV Formatter"):
        desc["summary"] = notes or f"Format data as {typ.split()[0]}"
        desc["pseudocode"] = f"output = {typ.split()[0].lower()}.format(data)"

    else:
        desc["summary"] = notes or f"{typ} operation"
        desc["pseudocode"] = f"# {typ}: {label}"

    return desc


def simplify_pipeline(parsed: dict) -> dict:
    """
    Take a parsed pipeline and produce a simplified developer view.

    Returns:
      - steps: ordered list of logical steps (business-relevant only)  
      - errorHandling: summary of error handling strategy
      - configBootstrap: description of config loading (collapsed from multiple snaps)
      - logging: description of logging/S3 patterns
      - metadata: pipeline metadata
    """
    nodes = parsed.get("nodes", [])
    edges = parsed.get("edges", [])
    metadata = parsed.get("metadata", {})
    parameters = parsed.get("parameters", [])

    # Build adjacency from edges (non-error only for main flow ordering)
    successors: dict[str, list[str]] = {}
    predecessors: dict[str, list[str]] = {}
    for e in edges:
        if not e.get("isError"):
            successors.setdefault(e["source"], []).append(e["target"])
            predecessors.setdefault(e["target"], []).append(e["source"])

    node_by_id = {n["id"]: n for n in nodes}

    # Classify nodes
    config_nodes = []
    plumbing_nodes = []
    business_nodes = []
    error_nodes = []
    logging_nodes = []

    for n in nodes:
        if _is_error_handler(n):
            error_nodes.append(n)
        elif _is_config_bootstrap(n):
            config_nodes.append(n)
        elif _is_s3_logging(n):
            logging_nodes.append(n)
        elif _is_plumbing(n):
            plumbing_nodes.append(n)
        else:
            business_nodes.append(n)

    # Sort business nodes by grid position (left-to-right, top-to-bottom)
    business_nodes.sort(key=lambda n: (n.get("gridX", 0), n.get("gridY", 0)))

    # Generate developer steps
    steps = []
    step_num = 1

    # Config bootstrap as single collapsed step
    if config_nodes:
        config_nodes.sort(key=lambda n: (n.get("gridX", 0), n.get("gridY", 0)))
        config_details = []
        config_pseudo = []
        has_dynamo = False
        has_conditional = False
        for cn in config_nodes:
            d = _describe_snap(cn)
            if cn["type"] == "DynamoDB Query":
                has_dynamo = True
                config_details.extend(d["details"])
                config_pseudo.append(d["pseudocode"])
            elif cn["type"] == "Conditional":
                has_conditional = True
                config_details.append("Environment mapping: org → env code")
                config_pseudo.append(d["pseudocode"])
            elif "restore" in cn.get("label", "").lower():
                config_pseudo.append("data = {**original_data, **config}")

        steps.append({
            "stepNumber": step_num,
            "title": "Load Configuration",
            "summary": "Initialize pipeline, resolve environment, and load configuration from DynamoDB config store.",
            "icon": "config",
            "details": [
                "Extract organisation from pipeline execution path.",
                "Map organisation to environment code (dev/sit/uat/pre/prd).",
            ] + config_details + [
                "Merge configuration into the data document."
            ],
            "pseudocode": "org = pipeline.project_path.split('/')[1]\n"
                + ("\n".join(config_pseudo) if config_pseudo else "config = load_config(org)"),
            "collapsedSnaps": [cn["label"] for cn in config_nodes],
        })
        step_num += 1

    # Business logic steps
    for n in business_nodes:
        d = _describe_snap(n)
        steps.append({
            "stepNumber": step_num,
            "title": d["title"],
            "summary": d["summary"],
            "icon": _icon_for_category(n.get("category", ""), n.get("type", "")),
            "details": d["details"],
            "pseudocode": d["pseudocode"],
            "snapType": d["snapType"],
            "snapCategory": d["category"],
            "originalSnapId": n["id"],
        })
        step_num += 1

    # Error handling summary
    error_summary = _build_error_summary(error_nodes, plumbing_nodes)

    # Logging summary
    logging_summary = _build_logging_summary(logging_nodes)

    return {
        "type": "simplified",
        "metadata": metadata,
        "parameters": parameters,
        "steps": steps,
        "errorHandling": error_summary,
        "logging": logging_summary,
        "stats": {
            "totalSnaps": len(nodes),
            "businessSteps": len(business_nodes),
            "configSnaps": len(config_nodes),
            "errorHandlingSnaps": len(error_nodes),
            "loggingSnaps": len(logging_nodes),
            "plumbingSnaps": len(plumbing_nodes),
        },
    }


def _icon_for_category(category: str, snap_type: str) -> str:
    if "Pipeline Execute" in snap_type:
        return "subprocess"
    icons = {
        "Transform": "transform",
        "Flow": "flow",
        "Database": "database",
        "REST": "http",
        "Read": "read",
        "Write": "write",
        "Cloud": "cloud",
        "Script": "script",
    }
    return icons.get(category, "step")


def _build_error_summary(error_nodes: list, plumbing_nodes: list) -> dict:
    strategies = []
    for n in error_nodes:
        label = n.get("label", "")
        notes = n.get("notes") or ""
        s = n.get("settings", {})
        if "validation" in label.lower():
            strategies.append({
                "type": "Validation Error",
                "description": notes or "Handles data validation failures",
                "snapLabel": label,
            })
        elif "child" in label.lower():
            strategies.append({
                "type": "Child Pipeline Error",
                "description": notes or "Handles errors from child pipeline execution",
                "snapLabel": label,
            })
        elif "dynamo" in label.lower() or "query" in label.lower():
            strategies.append({
                "type": "Database Error",
                "description": notes or "Handles DynamoDB query failures",
                "snapLabel": label,
            })
        elif "webex" in label.lower():
            strategies.append({
                "type": "Notification",
                "description": notes or "Sends error notification (e.g. Webex)",
                "snapLabel": label,
            })
        else:
            strategies.append({
                "type": "Error Handler",
                "description": notes or f"Error handling: {label}",
                "snapLabel": label,
            })

    summary = "Multiple error paths converge via Union snaps and are handled centrally." if len(error_nodes) > 2 else "Basic error handling."

    return {
        "summary": summary,
        "strategies": strategies,
        "recommendation": (
            "Implement try/except blocks around each major operation. "
            "Collect errors into a list and handle them at the end (log, notify, return error response). "
            "Consider a central error handler function."
        ),
    }


def _build_logging_summary(logging_nodes: list) -> dict:
    has_request_log = any("request" in (n.get("label") or "").lower() or "copy to s3" in (n.get("label") or "").lower() for n in logging_nodes)
    has_response_log = any("response" in (n.get("label") or "").lower() or "map s3" in (n.get("label") or "").lower() for n in logging_nodes)

    return {
        "summary": f"Pipeline uses S3-based message logging ({len(logging_nodes)} logging snap{'s' if len(logging_nodes) != 1 else ''}).",
        "logsRequest": has_request_log,
        "logsResponse": has_response_log,
        "recommendation": (
            "Implement structured logging (e.g. Python logging module or cloud logging service). "
            "Log request/response payloads at INFO level. Use correlation IDs (traceId) for traceability."
        ),
    }
