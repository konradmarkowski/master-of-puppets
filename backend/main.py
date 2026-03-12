import json
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from parser import parse_pipeline, parse_task
from simplifier import simplify_pipeline
from landscape import scan_landscape

app = FastAPI(title="Master of Puppets", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DATA_ROOT = Path("/data")
PIPELINE_EXTENSIONS = {".slp", ".slt"}


def _safe_path(rel_path: str) -> Path:
    """Resolve a relative path inside DATA_ROOT with path-traversal protection."""
    file_path = (DATA_ROOT / rel_path).resolve()
    if not str(file_path).startswith(str(DATA_ROOT.resolve())):
        raise HTTPException(status_code=400, detail="Invalid file path")
    if file_path.suffix not in PIPELINE_EXTENSIONS:
        raise HTTPException(status_code=400, detail="Invalid file type")
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="Pipeline not found")
    return file_path


def _build_tree(root: Path) -> list:
    """Recursively build a folder tree of pipeline/task files."""
    items = []
    if not root.exists():
        return items
    for entry in sorted(root.iterdir(), key=lambda e: (not e.is_dir(), e.name)):
        if entry.name.startswith("."):
            continue
        if entry.is_dir():
            children = _build_tree(entry)
            if children:  # only include folders that contain pipeline files
                items.append({
                    "name": entry.name,
                    "type": "folder",
                    "children": children,
                })
        elif entry.suffix in PIPELINE_EXTENSIONS:
            rel = entry.relative_to(DATA_ROOT)
            items.append({
                "name": entry.name,
                "type": "file",
                "path": str(rel).replace("\\", "/"),
                "ext": entry.suffix[1:],
            })
    return items


@app.get("/api/pipelines")
def list_pipelines():
    """Return a tree of all pipeline/task files under /data."""
    return _build_tree(DATA_ROOT)


@app.get("/api/pipelines/{file_path:path}/raw")
def get_pipeline(file_path: str):
    """Return the raw JSON content of a specific pipeline/task file."""
    safe = _safe_path(file_path)
    with open(safe, encoding="utf-8") as f:
        return json.load(f)


@app.get("/api/pipelines/{file_path:path}/parsed")
def get_pipeline_parsed(file_path: str):
    """Return a structured, parsed representation of a pipeline/task."""
    safe = _safe_path(file_path)
    with open(safe, encoding="utf-8") as f:
        data = json.load(f)

    if safe.suffix == ".slt":
        return parse_task(data)
    return parse_pipeline(data)


@app.get("/api/pipelines/{file_path:path}/simplified")
def get_pipeline_simplified(file_path: str):
    """Return a developer-oriented simplified view of a pipeline."""
    safe = _safe_path(file_path)
    if safe.suffix == ".slt":
        raise HTTPException(status_code=400, detail="Simplified view is only available for pipelines (.slp)")

    with open(safe, encoding="utf-8") as f:
        data = json.load(f)

    parsed = parse_pipeline(data)
    return simplify_pipeline(parsed)


@app.get("/api/landscape")
def get_landscape():
    """Return the cross-pipeline reference graph for all projects."""
    return scan_landscape(DATA_ROOT)
