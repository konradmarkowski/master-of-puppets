import json
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from parser import parse_pipeline, parse_task

app = FastAPI(title="Master of Puppets", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

PIPELINES_DIR = Path("/data/snaplogic-resource/snaplogic-Resource")
PIPELINE_EXTENSIONS = {".slp", ".slt"}


def _safe_path(name: str) -> Path:
    """Resolve a file name inside PIPELINES_DIR with path-traversal protection."""
    file_path = (PIPELINES_DIR / name).resolve()
    if not str(file_path).startswith(str(PIPELINES_DIR.resolve())):
        raise HTTPException(status_code=400, detail="Invalid file name")
    if file_path.suffix not in PIPELINE_EXTENSIONS:
        raise HTTPException(status_code=400, detail="Invalid file type")
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="Pipeline not found")
    return file_path


@app.get("/api/pipelines")
def list_pipelines():
    """Return list of available pipeline/task file names."""
    if not PIPELINES_DIR.exists():
        return []
    return sorted(
        f.name
        for f in PIPELINES_DIR.iterdir()
        if f.is_file() and f.suffix in PIPELINE_EXTENSIONS
    )


@app.get("/api/pipelines/{name}")
def get_pipeline(name: str):
    """Return the raw JSON content of a specific pipeline/task file."""
    file_path = _safe_path(name)
    with open(file_path, encoding="utf-8") as f:
        return json.load(f)


@app.get("/api/pipelines/{name}/parsed")
def get_pipeline_parsed(name: str):
    """Return a structured, parsed representation of a pipeline/task."""
    file_path = _safe_path(name)
    with open(file_path, encoding="utf-8") as f:
        data = json.load(f)

    if file_path.suffix == ".slt":
        return parse_task(data)
    return parse_pipeline(data)
