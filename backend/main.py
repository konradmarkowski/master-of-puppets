import json
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Master of Puppets", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

PIPELINES_DIR = Path("/data/snaplogic-resource/snaplogic-Resource")
PIPELINE_EXTENSIONS = {".slp", ".slt"}


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
    """Return the JSON content of a specific pipeline/task file."""
    file_path = (PIPELINES_DIR / name).resolve()

    # Path traversal protection
    if not str(file_path).startswith(str(PIPELINES_DIR.resolve())):
        raise HTTPException(status_code=400, detail="Invalid file name")

    if file_path.suffix not in PIPELINE_EXTENSIONS:
        raise HTTPException(status_code=400, detail="Invalid file type")

    if not file_path.exists():
        raise HTTPException(status_code=404, detail="Pipeline not found")

    with open(file_path, encoding="utf-8") as f:
        return json.load(f)
