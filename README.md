# Master of Puppets

SnapLogic pipeline interpreter and automatic documentation generator.

## Quick Start

```bash
docker compose up --build
```

Open **http://localhost** in your browser.

The app reads SnapLogic pipeline files (`.slp`, `.slt`) from the `source/` directory of the sibling `master-of-ipaas` repo.  
To override the source path, set the `SNAPLOGIC_SOURCE_PATH` environment variable:

```bash
SNAPLOGIC_SOURCE_PATH=/path/to/source docker compose up --build
```

## Architecture

| Service    | Tech         | Port |
|------------|------------- |------|
| `frontend` | Nginx + HTML | 80   |
| `backend`  | FastAPI      | 8000 (internal) |

- Frontend proxies `/api/*` requests to the backend via Nginx.
- Backend reads pipeline JSON files from a mounted volume.
