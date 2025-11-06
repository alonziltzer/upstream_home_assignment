from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_LAKE_PATH = PROJECT_ROOT / "data_lake"
DOCKER_PATH = PROJECT_ROOT / "pipeline" / "docker"
