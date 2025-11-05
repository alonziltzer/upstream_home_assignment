import time
import requests
import docker
import pytest
from dagster import materialize

from pipeline.dagster.resources import spark_session_resource
from pipeline.assets.all_assets_def import defs


@pytest.mark.integration
def test_integration():
    container = _load_container()
    try:
        is_service_up()
        _create_assets()

    finally:
        container.stop()
        container.remove()


def is_service_up():
    # Poll the server until the /upstream/vehicle_messages endpoint responds
    timeout_seconds = 30
    start = time.time()
    while time.time() - start < timeout_seconds:
        try:
            response = requests.get(
                "http://localhost:9900/upstream/vehicle_messages?amount=1"
            )
            if response.status_code == 200:
                data = response.json()
                print(f"Server responded with data: {data}")
                assert isinstance(data, list)
                assert len(data) > 0
                break
        except requests.exceptions.ConnectionError:
            # Server not ready yet
            time.sleep(1)
    else:
        pytest.fail("Server did not respond in time")


def _load_container():
    client = docker.from_env()
    image_tar_path = (
        "/Users/aziltzer/projects/upstream_home_assignment/"
        "pipeline/docker/upstream-interview-m1.tar"
    )

    # Load the Docker image
    with open(image_tar_path, "rb") as f:
        loaded_images = client.images.load(f.read())

    for image in loaded_images:
        print(f"Loaded image: {image.tags}")

    # Run the container
    return client.containers.run(
        "upstream-interview", detach=True, ports={"9900/tcp": 9900}
    )


def _create_assets():
    result = materialize(assets=defs.assets, resources=defs.resources)
    assert result.success
    with spark_session_resource() as spark:
        spark.read.parquet(
            "/Users/aziltzer/projects/upstream_home_assignment/data_lake/Bronze"
        ).show()
        spark.read.parquet(
            "/Users/aziltzer/projects/upstream_home_assignment/data_lake/Silver"
        ).show()
        spark.read.parquet(
            "/Users/aziltzer/projects/upstream_home_assignment/"
            "data_lake/Gold_vin_last_state_report"
        ).show()
        spark.read.parquet(
            "/Users/aziltzer/projects/upstream_home_assignment/"
            "data_lake/gold_top_10_fastest_vehicles_per_date_hour_report"
        ).show()
