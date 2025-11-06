import time
import requests
import docker
import pytest
from dagster import materialize
import glob

from pipeline.config import DOCKER_PATH, DATA_LAKE_PATH
from pipeline.dagster.resources import spark_session_resource
from pipeline.assets.all_assets_def import defs
import logging


@pytest.mark.integration
def test_integration():
    container = None
    try:
        container = _load_container()
        _is_service_up()
        _assert_assets()

    finally:
        container.stop()
        container.remove()


def _is_service_up():
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
                logging.info(f"Server responded with data: {data}")
                assert isinstance(data, list)
                assert len(data) > 0
                break
        except requests.exceptions.ConnectionError:
            # Server not ready yet
            time.sleep(1)
    else:
        pytest.fail("Server did not respond in time")


def _load_container():
    _assemble_tar()
    client = docker.from_env()
    image_tar_path = str(DOCKER_PATH) + "/upstream-interview-m1.tar"

    with open(image_tar_path, "rb") as f:
        loaded_images = client.images.load(f.read())

    for image in loaded_images:
        logging.info(f"Loaded image: {image.tags}")

    return client.containers.run(
        "upstream-interview", detach=True, ports={"9900/tcp": 9900}
    )


def _assert_assets():
    result = materialize(assets=defs.assets, resources=defs.resources)
    assert result.success
    with spark_session_resource() as spark:
        # todo add general asserts
        logging.info("Bronze")
        spark.read.parquet(str(DATA_LAKE_PATH) + "/Bronze").show()
        logging.info("Silver")
        spark.read.parquet(str(DATA_LAKE_PATH) + "/Silver").show()
        logging.info("Gold: vin_last_state_report")
        spark.read.parquet(str(DATA_LAKE_PATH) + "/Gold/vin_last_state_report").show()
        logging.info("Gold: top_10_fastest_vehicles_per_date_hour_report")
        spark.read.parquet(
            str(DATA_LAKE_PATH) + "/Gold/top_10_fastest_vehicles_per_date_hour_report"
        ).show()
        logging.info("Bonus: sql injection report")
        spark.read.parquet(str(DATA_LAKE_PATH) + "/Bonus/sql_injection_report").show()


def _assemble_tar():
    output_file = str(DOCKER_PATH) + "/upstream-interview-m1.tar"
    part_files = sorted(glob.glob(str(DOCKER_PATH) + "/upstream-interview-m1-part-*"))

    with open(output_file, "wb") as outfile:
        for part in part_files:
            logging.info(f"Merging {part}...")
            with open(part, "rb") as infile:
                outfile.write(infile.read())

    logging.info(f"Combined {len(part_files)} parts into {output_file}")
