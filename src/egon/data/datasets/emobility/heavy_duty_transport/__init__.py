from pathlib import Path
import csv
import zipfile

from loguru import logger
import requests

from egon.data import config, db
from egon.data.datasets import Dataset
from egon.data.datasets.emobility.heavy_duty_transport.db_classes import (
    EgonHeavyDutyTransportVoronoi,
)
from egon.data.datasets.emobility.heavy_duty_transport.h2_demand_distribution import (
    run_egon_truck,
)

WORKING_DIR = Path(".", "heavy_duty_transport").resolve()
DATASET_CFG = config.datasets()["mobility_hgv"]
TESTMODE_OFF = (
    config.settings()["egon-data"]["--dataset-boundary"] == "Everything"
)


def create_tables():
    engine = db.engine()
    EgonHeavyDutyTransportVoronoi.__table__.drop(bind=engine, checkfirst=True)
    EgonHeavyDutyTransportVoronoi.__table__.create(
        bind=engine, checkfirst=True
    )

    logger.debug("Created tables.")


def download_hgv_data():
    sources = DATASET_CFG["original_data"]["sources"]

    # Create the folder, if it does not exist
    if not WORKING_DIR.is_dir():
        WORKING_DIR.mkdir(parents=True)

    url = sources["BAST"]["url"]
    file = WORKING_DIR / sources["BAST"]["file"]

    response = requests.get(url)

    with open(file, "w") as f:
        writer = csv.writer(f)
        for line in response.iter_lines():
            writer.writerow(line.decode("ISO-8859-1").split(";"))

    logger.debug("Downloaded BAST data.")

    if not TESTMODE_OFF:
        url = sources["NUTS"]["url"]

        r = requests.get(url, stream=True)
        file = WORKING_DIR / sources["NUTS"]["file"]

        with open(file, "wb") as fd:
            for chunk in r.iter_content(chunk_size=512):
                fd.write(chunk)

        directory = WORKING_DIR / "_".join(
            sources["BAST"]["file"].split(".")[:-1]
        )

        with zipfile.ZipFile(file, "r") as zip_ref:
            zip_ref.extractall(directory)


class HeavyDutyTransport(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="HeavyDutyTransport",
            version="0.0.1.dev",
            dependencies=dependencies,
            tasks=(
                {
                    create_tables,
                    download_hgv_data,
                },
                run_egon_truck,
            ),
        )
