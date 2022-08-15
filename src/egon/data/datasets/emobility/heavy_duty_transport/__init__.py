from pathlib import Path
import csv

from loguru import logger
import requests

from egon.data import config, db
from egon.data.datasets import Dataset
from egon.data.datasets.emobility.heavy_duty_transport.db_classes import (
    EgonHeavyDutyTransportVoronoi,
)

WORKING_DIR = Path(".", "heavy_duty_transport").resolve()
DATASET_CFG = config.datasets()["mobility_hgv"]


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
    file = sources["BAST"]["file"]

    response = requests.get(url)

    with open(file, "w") as f:
        writer = csv.writer(f)
        for line in response.iter_lines():
            writer.writerow(line.decode("ISO-8859-1").split(";"))

    logger.debug("Downloaded BAST data.")


class HeavyDutyTransport(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="HeavyDutyTransport",
            version="0.0.1.dev",
            dependencies=dependencies,
            tasks=(
                create_tables,
                download_hgv_data,
            ),
        )
