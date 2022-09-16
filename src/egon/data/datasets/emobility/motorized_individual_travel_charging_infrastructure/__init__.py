from pathlib import Path
import zipfile

import requests

from egon.data import config, db
from egon.data.datasets import Dataset

WORKING_DIR = Path(".", "charging_infrastructure").resolve()
DATASET_CFG = config.datasets()["charging_infrastructure"]


def create_tables():
    """
    TODO
    Returns
    -------

    """
    db.engine()

    return False


def download_zip(url, target, chunk_size=128):
    r = requests.get(url, stream=True)

    target.parent.mkdir(parents=True, exist_ok=True)

    with open(target, "wb") as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)


def unzip_file(source, target):
    with zipfile.ZipFile(source, "r") as zip_ref:
        zip_ref.extractall(target)


def get_tracbev_data():
    tracbev_sources = DATASET_CFG["original_data"]["sources"]["tracbev"]
    file = WORKING_DIR / tracbev_sources["file"]

    download_zip(url=tracbev_sources["url"], target=file)

    directory = WORKING_DIR / file.split(".")[0]

    unzip_file(source=file, target=directory)


class MITChargingInfrastructure(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="MITChargingInfrastructure",
            version="0.0.1.dev",
            dependencies=dependencies,
            tasks=(
                {
                    create_tables,
                    get_tracbev_data,
                },
            ),
        )
