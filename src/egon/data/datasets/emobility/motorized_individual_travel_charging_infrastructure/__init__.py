from __future__ import annotations

from pathlib import Path
import zipfile

import requests

from egon.data import config, db
from egon.data.datasets import Dataset

WORKING_DIR = Path(".", "charging_infrastructure").resolve()
DATASET_CFG = config.datasets()["charging_infrastructure"]


def create_tables() -> None:
    """
    TODO
    Returns
    -------

    """
    db.engine()


def download_zip(url: str, target: Path, chunk_size: int = 128) -> None:
    r = requests.get(url, stream=True)

    target.parent.mkdir(parents=True, exist_ok=True)

    with open(target, "wb") as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)


def unzip_file(source: Path, target: Path) -> None:
    with zipfile.ZipFile(source, "r") as zip_ref:
        zip_ref.extractall(target)


def get_tracbev_data() -> None:
    tracbev_cfg = DATASET_CFG["original_data"]["sources"]["tracbev"]
    file = WORKING_DIR / tracbev_cfg["file"]

    download_zip(url=tracbev_cfg["url"], target=file)

    unzip_file(source=file, target=WORKING_DIR)


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
