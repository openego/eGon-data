"""
Motorized Individual Travel (MIT) Charging Infrastructure

Main module for preparation of static model data for cahrging infrastructure for
motorized individual travel.

**Contents of this module**
* Creation of DB tables
* Download and preprocessing of vehicle registration data from zenodo
* Determination of all potential charging locations for the four charging use cases
  home, work, public and hpc per mv grid district
* Write results to DB

**Configuration**

The config of this dataset can be found in *datasets.yml* in section
*charging_infrastructure*.

**Charging Infrastructure**

The charging infrastructure allocation is based on [TracBEV[(
https://github.com/rl-institut/tracbev). TracBEV is a tool for the regional allocation
of charging infrastructure. In practice this allows users to use results generated via
[SimBEV](https://github.com/rl-institut/simbev) and place the corresponding charging
points on a map. These are split into the four use cases home, work, public and hpc.
"""
from __future__ import annotations

from pathlib import Path
import zipfile

from loguru import logger
import requests

from egon.data import config, db
from egon.data.datasets import Dataset
from egon.data.datasets.emobility.motorized_individual_travel_charging_infrastructure.db_classes import (  # noqa: E501
    EgonEmobChargingInfrastructure,
)
from egon.data.datasets.emobility.motorized_individual_travel_charging_infrastructure.infrastructure_allocation import (  # noqa: E501
    run_tracbev,
)

WORKING_DIR = Path(".", "charging_infrastructure").resolve()
DATASET_CFG = config.datasets()["charging_infrastructure"]


def create_tables() -> None:
    """
    Create tables for charging infrastructure

    Returns
    -------
    None
    """
    engine = db.engine()
    EgonEmobChargingInfrastructure.__table__.drop(bind=engine, checkfirst=True)
    EgonEmobChargingInfrastructure.__table__.create(
        bind=engine, checkfirst=True
    )

    logger.debug("Created tables.")


def download_zip(url: str, target: Path, chunk_size: int | None = 128) -> None:
    """
    Download zip file from URL.

    Parameters
    ----------
    url : str
        URL to download the zip file from
    target : pathlib.Path
        Directory to save zip to
    chunk_size: int or None
        Size of chunks to download

    """
    r = requests.get(url, stream=True)

    target.parent.mkdir(parents=True, exist_ok=True)

    with open(target, "wb") as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)


def unzip_file(source: Path, target: Path) -> None:
    """
    Unzip zip file

    Parameters
    ----------
    source: Path
        Zip file path to unzip
    target: Path
        Directory to save unzipped content to

    """
    with zipfile.ZipFile(source, "r") as zip_ref:
        zip_ref.extractall(target)


def get_tracbev_data() -> None:
    """
    Wrapper function to get TracBEV data provided on Zenodo.
    """
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
                run_tracbev,
            ),
        )
