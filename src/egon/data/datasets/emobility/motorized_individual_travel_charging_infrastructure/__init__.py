"""
Motorized Individual Travel (MIT) Charging Infrastructure

Main module for preparation of static model data for charging infrastructure for
motorized individual travel.

Legacy methodology (`eGon2035`) only. The charging locations of the new
methodology are delivered with the vehicles and their events and imported
by the MIT dataset itself, cf.
:mod:`egon.data.datasets.emobility.motorized_individual_travel.charging_locations`.

"""

from __future__ import annotations

from pathlib import Path
import zipfile

from loguru import logger
import requests

from egon.data import config, db
from egon.data.datasets import Dataset, DatasetSources, DatasetTargets
from egon.data.datasets.emobility.mit_lgv_input_data import legacy_scenarios
from egon.data.datasets.emobility.motorized_individual_travel_charging_infrastructure.db_classes import (  # noqa: E501
    EgonEmobChargingInfrastructure,
)
from egon.data.datasets.emobility.motorized_individual_travel_charging_infrastructure.infrastructure_allocation import (  # noqa: E501
    run_tracbev,
)

WORKING_DIR = Path(".", "charging_infrastructure").resolve()


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
    r = requests.get(
        url,
        stream=True,
        headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64)"},
    )

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

    Legacy methodology only: scenarios on the new methodology take their
    charging sites from the delivered M1+N1 input data, cf.
    :func:`.charging_location_import.import_charging_locations`.
    """
    if not legacy_scenarios(config.settings()["egon-data"]["--scenarios"]):
        logger.info(
            "No scenario on the legacy methodology configured, skipping "
            "the TracBEV download."
        )
        return

    file = Path(MITChargingInfrastructure.targets.files["tracbev_download"])
    url = MITChargingInfrastructure.sources.urls["tracbev"]

    download_zip(url=url, target=file)

    unzip_file(source=file, target=WORKING_DIR)


class MITChargingInfrastructure(Dataset):

    sources = DatasetSources(
        urls={
            "tracbev": "https://zenodo.org/record/6466480/files/data.zip?download=1",
        },
        tables={
            "mv_grid_districts": "grid.egon_mv_grid_district",
            "buildings": "demand.egon_map_houseprofiles_buildings",
        },
        files={
            "tracbev_parameters": {
                "tracbev_config": {
                    "srid": 3035,
                    "files_to_use": [
                        "hpc_positions.gpkg",
                        "landuse.gpkg",
                        "poi_cluster.gpkg",
                        "public_positions.gpkg",
                    ],
                },
                "work_weight_retail": 0.8,
                "work_weight_commercial": 1.25,
                "work_weight_industrial": 1,
                "single_family_home_share": 0.6,
                "single_family_home_spots": 1.5,
                "multi_family_home_share": 0.4,
                "multi_family_home_spots": 10,
                "random_seed": 5,
                "cols_to_export": [
                    "mv_grid_id",
                    "use_case",
                    "weight",
                    "geometry",
                ],
            }
        },
    )

    targets = DatasetTargets(
        files={"tracbev_download": "charging_infrastructure/data.zip"},
        tables={
            "charging_infrastructure": (
                "grid.egon_emob_charging_infrastructure"
            ),
        },
    )

    """
    Preparation of static model data for charging infrastructure for
    motorized individual travel.

    The following is done:

    * Creation of DB tables
    * Download and preprocessing of vehicle registration data from zenodo
    * Determination of all potential charging locations for the four charging use cases
      home, work, public and hpc per MV grid district
    * Write results to DB

    For more information see data documentation on :ref:`mobility-demand-mit-ref`.

    *Dependencies*
      * :py:class:`MvGridDistricts <egon.data.datasets.mv_grid_districts.mv_grid_districts_setup>`
      * :py:func:`map_houseprofiles_to_buildings <egon.data.datasets.electricity_demand_timeseries.hh_buildings.map_houseprofiles_to_buildings>`

    *Resulting tables*
      * :py:class:`grid.egon_emob_charging_infrastructure
        <egon.data.datasets.emobility.motorized_individual_travel_charging_infrastructure.db_classes.EgonEmobChargingInfrastructure>`
        is created and filled (legacy methodology only)

    *Configuration*

    Sources and targets are declared as class attributes below.

    *Charging Infrastructure*

    This dataset serves the **legacy** methodology only. For scenarios on
    the new one the charging locations are part of the delivered M1+N1
    input data and are imported, together with their placements, by
    :py:class:`MotorizedIndividualTravel
    <egon.data.datasets.emobility.motorized_individual_travel.MotorizedIndividualTravel>`.

    For the legacy methodology the allocation is based on
    `TracBEV <https://github.com/rl-institut/tracbev>`_. TracBEV is a tool for the
    regional allocation of charging infrastructure. In practice this allows users to
    use results generated via `SimBEV <https://github.com/rl-institut/simbev>`_ and
    place the corresponding charging
    points on a map. These are split into the four use cases home, work, public and hpc.

    """

    #:
    name: str = "MITChargingInfrastructure"
    #:
    version: str = "0.2.0"

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(
                {create_tables, get_tracbev_data},
                run_tracbev,
            ),
        )
