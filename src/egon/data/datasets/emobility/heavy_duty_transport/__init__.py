"""
Main module for preparation of model data (static and timeseries) for
heavy duty transport.

**Contents of this module**

* Creation of DB tables
* Download and preprocessing of vehicle registration data from BAST
* Calculation of hydrogen demand based on a Voronoi distribution of counted
  truck traffic among NUTS 3 regions.
* Writing results to DB
* Mapping demand to H2 buses and writing to DB

"""

from pathlib import Path
import csv

from loguru import logger
import requests

from egon.data import config, db
from egon.data.datasets import Dataset, DatasetSources, DatasetTargets
from egon.data.datasets.emobility.heavy_duty_transport.create_h2_buses import (
    insert_hgv_h2_demand,
)
from egon.data.datasets.emobility.heavy_duty_transport.db_classes import (
    EgonHeavyDutyTransportVoronoi,
)
from egon.data.datasets.emobility.heavy_duty_transport.h2_demand_distribution import (  # noqa: E501
    run_egon_truck,
)

WORKING_DIR = Path(".", "heavy_duty_transport").resolve()



def create_tables():
    """
    Drops existing :py:class:`demand.egon_heavy_duty_transport_voronoi <egon.data.datasets.emobility.heavy_duty_transport.db_classes.EgonHeavyDutyTransportVoronoi>` is extended
    table and creates new one.

    """
    engine = db.engine()
    EgonHeavyDutyTransportVoronoi.__table__.drop(bind=engine, checkfirst=True)
    EgonHeavyDutyTransportVoronoi.__table__.create(
        bind=engine, checkfirst=True
    )

    logger.debug("Created tables.")


def download_hgv_data():
    """
    Downloads BAST data.

    The data is downloaded to file specified in *datasets.yml* in section
    *mobility_hgv/original_data/sources/BAST/file*.

    """
    sources = HeavyDutyTransport.sources
    targets = HeavyDutyTransport.targets

    # Create the folder, if it does not exist
    file_path = Path(targets.files["BAST_download"])
    file_path.parent.mkdir(parents=True, exist_ok=True)

    url = sources.urls["BAST"]

    response = requests.get(url)

    with open(file_path, "w") as f:
        writer = csv.writer(f, delimiter=";") 
        for line in response.iter_lines():
            writer.writerow(line.decode("ISO-8859-1").split(";"))

    logger.debug("Downloaded BAST data.")


class HeavyDutyTransport(Dataset):
    """
    Class for preparation of static and timeseries data for heavy duty transport.

    For more information see data documentation on :ref:`mobility-demand-hdt-ref`.

    *Dependencies*
      * :py:class:`Vg250 <egon.data.datasets.vg250.Vg250>`
      * :py:class:`EtragoSetup <egon.data.datasets.etrago_setup.EtragoSetup>`
      * :py:class:`GasAreaseGon2035 <egon.data.datasets.gas_areas.GasAreaseGon2035>`

    *Resulting tables*
      * :py:class:`demand.egon_heavy_duty_transport_voronoi
        <egon.data.datasets.emobility.heavy_duty_transport.db_classes.EgonHeavyDutyTransportVoronoi>`
        is created and filled
      * :py:class:`grid.egon_etrago_load<egon.data.datasets.etrago_setup.EgonPfHvLoad>`
        is extended
      * :py:class:`grid.egon_etrago_load_timeseries
        <egon.data.datasets.etrago_setup.EgonPfHvLoadTimeseries>` is extended

    *Configuration*

    The config of this dataset can be found in *datasets.yml* in section
    *mobility_hgv*.

    """
    sources = DatasetSources(
        urls={
            "BAST": "https://www.bast.de/DE/Themen/Digitales/HF_1/Massnahmen/verkehrszaehlung/Daten/2020_1/Jawe2020.csv?view=renderTcDataExportCSV&strTyp=A"
        },
        tables={
            "vg250_lan": "boundaries.vg250_lan",
            "mv_grid": "grid.egon_mv_grid_district",
        },
        files={
            "original_data": {
                "original_data": {
                    "sources": {
                        "BAST": {
                            "file": "Jawe2020.csv",
                            "relevant_columns": ["DTV_SV_MobisSo_Q", "Koor_WGS84_E", "Koor_WGS84_N"],
                            "srid": 4326
                        }
                    }
                },
                "tables": {
                    "srid": 3035,
                    "srid_buses": 4326
                },
                "constants": {
                    "leakage": True,
                    "leakage_rate": 0.005,
                    "hydrogen_consumption": 6.68,
                    "fcev_share": 1.0,
                    "scenarios": ["eGon2035", "eGon100RE"],
                    "carrier": "H2_hgv_load",
                    "energy_value_h2": 39.4,
                    "hours_per_year": 8760,
                    "fac": 0.001
                },
                "hgv_mileage": {
                    "eGon2035": 10000000000,
                    "eGon100RE": 40000000000
                }
            }
        }
    )

    targets = DatasetTargets(
        files={
            "BAST_download": "heavy_duty_transport/Jawe2020.csv"
        },
        tables={
            "voronoi": "demand.egon_heavy_duty_transport_voronoi",
            "etrago_load": "grid.egon_etrago_load",
            "etrago_load_timeseries": "grid.egon_etrago_load_timeseries",
        },
    )

    
    #:
    name: str = "HeavyDutyTransport"
    #:
    version: str = "0.0.5"

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(
                {
                    create_tables,
                    download_hgv_data,
                },
                run_egon_truck,
                insert_hgv_h2_demand,
            ),
        )
