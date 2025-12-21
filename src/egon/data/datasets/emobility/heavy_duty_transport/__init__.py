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

from egon.data import db
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


    # Create the folder, if it does not exist
    WORKING_DIR.mkdir(parents=True, exist_ok=True)

    url = HeavyDutyTransport.sources.urls["BAST"]
    file = Path(HeavyDutyTransport.targets.files["BAST_download"])

    response = requests.get(url)

    with open(file, "w") as f:
        writer = csv.writer(f)
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
            "BAST": "https://www.bast.de/DE/Verkehrstechnik/Fachthemen/v2-verkehrszaehlung/Daten/2020_1/Jawe2020.csv?view=renderTcDataExportCSV&cms_strTyp=A"
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
        }
    )
    
    srid: int = 3035

    srid_buses: int = 4326

    bast_srid: int = 4326

    bast_relevant_columns: list = [
    "DTV_SV_MobisSo_Q", 
    "Koor_WGS84_E", 
    "Koor_WGS84_N"
]
    
    carrier: str = "H2_hgv_load"
    
    scenarios_list: list = ["eGon2035", "eGon100RE"]
    
    energy_value_h2: float = 39.4
    
    hours_per_year: int = 8760
    
    fac: float = 0.001
    
    hgv_mileage: dict = {"eGon2035": 88700000000, "eGon100RE": 88700000000}
    leakage: bool = True
    leakage_rate: float = 0.015
    hydrogen_consumption: float = 9.0
    fcev_share: float = 1.0
    
    #:
    name: str = "HeavyDutyTransport"
    #:
    version: str = "0.0.8"

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