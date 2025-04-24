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
from egon.data.datasets import Dataset
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
DATASET_CFG = config.datasets()["mobility_hgv"]
TESTMODE_OFF = (
    config.settings()["egon-data"]["--dataset-boundary"] == "Everything"
)


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
    sources = DATASET_CFG["original_data"]["sources"]

    # Create the folder, if it does not exist
    WORKING_DIR.mkdir(parents=True, exist_ok=True)

    url = sources["BAST"]["url"]
    file = WORKING_DIR / sources["BAST"]["file"]

    response = requests.get(url)

    with open(file, "w") as f:
        writer = csv.writer(f)
        for line in response.iter_lines():
            writer.writerow(line.decode("ISO-8859-1").split(";"))

    logger.debug("Downloaded BAST data.")


class HeavyDutyTransport(Dataset):
    """
    Class for preparation of static and timeseries data for heavy duty transport.

    *Dependencies*
      * :py:class:`Vg250 <egon.data.datasets.vg250.Vg250>`
      * :py:class:`EtragoSetup <egon.data.datasets.etrago_setup.EtragoSetup>`
      * :py:class:`GasAreaseGon2035 <egon.data.datasets.gas_areas.GasAreaseGon2035>`
      * :py:class:`GasAreaseGon2037_2025
      <egon.data.datasets.gas_areas.GasAreaseGon2037_2025>`

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

    *Scenarios and variations*

    Assumptions can be changed within the *datasets.yml*.

    In the context of the eGon project, it is assumed that e-trucks will be
    completely hydrogen-powered and in both scenarios the hydrogen consumption is
    assumed to be 6.68 kg H2 per 100 km with an additional
    `supply chain leakage rate of 0.5 %
    <https://www.energy.gov/eere/fuelcells/doe-technical-targets-hydrogen-delivery>`_.

    ### Scenario NEP C 2037
    [needs to be updated]

    The ramp-up figures are taken from
    `Scenario C 2037 Grid Development Plan 2025-2037
    <https://www.netzentwicklungsplan.de/sites/default/files/paragraphs-files/
    NEP_2035_V2021_2_Entwurf_Teil1.pdf>`_. According to this, 100,000 e-trucks are
    expected in Germany in 2035, each covering an average of 100,000 km per year.
    In total this means 10 billion km.

    ### Scenario NEP C 2035

    The ramp-up figures are taken from
    `Scenario C 2035 Grid Development Plan 2021-2035
    <https://www.netzentwicklungsplan.de/sites/default/files/paragraphs-files/
    NEP_2035_V2021_2_Entwurf_Teil1.pdf>`_. According to this, 100,000 e-trucks are
    expected in Germany in 2035, each covering an average of 100,000 km per year.
    In total this means 10 billion km.

    ### Scenario eGon100RE

    In the case of the eGon100RE scenario it is assumed that the HGV traffic is
    completely hydrogen-powered. The total freight traffic with 40 Billion km is
    taken from the
    `BMWK Langfristszenarien GHG-emission free scenarios (SNF > 12 t zGG)
    <https://www.langfristszenarien.de/enertile-explorer-wAssets/docs/
    LFS3_Langbericht_Verkehr_final.pdf#page=17>`_.

    *Methodology*

    Using a Voronoi interpolation, the censuses of the BASt data is distributed
    according to the area fractions of the Voronoi fields within each mv grid or
    any other geometries like NUTS-3.

    """
    #:
    name: str = "HeavyDutyTransport"
    #:
    version: str = "0.0.2"
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
