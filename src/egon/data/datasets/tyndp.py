"""The central module containing all code dealing with downloading tyndp data"""

from urllib.request import urlretrieve
import os

from egon.data import config
from egon.data.datasets import Dataset, DatasetSources, DatasetTargets


class Tyndp(Dataset):
    """
    Downloads data for foreign countries from Ten-Year-Network-Developement Plan

    This dataset downloads installed generation capacities and load time series for
    foreign countries from the website of the Ten-Year-Network-Developement Plan 2024 from ENTSO-E.
    That data is stored into files and later on written into the database
    (see :py:class:`ElectricalNeighbours <egon.data.datasets.electrical_neighbours.ElectricalNeighbours>`).


    *Dependencies*
      * :py:class:`Setup <egon.data.datasets.database.Setup>`

    *Resulting tables*

    """

    #:
    name: str = "Tyndp"
    #:
    version: str = "0.0.4"

    sources = DatasetSources(
        files={
            "capacities_2035": "https://2024-data.entsos-tyndp-scenarios.eu/files/scenarios-outputs/DE2035CY2009.zip",
            "capacities_2040": "https://2024-data.entsos-tyndp-scenarios.eu/files/scenarios-outputs/DE2040CY2009.zip",
            "capacities_2050": "https://2024-data.entsos-tyndp-scenarios.eu/files/scenarios-outputs/DE2050CY2009.zip",
            "demand_2030": "https://eepublicdownloads.entsoe.eu/tyndp-documents/2020-data/Demand_TimeSeries_2030_DistributedEnergy.xlsx",
            "demand_2040": "https://eepublicdownloads.entsoe.eu/tyndp-documents/2020-data/Demand_TimeSeries_2040_DistributedEnergy.xlsx",
        }
    )

    targets = DatasetTargets(
        files={
            "capacities_2035": "DE2035CY2009.zip",
            "capacities_2040": "DE2040CY2009.zip",
            "capacities_2050": "DE2050CY2009.zip",
            "demand_2030": "Demand_TimeSeries_2030_DistributedEnergy.xlsx",
            "demand_2040": "Demand_TimeSeries_2040_DistributedEnergy.xlsx",
        }
    )

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(download),
        )


def download():
    """Download input data from TYNDP 2024
    Returns
    -------
    None.
    """

    if not os.path.exists("tyndp"):
        os.mkdir("tyndp")

    for dataset in [
        "capacities_2035",
        "capacities_2040",
        "capacities_2050",
        "demand_2030",
        "demand_2040",
    ]:
        source_url = Tyndp.sources.files[dataset]
        target_file = Tyndp.targets.files[dataset]

        urlretrieve(source_url, f"tyndp/{target_file}")
