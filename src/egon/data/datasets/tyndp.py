"""The central module containing all code dealing with downloading tyndp data
"""

import os
from egon.data import config
from egon.data.datasets import Dataset, DatasetSources, DatasetTargets
from urllib.request import urlretrieve


class Tyndp(Dataset):
    """
    Downloads data for foreign countries from Ten-Year-Network-Developement Plan

    This dataset downloads installed generation capacities and load time series for
    foreign countries from the website of the Ten-Year-Network-Developement Plan 2020 from ENTSO-E.
    That data is stored into files and later on written into the database
    (see :py:class:`ElectricalNeighbours <egon.data.datasets.electrical_neighbours.ElectricalNeighbours>`).


    *Dependencies*
      * :py:class:`Setup <egon.data.datasets.database.Setup>`

    *Resulting tables*

    """

    #:
    name: str = "Tyndp"
    #:
    version: str = "0.0.1"
    
    sources = DatasetSources(
        files={
            "capacities": "https://2020.entsos-tyndp-scenarios.eu/wp-content/uploads/2020/06/TYNDP-2020-Scenario-Datafile.xlsx.zip",
            "demand_2030": "https://eepublicdownloads.entsoe.eu/tyndp-documents/2020-data/Demand_TimeSeries_2030_DistributedEnergy.xlsx",
            "demand_2040": "https://eepublicdownloads.entsoe.eu/tyndp-documents/2020-data/Demand_TimeSeries_2040_DistributedEnergy.xlsx",
       }
    )

    targets = DatasetTargets(
        files={
            "capacities": "TYNDP-2020-Scenario-Datafile.xlsx.zip",
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
    """Download input data from TYNDP 2020
    Returns
    -------
    None.
    """
    sources = config.datasets()["tyndp"]["sources"]
    targets = config.datasets()["tyndp"]["targets"]

    if not os.path.exists("tyndp"):
        os.mkdir("tyndp")

    for dataset in ["capacities", "demand_2030", "demand_2040"]:
        target_file = targets[dataset]

        urlretrieve(sources[dataset], f"tyndp/{target_file}")
