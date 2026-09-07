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
    version: str = "0.0.6"

    sources = DatasetSources(
        files={
            "capacities_2035": "https://2024-data.entsos-tyndp-scenarios.eu/files/scenarios-outputs/DE2035CY2009.zip",
            "capacities_2040": "https://2024-data.entsos-tyndp-scenarios.eu/files/scenarios-outputs/DE2040CY2009.zip",
            "capacities_2050": "https://2024-data.entsos-tyndp-scenarios.eu/files/scenarios-outputs/DE2050CY2009.zip",
            # TYNDP 2020 capacities file, kept only for gas_neighbours'
            # gas-sector code, which has not yet been migrated to TYNDP
            # 2024 and still reads this file directly.
            "capacities_2020_gas_legacy": "https://2020.entsos-tyndp-scenarios.eu/wp-content/uploads/2020/06/TYNDP-2020-Scenario-Datafile.xlsx.zip",
            "demand": "https://2024-data.entsos-tyndp-scenarios.eu/files/scenarios-inputs/Demand-Profiles.zip",
        }
    )

    targets = DatasetTargets(
        files={
            "capacities_2035": "DE2035CY2009.zip",
            "capacities_2040": "DE2040CY2009.zip",
            "capacities_2050": "DE2050CY2009.zip",
            # Filename expected by gas_neighbours (grid.egon_data.datasets
            # .gas_neighbours.eGon2035), see comment on the source above.
            "capacities_2020_gas_legacy": "TYNDP-2020-Scenario-Datafile.xlsx.zip",
            "demand": "Demand-Profiles.zip",
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
    """Download input data from TYNDP 2024, plus the legacy TYNDP 2020
    capacities file still required by the not-yet-migrated gas_neighbours
    module.

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
        "capacities_2020_gas_legacy",
        "demand",
    ]:
        source_url = Tyndp.sources.files[dataset]
        target_file = Tyndp.targets.files[dataset]

        urlretrieve(source_url, f"tyndp/{target_file}")
