# -*- coding: utf-8 -*-

# This script is part of eGon-data.

# license text - to be added.

"""
Central module containing all code downloading hotmaps heat demand data.

The 2050 national heat demand of the Hotmaps current policy scenario for
buildings are used in the eGon100RE scenario for assumptions on national
heating demands in European countries, but not for Germany.
The data are downloaded to be used in the PyPSA-Eur-Sec scenario generator
(forked into open_ego).

"""

import os

from egon.data import subprocess
from egon.data.datasets import Dataset, DatasetSources, DatasetTargets
import egon.data.config


class HeatDemandEurope(Dataset):
    """
    Downloads annual heat demands for European countries from hotmaps

    This dataset downloads annual heat demands for all European countries for the year 2050 from
    hotmaps and stores the results into files. These are later used by pypsa-eur-sec.


    *Dependencies*
      * :py:func:`Setup <egon.data.datasets.database.setup>`

    """

    name: str = "heat-demands-europe"
    version: str = "0.3.0"  
    
    sources = DatasetSources(
        urls={
            "hotmaps_heat_demand": "https://gitlab.com/hotmaps/building-stock/-/raw/master/output_csv/3_indicator/1_Data_for_graphs/part_2_energy_demands/CSV_Actions_Total_energy_demand_by_building_type_in_2050_NUTS0.csv"
        }
    )
    targets = DatasetTargets(
        files={
            "heat_demand_europe": "pypsa-eur/resources/heat_demands_in_2050_NUTS0_hotmaps.csv"
        }
    )

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(download,),
        )


def download():
    """
    Download Hotmaps current policy scenario for building heat demands.

    The downloaded data contain residential and non-residential-sector
    national heat demands for different years.

    Parameters
    ----------
        None

    Returns
    -------
        None

    """

    url = HeatDemandEurope.sources.urls["hotmaps_heat_demand"]
    target_file = HeatDemandEurope.targets.files["heat_demand_europe"]

    os.makedirs(os.path.dirname(target_file), exist_ok=True)

    if not os.path.isfile(target_file):
        subprocess.run(
            f"curl {url} > {target_file}",
            shell=True,
        )
    return None