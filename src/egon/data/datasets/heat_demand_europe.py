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

import egon.data.config
from egon.data import subprocess
from egon.data.datasets import Dataset
import os


class HeatDemandEurope(Dataset):
    """
    Downloads annual heat demands for European countries from hotmaps

    This dataset downloads annual heat demands for all European countries for the year 2050 from
    hotmaps and stores the results into files. These are later used by pypsa-eur-sec.


    *Dependencies*
      * :py:class:`Setup <egon.data.datasets.database.Setup>`

    """

    #:
    name: str = "heat-demands-europe"
    #:
    version: str = (
        egon.data.config.datasets()[
            "hotmaps_current_policy_scenario_heat_demands_buildings"
        ]["targets"]["path"]
        + "_hotmaps.0.1"
    )

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(download),
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

    data_config = egon.data.config.datasets()

    # heat demands
    hotmapsheatdemands_config = data_config[
        "hotmaps_current_policy_scenario_heat_demands_buildings"
    ]

    target_file = hotmapsheatdemands_config["targets"]["path"]

    if not os.path.isfile(target_file):
        subprocess.run(
            f"curl { hotmapsheatdemands_config['sources']['url']} > {target_file}",
            shell=True,
        )
    return None
