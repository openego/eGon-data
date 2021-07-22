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


class HeatDemandAbroad(Dataset):

    data_config = egon.data.config.datasets()

    hotmapsheatdemands_config = data_config[
        "hotmaps_current_policy_scenario_heat_demands_buildings"][
        "original_data"
    ]

    target_file = hotmapsheatdemands_config["target"]["path"]

    def __init__(self, dependencies):
        super().__init__(
            name="heat-demands-abroad",
            version=self.target_file + "_hotmaps.0.0",
            dependencies=dependencies,
            tasks=(download_hotmaps_scenario_heat_demands))



def download_hotmaps_scenario_heat_demands():
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

    ToDo
    -------
        Check version numbering method. See also class definition!

    Notes
    -----
        (For the version management we can assume that the dataset
        will not change, unless the code is changed.)

    	Create a folder to save the downloaded csv in?

    """

    data_config = egon.data.config.datasets()

    # heat demands
    hotmapsheatdemands_config = data_config[
        "hotmaps_current_policy_scenario_heat_demands_buildings"][
        "original_data"
    ]

    target_file = hotmapsheatdemands_config["target"]["path"]

    if not os.path.isfile(target_file):
        subprocess.run(
            f"curl { hotmapsheatdemands_config['source']['url']} > {target_file}",
            shell=True,
        )
    return None
