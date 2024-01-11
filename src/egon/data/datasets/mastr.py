"""
Download Marktstammdatenregister (MaStR) datasets unit registry.
It incorporates two different datasets:

Dump 2021-04-30
* Source: https://zenodo.org/records/10480930
* Used technologies: PV plants, wind turbines, biomass, hydro plants,
  combustion, nuclear, gsgk, storage
* Data is further processed in dataset
  :py:class:`egon.data.datasets.power_plants.PowerPlants`

Dump 2022-11-17
* Source: https://zenodo.org/records/10480958
* Used technologies: PV plants, wind turbines, biomass, hydro plants
* Data is further processed in module
  :py:mod:`egon.data.datasets.power_plants.mastr` `PowerPlants`

Todo: Finish docstring
TBD
"""

from functools import partial
from pathlib import Path
from urllib.request import urlretrieve
import os

from egon.data.datasets import Dataset
import egon.data.config

WORKING_DIR_MASTR_OLD = Path(".", "bnetza_mastr", "dump_2021-05-03")
WORKING_DIR_MASTR_NEW = Path(".", "bnetza_mastr", "dump_2022-11-17")


def download_mastr_data():
    """Download MaStR data from Zenodo"""

    def download(dataset_name, download_dir):
        print(f"Downloading dataset {dataset_name} to {download_dir} ...")
        # Get parameters from config and set download URL
        data_config = egon.data.config.datasets()[dataset_name]
        zenodo_files_url = (
            f"https://zenodo.org/record/"
            f"{data_config['deposit_id']}/files/"
        )

        files = []
        for technology in data_config["technologies"]:
            files.append(
                f"{data_config['file_basename']}_{technology}_cleaned.csv"
            )
        files.append("location_elec_generation_raw.csv")

        # Retrieve specified files
        for filename in files:
            if not os.path.isfile(filename):
                urlretrieve(
                    zenodo_files_url + filename, download_dir / filename
                )

    if not os.path.exists(WORKING_DIR_MASTR_OLD):
        WORKING_DIR_MASTR_OLD.mkdir(exist_ok=True, parents=True)
    if not os.path.exists(WORKING_DIR_MASTR_NEW):
        WORKING_DIR_MASTR_NEW.mkdir(exist_ok=True, parents=True)

    download(dataset_name="mastr", download_dir=WORKING_DIR_MASTR_OLD)
    download(dataset_name="mastr_new", download_dir=WORKING_DIR_MASTR_NEW)


mastr_data_setup = partial(
    Dataset,
    name="MastrData",
    version="0.0.1",
    dependencies=[],
    tasks=(download_mastr_data,),
)
