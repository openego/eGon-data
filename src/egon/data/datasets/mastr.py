"""
Download Marktstammdatenregister (MaStR) datasets unit registry.
It incorporates two different datasets:

Dump 2021-05-03
* Source: https://sandbox.zenodo.org/record/808086
* Used technologies: PV plants, wind turbines, biomass, hydro plants,
  combustion, nuclear, gsgk, storage
* Data is further processed in dataset
  :py:class:`egon.data.datasets.power_plants.PowerPlants`

Dump 2022-11-17
* Source: https://sandbox.zenodo.org/record/1132839
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

WORKING_DIR = Path(".", "bnetza_mastr")


def download_mastr_data():
    """Download MaStR data from Zenodo"""

    def download(dataset_name, download_dir):
        print(f"Downloading dataset {dataset_name} to {download_dir} ...")
        # Get parameters from config and set download URL
        data_config = egon.data.config.datasets()[dataset_name]
        zenodo_files_url = (
            f"https://sandbox.zenodo.org/record/"
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

    dump_dir_old = WORKING_DIR / "dump_2021-05-03"
    dump_dir_new = WORKING_DIR / "dump_2022-11-17"

    if not os.path.exists(dump_dir_old):
        dump_dir_old.mkdir(exist_ok=True, parents=True)
    if not os.path.exists(dump_dir_new):
        dump_dir_new.mkdir(exist_ok=True, parents=True)

    download(dataset_name="mastr", download_dir=dump_dir_old)
    download(dataset_name="mastr_new", download_dir=dump_dir_new)


mastr_data_setup = partial(
    Dataset,
    name="MastrData",
    version="0.0.1",
    dependencies=[],
    tasks=(download_mastr_data,),
)
