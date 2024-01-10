"""
Download Marktstammdatenregister (MaStR) from Zenodo.

"""

from pathlib import Path
from urllib.request import urlretrieve
import os

from egon.data.datasets import Dataset
import egon.data.config

WORKING_DIR_MASTR_OLD = Path(".", "bnetza_mastr", "dump_2021-05-03")
WORKING_DIR_MASTR_NEW = Path(".", "bnetza_mastr", "dump_2022-11-17")


def download_mastr_data():
    """Download MaStR data from Zenodo."""

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

    if not os.path.exists(WORKING_DIR_MASTR_OLD):
        WORKING_DIR_MASTR_OLD.mkdir(exist_ok=True, parents=True)
    if not os.path.exists(WORKING_DIR_MASTR_NEW):
        WORKING_DIR_MASTR_NEW.mkdir(exist_ok=True, parents=True)

    download(dataset_name="mastr", download_dir=WORKING_DIR_MASTR_OLD)
    download(dataset_name="mastr_new", download_dir=WORKING_DIR_MASTR_NEW)


class mastr_data_setup(Dataset):
    """
    Download Marktstammdatenregister (MaStR) from Zenodo.

    *Dependencies*
      * :py:func:`Setup <egon.data.datasets.database.setup>`

    The downloaded data incorporates two different datasets:

    Dump 2021-05-03
      * Source: https://sandbox.zenodo.org/record/808086
      * Used technologies: PV plants, wind turbines, biomass, hydro plants,
        combustion, nuclear, gsgk, storage
      * Data is further processed in the :py:class:`PowerPlants
        <egon.data.datasets.power_plants.PowerPlants>` dataset

    Dump 2022-11-17
      * Source: https://sandbox.zenodo.org/record/1132839
      * Used technologies: PV plants, wind turbines, biomass, hydro plants
      * Data is further processed in module :py:mod:`mastr
        <egon.data.datasets.power_plants.mastr>` and :py:class:`PowerPlants
        <egon.data.datasets.power_plants.PowerPlants>`

    See documentation section :ref:`mastr-ref` for more information.

    """

    #:
    name: str = "MastrData"
    #:
    version: str = "0.0.2"
    #:
    tasks = (download_mastr_data,)

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=self.tasks,
        )
