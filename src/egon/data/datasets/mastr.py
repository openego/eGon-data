from functools import partial
from urllib.request import urlretrieve
import os

from egon.data.datasets import Dataset
import egon.data.config


def download_mastr_data(data_stages=None):
    """
    Download MaStR data from Zenodo

    Parameters
    ----------
    data_stages: list
        Select data stages you want to download data for. Possible values:
        'raw', 'cleaned'. Defaults to 'cleaned' if omitted.
    """
    # Process inputs
    if not data_stages:
        data_stages = ["cleaned"]

    # Get parameters from config and set download URL
    data_config = egon.data.config.datasets()["mastr"]
    zenodo_files_url = (
        f"https://sandbox.zenodo.org/record/{data_config['deposit_id']}/files/"
    )

    files = []
    for technology in data_config["technologies"]:
        # Download raw data
        if "raw" in data_stages:
            files.append(
                f"{data_config['file_basename']}_{technology}_raw.csv"
            )
        # Download cleaned data
        if "cleaned" in data_stages:
            files.append(
                f"{data_config['file_basename']}_{technology}_cleaned.csv"
            )
        files.append("datapackage.json")
        files.append("location_elec_generation_raw.csv")

    # Retrieve specified files
    for filename in files:
        if not os.path.isfile(filename):
            urlretrieve(zenodo_files_url + filename, filename)


mastr_data_setup = partial(
    Dataset,
    name="MastrData",
    version="0.0.0",
    dependencies=[],
    tasks=(download_mastr_data,),
)
