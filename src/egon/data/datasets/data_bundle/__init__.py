"""The central module containing all code dealing with small scale inpu-data
"""


from urllib.request import urlretrieve
import zipfile

import egon.data.config
from egon.data.datasets import Dataset


def download():
    """
    Download small scale imput data from Zenodo
    Parameters
    ----------

    """
    # Get parameters from config and set download URL
    sources = egon.data.config.datasets()["data-bundle"]["sources"]["zenodo"]
    url = f"""https://zenodo.org/record/{sources['deposit_id']}/files/data_bundle_egon_data.zip"""
    target_file = egon.data.config.datasets()["data-bundle"]["targets"]["file"]

    # Retrieve files
    urlretrieve(url, target_file)

    with zipfile.ZipFile(target_file, "r") as zip_ref:
        zip_ref.extractall(".")


class DataBundle(Dataset):
    def __init__(self, dependencies):
        deposit_id = egon.data.config.datasets()["data-bundle"]["sources"][
            "zenodo"
        ]["deposit_id"]
        super().__init__(
            name="DataBundle",
            version=str(deposit_id) + "-0.0.0",
            dependencies=dependencies,
            tasks=(download),
        )
