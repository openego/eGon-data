"""The central module containing all code dealing with small scale inpu-data
"""


from pathlib import Path
from urllib.request import urlretrieve
import shutil
import zipfile

from egon.data import config
from egon.data.datasets import Dataset


def download():
    """
    Download small scale imput data from Zenodo
    Parameters
    ----------

    """
    data_bundle_path = Path(".") / "data_bundle_egon_data"
    # Delete folder if it already exists
    if data_bundle_path.exists() and data_bundle_path.is_dir():
        shutil.rmtree(data_bundle_path)
    # Get parameters from config and set download URL
    sources = config.datasets()["data-bundle"]["sources"]["zenodo"]
    url = (
        f"https://zenodo.org/record/{sources['deposit_id']}/files/"
        "data_bundle_egon_data.zip"
    )
    target_file = config.datasets()["data-bundle"]["targets"]["file"]

    # Retrieve files
    urlretrieve(url, target_file)

    with zipfile.ZipFile(target_file, "r") as zip_ref:
        zip_ref.extractall(".")


class DataBundle(Dataset):
    def __init__(self, dependencies):
        deposit_id = config.datasets()["data-bundle"]["sources"]["zenodo"][
            "deposit_id"
        ]
        super().__init__(
            name="DataBundle",
            version=f"{deposit_id}-0.0.1",
            dependencies=dependencies,
            tasks=(download,),
        )
