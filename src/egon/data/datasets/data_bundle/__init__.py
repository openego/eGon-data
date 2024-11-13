"""The central module containing all code dealing with small scale input-data
"""


from pathlib import Path
from urllib.request import urlretrieve
import shutil
import zipfile

from egon.data import config
from egon.data.datasets import Dataset


def download():
    """
    Download small scale input data from Zenodo
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

    # check if file exists
    if not Path(target_file).exists():
        # Retrieve files
        urlretrieve(url, target_file)

    with zipfile.ZipFile(target_file, "r") as zip_ref:
        zip_ref.extractall(".")

    powerd_data_bundle_path = Path(".") / "data_bundle_powerd_data"
    # Delete folder if it already exists
    if powerd_data_bundle_path.exists() and powerd_data_bundle_path.is_dir():
        shutil.rmtree(powerd_data_bundle_path)

    url = f"""https://zenodo.org/record/{sources['deposit_id_powerd']}/files/data_bundle_powerd_data.zip"""
    target_file = egon.data.config.datasets()["data-bundle"]["targets"]["file_powerd"]

    # check if file exists
    if not Path(target_file).exists():
        # Retrieve files
        urlretrieve(url, target_file)

    with zipfile.ZipFile(target_file, "r") as zip_ref:
        zip_ref.extractall(".")


class DataBundle(Dataset):
    def __init__(self, dependencies):
        deposit_id = egon.data.config.datasets()["data-bundle"]["sources"][
            "zenodo"
        ]["deposit_id"]
        deposit_id_powerd = egon.data.config.datasets()["data-bundle"]["sources"][
            "zenodo"
        ]["deposit_id"]
        super().__init__(
            name="DataBundle",
            version=f"{deposit_id}-{deposit_id_powerd}-0.0.3",
            dependencies=dependencies,
            tasks=(download,),
        )
