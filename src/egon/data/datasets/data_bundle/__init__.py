"""The central module containing all code dealing with small scale input-data"""

from pathlib import Path
from urllib.request import urlretrieve
import shutil
import zipfile

from egon.data import config, logger
from egon.data.datasets import Dataset, DatasetSources, DatasetTargets


def download():
    """
    Download small scale input data from Zenodo
    Parameters
    ----------

    """
    data_bundle_path = Path(".") / "data_bundle_egon_data"

    # Local development escape hatch: re-extracting the bundle costs tens of
    # GB of disk churn and the contents never change between runs. Set
    #
    #   --skip-data-bundle-extraction: true
    #
    # in egon-data.configuration.yaml to reuse an already extracted bundle.
    # Absent from the config (the default) the dataset behaves as before.
    if config.settings()["egon-data"].get(
        "--skip-data-bundle-extraction", False
    ):
        if data_bundle_path.exists() and data_bundle_path.is_dir():
            logger.info(
                f"Reusing existing data bundle at {data_bundle_path} "
                f"because '--skip-data-bundle-extraction' is set."
            )
            return
        logger.warning(
            f"'--skip-data-bundle-extraction' is set but "
            f"{data_bundle_path} does not exist. Extracting anyway."
        )

    # Delete folder if it already exists
    if data_bundle_path.exists() and data_bundle_path.is_dir():
        shutil.rmtree(data_bundle_path)
    # Get parameters from config and set download URL

    url = DataBundle.sources.urls["zenodo_data_bundle"]["url"]
    target_file = DataBundle.targets.files["data_bundle"]

    # check if file exists
    if not Path(target_file).exists():
        # Retrieve files
        urlretrieve(url, target_file)

    with zipfile.ZipFile(target_file, "r") as zip_ref:
        zip_ref.extractall(".")


class DataBundle(Dataset):

    sources = DatasetSources(
        urls={
            "zenodo_data_bundle": {
                "url": "https://zenodo.org/record/16576506/files/data_bundle_egon_data.zip"
            }
        }
    )

    targets = DatasetTargets(
        files={"data_bundle": "data_bundle_egon_data.zip"}
    )

    def __init__(self, dependencies):

        super().__init__(
            name="DataBundle",
            version="0.0.3",
            dependencies=dependencies,
            tasks=(download,),
        )
