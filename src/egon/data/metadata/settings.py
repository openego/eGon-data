from pathlib import Path

# The oemetadata version identifier must comply with on of:
# https://github.com/OpenEnergyPlatform/omi/blob/7e620182b0ea8eb2e437bd650b5cda3db0532307/src/omi/base.py#L17 # noqa E501
OEMETADATA_VERSION = "OEMetadata-2.0"

# Path to the base directory where the dataset metadata YAML-files are stored
# to be used by the OMI tool to generate oemetadata python dict objects or
# json files. One OEMetadata dataset per DAG task group; the task-group ->
# dataset-id mapping lives in dataset_metadata/dataset_ids.yaml (it replaced
# the former single OEM_DATASET_ID constant).
OEM_BASE_DIR = Path(__file__).parent / "dataset_metadata"
# Where to write generated oemetadata JSON file (used to describe tabular
# datapackages as used in frictionless and oemof datapackages)
OEM_OUT_DIR = Path("workdir/oemetadata/")


EGON_ATTRIBUTION: str = "© eGon development team"
REGON_ATTRIBUTION: str = "© ReGon development team"
