"""The central module containing all code dealing with importing VG250 data.

This module either directly contains the code dealing with importing VG250
data, or it re-exports everything needed to handle it. Please refrain
from importing code from any modules below this one, because it might
lead to unwanted behaviour.

If you have to import code from a module below this one because the code
isn't exported from this module, please file a bug, so we can fix this.
"""

from urllib.request import urlretrieve
import os
import egon.data.config


def download_vg250_files():
    """Download VG250 (Verwaltungsgebiete) shape files."""
    data_config = egon.data.config.datasets()
    vg250_config = data_config["vg250"]["original_data"]

    target_file = os.path.join(
        os.path.dirname(__file__), vg250_config["target"]["path"]
    )

    if not os.path.isfile(target_file):
        urlretrieve(vg250_config["source"]["url"], target_file)
