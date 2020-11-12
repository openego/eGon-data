import os

import yaml

import egon


def datasets(config_file=None):
    """Return dataset configuration.

    Parameters
    ----------
    config_file : str
        Path of the dataset configuration file

    Returns
    -------
    dict
        Dataset configuration
    """
    if not config_file:
        package_path = egon.data.__path__[0]
        config_file = os.path.join(package_path, "datasets.yml")

    return yaml.load(open(config_file), Loader=yaml.SafeLoader)
