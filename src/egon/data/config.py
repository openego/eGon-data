import os

import yaml

import egon


def datasets(config_file=None):
    """
    Return data set configuration

    Parameters
    ----------
    config_file : str
        Path to the data set configuration file

    Returns
    -------
    dict
        Data set configuration
    """

    if not config_file:
        package_path = egon.data.__path__[0]
        config_file = os.path.join(package_path, "data_sets.yml")

    return yaml.load(open(config_file), Loader=yaml.SafeLoader)
