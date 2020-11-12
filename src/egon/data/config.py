import os

import yaml

import egon


def datasets(config_file=None):
    """Return dataset configuration.

    Parameters
    ----------
    config_file : str, optional
        Path of the dataset configuration file in YAML format. If not
        supplied, a default configuration shipped with this package is
        used.

    Returns
    -------
    dict
        A nested dictionary containing the configuration as parsed from
        the supplied file, or the default configuration if no file was
        given.

    """
    if not config_file:
        package_path = egon.data.__path__[0]
        config_file = os.path.join(package_path, "datasets.yml")

    return yaml.load(open(config_file), Loader=yaml.SafeLoader)
