from pathlib import Path
import os

import yaml

import egon


def paths(pid=None):
    """Obtain configuration file paths.

    If no `pid` is supplied, return the location of the standard
    configuration file. If `pid` is the string `"current"`, the
    path to the configuration file containing the configuration specific
    to the currently running process, i.e. the configuration obtained by
    overriding the values from the standard configuration file with the
    values explicitly supplied when the currently running process was
    invoked, is returned. If `pid` is the string `"*"` a list of all
    configuration belonging to currently running `egon-data` processes
    is returned. This can be used for error checking, because there
    should only ever be one such file.
    """
    pid = os.getpid() if pid == "current" else pid
    insert = f".pid-{pid}" if pid is not None else ""
    filename = f"egon-data{insert}.configuration.yaml"
    if pid == "*":
        return [p.absolute() for p in Path(".").glob(filename)]
    else:
        return [(Path(".") / filename).absolute()]


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
