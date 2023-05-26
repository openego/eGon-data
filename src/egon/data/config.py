from __future__ import annotations

from pathlib import Path
import os
import sys

import yaml

from egon.data import logger
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


# TODO: Add a command for this, so it's easy to double check the
#       configuration.
def settings() -> dict[str, dict[str, str]]:
    """Return a nested dictionary containing the configuration settings.

    It's a nested dictionary because the top level has command names as keys
    and dictionaries as values where the second level dictionary has command
    line switches applicable to the command as keys and the supplied values
    as values.

    So you would obtain the ``--database-name`` configuration setting used
    by the current invocation of of ``egon-data`` via

    .. code-block:: python

        settings()["egon-data"]["--database-name"]

    """
    files = paths(pid="*") + paths()
    if not files[0].exists():
        logger.warning(
            f"Configuration file:"
            f"\n\n{files[0]}\n\nnot found.\nUsing defaults."
        )
        return {
            "egon-data": {
                "--airflow-database-name": "airflow",
                "--airflow-port": 8080,
                "--compose-project-name": "egon-data",
                "--database-host": "127.0.0.1",
                "--database-name": "egon-data",
                "--database-password": "data",
                "--database-port": "59734",
                "--database-user": "egon",
                "--dataset-boundary": "Everything",
                "--docker-container-name":
                    "egon-data-local-database-container",
                "--jobs": 1,
                "--random-seed": 42,
                "--processes-per-task": 1,
                "--scenarios": ["status2019", "eGon2035"],
            }
        }
    with open(files[0]) as f:
        return yaml.safe_load(f)


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

def set_numexpr_threads():
    """Sets maximum threads used by NumExpr

    Returns
    -------
    None

    """
    # Read maximum number of threads per task from egon-data.configuration.yaml
    num_processes = settings()["egon-data"]["--processes-per-task"]

    os.environ['NUMEXPR_MAX_THREADS'] = str(num_processes)
    os.environ['NUMEXPR_NUM_THREADS'] = str(num_processes)
