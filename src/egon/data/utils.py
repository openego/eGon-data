import egon
import os
import yaml


def data_set_configuration(config_file=None):
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


def egon_data_db_credentials():
    """Return local database connection parameters

    Returns
    -------
    dict
        Complete DB connection information
    """

    # Read database configuration from docker-compose.yml
    package_path = egon.data.__path__[0]
    docker_compose_file = os.path.join(package_path, "airflow", "docker-compose.yml")
    docker_compose = yaml.load(open(docker_compose_file), Loader=yaml.SafeLoader)

    # Select basic connection details
    docker_db_config = docker_compose['services']['egon-data-local-database']["environment"]

    # Add HOST and PORT
    docker_db_config_additional = docker_compose['services']['egon-data-local-database']["ports"][0].split(":")
    docker_db_config["HOST"] = docker_db_config_additional[0]
    docker_db_config["PORT"] = docker_db_config_additional[1]

    return docker_db_config
