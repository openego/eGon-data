import egon
import os
import yaml
from sqlalchemy import create_engine, text


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
    docker_compose_file = os.path.join(package_path, "airflow",
                                       "docker-compose.yml")
    docker_compose = yaml.load(open(docker_compose_file),
                               Loader=yaml.SafeLoader)

    # Select basic connection details
    docker_db_config = docker_compose['services']['egon-data-local-database'][
        "environment"]

    # Add HOST and PORT
    docker_db_config_additional = \
        docker_compose['services']['egon-data-local-database']["ports"][
            0].split(":")
    docker_db_config["HOST"] = docker_db_config_additional[0]
    docker_db_config["PORT"] = docker_db_config_additional[1]

    return docker_db_config


def execute_sql(sql_string):
    """
    Execute a SQL expression given as string

    The SQL expression passed as plain string is convert to a
    `sqlalchemy.sql.expression.TextClause`.

    Parameters
    ----------
    sql_string : str
        SQL expression

    """

    db_config = egon_data_db_credentials()

    engine_local = create_engine(
        f"postgresql+psycopg2://{db_config['POSTGRES_USER']}:"
        f"{db_config['POSTGRES_PASSWORD']}@{db_config['HOST']}:"
        f"{db_config['PORT']}/{db_config['POSTGRES_DB']}", echo=False)

    with engine_local.connect().execution_options(autocommit=True) as con:
        con.execute(text(sql_string))


def submit_comment(json, schema, table):
    """
    Add comment to table

    Parameters
    ----------
    json : str
        JSON string reflecting comment
    schema : str
        Desired database schema
    table : str
        Desired database table
    """

    prefix_str = "COMMENT ON TABLE {0}.{1} IS ".format(schema, table)

    check_json_str = "SELECT obj_description('{0}.{1}'::regclass)::json" \
        .format(schema, table)

    execute_sql(prefix_str + json + ";")

    execute_sql(check_json_str)
