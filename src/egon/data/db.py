from pathlib import Path
import os

from sqlalchemy import create_engine, text
import yaml

import egon


def credentials():
    """Return local database connection parameters.

    Returns
    -------
    dict
        Complete DB connection information
    """
    # Read database configuration from docker-compose.yml
    package_path = egon.data.__path__[0]
    docker_compose_file = os.path.join(
        package_path, "airflow", "docker-compose.yml"
    )
    docker_compose = yaml.load(
        open(docker_compose_file), Loader=yaml.SafeLoader
    )

    # Select basic connection details
    docker_db_config = docker_compose["services"]["egon-data-local-database"][
        "environment"
    ]

    # Add HOST and PORT
    docker_db_config_additional = docker_compose["services"][
        "egon-data-local-database"
    ]["ports"][0].split(":")
    docker_db_config["HOST"] = docker_db_config_additional[0]
    docker_db_config["PORT"] = docker_db_config_additional[1]

    custom = Path("local-database.yaml")
    if custom.is_file():
        with open(custom) as f:
            docker_db_config.update(yaml.safe_load(f))
    return docker_db_config


def engine():
    """Engine for local database."""
    db_config = credentials()
    return create_engine(
        f"postgresql+psycopg2://{db_config['POSTGRES_USER']}:"
        f"{db_config['POSTGRES_PASSWORD']}@{db_config['HOST']}:"
        f"{db_config['PORT']}/{db_config['POSTGRES_DB']}",
        echo=False,
    )


def execute_sql(sql_string):
    """Execute a SQL expression given as string.

    The SQL expression passed as plain string is convert to a
    `sqlalchemy.sql.expression.TextClause`.

    Parameters
    ----------
    sql_string : str
        SQL expression

    """
    engine_local = engine()

    with engine_local.connect().execution_options(autocommit=True) as con:
        con.execute(text(sql_string))


def submit_comment(json, schema, table):
    """Add comment to table.

    We use `Open Energy Metadata <https://github.com/OpenEnergyPlatform/
    oemetadata/blob/develop/metadata/v140/metadata_key_description.md>`_
    standard for describging our data. Metadata is stored as JSON in the table
    comment.

    Parameters
    ----------
    json : str
        JSON string reflecting comment
    schema : str
        The target table's database schema
    table : str
        Database table on which to put the given comment
    """
    prefix_str = "COMMENT ON TABLE {0}.{1} IS ".format(schema, table)

    check_json_str = (
        "SELECT obj_description('{0}.{1}'::regclass)::json".format(
            schema, table
        )
    )

    execute_sql(prefix_str + json + ";")

    # Query table comment and cast it into JSON
    # The query throws an error if JSON is invalid
    execute_sql(check_json_str)


def airflow_db_connection():
    """Define connection to egon data db via env variable.

    This connection can be accessed by Operators and Hooks using
    :code:`postgres_conn_id='egon_data'`.
    """

    cred = credentials()

    os.environ["AIRFLOW_CONN_EGON_DATA"] = \
        f"postgresql://{cred['POSTGRES_USER']}:{cred['POSTGRES_PASSWORD']}" \
            f"@{cred['HOST']}:{cred['PORT']}/{cred['POSTGRES_DB']}"
