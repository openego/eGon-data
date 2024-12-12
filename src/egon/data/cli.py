"""
Module that contains the command line app.

Why does this file exist, and why not put this in __main__?

  You might be tempted to import things from __main__ later, but that will
  cause problems: the code will get executed twice:

  - When you run `python -megon.data` python will execute
    ``__main__.py`` as a script. That means there won't be any
    ``egon.data.__main__`` in ``sys.modules``.
  - When you import __main__ it will get executed again (as a module) because
    there's no ``egon.data.__main__`` in ``sys.modules``.

  Also see (1) from http://click.pocoo.org/5/setuptools/#setuptools-integration
"""
import os
import shutil
import socket
import subprocess
import sys
import time
from multiprocessing import Process
from pathlib import Path

import click
import yaml
from psycopg2 import OperationalError as PSPGOE

import egon.data
import egon.data.airflow
import egon.data.config as config
import importlib_resources as resources
from egon.data import logger
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError as SQLAOE
from sqlalchemy.orm import Session


@click.group(
    name="egon-data", context_settings={"help_option_names": ["-h", "--help"]}
)
@click.option(
    "--airflow-database-name",
    default="airflow",
    metavar="DB",
    help=("Specify the name of the airflow metadata database."),
    show_default=True,
)
@click.option(
    "--database-name",
    "--database",
    default="egon-data",
    metavar="DB",
    help=(
        "Specify the name of the local database. The database will be"
        " created if it doesn't already exist.\n\n\b"
        ' Note: "--database" is deprecated and will be removed in the'
        " future. Please use the longer but consistent"
        ' "--database-name".'
    ),
    show_default=True,
)
@click.option(
    "--database-user",
    default="egon",
    metavar="USERNAME",
    help=("Specify the user used to access the local database."),
    show_default=True,
)
@click.option(
    "--database-host",
    default="127.0.0.1",
    metavar="HOST",
    help=("Specify the host on which the local database is running."),
    show_default=True,
)
@click.option(
    "--database-port",
    default="59734",
    metavar="PORT",
    help=("Specify the port on which the local DBMS is listening."),
    show_default=True,
)
@click.option(
    "--database-password",
    default="data",
    metavar="PW",
    help=("Specify the password used to access the local database."),
    show_default=True,
)
@click.option(
    "--dataset-boundary",
    type=click.Choice(["Everything", "Schleswig-Holstein"]),
    default="Everything",
    help=(
        "Choose to limit the processed data to one of the available"
        " built-in boundaries."
    ),
    show_default=True,
)
@click.option(
    "--household-electrical-demand-source",
    type=click.Choice(["bottom-up-profiles", "slp"]),
    default="slp",
    help=(
        "Choose the source to calculate and allocate household electrical"
        "demands. There are currently two options:"
        "'bottom-up-profiles' and 'slp' (Standard Load Profiles)"
    ),
    show_default=True,
)
@click.option(
    "--jobs",
    default=1,
    metavar="N",
    help=(
        "Spawn at maximum N tasks in parallel. Remember that in addition"
        " to that, there's always the scheduler and probably the server"
        " running."
    ),
    show_default=True,
)
@click.option(
    "--processes-per-task",
    default=1,
    metavar="N_PROCESS",
    help=(
        "Each task can use at maximum N_PROCESS parallel processes. Remember"
        " that in addition to that, tasks can run in parallel (see N) and"
        " there's always the scheduler and probably the serverrunning."
    ),
    show_default=True,
)
@click.option(
    "--docker-container-name",
    default="egon-data-local-database-container",
    metavar="NAME",
    help=(
        "The name of the Docker container containing the local database."
        " You usually can stick to the default, unless you run into errors"
        " due to clashing names and don't want to delete or rename your old"
        " containers."
    ),
    show_default=True,
)
@click.option(
    "--compose-project-name",
    default="egon-data",
    metavar="PROJECT",
    help=(
        "The name of the Docker project."
        " Different compose_project_names are needed to run multiple instances"
        " of egon-data on the same machine."
    ),
    show_default=True,
)
@click.option(
    "--airflow-port",
    default=8080,
    metavar="AIRFLOW_PORT",
    help=("Specify the port on which airflow runs."),
    show_default=True,
)
@click.option(
    "--random-seed",
    default=42,
    metavar="RANDOM_SEED",
    help=(
        "Random seed used by some tasks in the pipeline to ensure "
        " deterministic behaviour. All published results in the eGon project "
        " will be created with the default value so keep it if you want to "
        " make sure to get the same results."
    ),
    show_default=True,
)
@click.option(
    "--scenarios",
    default=["status2019", "eGon2035"],
    metavar="SCENARIOS",
    help=("List of scenario names for which a data model shall be created."),
    multiple=True,
    show_default=True,
)
@click.option(
    "--run-pypsa-eur",
    default=False,
    metavar="RUN_PYPSA_EUR",
    help=(
        "State if pypsa-eur should be executed and installed within egon-data."
        " If set to false, a predefined network from the data bundle is used."
    ),
    show_default=True,
)
@click.version_option(version=egon.data.__version__)
@click.pass_context
def egon_data(context, **kwargs):
    """Run and control the eGo^n data processing pipeline.

    It is recommended to create a dedicated working directory in which to
    run `egon-data` because `egon-data` will use it's working directory to
    store configuration files and other data generated during a workflow
    run. Go to to a location where you want to store eGon-data project data
    and create a new directory via:

        `mkdir egon-data-production && cd egon-data-production`

    Of course you are free to choose a different directory name.

    It is also recommended to use separate directories for production and
    test mode. In test mode, you should also use a different database. This
    will be created and used by typing e.g.:

        `egon-data --database-name 'test-egon-data' serve`

    It is important that options (e.g. `--database-name`) are placed before
    commands (e.g. `serve`).

    For using a smaller dataset in the test mode, use the option
    `--dataset-boundary`. The complete command for starting Aiflow in test
    mode with using a separate database is

        `egon-data --database-name 'test-egon-data' --dataset-boundary
        'Schleswig-Holstein' serve`

    Whenever `egon-data` is executed, it searches for the configuration file
    "egon-data.configuration.yaml" in CWD. If that file doesn't exist,
    `egon-data` will create one, containing the command line parameters
    supplied, as well as the defaults for those switches for which no value
    was supplied.
    This means, run the above command that specifies a custom database once.
    Afterwards, it's sufficient to execute `egon-data serve` in the same
    directory and the same configuration will be used. You can also edit the
    configuration the file "egon-data.configuration.yaml" manually.

    Last but not least, if you're using the default behaviour of setting
    up the database in a Docker container, the working directory will
    also contain a directory called "docker", containing the database
    data as well as other volumes used by the "Docker"ed database.

    """

    # Adapted from the `merge_copy` implementation at:
    #
    #   https://stackoverflow.com/questions/29847098/the-best-way-to-merge-multi-nested-dictionaries-in-python-2-7
    #
    def merge(d1, d2):
        return {
            k: d1[k]
            if k in d1 and k not in d2
            else d2[k]
            if k not in d1 and k in d2
            else merge(d1[k], d2[k])
            if isinstance(d1[k], dict) and isinstance(d2[k], dict)
            else d2[k]
            for k in set(d1).union(d2)
        }

    def options(value, check=None):
        check = value if check is None else check
        flags = {p.opts[0]: value(p) for p in egon_data.params if check(p)}
        return {"egon-data": flags}

    options = {
        "cli": options(lambda o: kwargs[o.name], lambda o: o.name in kwargs),
        "defaults": options(lambda o: o.default),
    }

    combined = merge(options["defaults"], options["cli"])
    if not config.paths()[0].exists():
        with open(config.paths()[0], "w") as f:
            f.write(yaml.safe_dump(combined))
    else:
        with open(config.paths()[0], "r") as f:
            stored = yaml.safe_load(f)
        with open(config.paths()[0], "w") as f:
            f.write(yaml.safe_dump(merge(combined, stored)))

    # Alternatively:
    #   `if config.paths(pid="*") != [config.paths(pid="current")]:`
    #   or compare file contents.
    if len(config.paths(pid="*")) > 1:
        logger.error(
            "Found more than one configuration file belonging to a"
            " specific `egon-data` process. Unable to decide which one"
            " to use.\nExiting."
        )
        sys.exit(1)

    if len(config.paths(pid="*")) == 1:
        logger.info(
            "Ignoring supplied options. Found a configuration file"
            " belonging to a different `egon-data` process. Using that"
            " one."
        )
        with open(config.paths(pid="*")[0]) as f:
            options = yaml.load(f, Loader=yaml.SafeLoader)
    else:  # len(config.paths(pid="*")) == 0, so need to create one.

        with open(config.paths()[0]) as f:
            options["file"] = yaml.load(f, Loader=yaml.SafeLoader)

        options = dict(
            options.get("file", {}),
            **{
                flag: options["file"][flag]
                for flag in options["cli"]
                if options["cli"][flag] != options["defaults"][flag]
            },
        )

        with open(config.paths(pid="current")[0], "w") as f:
            f.write(yaml.safe_dump(options))

    def render(template, target, update=True, inserts={}, **more_inserts):
        os.makedirs(target.parent, exist_ok=True)
        rendered = resources.read_text(egon.data.airflow, template).format(
            **dict(inserts, **more_inserts)
        )
        if not target.exists():
            with open(target, "w") as f:
                f.write(rendered)
        elif update:
            with open(target, "r") as f:
                old = f.read()
            if old != rendered:
                with open(target, "w") as f:
                    f.write(rendered)

    os.environ["AIRFLOW_HOME"] = str((Path(".") / "airflow").absolute())

    options = options["egon-data"]
    render(
        "airflow.cfg",
        Path(".") / "airflow" / "airflow.cfg",
        inserts=options,
        dags=str(resources.files(egon.data.airflow).absolute()),
    )
    render(
        "docker-compose.yml",
        Path(".") / "docker" / "docker-compose.yml",
        update=False,
        inserts=options,
        airflow=resources.files(egon.data.airflow),
        gid=os.getgid(),
        uid=os.getuid(),
    )
    (Path(".") / "docker" / "database-data").mkdir(parents=True, exist_ok=True)

    # Copy webserver_config.py to disable authentification on webinterface
    shutil.copy2(
        os.path.dirname(egon.data.airflow.__file__) + "/webserver_config.py",
        Path(".") / "airflow/webserver_config.py",
    )

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        code = s.connect_ex(
            (options["--database-host"], int(options["--database-port"]))
        )
    if code != 0:
        subprocess.run(
            [
                "docker-compose",
                "-p",
                options["--compose-project-name"],
                "up",
                "-d",
                "--build",
            ],
            cwd=str((Path(".") / "docker").absolute()),
        )
        time.sleep(1.5)  # Give the container time to boot.

    # TODO: Since "AIRFLOW_HOME" needs to be set before importing `conf`, the
    #       import can only be done inside this function, which is generally
    #       frowned upon, instead of at the module level. Maybe there's a
    #       better way to encapsulate this?
    from airflow.configuration import conf as airflow_cfg
    from airflow.models import Connection

    engine = create_engine(
        (
            "postgresql+psycopg2://{--database-user}:{--database-password}"
            "@{--database-host}:{--database-port}"
            "/{--airflow-database-name}"
        ).format(**options),
        echo=False,
    )
    while True:  # Might still not be done booting. Poke it it's up.
        try:
            connection = engine.connect()
            break
        except PSPGOE:
            pass
        except SQLAOE:
            pass
    with connection.execution_options(
        isolation_level="AUTOCOMMIT"
    ) as connection:
        databases = [
            row[0]
            for row in connection.execute("SELECT datname FROM pg_database;")
        ]
        if not options["--database-name"] in databases:
            connection.execute(
                f'CREATE DATABASE "{options["--database-name"]}";'
            )

    subprocess.run(["airflow", "db", "init"])

    # TODO: Constrain SQLAlchemy's lower version to 1.4 and use a `with` block
    #       like the one in the last commented line to avoid an explicit
    #       `commit`. This can then also be used to get rid of the
    #       `egon.data.db.session_scope` context manager and use the new
    #       buil-in one instead. And we can migrate to the SQLA 2.0 query
    #       API.
    # with Session(engine) as airflow, airflow.begin():
    engine = create_engine(airflow_cfg.get("core", "SQL_ALCHEMY_CONN"))
    airflow = Session(engine)
    connection = (
        airflow.query(Connection).filter_by(conn_id="egon_data").one_or_none()
    )
    connection = connection if connection else Connection(conn_id="egon_data")
    connection.login = options["--database-user"]
    connection.password = options["--database-password"]
    connection.host = options["--database-host"]
    connection.port = options["--database-port"]
    connection.schema = options["--database-name"]
    connection.conn_type = "pgsql"
    airflow.add(connection)
    airflow.commit()

    # TODO: This should probably rather be done during the database
    #       initialization workflow task.
    from egon.data.datasets import setup

    setup()


@egon_data.command(
    add_help_option=False,
    context_settings=dict(allow_extra_args=True, ignore_unknown_options=True),
)
@click.pass_context
def airflow(context):
    subprocess.run(["airflow"] + context.args)


@egon_data.command(
    context_settings={
        "allow_extra_args": True,
        "help_option_names": ["-h", "--help"],
        "ignore_unknown_options": True,
    }
)
@click.pass_context
def serve(context):
    """Start the airflow webapp controlling the egon-data pipeline.

    Airflow needs, among other things, a metadata database and a running
    scheduler. This command acts as a shortcut, creating the database if it
    doesn't exist and starting the scheduler in the background before starting
    the webserver.

    Any OPTIONS other than `-h`/`--help` will be forwarded to
    `airflow webserver`, so you can for example specify an alternate port
    for the webapp to listen on via `egon-data serve -p PORT_NUMBER`.
    Find out more about the possible webapp options via:

        `egon-data airflow webserver --help`.
    """
    scheduler = Process(
        target=subprocess.run,
        args=(["airflow", "scheduler"],),
        kwargs=dict(stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL),
    )
    scheduler.start()
    subprocess.run(["airflow", "webserver"] + context.args)


def main():
    try:
        egon_data.main(sys.argv[1:])
    finally:
        try:
            config.paths(pid="current")[0].unlink()
        except FileNotFoundError:
            pass
