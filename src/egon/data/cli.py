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
from multiprocessing import Process
from pathlib import Path
import os
import socket
import subprocess
import sys
import time

from psycopg2 import OperationalError as PSPGOE
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError as SQLAOE
import click
import importlib_resources as resources
import yaml

from egon.data import logger
import egon.data
import egon.data.airflow
import egon.data.config as config


@click.command(
    add_help_option=False,
    context_settings=dict(allow_extra_args=True, ignore_unknown_options=True),
)
@click.pass_context
def airflow(context):
    subprocess.run(["airflow"] + context.args)


@click.command()
@click.pass_context
def serve(context):
    """Start the airflow webapp controlling the egon-data pipeline.

    Airflow needs, among other things, a metadata database and a running
    scheduler. This command acts as a shortcut, creating the database if it
    doesn't exist and starting the scheduler in the background before starting
    the webserver.

    """
    subprocess.run(["airflow", "initdb"])
    scheduler = Process(
        target=subprocess.run,
        args=(["airflow", "scheduler"],),
        kwargs=dict(stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL),
    )
    scheduler.start()
    subprocess.run(["airflow", "webserver"])


@click.group(name="egon-data")
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
@click.version_option(version=egon.data.__version__)
@click.pass_context
def egon_data(context, **kwargs):
    """Run and control the eGo^n data processing pipeline.

    It is recommended to create a dedicated working directory in which
    to run `egon-data` because `egon-data` will use
    it's working directory to store configuration files and other data
    generated during a workflow run.
    Goto to a location where you want to store eGon-data project data and
    create a new directy by (feel free to choose a different directory name):

    `mkdir egon-data-production && cd egon-data-production`

    It is also recommended to use separate directories for production
    and test mode. 
    In test mode, you should also use a different database.
    This will be created and used by typing e.g.

    `egon-data --database-name 'test-egon-data' serve`

    Whenever `egon-data` is executed, it searches for the configuration file
    "egon-data.configuration.yaml" in PWD. If that file doesn't exist,
    `egon-data` will create one, containing the command line parameters
    supplied, as well as the defaults for those switches for which no value
    was supplied.
    This means, run the above command that specifies a custom database once.
    Afterwards, it's sufficient to execute `egon-data serve` in the same
    directory and the same configuration will be used.
    You can also edit the configuration the file
    "egon-data.configuration.yaml" manually.

    Last but not least, if you're using the default behaviour of setting
    up the database in a Docker container, the working directory will
    also contain a directory called "docker", containing the database
    data as well as other volumes used by the dockered database.

    """

    def options(value, check=None):
        check = value if check is None else check
        flags = {p.opts[0]: value(p) for p in egon_data.params if check(p)}
        return {"egon-data": flags}

    options = {
        "cli": options(lambda o: kwargs[o.name], lambda o: o.name in kwargs),
        "defaults": options(lambda o: o.default),
    }

    if not config.paths()[0].exists():
        with open(config.paths()[0], "w") as f:
            f.write(
                yaml.safe_dump(dict(options["defaults"], **options["cli"]))
            )

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
                flag: options["cli"][flag]
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
    )

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        code = s.connect_ex(
            (options["--database-host"], int(options["--database-port"]))
        )
    if code != 0:
        subprocess.run(
            ["docker-compose", "up", "-d", "--build"],
            cwd=str((Path(".") / "docker").absolute()),
        )
        time.sleep(1.5)  # Give the container time to boot.

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


def main():
    egon_data.add_command(airflow)
    egon_data.add_command(serve)
    try:
        egon_data.main(sys.argv[1:])
    finally:
        try:
            config.paths(pid="current")[0].unlink()
        except FileNotFoundError:
            pass
