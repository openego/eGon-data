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
import os
import os.path
import subprocess

import click
import yaml

import egon.data
import egon.data.airflow


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


@click.group()
@click.option(
    "--database",
    metavar="DB",
    help=("Specify the name of the local database."),
)
@click.option(
    "--database-host",
    metavar="HOST",
    help=("Specify the host on which the local database is running."),
)
@click.option(
    "--database-password",
    metavar="PW",
    help=("Specify the password used to access the local database."),
)
@click.option(
    "--database-port",
    metavar="PORT",
    help=("Specify the port on which the local DBMS is listening."),
)
@click.option(
    "--database-user",
    metavar="USERNAME",
    help=("Specify the user used to access the local database."),
)
@click.version_option(version=egon.data.__version__)
@click.pass_context
def main(context, **kwargs):
    os.environ["AIRFLOW_HOME"] = os.path.dirname(egon.data.airflow.__file__)
    translations = {
        "database": "POSTGRES_DB",
        "database_password": "POSTGRES_PASSWORD",
        "database_host": "HOST",
        "database_port": "PORT",
        "database_user": "POSTGRES_USER",
    }
    database_configuration = {
        translations[k]: kwargs[k]
        for k in kwargs
        if "database" in k
        if kwargs[k] is not None
    }
    if database_configuration:
        with open("local-database.yaml", "w") as configuration:
            configuration.write(yaml.safe_dump(database_configuration))


main.name = "egon-data"
main.add_command(airflow)
main.add_command(serve)
