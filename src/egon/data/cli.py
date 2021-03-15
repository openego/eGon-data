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
from textwrap import wrap
import os
import subprocess
import sys

import click
import importlib_resources as resources
import yaml

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
    "--database-name",
    "--database",
    default="egon-data",
    metavar="DB",
    help=(
        "Specify the name of the local database.\n\n\b"
        ' Note: "--database" is deprecated and will be removed in the'
        " future. Please use the longer but consistent"
        ' "--database-name".'
    ),
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
    "--database-password",
    default="data",
    metavar="PW",
    help=("Specify the password used to access the local database."),
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
    "--database-user",
    default="egon",
    metavar="USERNAME",
    help=("Specify the user used to access the local database."),
    show_default=True,
)
@click.version_option(version=egon.data.__version__)
@click.pass_context
def egon_data(context, **kwargs):
    """Run and control the eGo^n data processing pipeline.

    It is recommended to create a dedicated working directory in which
    to run `egon-data` because `egon-data` because `egon-data` will use
    it's working directory to store configuration files and other data
    generated during a workflow run.

    You can configure `egon-data` by putting a file named
    "egon-data.configuration.yaml" into the directory from which you are
    running `egon-data`. If that file doesn't exist, `egon-data` will
    create one, containing the command line parameters supplied, as well
    as the defaults for those switches for which no value was supplied.
    Last but not least, if you're using the default behaviour of setting
    up the database in a Docker container, the working directory will
    also contain a directory called "docker", containing the database
    data as well as other volumes used by the dockered database.

    """
    options = {
        "cli": {
            "egon-data": {
                option.opts[0]: kwargs[option.name]
                for option in egon_data.params
                if option.name in kwargs
            }
        },
        "defaults": {
            "egon-data": {
                option.opts[0]: option.default
                for option in egon_data.params
                if option.default
            }
        },
    }

    if not config.paths()[0].exists():
        with open(config.paths()[0], "w") as f:
            f.write(yaml.safe_dump(options["defaults"]))

    if len(config.paths(pid="*")) > 1:
        message = (
            "Found more than one configuration file belonging to a"
            " specific `egon-data` process. Unable to decide which one"
            " to use.\nExiting."
        )
        click.echo(
            "\n".join(wrap(message, click.get_terminal_size()[0])),
            err=True,
        )
        sys.exit(1)

    if len(config.paths(pid="*")) == 1:
        message = (
            "Ignoring supplied options. Found a configuration file"
            " belonging to a different `egon-data` process. Using that"
            " one."
        )
        click.echo(
            "\n".join(wrap(message, click.get_terminal_size()[0])),
            err=True,
        )
        with open(config.paths(pid="*")[0]) as f:
            options = yaml.load(f, Loader=yaml.SafeLoader)
    else:  # len(config.paths(pid="*")) == 0, so need to create one.
        with open(config.paths()[0]) as f:
            options["file"] = yaml.load(f, Loader=yaml.SafeLoader)
        options = dict(options.get("file", {}), **options["cli"])
        with open(config.paths(pid="current")[0], "w") as f:
            f.write(yaml.safe_dump(options))

    os.environ["AIRFLOW_HOME"] = str(resources.files(egon.data.airflow))

    os.makedirs(Path(".") / "docker", exist_ok=True)
    template = "docker-compose.yml"
    target = Path(".") / "docker" / template
    if not target.exists():
        with open(target, "w") as compose_file:
            compose_file.write(
                resources.read_text(egon.data.airflow, template).format(
                    **options["egon-data"], airflow=os.environ["AIRFLOW_HOME"]
                )
            )


def main():
    egon_data.add_command(airflow)
    egon_data.add_command(serve)
    try:
        egon_data.main(sys.argv[1:])
    finally:
        config.paths(pid="current")[0].unlink(missing_ok=True)
