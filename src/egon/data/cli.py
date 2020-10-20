"""
Module that contains the command line app.

Why does this file exist, and why not put this in __main__?

  You might be tempted to import things from __main__ later, but that will cause
  problems: the code will get executed twice:

  - When you run `python -megon.data` python will execute
    ``__main__.py`` as a script. That means there won't be any
    ``egon.data.__main__`` in ``sys.modules``.
  - When you import __main__ it will get executed again (as a module) because
    there's no ``egon.data.__main__`` in ``sys.modules``.

  Also see (1) from http://click.pocoo.org/5/setuptools/#setuptools-integration
"""
import os
import os.path
import subprocess
from multiprocessing import Process

import click

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
    """ Start the airflow webapp controlling the egon-data pipeline.

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
@click.version_option(version=egon.data.__version__)
@click.pass_context
def main(context):
    os.environ["AIRFLOW_HOME"] = os.path.dirname(egon.data.airflow.__file__)


main.name = "egon-data"
main.add_command(airflow)
main.add_command(serve)
