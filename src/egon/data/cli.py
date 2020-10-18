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
import click

import egon.data


@click.group()
@click.version_option(version=egon.data.__version__)
@click.pass_context
def main(context):
    pass


main.name = "egon-data"
