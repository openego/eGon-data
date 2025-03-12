from textwrap import wrap

from loguru import logger
import click
import shutil
__version__ = "0.0.0"


def echo(message):
    prefix, message = message.split(" - ")
    lines = message.split("\n")
    width = min(72, shutil.get_terminal_size()[0])
    wraps = ["\n".join(wrap(line, width)) for line in lines]
    message = "\n".join([prefix] + wraps)
    click.echo(message, err=True)


logger.remove()
logger.add(echo, colorize=True)
