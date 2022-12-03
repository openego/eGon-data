from loguru import logger
import click

__version__ = "0.0.0"


logger.remove()
logger.add(lambda m: click.echo(m, err=True), colorize=True)
