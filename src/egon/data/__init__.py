import re

from loguru import logger
import click
import loguru
import rich.console
import rich.traceback

__version__ = "0.0.0"


def format(record):
    result = loguru._defaults.LOGURU_FORMAT
    exception = record["exception"]
    if exception:
        console = rich.console.Console(color_system=None)
        traceback = rich.traceback.Traceback(
            rich.traceback.Traceback.extract(
                exc_type=exception.type,
                exc_value=exception.value,
                traceback=exception.traceback,
                show_locals=True,
            )
        )
        with console.capture() as capture:
            console.print(traceback)
        capture = capture.get()
        # Regular expression copied from:
        #   https://loguru.readthedocs.io/en/stable/api/logger.html#color
        capture = re.sub(r"\\?</?((?:[fb]g\s)?[^<>\s]*)>", r"\\\g<0>", capture)
        capture = re.sub("{", "{{", capture)
        capture = re.sub("}", "}}", capture)
        result = f"{result}\n{capture}"
    return result


logger.remove()
logger.add(lambda m: click.echo(m, err=True), format=format, enqueue=True)
