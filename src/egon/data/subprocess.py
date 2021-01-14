"""Exensions to Python's :py:mod:`subprocess` module.

More specifically, this module provides a customized version of
:py:func:`subprocess.run`, which always sets `check=True`,
`capture_output=True`, enhances the raised exceptions string representation
with additional output information and makes it slightly more readable when
encountered in a stack trace.
"""

from locale import getpreferredencoding
from textwrap import indent, wrap
import itertools
import subprocess


class CalledProcessError(subprocess.CalledProcessError):
    """A more verbose version of :py:class:`subprocess.CalledProcessError`.

    Replaces the standard string representation of a
    :py:class:`subprocess.CalledProcessError` with one that has more output and
    error information and is formatted to be more readable in a stack trace.
    """

    def __str__(self):
        errors = self.stderr.decode(getpreferredencoding()).split("\n")
        outputs = self.stdout.decode(getpreferredencoding()).split("\n")

        lines = itertools.chain(
            wrap(f"{super().__str__()}"),
            ["Output:"],
            *(
                wrap(output, initial_indent="  ", subsequent_indent="  ")
                for output in outputs
            ),
            ["Errors:"],
            *(
                wrap(error, initial_indent="  ", subsequent_indent="  ")
                for error in errors
            ),
        )
        lines = indent("\n".join(lines), "| ")
        return f"\n{lines}"


def run(*args, **kwargs):
    """A "safer" version of :py:func:`subprocess.run`.

    "Safer" in this context means that this version always raises
    :py:class:`CalledProcessError` if the process in question returns a
    non-zero exit status. This is done by setting `check=True` and
    `capture_output=True`, so you don't have to specify these yourself anymore.
    You can though, if you want to override these defaults.
    Other than that, the function accepts the same parameters as
    :py:func:`subprocess.run`.
    """
    kwargs["check"] = kwargs.get("check", True)
    kwargs["capture_output"] = kwargs.get("capture_output", True)
    try:
        subprocess.run(*args, **kwargs)
    except subprocess.CalledProcessError as cpe:
        raise CalledProcessError(
            cpe.returncode, cpe.cmd, output=cpe.output, stderr=cpe.stderr
        ) from None
