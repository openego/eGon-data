from click.testing import CliRunner

from egon.data import __version__
from egon.data.cli import main


def test_main():
    runner = CliRunner()
    result = runner.invoke(main, ["--version"])

    assert result.output == "{name}, version {version}\n".format(
        name=main.name, version=__version__
    )
    assert result.exit_code == 0
