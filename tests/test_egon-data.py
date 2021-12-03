from importlib import import_module

from click.testing import CliRunner
import pytest

from egon.data import __version__
from egon.data.cli import egon_data


def test_main():
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(egon_data, ["--version"])

    assert result.output == "{name}, version {version}\n".format(
        name=egon_data.name, version=__version__
    )
    assert result.exit_code == 0


@pytest.mark.skip(
    reason=(
        "Needs `docker` and/or PostgreSQL and we're currently not making sure"
        "\nthese are present on the continuous integration service(s) we use."
    )
)
def test_airflow():
    """ Test that `egon-data airflow` correctly forwards to airflow. """
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(egon_data, ["airflow", "--help"])
    assert result.output == ""


def test_pipeline_importability():
    error = None
    for m in ["egon.data.airflow.dags.pipeline"]:
        try:
            import_module(m)
        except Exception as e:
            error = e
        assert error is None, (
            "\nDid not expect an error when importing:\n\n  `{}`\n\nGot: {}"
        ).format(m, error)
