from importlib import import_module

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


def test_airflow():
    """ Test that `egon-data airflow` correctly forwards to airflow. """
    runner = CliRunner()
    result = runner.invoke(main, ["airflow", "--help"])
    assert result.output == ""


def test_pipeline_and_tasks_importability():
    error = None
    for m in ["egon.data.airflow.dags.pipeline", "egon.data.airflow.tasks"]:
        try:
            import_module(m)
        except Exception as e:
            error = e
        assert error is None, (
            "\nDid not expect an error when importing:\n\n  `{}`\n\nGot: {}"
        ).format(m, error)
