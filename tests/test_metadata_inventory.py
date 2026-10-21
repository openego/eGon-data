from egon.data.metadata.inventory import _scan_module


def test_list_targets_are_qualified_with_class_schema(tmp_path):
    module = tmp_path / "osm_like.py"
    module.write_text(
        "class OpenStreetMap(Dataset):\n"
        '    schema: str = "openstreetmap"\n'
        '    targets = DatasetTargets(tables=["osm_line", "osm_ways"])\n'
    )
    (io,) = _scan_module(module)
    assert io.target_tables == {
        "osm_line": "openstreetmap.osm_line",
        "osm_ways": "openstreetmap.osm_ways",
    }


def test_classes_outside_the_dag_are_not_expected():
    from egon.data.metadata.inventory import UNMAPPED, ClassIO, expected_tables

    wired = ClassIO(name="A", module="a.py", task_group="setup")
    wired.target_tables = {"t": "s.wired"}
    unwired = ClassIO(name="B", module="b.py", task_group=UNMAPPED)
    unwired.target_tables = {"t": "s.unwired"}
    assert list(expected_tables([wired, unwired])) == ["s.wired"]


def test_shared_tables_need_an_owner():
    import pytest

    from egon.data.metadata.inventory import ClassIO, owner_group

    a = ClassIO(name="A", module="a.py", task_group="setup")
    b = ClassIO(name="B", module="b.py", task_group="etrago_input")
    assert owner_group("s.t", [a], owners={}) == "setup"
    assert (
        owner_group("s.t", [a, b], {"s.t": "etrago_input"}) == "etrago_input"
    )
    with pytest.raises(ValueError, match="table_owners"):
        owner_group("s.t", [a, b], owners={})


def test_status_names_classes_outside_the_dag(monkeypatch):
    from click.testing import CliRunner

    from egon.data.metadata import cli, inventory

    real = inventory.scan_declared_io()
    ghost = inventory.ClassIO(name="GhostDataset", module="ghost.py")
    ghost.target_tables = {"t": "s.ghost"}
    monkeypatch.setattr(inventory, "scan_declared_io", lambda: real + [ghost])
    result = CliRunner().invoke(cli.metadata, ["status"])
    assert (
        "[unmapped] GhostDataset declares 1 target table(s)" in result.output
    )
