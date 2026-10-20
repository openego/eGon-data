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
