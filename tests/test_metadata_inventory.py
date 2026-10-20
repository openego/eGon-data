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
