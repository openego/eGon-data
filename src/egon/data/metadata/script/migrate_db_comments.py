#!/usr/bin/env python3
"""One-off migration: OEMetadata v1.4 table comments -> v2 resource YAMLs.

Reads a JSON dump ``{"schema.table": <comment metadata>}`` of the table
comments of a full-run database (wayfinder ticket 011: server run
``regon_dev_02-10``), converts every table that the inventory expects but
the store lacks, and writes it as a resource YAML into the store. Curated
YAMLs already in the store are never touched.

The comments are OEMetadata v1.4, below OMI's conversion floor; their
``metadataVersion`` is bumped to v1.5.2 first (agreed workaround, spot-checked
on all 92 comments). Only technical defects are repaired: NaN -> null and the
publication placeholder ``@id`` -> null. Content gaps stay visible as TODOs.

Usage: python migrate_db_comments.py <dump.json>
"""

from copy import deepcopy
from datetime import date
from pathlib import Path
import json
import math
import sys

import yaml

from omi.conversion import convert_metadata
from omi.creation.init import add_resource_from_oem_metadata
from omi.validation import validate_metadata

from egon.data.metadata.inventory import (
    expected_tables,
    load_dataset_ids,
    scan_declared_io,
)
from egon.data.metadata.script.migrate_json_to_split_yaml import (
    _strip_resource_by_template,
)
from egon.data.metadata.settings import OEM_BASE_DIR, OEMETADATA_VERSION

MIGRATION_CONTRIBUTOR = {
    "title": "eGon-data metadata migration",
    "path": "https://github.com/openego/eGon-data",
    "roles": ["dataCurator"],
    "organization": None,
    "date": date.today().isoformat(),
    "object": "metadata",
    "comment": "Migrated from the OEMetadata v1.4 table comment of a full"
    " eGon-data run to OEMetadata v2 with OMI.",
}


SHARED_TEMPLATE = OEM_BASE_DIR / "datasets" / "egon_input_data.template.yaml"


def ensure_dataset(ds_id: str, group: str) -> None:
    """Create a dataset with the shared eGon template, like the first four."""
    datasets = OEM_BASE_DIR / "datasets"
    dataset_yaml = datasets / f"{ds_id}.dataset.yaml"
    if dataset_yaml.exists():
        return
    words = group.replace("_", " ")
    dataset_yaml.write_text(
        "version: OEMetadata-2.0\n"
        "dataset:\n"
        f"  name: '{ds_id}'\n"
        f"  title: 'eGon-data — {words}'\n"
        f"  description: '{words.capitalize()} datasets produced by the"
        f" eGon-data pipeline ({group} task group).'\n"
        "  '@id': null\n",
        encoding="utf-8",
    )
    (datasets / f"{ds_id}.template.yaml").write_text(
        SHARED_TEMPLATE.read_text(encoding="utf-8"), encoding="utf-8"
    )
    print(f"[dataset]  {ds_id}")


def _clean(value):
    """Replace NaN and the placeholder @id by None, undo legacy wrapping."""
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, str) and len(value) > 1 and value[0] == value[-1] == '"':
        return value[1:-1]  # quoted twice by the legacy code
    if isinstance(value, dict) and isinstance(value.get("name"), dict):
        return _clean(value["name"])  # license nested in its own "name"
    if isinstance(value, dict):
        return {
            k: (None if k == "@id" and v == "WILL_BE_SET_AT_PUBLICATION"
                else _clean(v))
            for k, v in value.items()
        }
    if isinstance(value, list):
        return [_clean(v) for v in value]
    return value


def convert(comment: dict) -> dict:
    """Convert one v1.4 table comment to a validated v2 document."""
    doc = deepcopy(comment)
    doc.setdefault("metaMetadata", {})["metadataVersion"] = "OEP-1.5.2"
    v2 = _clean(convert_metadata(doc, "OEMetadata-2.0"))
    resource = v2["resources"][0]
    # OMI appends its generic conversion contributor; ours says what and from
    # where, so keep the original contributors plus one migration entry.
    resource["contributors"] = [
        c
        for c in resource.get("contributors", [])
        if c.get("title") != "Open Energy Platform oemetadata conversion to v2"
    ] + [dict(MIGRATION_CONTRIBUTOR)]
    validate_metadata(v2, check_license=False)
    return v2


def strip_template_fields(path: Path, ds_id: str) -> None:
    """Drop what the dataset template already provides, like the first 20."""
    tmpl_path = OEM_BASE_DIR / "datasets" / f"{ds_id}.template.yaml"
    template = yaml.safe_load(tmpl_path.read_text(encoding="utf-8")) or {}
    resource = yaml.safe_load(path.read_text(encoding="utf-8"))
    for key in ("keywords", "topics", "languages"):
        # template lists are concatenated at assembly: keep only additions
        extra = [
            v
            for v in resource.get(key) or []
            if v not in ("", None) and v not in (template.get(key) or [])
        ]
        if extra:
            resource[key] = extra
        else:
            resource.pop(key, None)
    resource = _strip_resource_by_template(resource, template)
    with path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(resource, f, sort_keys=False, allow_unicode=True)


def main(dump_path: str) -> None:
    comments = json.loads(Path(dump_path).read_text(encoding="utf-8"))
    ids = load_dataset_ids()
    expected = expected_tables(scan_declared_io())
    written, skipped = 0, []
    for table, producers in sorted(expected.items()):
        if table not in comments:
            continue
        ds_id = ids.get(producers[0].task_group)
        if ds_id is None:
            skipped.append(f"{table}: task group has no dataset id")
            continue
        safe = table.replace(".", "_")
        path = OEM_BASE_DIR / "resources" / ds_id / f"{safe}.resource.yaml"
        if path.exists():
            continue  # curated YAML wins
        try:
            v2 = convert(comments[table])
        except Exception as error:  # noqa: BLE001 -- report and go on
            skipped.append(f"{table}: {error}")
            continue
        v2["resources"][0]["name"] = table
        ensure_dataset(ds_id, producers[0].task_group)
        add_resource_from_oem_metadata(
            OEM_BASE_DIR,
            ds_id,
            v2,
            resource_name=safe,
            oem_version=OEMETADATA_VERSION,
        )
        strip_template_fields(path, ds_id)
        written += 1
        print(f"[migrated] {ds_id}/{safe}")
    print(f"{written} resources migrated, {len(skipped)} skipped")
    for line in skipped:
        print(f"[skipped]  {line}")


if __name__ == "__main__":
    main(sys.argv[1])
