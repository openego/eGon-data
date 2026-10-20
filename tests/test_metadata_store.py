"""CI gate for the metadata store: complete coverage, valid OEMetadata.

Runs without a database: the inventory is static (AST) and the store is
plain YAML. Column drift against a live database is checked by
``egon-data metadata init`` on a developer machine, not here.
"""

import warnings

from omi.creation.assembler import assemble_metadata_dict
from omi.creation.coverage import coverage_report
from omi.creation.utils import discover_dataset_ids
from omi.validation import validate_metadata
import pytest

from egon.data.metadata.inventory import (
    expected_tables,
    load_table_owners,
    owner_group,
    scan_declared_io,
)
from egon.data.metadata.settings import OEM_BASE_DIR

EXPECTED = expected_tables(scan_declared_io())


def test_every_produced_table_has_metadata():
    report = coverage_report(OEM_BASE_DIR, sorted(EXPECTED))
    assert not report.missing, (
        "Tables without metadata -- run `egon-data metadata init --scope"
        f" <YourDataset>`: {report.missing}"
    )
    if report.skeleton:
        warnings.warn(
            f"{len(report.skeleton)} resources still have TODO placeholders"
            " (`egon-data metadata status` lists them)"
        )


def test_every_shared_table_has_an_owner():
    owners = load_table_owners()
    for table, producers in EXPECTED.items():
        owner_group(table, producers, owners)  # raises when ambiguous


@pytest.mark.parametrize("dataset_id", discover_dataset_ids(OEM_BASE_DIR))
def test_dataset_assembles_to_valid_oemetadata(dataset_id):
    document = assemble_metadata_dict(OEM_BASE_DIR, dataset_id)
    for resource in document["resources"]:
        validate_metadata(
            {**document, "resources": [resource]}, check_license=False
        )
