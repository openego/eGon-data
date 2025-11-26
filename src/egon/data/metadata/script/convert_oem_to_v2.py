#!/usr/bin/env python3

"""
Script to convert metadata files from OEMetadata-1.x to OEMetadata-2.0 format.

Requires at least oemetadata in version OEP-1.5.2.
"""

from pathlib import Path
import json
import logging

from omi.conversion import convert_metadata

logger = logging.getLogger(__name__)

# Metadata input/output directories
SOURCE_DIR = Path(__file__).parent.parent / "results"
TARGET_DIR = Path(__file__).parent.parent / "results" / "converted"

# OMI target version for metadata version conversion
TARGET_VERSION = "OEMetadata-2.0"


def run_conversion():
    source_dir = SOURCE_DIR
    target_dir = TARGET_DIR
    target_version = TARGET_VERSION

    if not target_version:
        logger.error("TARGET_VERSION is not defined in settings.py")
        return

    if not source_dir.exists():
        logger.error(f"Source directory does not exist: {source_dir}")
        return

    target_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Starting metadata conversion")
    logger.debug(f"Source: {source_dir}")
    logger.debug(f"Target: {target_dir}")
    logger.debug(f"Target version: {target_version}")

    for json_file in sorted(source_dir.glob("*.json")):
        if not json_file.is_file():
            continue

        logger.info(f"Converting {json_file.name}")
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                metadata = json.load(f)

            converted = convert_metadata(metadata, target_version)

            output_path = target_dir / json_file.name
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(converted, f, ensure_ascii=False, indent=2)

            logger.info(f"Saved converted metadata: {output_path}")

        except Exception:
            logger.exception(f"Error converting file: {json_file.name}")

    logger.info("Metadata conversion completed.")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    run_conversion()
