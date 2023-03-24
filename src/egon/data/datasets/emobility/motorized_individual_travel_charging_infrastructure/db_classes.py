"""
DB tables / SQLAlchemy ORM classes for charging infrastructure
"""

import datetime
import json

from geoalchemy2 import Geometry
from sqlalchemy import Column, Float, Integer, String
from sqlalchemy.ext.declarative import declarative_base

from egon.data import config, db
from egon.data.metadata import (
    context,
    contributors,
    generate_resource_fields_from_db_table,
    license_odbl,
    meta_metadata,
)

Base = declarative_base()
DATASET_CFG = config.datasets()["charging_infrastructure"]


class EgonEmobChargingInfrastructure(Base):

    __tablename__ = DATASET_CFG["targets"]["charging_infrastructure"]["table"]
    __table_args__ = {
        "schema": DATASET_CFG["targets"]["charging_infrastructure"]["schema"]
    }

    cp_id = Column(Integer, primary_key=True)
    mv_grid_id = Column(Integer)
    use_case = Column(String)
    weight = Column(Float)
    geometry = Column(
        Geometry(
            srid=DATASET_CFG["original_data"]["sources"]["tracbev"]["srid"]
        )
    )


def add_metadata():
    """
    Add metadata to table grid.egon_emob_charging_infrastructure
    """
    contris = contributors(["kh", "kh"])

    contris[0]["date"] = "2023-03-14"

    contris[0]["object"] = "metadata"
    contris[1]["object"] = "dataset"

    contris[0]["comment"] = "add metadata to dataset."
    contris[1]["comment"] = "Add worflow to generate dataset."

    meta = {
        "name": "grid.egon_emob_charging_infrastructure",
        "title": "eGon Electromobility Charging Infrastructure",
        "id": "WILL_BE_SET_AT_PUBLICATION",
        "description": (
            "Identified sites for charging infrastructure for motorized "
            "individual travel using TracBEV"
        ),
        "language": "en-US",
        "keywords": [
            "mit",
            "charging",
            "infrastructure",
            "electromobility",
            "tracbev",
        ],
        "publicationDate": datetime.date.today().isoformat(),
        "context": context(),
        "spatial": {
            "location": None,
            "extent": "Germany",
            "resolution": "1 m",
        },
        "temporal": {
            "referenceDate": "2022-04-21",
            "timeseries": {
                "start": "",
                "end": "",
                "resolution": "",
                "alignment": "",
                "aggregationType": "",
            },
        },
        "sources": [
            {
                "title": "TracBEV input data",
                "description": (
                    "This data set is used with the software tool TracBEV to "
                    "calculate locations for charging infrastructure from "
                    "SimBEV results."
                ),
                "path": "https://zenodo.org/record/6466480#.YmE9xtPP1hE",
                "licenses": [license_odbl(attribution="© Schiel, Moritz")],
            }
        ],
        "licenses": [
            license_odbl(attribution="© eGon development team"),
        ],
        "contributors": contris,
        "resources": [
            {
                "profile": "tabular-data-resource",
                "name": "grid.egon_emob_charging_infrastructure",
                "path": "None",
                "format": "PostgreSQL",
                "encoding": "UTF-8",
                "schema": {
                    "fields": generate_resource_fields_from_db_table(
                        DATASET_CFG["targets"]["charging_infrastructure"][
                            "schema"
                        ],
                        DATASET_CFG["targets"]["charging_infrastructure"][
                            "table"
                        ],
                    ),
                    "primaryKey": "cp_id",
                },
                "dialect": {"delimiter": "", "decimalSeparator": ""},
            }
        ],
        "review": {"path": "", "badge": ""},
        "metaMetadata": meta_metadata(),
        "_comment": {
            "metadata": (
                "Metadata documentation and explanation (https://github.com/Op"
                "enEnergyPlatform/oemetadata/blob/master/metadata/v141/metadat"
                "a_key_description.md)"
            ),
            "dates": (
                "Dates and time must follow the ISO8601 including time zone "
                "(YYYY-MM-DD or YYYY-MM-DDThh:mm:ss±hh)"
            ),
            "units": "Use a space between numbers and units (100 m)",
            "languages": (
                "Languages must follow the IETF (BCP47) format (en-GB, en-US, "
                "de-DE)"
            ),
            "licenses": (
                "License name must follow the SPDX License List "
                "(https://spdx.org/licenses/)"
            ),
            "review": (
                "Following the OEP Data Review (https://github.com/OpenEnergyP"
                "latform/data-preprocessing/wiki)"
            ),
            "none": "If not applicable use (none)",
        },
    }

    db.submit_comment(
        f"'{json.dumps(meta)}'",
        DATASET_CFG["targets"]["charging_infrastructure"]["schema"],
        DATASET_CFG["targets"]["charging_infrastructure"]["table"],
    )
