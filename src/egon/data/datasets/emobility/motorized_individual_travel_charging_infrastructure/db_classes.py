"""
DB tables / SQLAlchemy ORM classes for charging infrastructure
"""

import datetime
import json

from geoalchemy2 import Geometry
from omi.dialects import get_dialect
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Float,
    ForeignKey,
    Integer,
    String,
)
from sqlalchemy.ext.declarative import declarative_base

from egon.data import config, db
from egon.data.datasets import load_sources_and_targets
from egon.data.datasets.emobility.mit_lgv_input_data import (
    ZENODO_ENVIRONMENT,
    ZENODO_URLS,
)
from egon.data.datasets.mv_grid_districts import MvGridDistricts
from egon.data.metadata import (
    context,
    contributors,
    generate_resource_fields_from_db_table,
    license_ccby,
    license_odbl,
    meta_metadata,
)

Base = declarative_base()


class EgonEmobChargingInfrastructure(Base):
    """
    Class definition of table grid.egon_emob_charging_infrastructure.
    """

    __tablename__ = "egon_emob_charging_infrastructure"
    __table_args__ = {"schema": "grid"}

    cp_id = Column(Integer, primary_key=True)
    mv_grid_id = Column(Integer)
    use_case = Column(String)
    weight = Column(Float)

    # SRID 3035 from YML)
    geometry = Column(Geometry(srid=3035))


class EgonEvMitLgvChargingLocation(Base):
    """
    Class definition of table demand.egon_ev_mit_lgv_charging_location.

    The charging sites delivered with the M1+N1 input data. Generated
    together with the vehicles and their events, so the two are mutually
    consistent.

    **Columns**

    location_id:
        Provider-side id of the site. Unique across scenarios, but
        **sparse** -- do not assume density.
    charging_points:
        Number of charging points at the site.
    average_charging_capacity:
        Average charging capacity of a charging point at the site, in
        kW. Zero for a handful of `street` sites in delivery v1.4.
    use_case:
        Charging use case of the site, one of depot, home_detached,
        home_apartment, work, street, retail, urban_fast, highway_fast.
    candidate_uid:
        Provider-side site id within the use case.
    is_synthetic_location:
        Whether the site is a fallback centroid rather than a real
        candidate site. In delivery v1.4 all synthetic sites are
        `highway_fast` municipality centroids, generated where a
        municipality has no real candidate. Consumers placing high power
        charging infrastructure need to be able to tell them apart.
    mv_grid_id:
        MV grid district the site lies in, from a point-in-polygon join.
        NULL for sites outside every grid district; those are logged,
        not dropped.

    Notes
    -----
    `scenario` is part of the primary key so that one scenario can be
    deleted and rewritten independently.

    This table is created and filled by the charging infrastructure
    dataset, not by the MIT dataset, although it lives in the same
    schema as the other M1+N1 tables. The two datasets run in parallel,
    so exactly one of them may own it.
    """

    __tablename__ = "egon_ev_mit_lgv_charging_location"
    __table_args__ = {"schema": "demand"}

    location_id = Column(BigInteger, primary_key=True)
    scenario = Column(String, primary_key=True)
    charging_points = Column(Integer)
    average_charging_capacity = Column(Integer)
    use_case = Column(String(20))
    candidate_uid = Column(String)
    is_synthetic_location = Column(Boolean)
    mv_grid_id = Column(
        Integer, ForeignKey(MvGridDistricts.bus_id), index=True
    )
    geom = Column(Geometry("POINT", 3035))


def add_metadata():
    """
    Add metadata to the tables of the charging infrastructure dataset
    """
    sources, targets = load_sources_and_targets("MITChargingInfrastructure")

    full_table_name = targets.tables["charging_infrastructure"]
    target_schema, target_table = full_table_name.split(".")

    contris = contributors(["kh", "kh"])

    contris[0]["date"] = "2023-03-14"

    contris[0]["object"] = "metadata"
    contris[1]["object"] = "dataset"

    contris[0]["comment"] = "Add metadata to dataset."
    contris[1]["comment"] = "Add workflow to generate dataset."

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
            "timeseries": {},
        },
        "sources": [
            {
                "title": "TracBEV input data",
                "description": (
                    "This data set is used with the software tool TracBEV to "
                    "calculate locations for charging infrastructure from "
                    "SimBEV results."
                ),
                "path": sources.urls["tracbev"],
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
                        target_schema,
                        target_table,
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

    dialect = get_dialect(f"oep-v{meta_metadata()['metadataVersion'][4:7]}")()

    meta = dialect.compile_and_render(dialect.parse(json.dumps(meta)))

    db.submit_comment(
        f"'{json.dumps(meta)}'",
        target_schema,
        target_table,
    )

    _add_charging_location_metadata(contris)


def _add_charging_location_metadata(contris):
    """
    Add metadata to table demand.egon_ev_mit_lgv_charging_location
    """
    schema = EgonEvMitLgvChargingLocation.__table__.schema
    table = EgonEvMitLgvChargingLocation.__table__.name
    name = f"{schema}.{table}"

    meta = {
        "name": name,
        "title": "eGon EV/LGV charging locations",
        "id": "WILL_BE_SET_AT_PUBLICATION",
        "description": (
            "Charging sites for motorized individual travel (M1) and "
            "light commercial vehicles (N1), delivered together with "
            "the vehicle profiles and their events so that the two are "
            "mutually consistent. is_synthetic_location marks fallback "
            "centroids rather than real candidate sites. mv_grid_id is "
            "NULL for sites outside every MV grid district. The mapping "
            "of charging events to these locations is not part of the "
            "database; it is available as "
            "ev_mapping_event_location.parquet in the extracted input "
            "data archive."
        ),
        "language": "en-US",
        "keywords": [
            "mit",
            "lgv",
            "charging",
            "infrastructure",
            "electromobility",
            "geolis",
        ],
        "publicationDate": datetime.date.today().isoformat(),
        "context": context(),
        "spatial": {
            "location": None,
            "extent": "Germany",
            "resolution": "1 m",
        },
        "temporal": {
            "referenceDate": datetime.date.today().isoformat(),
            "timeseries": {},
        },
        "sources": [
            {
                "title": (f"eMobility MIT/LGV input data ({scenario_name})"),
                "description": (
                    "Vehicle pool, events, vehicle counts per "
                    "municipality, charging locations and the "
                    "allocation of vehicles to municipalities, "
                    "generated with simBEV and GeoLIS for scenario "
                    f"{scenario_name}"
                ),
                "path": url,
                "licenses": [
                    license_ccby(attribution="© Reiner Lemoine Institut")
                ],
            }
            for scenario_name, url in ZENODO_URLS[ZENODO_ENVIRONMENT].items()
        ],
        "licenses": [
            license_odbl(attribution="© eGon development team"),
        ],
        "contributors": contris,
        "resources": [
            {
                "profile": "tabular-data-resource",
                "name": name,
                "path": "None",
                "format": "PostgreSQL",
                "encoding": "UTF-8",
                "schema": {
                    "fields": generate_resource_fields_from_db_table(
                        schema,
                        table,
                    ),
                    "primaryKey": ["location_id", "scenario"],
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

    dialect = get_dialect(f"oep-v{meta_metadata()['metadataVersion'][4:7]}")()

    meta = dialect.compile_and_render(dialect.parse(json.dumps(meta)))

    db.submit_comment(
        f"'{json.dumps(meta)}'",
        schema,
        table,
    )
