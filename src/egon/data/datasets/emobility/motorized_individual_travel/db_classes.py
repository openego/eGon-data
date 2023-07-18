"""
DB tables / SQLAlchemy ORM classes for motorized individual travel
"""
import datetime
import json

from omi.dialects import get_dialect
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    SmallInteger,
    String,
)
from sqlalchemy.dialects.postgresql import REAL
from sqlalchemy.ext.declarative import declarative_base

from egon.data import db
from egon.data.datasets.emobility.motorized_individual_travel.helpers import (
    read_simbev_metadata_file,
)
from egon.data.datasets.mv_grid_districts import MvGridDistricts
from egon.data.datasets.scenario_parameters import EgonScenario
from egon.data.metadata import (
    context,
    contributors,
    generate_resource_fields_from_db_table,
    license_agpl,
    license_ccby,
    license_odbl,
    meta_metadata,
    oep_metadata_version,
    sources,
)

Base = declarative_base()


class EgonEvPool(Base):
    """Motorized individual travel: EV pool

    Each row is one EV, uniquely defined by either (`ev_id`) or
    (`rs7_id`, `type`, `simbev_id`).

    Columns
    -------
    ev_id:
        Unique id of EV
    rs7_id:
        id of RegioStar7 region
    type:
        type of EV, one of
            * bev_mini
            * bev_medium
            * bev_luxury
            * phev_mini
            * phev_medium
            * phev_luxury
    simbev_ev_id:
        id of EV as exported by simBEV
    """

    __tablename__ = "egon_ev_pool"
    __table_args__ = {"schema": "demand"}

    scenario = Column(String, ForeignKey(EgonScenario.name), primary_key=True)
    ev_id = Column(Integer, primary_key=True)
    rs7_id = Column(SmallInteger)
    type = Column(String(11))
    simbev_ev_id = Column(Integer)

    # trips = relationship(
    #     "EgonEvTrip", cascade="all, delete", back_populates="ev"
    # )
    # mvgds = relationship(
    #     "EgonEvMvGridDistrict", cascade="all, delete", back_populates="ev"
    # )


class EgonEvTrip(Base):
    """Motorized individual travel: EVs' trips

    Each row is one event of a specific electric vehicle which is
    uniquely defined by `rs7_id`, `ev_id` and `event_id`.

    Columns
    -------
    scenario:
        Scenario
    event_id:
        Unique id of EV event
    egon_ev_pool_ev_id:
        id of EV, references EgonEvPool.ev_id
    simbev_event_id:
        id of EV event, unique within a specific EV dataset
    location:
        Location of EV event, one of
            * "0_work"
            * "1_business"
            * "2_school"
            * "3_shopping"
            * "4_private/ridesharing"
            * "5_leisure"
            * "6_home"
            * "7_charging_hub"
            * "driving"
    use_case:
        Use case of EV event, one of
            * "public" (public charging)
            * "home" (private charging at 6_home)
            * "work" (private charging at 0_work)
            * <empty> (driving events)
    charging_capacity_nominal:
        Nominal charging capacity in kW
    charging_capacity_grid:
        Charging capacity at grid side in kW,
        includes efficiency of charging infrastructure
    charging_capacity_battery:
        Charging capacity at battery side in kW,
        includes efficiency of car charger
    soc_start:
        State of charge at start of event
    soc_start:
        State of charge at end of event
    charging_demand:
        Energy demand during parking/charging event in kWh.
        0 if no charging takes place.
    park_start:
        Start timestep of parking event (15min interval, e.g. 4 = 1h)
    park_end:
        End timestep of parking event (15min interval)
    drive_start:
        Start timestep of driving event (15min interval)
    drive_end:
        End timestep of driving event (15min interval)
    consumption:
        Energy demand during driving event in kWh

    Notes
    -----
    pgSQL's REAL is sufficient for floats as simBEV rounds output to 4 digits.
    """

    __tablename__ = "egon_ev_trip"
    __table_args__ = {"schema": "demand"}

    # scenario = Column(
    #    String, ForeignKey(EgonEvPool.scenario), primary_key=True
    # )
    scenario = Column(String, ForeignKey(EgonScenario.name), primary_key=True)
    event_id = Column(Integer, primary_key=True)
    # egon_ev_pool_ev_id = Column(
    #    Integer, ForeignKey(EgonEvPool.ev_id), nullable=False, index=True
    # )
    egon_ev_pool_ev_id = Column(Integer, nullable=False, index=True)
    simbev_event_id = Column(Integer)
    location = Column(String(21))
    use_case = Column(String(8))
    charging_capacity_nominal = Column(REAL)
    charging_capacity_grid = Column(REAL)
    charging_capacity_battery = Column(REAL)
    soc_start = Column(REAL)
    soc_end = Column(REAL)
    charging_demand = Column(REAL)
    park_start = Column(Integer)
    park_end = Column(Integer)
    drive_start = Column(Integer)
    drive_end = Column(Integer)
    consumption = Column(REAL)

    # __table_args__ = (
    #    ForeignKeyConstraint([scenario, egon_ev_pool_ev_id],
    #                         [EgonEvPool.scenario, EgonEvPool.ev_id]),
    #    {"schema": "demand"},
    # )

    # ev = relationship("EgonEvPool", back_populates="trips")


class EgonEvCountRegistrationDistrict(Base):
    """Electric vehicle counts per registration district"""

    __tablename__ = "egon_ev_count_registration_district"
    __table_args__ = {"schema": "demand"}

    scenario = Column(String, ForeignKey(EgonScenario.name), primary_key=True)
    scenario_variation = Column(String, primary_key=True)
    ags_reg_district = Column(Integer, primary_key=True)
    reg_district = Column(String)
    bev_mini = Column(Integer)
    bev_medium = Column(Integer)
    bev_luxury = Column(Integer)
    phev_mini = Column(Integer)
    phev_medium = Column(Integer)
    phev_luxury = Column(Integer)


class EgonEvCountMunicipality(Base):
    """Electric vehicle counts per municipality"""

    __tablename__ = "egon_ev_count_municipality"
    __table_args__ = {"schema": "demand"}

    scenario = Column(String, ForeignKey(EgonScenario.name), primary_key=True)
    scenario_variation = Column(String, primary_key=True)
    ags = Column(Integer, primary_key=True)
    bev_mini = Column(Integer)
    bev_medium = Column(Integer)
    bev_luxury = Column(Integer)
    phev_mini = Column(Integer)
    phev_medium = Column(Integer)
    phev_luxury = Column(Integer)
    rs7_id = Column(SmallInteger)


class EgonEvCountMvGridDistrict(Base):
    """Electric vehicle counts per MV grid district"""

    __tablename__ = "egon_ev_count_mv_grid_district"
    __table_args__ = {"schema": "demand"}

    scenario = Column(String, ForeignKey(EgonScenario.name), primary_key=True)
    scenario_variation = Column(String, primary_key=True)
    bus_id = Column(
        Integer, ForeignKey(MvGridDistricts.bus_id), primary_key=True
    )
    bev_mini = Column(Integer)
    bev_medium = Column(Integer)
    bev_luxury = Column(Integer)
    phev_mini = Column(Integer)
    phev_medium = Column(Integer)
    phev_luxury = Column(Integer)
    rs7_id = Column(SmallInteger)


class EgonEvMvGridDistrict(Base):
    """List of electric vehicles per MV grid district"""

    __tablename__ = "egon_ev_mv_grid_district"
    __table_args__ = {"schema": "demand"}

    id = Column(Integer, primary_key=True)
    scenario = Column(String, ForeignKey(EgonScenario.name), index=True)
    scenario_variation = Column(String, index=True)
    bus_id = Column(Integer, ForeignKey(MvGridDistricts.bus_id), index=True)
    # egon_ev_pool_ev_id = Column(Integer, ForeignKey(EgonEvPool.ev_id))
    egon_ev_pool_ev_id = Column(Integer, nullable=False)

    # ev = relationship("EgonEvPool", back_populates="mvgds")


class EgonEvMetadata(Base):
    """List of EV Pool Metadata"""

    __tablename__ = "egon_ev_metadata"
    __table_args__ = {"schema": "demand"}

    scenario = Column(String, primary_key=True, index=True)
    eta_cp = Column(Float)
    stepsize = Column(Integer)
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    soc_min = Column(Float)
    grid_timeseries = Column(Boolean)
    grid_timeseries_by_usecase = Column(Boolean)


def add_metadata():
    """
    Add metadata to tables egon_ev_metadata, egon_ev_mv_grid_district,
    egon_ev_trip in schema demand
    """
    # egon_ev_metadata
    schema = "demand"
    meta_run_config = read_simbev_metadata_file("eGon100RE", "config").loc[
        "basic"
    ]

    contris = contributors(["kh", "kh"])

    contris[0]["date"] = "2023-03-17"

    contris[0]["object"] = "metadata"
    contris[1]["object"] = "dataset"

    contris[0]["comment"] = "Add metadata to dataset."
    contris[1]["comment"] = "Add workflow to generate dataset."

    table = "egon_ev_metadata"
    name = f"{schema}.{table}"

    meta = {
        "name": name,
        "title": "eGon EV metadata",
        "id": "WILL_BE_SET_AT_PUBLICATION",
        "description": (
            "Metadata regarding the generation of EV trip profiles with SimBEV"
        ),
        "language": "en-US",
        "keywords": ["ev", "mit", "simbev", "metadata", "parameters"],
        "publicationDate": datetime.date.today().isoformat(),
        "context": context(),
        "spatial": {
            "location": "none",
            "extent": "none",
            "resolution": "none",
        },
        "temporal": {
            "referenceDate": f"{meta_run_config.start_date}",
            "timeseries": {},
        },
        "sources": [
            sources()["egon-data"],
            {
                "title": "SimBEV",
                "description": (
                    "Simulation of electric vehicle charging demand"
                ),
                "path": "https://github.com/rl-institut/simbev",
                "licenses": [
                    license_ccby(attribution="© Reiner Lemoine Institut")
                ],
            },
            {
                "title": "SimBEV",
                "description": (
                    "Simulation of electric vehicle charging demand"
                ),
                "path": "https://github.com/rl-institut/simbev",
                "licenses": [
                    license_agpl(attribution="© Reiner Lemoine Institut")
                ],
            },
        ],
        "licenses": [license_ccby()],
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
                    "primaryKey": "scenario",
                },
                "dialect": {"delimiter": "", "decimalSeparator": ""},
            }
        ],
        "review": {"path": "", "badge": ""},
        "metaMetadata": meta_metadata(),
        "_comment": {
            "metadata": (
                "Metadata documentation and explanation (https://github."
                "com/OpenEnergyPlatform/oemetadata/blob/master/metadata/"
                "v141/metadata_key_description.md)"
            ),
            "dates": (
                "Dates and time must follow the ISO8601 including time "
                "zone (YYYY-MM-DD or YYYY-MM-DDThh:mm:ss±hh)"
            ),
            "units": "Use a space between numbers and units (100 m)",
            "languages": (
                "Languages must follow the IETF (BCP47) format (en-GB, "
                "en-US, de-DE)"
            ),
            "licenses": (
                "License name must follow the SPDX License List "
                "(https://spdx.org/licenses/)"
            ),
            "review": (
                "Following the OEP Data Review (https://github.com/"
                "OpenEnergyPlatform/data-preprocessing/wiki)"
            ),
            "none": "If not applicable use (none)",
        },
    }

    dialect = get_dialect(oep_metadata_version())()

    meta = dialect.compile_and_render(dialect.parse(json.dumps(meta)))

    db.submit_comment(
        f"'{json.dumps(meta)}'",
        schema,
        table,
    )

    table = "egon_ev_mv_grid_district"
    name = f"{schema}.{table}"

    meta = {
        "name": name,
        "title": "eGon EV MV grid district",
        "id": "WILL_BE_SET_AT_PUBLICATION",
        "description": ("EV mapping to MV grids"),
        "language": "en-US",
        "keywords": ["ev", "mit", "simbev", "mv", "grid"],
        "publicationDate": datetime.date.today().isoformat(),
        "context": context(),
        "spatial": {
            "location": "none",
            "extent": "Germany",
            "resolution": "Grid district",
        },
        "temporal": {
            "referenceDate": f"{meta_run_config.start_date}",
            "timeseries": {},
        },
        "sources": [
            sources()["bgr_inspee"],
            sources()["bgr_inspeeds"],
            sources()["bgr_inspeeds_data_bundle"],
            sources()["bgr_inspeeds_report"],
            sources()["demandregio"],
            sources()["dsm-heitkoetter"],
            sources()["egon-data"],
            sources()["era5"],
            sources()["hotmaps_industrial_sites"],
            sources()["mastr"],
            sources()["nep2021"],
            sources()["openffe_gas"],
            sources()["openstreetmap"],
            sources()["peta"],
            sources()["pipeline_classification"],
            sources()["SciGRID_gas"],
            sources()["schmidt"],
            sources()["technology-data"],
            sources()["tyndp"],
            sources()["vg250"],
            sources()["zensus"],
            {
                "title": "SimBEV",
                "description": (
                    "Simulation of electric vehicle charging demand"
                ),
                "path": "https://github.com/rl-institut/simbev",
                "licenses": [
                    license_ccby(attribution="© Reiner Lemoine Institut")
                ],
            },
            {
                "title": "SimBEV",
                "description": (
                    "Simulation of electric vehicle charging demand"
                ),
                "path": "https://github.com/rl-institut/simbev",
                "licenses": [
                    license_agpl(attribution="© Reiner Lemoine Institut")
                ],
            },
        ],
        "licenses": [license_odbl()],
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
                    "primaryKey": "id",
                },
                "dialect": {"delimiter": "", "decimalSeparator": ""},
            }
        ],
        "review": {"path": "", "badge": ""},
        "metaMetadata": meta_metadata(),
        "_comment": {
            "metadata": (
                "Metadata documentation and explanation (https://github."
                "com/OpenEnergyPlatform/oemetadata/blob/master/metadata/"
                "v141/metadata_key_description.md)"
            ),
            "dates": (
                "Dates and time must follow the ISO8601 including time "
                "zone (YYYY-MM-DD or YYYY-MM-DDThh:mm:ss±hh)"
            ),
            "units": "Use a space between numbers and units (100 m)",
            "languages": (
                "Languages must follow the IETF (BCP47) format (en-GB, "
                "en-US, de-DE)"
            ),
            "licenses": (
                "License name must follow the SPDX License List "
                "(https://spdx.org/licenses/)"
            ),
            "review": (
                "Following the OEP Data Review (https://github.com/"
                "OpenEnergyPlatform/data-preprocessing/wiki)"
            ),
            "none": "If not applicable use (none)",
        },
    }

    dialect = get_dialect(oep_metadata_version())()

    meta = dialect.compile_and_render(dialect.parse(json.dumps(meta)))

    db.submit_comment(
        f"'{json.dumps(meta)}'",
        schema,
        table,
    )

    table = "egon_ev_trip"
    name = f"{schema}.{table}"

    meta = {
        "name": name,
        "title": "eGon EV trip profiles",
        "id": "WILL_BE_SET_AT_PUBLICATION",
        "description": ("EV trip profiles generated with SimBEV"),
        "language": "en-US",
        "keywords": ["ev", "mit", "simbev", "trip", "profiles"],
        "publicationDate": datetime.date.today().isoformat(),
        "context": context(),
        "spatial": {
            "location": "none",
            "extent": "Germany",
            "resolution": "none",
        },
        "temporal": {
            "referenceDate": f"{meta_run_config.start_date}",
            "timeseries": {},
        },
        "sources": [
            sources()["bgr_inspee"],
            sources()["bgr_inspeeds"],
            sources()["bgr_inspeeds_data_bundle"],
            sources()["bgr_inspeeds_report"],
            sources()["demandregio"],
            sources()["dsm-heitkoetter"],
            sources()["egon-data"],
            sources()["era5"],
            sources()["hotmaps_industrial_sites"],
            sources()["mastr"],
            sources()["nep2021"],
            sources()["openffe_gas"],
            sources()["openstreetmap"],
            sources()["peta"],
            sources()["pipeline_classification"],
            sources()["SciGRID_gas"],
            sources()["schmidt"],
            sources()["technology-data"],
            sources()["tyndp"],
            sources()["vg250"],
            sources()["zensus"],
            {
                "title": "SimBEV",
                "description": (
                    "Simulation of electric vehicle charging demand"
                ),
                "path": "https://github.com/rl-institut/simbev",
                "licenses": [
                    license_ccby(attribution="© Reiner Lemoine Institut")
                ],
            },
            {
                "title": "SimBEV",
                "description": (
                    "Simulation of electric vehicle charging demand"
                ),
                "path": "https://github.com/rl-institut/simbev",
                "licenses": [
                    license_agpl(attribution="© Reiner Lemoine Institut")
                ],
            },
        ],
        "licenses": [license_odbl()],
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
                    "primaryKey": ["scenario", "event_id"],
                },
                "dialect": {"delimiter": "", "decimalSeparator": ""},
            }
        ],
        "review": {"path": "", "badge": ""},
        "metaMetadata": meta_metadata(),
        "_comment": {
            "metadata": (
                "Metadata documentation and explanation (https://github."
                "com/OpenEnergyPlatform/oemetadata/blob/master/metadata/"
                "v141/metadata_key_description.md)"
            ),
            "dates": (
                "Dates and time must follow the ISO8601 including time "
                "zone (YYYY-MM-DD or YYYY-MM-DDThh:mm:ss±hh)"
            ),
            "units": "Use a space between numbers and units (100 m)",
            "languages": (
                "Languages must follow the IETF (BCP47) format (en-GB, "
                "en-US, de-DE)"
            ),
            "licenses": (
                "License name must follow the SPDX License List "
                "(https://spdx.org/licenses/)"
            ),
            "review": (
                "Following the OEP Data Review (https://github.com/"
                "OpenEnergyPlatform/data-preprocessing/wiki)"
            ),
            "none": "If not applicable use (none)",
        },
    }

    dialect = get_dialect(oep_metadata_version())()

    meta = dialect.compile_and_render(dialect.parse(json.dumps(meta)))

    db.submit_comment(
        f"'{json.dumps(meta)}'",
        schema,
        table,
    )
