"""
DB tables / SQLAlchemy ORM classes for motorized individual travel
"""

import datetime
import json

from omi.dialects import get_dialect
from sqlalchemy import (
    BigInteger,
    Column,
    ForeignKey,
    Index,
    Integer,
    SmallInteger,
    String,
)
from sqlalchemy.dialects.postgresql import JSONB, REAL
from sqlalchemy.ext.declarative import declarative_base

from egon.data import config, db
from egon.data.datasets.emobility.mit_lgv_input_data import (
    ZENODO_ENVIRONMENT,
    ZENODO_URLS,
)
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
    sources,
)

Base = declarative_base()


class EgonEvMitLgvPool(Base):
    """
    Class definition of table demand.egon_ev_mit_lgv_pool.

    Each row is one EV profile of the pool, uniquely defined by
    (`scenario`, `ev_id`).

    **Columns**

    scenario:
        Scenario
    ev_id:
        Unique id of EV
    rs7_id:
        id of RegioStar7 region
    type:
        type of EV. New methodology (nine types, cf. `EV_TYPES`)
            * bev_mini, bev_medium, bev_luxury
            * phev_mini, phev_medium, phev_luxury
            * bev_commercial, phev_commercial
              (commercially registered passenger cars, M1)
            * bev_light_duty_vehicle
              (light commercial vehicle < 3.5 t, N1)
        Legacy methodology: the six private types only.
    simbev_ev_id:
        id of EV as exported by simBEV. Legacy methodology only, NULL
        for the new one (D16).
    """

    __tablename__ = "egon_ev_mit_lgv_pool"
    __table_args__ = {"schema": "demand"}

    scenario = Column(String, ForeignKey(EgonScenario.name), primary_key=True)
    ev_id = Column(Integer, primary_key=True)
    rs7_id = Column(SmallInteger)
    # `bev_light_duty_vehicle` is 22 characters, the longest value.
    type = Column(String(24))
    simbev_ev_id = Column(Integer)


class EgonEvMitLgvTrip(Base):
    """
    Class definition of table demand.egon_ev_mit_lgv_trip.

    Each row is one event (driving or parking) of a specific electric
    vehicle profile, uniquely defined by (`scenario`, `event_id`).

    **Columns**

    scenario:
        Scenario
    event_id:
        Unique id of EV event
    ev_id:
        id of EV, references EgonEvMitLgvPool.ev_id
    simbev_event_id:
        id of EV event, unique within a specific EV dataset. Legacy
        methodology only, NULL for the new one (D16).
    location:
        Location of EV event. Two parallel vocabularies in the new
        methodology, selected by the vehicle's `vehicle_group`:

        * `private`: home, private, leisure, work, shopping, business,
          school, hpc, driving
        * `pkw_commercial` and `light_duty_vehicle`: nach_hause,
          arbeitsplatz, einkauf, freizeit, sonstige_privat,
          sonstige_dienstlich, personen, dienstleistung, gueter,
          rueckfahrt_betrieb, hpc, driving

        Legacy methodology: "0_work", "1_business", "2_school",
        "3_shopping", "4_private/ridesharing", "5_leisure", "6_home",
        "7_charging_hub", "driving".
    use_case:
        Charging use case of the EV event. New methodology (the
        delivered `charging_use_case`, renamed on import, D12): depot,
        home_detached, home_apartment, work, street, retail, urban_fast,
        highway_fast, or NULL for driving events and for parking events
        without charging.
        Legacy methodology: "public", "home", "work" or empty.
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
    soc_end:
        State of charge at end of event
    charging_demand:
        Energy demand during parking/charging event in kWh (battery
        side). 0 if no charging takes place.
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
    pgSQL's REAL is sufficient for floats as simBEV rounds output to 4
    digits.

    The index on (`scenario`, `ev_id`, `event_id`) is what makes the
    "first event of an EV" lookup of
    :func:`egon.data.datasets.emobility.motorized_individual_travel.model_timeseries.first_event_soc`
    cheap; do not drop it.
    """

    __tablename__ = "egon_ev_mit_lgv_trip"
    __table_args__ = (
        Index(
            "idx_egon_ev_mit_lgv_trip_scenario_ev_event",
            "scenario",
            "ev_id",
            "event_id",
        ),
        {"schema": "demand"},
    )

    scenario = Column(String, ForeignKey(EgonScenario.name), primary_key=True)
    event_id = Column(BigInteger, primary_key=True)
    ev_id = Column(Integer, nullable=False, index=True)
    simbev_event_id = Column(Integer)
    # Longest new value: `sonstige_dienstlich` (19).
    location = Column(String(21))
    # Longest new value: `home_apartment` (14).
    use_case = Column(String(20))
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


class EgonEvMitLgvCountRegistrationDistrict(Base):
    """
    Class definition of table
    demand.egon_ev_mit_lgv_count_registration_district.

    Contains electric vehicle counts per registration district.

    Legacy methodology only: the new one takes the vehicle counts per
    municipality as delivered input data and therefore has no
    registration district level (D5).
    """

    __tablename__ = "egon_ev_mit_lgv_count_registration_district"
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


class EgonEvMitLgvCountMunicipality(Base):
    """
    Class definition of table demand.egon_ev_mit_lgv_count_municipality.

    Contains electric vehicle counts per municipality.

    The three commercial columns are NULL for legacy scenarios (D13).
    """

    __tablename__ = "egon_ev_mit_lgv_count_municipality"
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
    bev_commercial = Column(Integer)
    phev_commercial = Column(Integer)
    bev_light_duty_vehicle = Column(Integer)
    rs7_id = Column(SmallInteger)


class EgonEvMitLgvCountMvGridDistrict(Base):
    """
    Class definition of table
    demand.egon_ev_mit_lgv_count_mv_grid_district.

    Contains electric vehicle counts per MV grid district.

    The three commercial columns are NULL for legacy scenarios (D13).
    """

    __tablename__ = "egon_ev_mit_lgv_count_mv_grid_district"
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
    bev_commercial = Column(Integer)
    phev_commercial = Column(Integer)
    bev_light_duty_vehicle = Column(Integer)
    rs7_id = Column(SmallInteger)


class EgonEvMitLgvMvGridDistrict(Base):
    """
    Class definition of table demand.egon_ev_mit_lgv_mv_grid_district.

    Contains the list of electric vehicles per MV grid district: one row
    per vehicle instance.

    **Columns**

    ags:
        Municipality the vehicle instance was delivered for. New
        methodology only (D14), NULL for legacy scenarios, which draw
        vehicles per grid district without a municipal reference.
    """

    __tablename__ = "egon_ev_mit_lgv_mv_grid_district"
    __table_args__ = {"schema": "demand"}

    id = Column(BigInteger, primary_key=True)
    scenario = Column(String, ForeignKey(EgonScenario.name), index=True)
    scenario_variation = Column(String, index=True)
    bus_id = Column(Integer, ForeignKey(MvGridDistricts.bus_id), index=True)
    ev_id = Column(Integer, nullable=False)
    ags = Column(Integer, index=True)


class EgonEvMitLgvMappingEvMunicipality(Base):
    """
    Class definition of table
    demand.egon_ev_mit_lgv_mapping_ev_municipality.

    The delivered allocation of vehicle instances to municipalities: one
    row per vehicle (D7). A pool EV occurring *n* times in the same
    `ags` yields *n* rows with distinct `id`.

    New methodology only.
    """

    __tablename__ = "egon_ev_mit_lgv_mapping_ev_municipality"
    __table_args__ = {"schema": "demand"}

    id = Column(BigInteger, primary_key=True)
    scenario = Column(String, ForeignKey(EgonScenario.name), primary_key=True)
    ev_id = Column(Integer, nullable=False, index=True)
    ags = Column(Integer, nullable=False, index=True)


class EgonEvMitLgvMetadata(Base):
    """
    Class definition of table demand.egon_ev_mit_lgv_metadata.

    Holds the run configurations of the input data generation, verbatim
    (D17).

    **Columns**

    simbev_config:
        The complete `metadata_simbev_run.json`, including the technical
        data per vehicle type.
    geolis_config:
        The complete `metadata_geolis_run.json`. NULL for legacy
        scenarios, which ship no GeoLIS file.

    Notes
    -----
    The values inside `config.basic` are **strings** (`"0.9"`, `"15"`,
    `"0.1"`); cast at the point of use, do not assume numeric types.

    Earlier versions mirrored individual config keys as columns. The
    delivered key set is not stable -- between the old bundle and
    delivery v1.4 it lost `grid_timeseries` and
    `grid_timeseries_by_usecase` and gained a dozen others -- so the
    document is now stored whole.
    """

    __tablename__ = "egon_ev_mit_lgv_metadata"
    __table_args__ = {"schema": "demand"}

    scenario = Column(String, primary_key=True, index=True)
    simbev_config = Column(JSONB)
    geolis_config = Column(JSONB)


def _simbev_sources():
    """OEP metadata source entries of the simBEV tool."""
    return [
        {
            "title": "SimBEV",
            "description": ("Simulation of electric vehicle charging demand"),
            "path": "https://github.com/rl-institut/simbev",
            "licenses": [
                license_ccby(attribution="© Reiner Lemoine Institut")
            ],
        },
        {
            "title": "SimBEV",
            "description": ("Simulation of electric vehicle charging demand"),
            "path": "https://github.com/rl-institut/simbev",
            "licenses": [
                license_agpl(attribution="© Reiner Lemoine Institut")
            ],
        },
    ]


def _delivered_input_data_sources():
    """OEP metadata source entries of the delivered M1+N1 bundles.

    One entry per scenario, so the provenance of the new methodology is
    reconstructible from the metadata alone (D18). Together with the run
    configurations stored verbatim in
    :class:`EgonEvMitLgvMetadata` this covers the complete input side.
    """
    return [
        {
            "title": f"eMobility MIT/LGV input data ({scenario_name})",
            "description": (
                "Vehicle pool, events, vehicle counts per municipality, "
                "charging locations and the allocation of vehicles to "
                "municipalities, generated with simBEV and GeoLIS for "
                f"scenario {scenario_name}"
            ),
            "path": url,
            "licenses": [
                license_ccby(attribution="© Reiner Lemoine Institut")
            ],
        }
        for scenario_name, url in ZENODO_URLS[ZENODO_ENVIRONMENT].items()
    ]


def _all_egon_sources():
    """The full list of upstream sources of the egon-data pipeline."""
    return [
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
    ]


_METADATA_COMMENT = {
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
}


def _submit_table_metadata(
    schema,
    table,
    title,
    description,
    keywords,
    primary_key,
    reference_date,
    contris,
    table_sources,
    table_licenses,
    spatial=None,
):
    """Render and submit the OEP metadata string of one table."""
    name = f"{schema}.{table}"

    meta = {
        "name": name,
        "title": title,
        "id": "WILL_BE_SET_AT_PUBLICATION",
        "description": description,
        "language": "en-US",
        "keywords": keywords,
        "publicationDate": datetime.date.today().isoformat(),
        "context": context(),
        "spatial": spatial
        or {"location": "none", "extent": "none", "resolution": "none"},
        "temporal": {
            "referenceDate": reference_date,
            "timeseries": {},
        },
        "sources": table_sources,
        "licenses": table_licenses,
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
                    "primaryKey": primary_key,
                },
                "dialect": {"delimiter": "", "decimalSeparator": ""},
            }
        ],
        "review": {"path": "", "badge": ""},
        "metaMetadata": meta_metadata(),
        "_comment": _METADATA_COMMENT,
    }

    dialect = get_dialect(f"oep-v{meta_metadata()['metadataVersion'][4:7]}")()
    meta = dialect.compile_and_render(dialect.parse(json.dumps(meta)))

    db.submit_comment(f"'{json.dumps(meta)}'", schema, table)


def add_metadata():
    """Add OEP metadata to all tables of the MIT/LGV dataset.

    Covers the tables of both methodologies, the two tables imported
    from the delivered bundle and the three flexibility diagnostics of
    :mod:`egon.data.datasets.emobility.motorized_individual_travel.flex_diagnostics`.
    """
    schema = "demand"

    # The simBEV run config is only used to describe the parameters in
    # the metadata string, so any configured scenario will do.
    meta_run_config = read_simbev_metadata_file(
        config.settings()["egon-data"]["--scenarios"][0], "config"
    ).loc["basic"]
    reference_date = f"{meta_run_config.start_date}"

    contris = contributors(["kh", "kh", "ja", "ja"])
    contris[0]["date"] = "2023-03-17"
    contris[1]["date"] = "2023-03-17"
    contris[2]["date"] = "2026-09-08"
    contris[3]["date"] = "2026-09-08"
    contris[0]["object"] = "metadata"
    contris[1]["object"] = "dataset"
    contris[2]["object"] = "metadata"
    contris[3]["object"] = "dataset"
    contris[0]["comment"] = "Add metadata to dataset."
    contris[1]["comment"] = "Add workflow to generate dataset."
    contris[2]["comment"] = "Add metadata of M1+N1 tables."
    contris[3]["comment"] = "Add M1+N1 methodology."

    simbev = _simbev_sources()
    delivered = _delivered_input_data_sources()
    everything = _all_egon_sources()

    germany_grid_district = {
        "location": "none",
        "extent": "Germany",
        "resolution": "Grid district",
    }
    germany_municipality = {
        "location": "none",
        "extent": "Germany",
        "resolution": "Municipality",
    }

    tables = [
        {
            "table": "egon_ev_mit_lgv_metadata",
            "title": "eGon EV/LGV run metadata",
            "description": (
                "Run configurations of the input data generation for "
                "motorized individual travel (M1) and light commercial "
                "vehicles (N1): the complete simBEV and GeoLIS run "
                "configurations as delivered, stored verbatim as JSONB. "
                "geolis_config is NULL for scenarios on the legacy "
                "methodology, which ship no GeoLIS run."
            ),
            "keywords": ["ev", "mit", "lgv", "simbev", "geolis", "metadata"],
            "primary_key": "scenario",
            "sources": [sources()["egon-data"]] + simbev + delivered,
            "licenses": [license_ccby()],
        },
        {
            "table": "egon_ev_mit_lgv_pool",
            "title": "eGon EV/LGV profile pool",
            "description": (
                "Pool of pre-generated vehicle profiles for motorized "
                "individual travel (M1) and light commercial vehicles "
                "(N1). Each row is one profile; profiles are "
                "instantiated multiple times across Germany."
            ),
            "keywords": ["ev", "mit", "lgv", "simbev", "pool"],
            "primary_key": ["scenario", "ev_id"],
            "sources": [sources()["egon-data"]] + simbev + delivered,
            "licenses": [license_odbl()],
        },
        {
            "table": "egon_ev_mit_lgv_trip",
            "title": "eGon EV/LGV trip profiles",
            "description": (
                "Driving and parking events of the vehicle profile "
                "pool. use_case carries the charging use case of the "
                "event and is NULL for driving events and for parking "
                "events without charging."
            ),
            "keywords": ["ev", "mit", "lgv", "simbev", "trip", "profiles"],
            "primary_key": ["scenario", "event_id"],
            "sources": everything + simbev + delivered,
            "licenses": [license_odbl()],
        },
        {
            "table": "egon_ev_mit_lgv_count_registration_district",
            "title": "eGon EV count per registration district",
            "description": (
                "Number of electric vehicles per registration district. "
                "Written for scenarios on the legacy methodology only; "
                "the new methodology takes the vehicle counts per "
                "municipality as delivered input data."
            ),
            "keywords": ["ev", "mit", "count", "registration district"],
            "primary_key": [
                "scenario",
                "scenario_variation",
                "ags_reg_district",
            ],
            "sources": everything + simbev,
            "licenses": [license_odbl()],
        },
        {
            "table": "egon_ev_mit_lgv_count_municipality",
            "title": "eGon EV/LGV count per municipality",
            "description": (
                "Number of vehicles per type and municipality. The "
                "columns bev_commercial, phev_commercial and "
                "bev_light_duty_vehicle are NULL for scenarios on the "
                "legacy methodology."
            ),
            "keywords": ["ev", "mit", "lgv", "count", "municipality"],
            "primary_key": ["scenario", "scenario_variation", "ags"],
            "sources": everything + simbev + delivered,
            "licenses": [license_odbl()],
            "spatial": germany_municipality,
        },
        {
            "table": "egon_ev_mit_lgv_count_mv_grid_district",
            "title": "eGon EV/LGV count per MV grid district",
            "description": (
                "Number of vehicles per type and MV grid district. The "
                "columns bev_commercial, phev_commercial and "
                "bev_light_duty_vehicle are NULL for scenarios on the "
                "legacy methodology."
            ),
            "keywords": ["ev", "mit", "lgv", "count", "mv", "grid"],
            "primary_key": ["scenario", "scenario_variation", "bus_id"],
            "sources": everything + simbev + delivered,
            "licenses": [license_odbl()],
            "spatial": germany_grid_district,
        },
        {
            "table": "egon_ev_mit_lgv_mv_grid_district",
            "title": "eGon EV/LGV MV grid district",
            "description": (
                "Allocation of vehicle instances to MV grid districts: "
                "one row per vehicle. ags names the municipality the "
                "vehicle was delivered for and is NULL for scenarios on "
                "the legacy methodology."
            ),
            "keywords": ["ev", "mit", "lgv", "mv", "grid"],
            "primary_key": "id",
            "sources": everything + simbev + delivered,
            "licenses": [license_odbl()],
            "spatial": germany_grid_district,
        },
        {
            "table": "egon_ev_mit_lgv_mapping_ev_municipality",
            "title": "eGon EV/LGV vehicles per municipality",
            "description": (
                "Delivered allocation of vehicle instances to "
                "municipalities: one row per vehicle. New methodology "
                "only. The companion mapping of charging events to "
                "charging locations is not imported into the database; "
                "it is available as ev_mapping_event_location.parquet "
                "in the extracted input data archive."
            ),
            "keywords": ["ev", "mit", "lgv", "municipality", "mapping"],
            "primary_key": ["id", "scenario"],
            "sources": [sources()["egon-data"], sources()["vg250"]]
            + simbev
            + delivered,
            "licenses": [license_odbl()],
            "spatial": germany_municipality,
        },
        {
            "table": "egon_ev_mit_lgv_flex_timeseries",
            "title": "eGon EV/LGV charging and driving load",
            "description": (
                "Flexibility diagnostics per MV grid district: the "
                "grid-side dumb charging load, the share of it that "
                "occurs at a use case eligible for flexible charging, "
                "and the battery-side driving load, all hourly for one "
                "year. charging_load_grid_flex is a potential, not a "
                "realised flexibility. Sum(driving_load) equals "
                "eta_cp * Sum(charging_load_grid) up to the PHEV fuel "
                "share and the annual battery state-of-charge drift."
            ),
            "keywords": [
                "ev",
                "mit",
                "lgv",
                "flexibility",
                "charging",
                "timeseries",
            ],
            "primary_key": ["scenario", "bus_id"],
            "sources": everything + simbev + delivered,
            "licenses": [license_odbl()],
            "spatial": germany_grid_district,
        },
        {
            "table": "egon_ev_mit_lgv_energy_balance",
            "title": "eGon EV/LGV annual energy balance",
            "description": (
                "Annual energy balance per MV grid district, charging "
                "use case and vehicle type, carrying both the "
                "battery-side and the grid-side charging energy. "
                "driving_consumption_MWh is the raw consumption of the "
                "events and is not interchangeable with the modelled "
                "driving load of egon_ev_mit_lgv_flex_timeseries: the "
                "latter is filled only where the state of charge "
                "actually decreases, so PHEV kilometres driven on fuel "
                "do not enter it."
            ),
            "keywords": [
                "ev",
                "mit",
                "lgv",
                "energy balance",
                "use case",
                "flexibility",
            ],
            "primary_key": ["scenario", "bus_id", "use_case", "type"],
            "sources": everything + simbev + delivered,
            "licenses": [license_odbl()],
            "spatial": germany_grid_district,
        },
        {
            "table": "egon_ev_mit_lgv_charging_profile_use_case",
            "title": "eGon EV/LGV charging profile per use case",
            "description": (
                "Grid-side dumb charging load per MV grid district, "
                "charging use case and hour. Only use cases that occur "
                "in a grid district get a row. Aggregated over all use "
                "cases this equals charging_load_grid of "
                "egon_ev_mit_lgv_flex_timeseries."
            ),
            "keywords": [
                "ev",
                "mit",
                "lgv",
                "charging",
                "use case",
                "timeseries",
            ],
            "primary_key": ["scenario", "bus_id", "use_case"],
            "sources": everything + simbev + delivered,
            "licenses": [license_odbl()],
            "spatial": germany_grid_district,
        },
    ]

    for spec in tables:
        _submit_table_metadata(
            schema=schema,
            table=spec["table"],
            title=spec["title"],
            description=spec["description"],
            keywords=spec["keywords"],
            primary_key=spec["primary_key"],
            reference_date=reference_date,
            contris=contris,
            table_sources=spec["sources"],
            table_licenses=spec["licenses"],
            spatial=spec.get("spatial"),
        )
