"""
DB tables / SQLAlchemy ORM classes for motorized individual travel
"""


from geoalchemy2 import Geometry
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    ForeignKey,
    Index,
    Integer,
    SmallInteger,
    String,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, REAL
from sqlalchemy.ext.declarative import declarative_base

from egon.data.datasets.mv_grid_districts import MvGridDistricts
from egon.data.datasets.scenario_parameters import EgonScenario

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
    n_vehicles:
        Number of vehicles instantiated from the profile across all of
        Germany, i.e. how often each of its charging events is placed.
        New methodology only, NULL for the legacy one.
    """

    __tablename__ = "egon_ev_mit_lgv_pool"
    __table_args__ = {"schema": "demand"}

    scenario = Column(String, ForeignKey(EgonScenario.name), primary_key=True)
    ev_id = Column(Integer, primary_key=True)
    rs7_id = Column(SmallInteger)
    # `bev_light_duty_vehicle` is 22 characters, the longest value.
    type = Column(String(24))
    simbev_ev_id = Column(Integer)
    n_vehicles = Column(Integer)


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
        End timestep of parking event (15min interval). For `street`
        events of the new methodology the shortened end GeoLIS placed
        them with, not the delivered one (`limit_street_park_time`).
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


class EgonEvMitLgvChargingLocation(Base):
    """
    Class definition of table demand.egon_ev_mit_lgv_charging_location.

    The charging locations delivered with the M1+N1 input data, generated
    together with the vehicles and their events, and their assignment to
    the grid. New methodology only.

    **Columns**

    location_id:
        Provider-side id of the location, unique per scenario.
    charging_points:
        Number of charging points at the location. A point is occupied
        while a vehicle is plugged in, `[park_start, park_end)`; no
        location holds more vehicles at once than it has points.
    average_charging_capacity:
        Average charging capacity of a charging point, in kW.
    use_case:
        Charging use case of the location, one of depot, home_detached,
        home_apartment, work, street, retail, urban_fast, highway_fast.
    candidate_uid:
        Provider-side id of the location within its use case.
    is_synthetic_location:
        Whether the location is a fallback centroid rather than a real
        candidate site. Occurs in any use case.
    mv_grid_id:
        MV grid district the location lies in (point-in-polygon), or the
        nearest one for a location outside every district, cf.
        `assignment`.
    grid_connection_id:
        Groups the locations at exactly identical coordinates, which
        share one grid connection. Unique per scenario.
    voltage_level:
        Voltage level (1-7) of the grid connection, from the summed
        installed capacity of all its locations.
    model_type:
        `district` (voltage levels 4-7): part of the district model of
        `mv_grid_id`; `transmission` (1-3): a model of its own per grid
        connection.
    bus_id:
        eTraGo AC bus of the model: the MV grid district's bus for
        voltage levels 3-7, the EHV substation's bus for levels 1-2.
    assignment:
        `within` if the location lies in `mv_grid_id`, `nearest` if it
        was assigned to the nearest district.

    Notes
    -----
    The placements -- which charging events happen at a location -- are
    in :class:`EgonEvMitLgvLocationEvents`.
    """

    __tablename__ = "egon_ev_mit_lgv_charging_location"
    __table_args__ = (
        Index(
            "idx_egon_ev_mit_lgv_charging_location_model",
            "scenario",
            "model_type",
            "bus_id",
        ),
        Index(
            "idx_egon_ev_mit_lgv_charging_location_connection",
            "scenario",
            "grid_connection_id",
        ),
        {"schema": "demand"},
    )

    location_id = Column(BigInteger, primary_key=True)
    scenario = Column(String, ForeignKey(EgonScenario.name), primary_key=True)
    charging_points = Column(Integer)
    average_charging_capacity = Column(Integer)
    use_case = Column(String(20))
    candidate_uid = Column(String)
    is_synthetic_location = Column(Boolean)
    mv_grid_id = Column(
        Integer, ForeignKey(MvGridDistricts.bus_id), index=True
    )
    grid_connection_id = Column(Integer)
    voltage_level = Column(SmallInteger)
    model_type = Column(String(12))
    bus_id = Column(Integer)
    assignment = Column(String(8))
    geom = Column(Geometry("POINT", 3035))


class EgonEvMitLgvLocationEvents(Base):
    """
    Class definition of table demand.egon_ev_mit_lgv_location_events.

    The placements of the new methodology: one row per charging location,
    listing the charging event of every placement there. A placement is
    one vehicle's realisation of one charging event of its profile at one
    location, so an event id repeats in an array when several vehicles
    of the same profile charge at the location at the same time.

    Folded from the delivered `ev_mapping_event_location.parquet`, which
    has one row per placement (~5e8 for status2024, ~5e9 for each reGon
    scenario) -- too many for one row each in the database.

    **Columns**

    event_ids:
        Ids of the charging events, references
        `demand.egon_ev_mit_lgv_trip.event_id` of the same scenario.

    Notes
    -----
    The placements of the charging locations of one MV grid district::

        SELECT cl.location_id, t.*
        FROM demand.egon_ev_mit_lgv_location_events le
        JOIN demand.egon_ev_mit_lgv_charging_location cl
            USING (scenario, location_id)
        CROSS JOIN unnest(le.event_ids) AS e(event_id)
        JOIN demand.egon_ev_mit_lgv_trip t
            ON t.scenario = le.scenario AND t.event_id = e.event_id
        WHERE le.scenario = :scenario AND cl.mv_grid_id = :mv_grid_id;
    """

    __tablename__ = "egon_ev_mit_lgv_location_events"
    __table_args__ = {"schema": "demand"}

    scenario = Column(String, ForeignKey(EgonScenario.name), primary_key=True)
    location_id = Column(BigInteger, primary_key=True)
    event_ids = Column(ARRAY(Integer))


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
    # SQL NULL rather than the JSON value `null` for eGon2035, which ships
    # no GeoLIS run, so `geolis_config IS NULL` finds it.
    geolis_config = Column(JSONB(none_as_null=True))
