"""
HGV charging integration for egon-data.

Reads precomputed HGV charging demand input files (sites, charging points,
events, profiles) and writes:
  - demand.egon_ev_hgv_charging_site      (Table 1)
  - demand.egon_ev_hgv_charging_point     (Table 2)
  - demand.egon_ev_hgv_charging_event     (Table 3)
  - demand.egon_ev_hgv_profile            (Table 4)
  - grid.egon_etrago_load              (extended)
  - grid.egon_etrago_load_timeseries   (extended)

Scenarios:
  reGon2037 → input data "C 2037"
  reGon2045 → input data "C 2045"

Only scenarios that are both in SCENARIO_MAP and in the pipeline's configured
--scenarios are processed (see active_scenario_map()).
"""

from egon_validation import (
    DataTypeValidation,
    NotNullAndNotNaNValidation,
    SRIDSpecificValidation,
    ValueSetValidation,
)
from loguru import logger

from egon.data import db
from egon.data.datasets import Dataset, DatasetSources, DatasetTargets
from egon.data.datasets.emobility.hgv_charging.db_classes import (
    EgonHgvChargingEvent,
    EgonHgvChargingPoint,
    EgonHgvChargingSite,
    EgonHgvProfile,
)
from egon.data.datasets.emobility.hgv_charging.etrago_integration import write_etrago
from egon.data.datasets.emobility.hgv_charging.fill_tables import (
    extract_input_data,
    fill_hgv_tables,
)
from egon.data.datasets.emobility.hgv_charging.scenarios import (
    SCENARIO_MAP,
    active_scenario_map,
)
from egon.data.datasets.emobility.hgv_charging.spatial_assignment import (
    spatial_assignment,
)
from egon.data.validation import TableValidation, resolve_boundary_dependence
from egon.data.validation.rules.custom.sanity import (
    HgvAnnualEnergy,
    HgvChargingPointProfiles,
    HgvChargingPointRange,
    HgvChargingPointSites,
    HgvDrivingLoadEnergy,
    HgvEventContract,
    HgvEventEnergy,
    HgvEventReferences,
    HgvHighwayDayLoad,
    HgvHighwayDayPeak,
    HgvLinkPower,
    HgvMinimumSites,
    HgvModelComponents,
    HgvModelCount,
    HgvOrphanTimeseries,
    HgvProfiles,
    HgvScenarioCoverage,
    HgvSiteBusAssignment,
    HgvSitePower,
    HgvSiteValues,
    HgvSlotChains,
    HgvStoreCapacity,
    HgvStoreInitial,
    HgvTimeseriesExists,
    HgvTimeseriesValues,
)

#: Minimum number of sites per scenario and location type, measured on full
#: runs with the boundaries Schleswig-Holstein and Everything (2026-10-16).
#: The sites come from the input data, so both scenarios have the same.
MIN_SITES = {
    scn: {
        "Schleswig-Holstein": {"depot": 179, "highway": 17},
        "Everything": {"depot": 5_740, "highway": 352},
    }
    for scn in SCENARIO_MAP
}

#: Annual energy per scenario and location type in MWh, measured on full
#: runs (2026-10-16). For Everything it equals the national targets of the
#: input data (HGV_Scenario.csv: depot and highway charging, N2 + N3 +
#: N3S). Highway is day plus night energy.
ANNUAL_ENERGY_MWH = {
    "reGon2037": {
        "Schleswig-Holstein": {"depot": 884_317, "highway": 238_535},
        "Everything": {"depot": 32_615_385, "highway": 7_384_615},
    },
    "reGon2045": {
        "Schleswig-Holstein": {"depot": 840_738, "highway": 665_414},
        "Everything": {"depot": 30_900_000, "highway": 20_600_000},
    },
}

#: Depot categories of the input data (DEPOT_CATEGORIES_INCLUDED in the
#: input data generation); NULL for highway sites.
DEPOT_CATEGORIES = [
    "Baumarkt/Moebelhaus",
    "Einzelhandellager",
    "Hafen/Bahnhof",
    "Lebensmittelgrosshandel",
    "Logistik",
    "Paketzentrum",
]

#: Sectors of the charging points, events (use_case) and profiles.
SECTORS = [
    "hgv_depot_n2",
    "hgv_depot_n3",
    "hgv_depot_n3s",
    "hgv_highway_day",
    "hgv_highway_night",
]

#: Voltage levels spatial_assignment can assign (it never assigns 2).
VOLTAGE_LEVELS = [1, 3, 4, 5, 6, 7]


def create_tables():
    """Drop and recreate Tables 1–4. Table 5 is created during eDisGo integration."""
    engine = db.engine()
    for model in [
        EgonHgvChargingSite,
        EgonHgvChargingPoint,
        EgonHgvChargingEvent,
        EgonHgvProfile,
    ]:
        model.__table__.drop(bind=engine, checkfirst=True)
        model.__table__.create(bind=engine, checkfirst=True)
    logger.debug("Created HGV charging tables.")


def hgv_charging_validation_rules():
    """
    Validation rules for the tables written by HGVCharging.

    Content checks run per scenario of ``SCENARIO_MAP``, one rule instance
    per scenario. See
    :mod:`egon.data.validation.rules.custom.sanity.hgv_charging`.

    Returns
    -------
    list
        TableValidation specs and rule instances.
    """
    sites = HGVCharging.targets.tables["charging_site"]
    cps = HGVCharging.targets.tables["charging_point"]
    events = HGVCharging.targets.tables["charging_event"]
    profiles = HGVCharging.targets.tables["profile"]
    buses = "grid.egon_etrago_bus"
    links = "grid.egon_etrago_link"
    stores = "grid.egon_etrago_store"
    loads = "grid.egon_etrago_load"
    timeseries = {
        "link": "grid.egon_etrago_link_timeseries",
        "store": "grid.egon_etrago_store_timeseries",
        "load": "grid.egon_etrago_load_timeseries",
    }
    components = {"link": links, "store": stores, "load": loads}
    site_suffix = sites.split(".")[-1]
    scenarios = list(SCENARIO_MAP)

    # The site rules are listed one by one instead of via TableValidation,
    # which always adds a whole-table NOT NULL check: highway sites have no
    # category and area, depots no day/night split. geom (ambiguous "<>"
    # in PostGIS), voltage_level (smallint, the value set rule compares as
    # text) and category_name (NULL allowed) are checked by HgvSiteValues.
    # The event table gets no standard rules: each would scan its 140+
    # million rows per scenario; HgvEventContract covers them.
    rules = [
        DataTypeValidation(
            table=sites,
            rule_id=f"DATA_TYPES.{site_suffix}",
            column_types={
                "site_id": "integer",
                "scenario": "character varying",
                "location_type": "text",
                "category_name": "text",
                "geom": "geometry",
                "N2": "boolean",
                "N3": "boolean",
                "N3S": "boolean",
                "el_demand_N2_mwh": "double precision",
                "el_demand_N3_mwh": "double precision",
                "el_demand_N3S_mwh": "double precision",
                "el_demand_day_mwh": "double precision",
                "el_demand_night_mwh": "double precision",
                "p_set_aggregated_mw": "double precision",
                "mv_grid_id": "integer",
                "bus_id": "bigint",
                "voltage_level": "smallint",
            },
        ),
        NotNullAndNotNaNValidation(
            table=sites,
            rule_id=f"NOT_NAN.{site_suffix}",
            # Quoted: the rule puts the names into SQL as they are.
            columns=[
                "site_id",
                "scenario",
                "location_type",
                '"N2"',
                '"N3"',
                '"N3S"',
                '"el_demand_N2_mwh"',
                '"el_demand_N3_mwh"',
                '"el_demand_N3S_mwh"',
                "p_set_aggregated_mw",
                "mv_grid_id",
                "bus_id",
                "voltage_level",
                "carrier",
            ],
        ),
        ValueSetValidation(
            table=sites,
            rule_id=f"VALUE_SET_SCENARIO.{site_suffix}",
            column="scenario",
            expected_values=scenarios,
        ),
        ValueSetValidation(
            table=sites,
            rule_id=f"VALUE_SET_LOCATION_TYPE.{site_suffix}",
            column="location_type",
            expected_values=["depot", "highway"],
        ),
        SRIDSpecificValidation(
            table=sites,
            rule_id=f"SRID.{site_suffix}.geom",
            geom="geom",
            expected_srid=EgonHgvChargingSite.__table__.c.geom.type.srid,
        ),
        HgvSiteValues(
            table=sites,
            rule_id="SANITY_HGV_SITE_VALUES",
            voltage_levels=VOLTAGE_LEVELS,
            categories=DEPOT_CATEGORIES,
        ),
        TableValidation(
            table_name=cps,
            data_type_columns={
                "cp_id": "integer",
                "scenario": "character varying",
                "site_id": "integer",
                "vehicle_class": "text",
                "num_cp": "integer",
                "p_set_mw": "double precision",
                "sector": "text",
                "time_of_day": "text",
            },
            value_set_columns={
                "scenario": scenarios,
                "sector": SECTORS,
                "vehicle_class": ["N2", "N3", "N3S", "all"],
                "time_of_day": ["day", "night"],
            },
        ),
        TableValidation(
            table_name=profiles,
            data_type_columns={
                "sector": "text",
                "scenario": "character varying",
                "profile": "array",
            },
            value_set_columns={"scenario": scenarios, "sector": SECTORS},
        ),
    ]

    for component, table in [("bus", buses), *components.items()]:
        rules.append(
            HgvScenarioCoverage(
                table=table,
                rule_id=f"SANITY_HGV_SCENARIOS.{component}",
                component=component,
            )
        )
    for component, table in timeseries.items():
        rules.append(
            HgvScenarioCoverage(
                table=table,
                rule_id=f"SANITY_HGV_SCENARIOS.{component}_timeseries",
                component=f"{component}_timeseries",
            )
        )

    for scn in scenarios:
        rules += [
            # Sites
            HgvMinimumSites(
                table=sites,
                rule_id=f"SANITY_HGV_SITE_COUNT.{scn}",
                scenario=scn,
                min_sites=resolve_boundary_dependence(MIN_SITES[scn]),
            ),
            HgvAnnualEnergy(
                table=sites,
                rule_id=f"SANITY_HGV_ANNUAL_ENERGY.{scn}",
                scenario=scn,
                expected_mwh=resolve_boundary_dependence(
                    ANNUAL_ENERGY_MWH[scn]
                ),
            ),
            HgvSitePower(
                table=sites,
                rule_id=f"SANITY_HGV_SITE_POWER.{scn}",
                scenario=scn,
            ),
            HgvSiteBusAssignment(
                table=sites,
                rule_id=f"SANITY_HGV_BUS_ASSIGNMENT.{scn}",
                scenario=scn,
            ),
            # Charging points
            HgvChargingPointRange(
                table=cps,
                rule_id=f"SANITY_HGV_CP_RANGE.{scn}",
                scenario=scn,
            ),
            HgvChargingPointSites(
                table=cps,
                rule_id=f"SANITY_HGV_CP_SITES.{scn}",
                scenario=scn,
            ),
            HgvChargingPointProfiles(
                table=cps,
                rule_id=f"SANITY_HGV_CP_PROFILES.{scn}",
                scenario=scn,
            ),
            # Events
            HgvEventContract(
                table=events,
                rule_id=f"SANITY_HGV_EVENT_CONTRACT.{scn}",
                scenario=scn,
            ),
            HgvEventReferences(
                table=events,
                rule_id=f"SANITY_HGV_EVENT_REFERENCES.{scn}",
                scenario=scn,
            ),
            HgvEventEnergy(
                table=events,
                rule_id=f"SANITY_HGV_EVENT_ENERGY.{scn}",
                scenario=scn,
            ),
            HgvSlotChains(
                table=events,
                rule_id=f"SANITY_HGV_SLOT_CHAINS.{scn}",
                scenario=scn,
            ),
            # Profiles
            HgvProfiles(
                table=profiles,
                rule_id=f"SANITY_HGV_PROFILES.{scn}",
                scenario=scn,
                sectors=SECTORS,
            ),
            # eTraGo
            HgvModelCount(
                table=buses,
                rule_id=f"SANITY_HGV_MODEL_COUNT.{scn}",
                scenario=scn,
            ),
            HgvLinkPower(
                table=links,
                rule_id=f"SANITY_HGV_LINK_POWER.{scn}",
                scenario=scn,
            ),
            HgvStoreCapacity(
                table=stores,
                rule_id=f"SANITY_HGV_STORE_CAPACITY.{scn}",
                scenario=scn,
            ),
            HgvStoreInitial(
                table=stores,
                rule_id=f"SANITY_HGV_STORE_INITIAL.{scn}",
                scenario=scn,
            ),
            HgvDrivingLoadEnergy(
                table=loads,
                rule_id=f"SANITY_HGV_DRIVING_LOAD.{scn}",
                scenario=scn,
            ),
            HgvHighwayDayLoad(
                table=loads,
                rule_id=f"SANITY_HGV_HIGHWAY_DAY_LOAD.{scn}",
                scenario=scn,
            ),
            HgvHighwayDayPeak(
                table=loads,
                rule_id=f"SANITY_HGV_HIGHWAY_DAY_PEAK.{scn}",
                scenario=scn,
            ),
            HgvOrphanTimeseries(
                table=timeseries["load"],
                rule_id=f"SANITY_HGV_ORPHAN_TIMESERIES.{scn}",
                scenario=scn,
            ),
        ]
        for component, table in components.items():
            rules += [
                HgvModelComponents(
                    table=table,
                    rule_id=f"SANITY_HGV_MODEL_{component.upper()}.{scn}",
                    scenario=scn,
                    component=component,
                ),
                HgvTimeseriesExists(
                    table=timeseries[component],
                    rule_id=(
                        f"SANITY_HGV_{component.upper()}_TIMESERIES.{scn}"
                    ),
                    scenario=scn,
                    component=component,
                ),
                HgvTimeseriesValues(
                    table=timeseries[component],
                    rule_id=(
                        f"SANITY_HGV_{component.upper()}_TIMESERIES_VALUES"
                        f".{scn}"
                    ),
                    scenario=scn,
                    component=component,
                ),
            ]
    return rules


class HGVCharging(Dataset):
    """
    Integrates HGV charging demand into egon-data.

    Reads precomputed HGV charging demand input files (sites, charging points,
    events, profiles) and populates four new demand tables plus extends
    egon_etrago_load / _timeseries.

    *Dependencies*
      * :py:class:`MvGridDistricts <egon.data.datasets.mv_grid_districts>`
      * :py:class:`EtragoSetup <egon.data.datasets.etrago_setup.EtragoSetup>`
      * :py:class:`Vg250 <egon.data.datasets.vg250.Vg250>`
      * :py:class:`ScenarioParameters <egon.data.datasets.scenario_parameters.ScenarioParameters>`
      * :py:class:`Osmtgmod <egon.data.datasets.osmtgmod.Osmtgmod>`

    *Configuration*

    The config of this dataset is in *datasets.yml* under *mobility_hgv_charging*.
    ``original_data.sources.hgv_input_dir`` points at the directory holding one
    subfolder per scenario with the input files; it ships in the data bundle
    (see :py:class:`DataBundle <egon.data.datasets.data_bundle.DataBundle>`)
    and is resolved relative to the egon-data working directory.
    """

    sources = DatasetSources(
        files={
            # One subfolder per scenario (reGon2037, reGon2045), each holding
            # sites.gpkg, charging_points.csv, charging_events.csv and
            # profiles.csv. Read-only input from the data bundle -- kept in
            # sync with datasets.yml's hgv_input_dir.
            "hgv_input_dir": "data_bundle_egon_data/hgv_charging",
        },
        tables={
            "mv_grid_district": "grid.egon_mv_grid_district",
            "etrago_load": "grid.egon_etrago_load",
            "etrago_load_timeseries": "grid.egon_etrago_load_timeseries",
        },
    )

    targets = DatasetTargets(
        tables={
            "charging_site": "demand.egon_ev_hgv_charging_site",
            "charging_point": "demand.egon_ev_hgv_charging_point",
            "charging_event": "demand.egon_ev_hgv_charging_event",
            "profile": "demand.egon_ev_hgv_profile",
            "etrago_load": "grid.egon_etrago_load",
            "etrago_load_timeseries": "grid.egon_etrago_load_timeseries",
        },
    )

    name: str = "HGVCharging"
    version: str = "0.0.4"

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(
                create_tables,
                extract_input_data,
                fill_hgv_tables,
                spatial_assignment,
                write_etrago,
            ),
            validation={"data-quality": hgv_charging_validation_rules()},
            proceed_on_validation_failure=True,
        )
