"""
Public bus charging (vehicle class M3) for egon-data.

Reads precomputed depot locations and hourly charging series and writes:

* ``demand.egon_ev_bus_charging_depot`` -- one row per depot per scenario,
  with geometry, voltage level and the full 8760-step ``p_set``. This is the
  per-location table for eDisGo.
* ``grid.egon_etrago_load`` / ``grid.egon_etrago_load_timeseries`` -- one
  load per eTraGo bus per scenario, carrier ``land_transport_bus``, with the
  depots' series summed.

Scenarios carrying bus data: ``status2024``, ``reGon2037``, ``reGon2045``.
``eGon2035`` is deliberately unchanged and ``eGon100RE`` is discarded, so
neither gets bus loads -- zero bus demand in those scenarios is correct.

Depots are modelled as ordinary fixed loads, not charging parks: there is no
flexibility, no flex model and no lowflex variant. Design decisions are
recorded in ``docs/adr/0001``-``0003`` of the FC_M3 working directory.
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
from egon.data.datasets.emobility.public_bus_charging.db_classes import (
    EgonEvBusChargingDepot,
)
from egon.data.datasets.emobility.public_bus_charging.etrago_integration import (  # noqa: E501
    write_etrago,
)
from egon.data.datasets.emobility.public_bus_charging.fill_tables import (
    fill_depot_table,
)
from egon.data.datasets.emobility.public_bus_charging.scenarios import (
    SCENARIO_FILES,
)
from egon.data.datasets.emobility.public_bus_charging.spatial_assignment import (  # noqa: E501
    spatial_assignment,
)
from egon.data.validation import resolve_boundary_dependence
from egon.data.validation.rules.custom.sanity import (
    PublicBusAnnualEnergy,
    PublicBusBusAssignment,
    PublicBusEtragoConsistency,
    PublicBusInputConsistency,
    PublicBusMinimumDepots,
    PublicBusScenarioCoverage,
    PublicBusSeriesConsistency,
    PublicBusVoltageLevel,
)

#: Minimum number of depots per scenario: about 95 % of the counts measured
#: on full runs (Schleswig-Holstein: 65 in every scenario; Everything: 1050,
#: 1345 and 1345 of 1053, 1352 and 1352 input depots, 2026-10). A broken
#: spatial join or a CRS error drops more.
MIN_DEPOTS = {
    "status2024": {"Schleswig-Holstein": 62, "Everything": 997},
    "reGon2037": {"Schleswig-Holstein": 62, "Everything": 1277},
    "reGon2045": {"Schleswig-Holstein": 62, "Everything": 1277},
}

#: Total annual energy per scenario in MWh, measured on full runs with the
#: input data of 2026-10-08. The check accepts +/-5 % and otherwise warns:
#: a deviation means the input data has changed (the previous input version
#: was 5-8 % higher) or depots with much energy were dropped. Re-measure
#: after a deliberate input update.
ANNUAL_ENERGY_MWH = {
    "status2024": {"Schleswig-Holstein": 14_809, "Everything": 227_295},
    "reGon2037": {"Schleswig-Holstein": 105_012, "Everything": 2_941_045},
    "reGon2045": {"Schleswig-Holstein": 158_921, "Everything": 4_150_252},
}


def bus_validation_rules():
    """
    Validation rules for the tables written by PublicBusCharging.

    Content checks run per scenario with bus data, one rule instance per
    scenario. See
    :mod:`egon.data.validation.rules.custom.sanity.public_bus_charging`.

    Returns
    -------
    list
        Rule instances.
    """
    depots = PublicBusCharging.targets.tables["charging_depot"]
    loads = PublicBusCharging.targets.tables["etrago_load"]
    suffix = depots.split(".")[-1]

    # The formal rules are listed one by one instead of via TableValidation,
    # which always adds a whole-table NOT NULL check: mv_grid_id is NULL by
    # design for depots at the EHV levels. geom is left out of the NOT NULL
    # check (its "<>" operator is ambiguous in PostGIS) and checked by
    # PublicBusBusAssignment; voltage_level is left out of the value sets
    # (smallint) and checked by PublicBusVoltageLevel. depot_name may be
    # NULL: 28 real (OSM) depots have no name in the input, and the name is
    # descriptive only.
    rules = [
        DataTypeValidation(
            table=depots,
            rule_id=f"DATA_TYPES.{suffix}",
            column_types={
                "depot_id": "character varying",
                "scenario": "character varying",
                "depot_name": "text",
                "depot_type": "text",
                "fleet_size": "double precision",
                "geom": "geometry",
                "bus_id": "bigint",
                "mv_grid_id": "integer",
                "voltage_level": "smallint",
                "peak_load_mw": "double precision",
                "annual_demand_mwh": "double precision",
                "p_set": "array",
            },
        ),
        NotNullAndNotNaNValidation(
            table=depots,
            rule_id=f"NOT_NAN.{suffix}",
            columns=[
                "depot_id",
                "scenario",
                "depot_type",
                "fleet_size",
                "bus_id",
                "voltage_level",
                "peak_load_mw",
                "annual_demand_mwh",
                "p_set",
            ],
        ),
        ValueSetValidation(
            table=depots,
            rule_id=f"VALUE_SET_SCENARIO.{suffix}",
            column="scenario",
            expected_values=list(SCENARIO_FILES),
        ),
        ValueSetValidation(
            table=depots,
            rule_id=f"VALUE_SET_DEPOT_TYPE.{suffix}",
            column="depot_type",
            expected_values=["real", "standin"],
        ),
        SRIDSpecificValidation(
            table=depots,
            rule_id=f"SRID.{suffix}.geom",
            geom="geom",
            expected_srid=EgonEvBusChargingDepot.__table__.c.geom.type.srid,
        ),
        PublicBusScenarioCoverage(
            table=loads, rule_id="SANITY_PUBLIC_BUS_SCENARIOS"
        ),
    ]

    for scn in SCENARIO_FILES:
        rules += [
            PublicBusMinimumDepots(
                table=depots,
                rule_id=f"SANITY_PUBLIC_BUS_DEPOT_COUNT.{scn}",
                scenario=scn,
                min_depots=resolve_boundary_dependence(MIN_DEPOTS[scn]),
            ),
            PublicBusSeriesConsistency(
                table=depots,
                rule_id=f"SANITY_PUBLIC_BUS_SERIES.{scn}",
                scenario=scn,
            ),
            PublicBusVoltageLevel(
                table=depots,
                rule_id=f"SANITY_PUBLIC_BUS_VOLTAGE_LEVEL.{scn}",
                scenario=scn,
            ),
            PublicBusBusAssignment(
                table=depots,
                rule_id=f"SANITY_PUBLIC_BUS_BUS_ASSIGNMENT.{scn}",
                scenario=scn,
            ),
            PublicBusAnnualEnergy(
                table=depots,
                rule_id=f"SANITY_PUBLIC_BUS_ANNUAL_ENERGY.{scn}",
                scenario=scn,
                expected_mwh=resolve_boundary_dependence(
                    ANNUAL_ENERGY_MWH[scn]
                ),
            ),
            PublicBusInputConsistency(
                table=depots,
                rule_id=f"SANITY_PUBLIC_BUS_INPUT.{scn}",
                scenario=scn,
            ),
            PublicBusEtragoConsistency(
                table=loads,
                rule_id=f"SANITY_PUBLIC_BUS_ETRAGO.{scn}",
                scenario=scn,
            ),
        ]
    return rules


def create_tables():
    """Drop and recreate demand.egon_ev_bus_charging_depot."""
    engine = db.engine()
    EgonEvBusChargingDepot.__table__.drop(bind=engine, checkfirst=True)
    EgonEvBusChargingDepot.__table__.create(bind=engine, checkfirst=True)
    logger.debug("Created public bus charging tables.")


class PublicBusCharging(Dataset):
    """
    Integrates public bus (vehicle class M3) charging demand into egon-data.

    *Dependencies*
      * :py:class:`DataBundle <egon.data.datasets.data_bundle.DataBundle>`
      * :py:class:`MvGridDistricts <egon.data.datasets.mv_grid_districts>`
      * :py:class:`SubstationVoronoi
        <egon.data.datasets.substation_voronoi.SubstationVoronoi>`
      * :py:class:`EtragoSetup <egon.data.datasets.etrago_setup.EtragoSetup>`
      * :py:class:`ScenarioParameters
        <egon.data.datasets.scenario_parameters.ScenarioParameters>`
      * :py:class:`Osmtgmod <egon.data.datasets.osmtgmod.Osmtgmod>`
      * :py:class:`Vg250 <egon.data.datasets.vg250.Vg250>`

    *Resulting tables*
      * :py:class:`demand.egon_ev_bus_charging_depot
        <egon.data.datasets.emobility.public_bus_charging.db_classes.EgonEvBusChargingDepot>`
      * ``grid.egon_etrago_load`` and ``grid.egon_etrago_load_timeseries``
        are extended with carrier ``land_transport_bus``
    """

    sources = DatasetSources(
        files={
            # depots.gpkg plus one gzipped wide CSV per scenario. Read-only
            # input from the data bundle, resolved relative to the egon-data
            # working directory.
            "bus_input_dir": "data_bundle_egon_data/bus_charging",
        },
        tables={
            "mv_grid_district": "grid.egon_mv_grid_district",
            "ehv_substation_voronoi": "grid.egon_ehv_substation_voronoi",
        },
    )

    targets = DatasetTargets(
        tables={
            "charging_depot": "demand.egon_ev_bus_charging_depot",
            "etrago_load": "grid.egon_etrago_load",
            "etrago_load_timeseries": "grid.egon_etrago_load_timeseries",
        },
    )

    name: str = "PublicBusCharging"
    version: str = "0.0.2"

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(
                create_tables,
                fill_depot_table,
                spatial_assignment,
                write_etrago,
            ),
            validation={"data-quality": bus_validation_rules()},
            proceed_on_validation_failure=True,
            # Name the validation task like the dataset's other tasks,
            # which are prefixed with their module path.
            validation_task_prefix=__name__.replace("egon.data.datasets.", ""),
        )
