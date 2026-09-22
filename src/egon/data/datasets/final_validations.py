"""
Dataset for cross-cutting validations that run at the end of the pipeline.

This module provides the FinalValidations dataset which contains validation rules
that check data consistency across multiple datasets. These validations should run
after all data generation is complete, but before the final validation report.
"""

from egon_validation import ArrayCardinalityValidation

from egon.data.datasets import Dataset
from egon.data.validation import (
    TableValidation,
    resolve_boundary_dependence,
)


def notasks():
    """
    Placeholder task function.

    This dataset has no data generation tasks - it only runs validation rules
    defined in the validation dict. The validation framework automatically creates
    validation tasks from the rules.

    Returns
    -------
    None
    """
    return None


class FinalValidations(Dataset):
    """
    Cross-cutting validations that run at the end of the pipeline.

    This dataset contains validation rules that check data consistency across
    multiple datasets and should run after all data generation is complete.

    The validations are organized by category and run automatically as part of
    the dataset's validation tasks. Results are collected by ValidationReport.

    Currently, two categories are defined:

    ``data-quality``
      One :class:`TableValidation` per eTraGo interface table: exact row
      count per dataset boundary, the full column type map, the columns
      that are never NULL, SRID checks for the geometry columns and the
      permitted ``scn_name`` / ``carrier`` values.

    ``timeseries_length``
      One :class:`ArrayCardinalityValidation` per array column that
      actually holds arrays, asserting the 8760 h resolution.

    *Dependencies*
      Should depend on all datasets whose data is validated by the rules
      defined here.

    *Validation Results*
      Results are written to
      validation_runs/{run_id}/tasks/FinalValidations.validate.*/ and
      collected by the ValidationReport dataset.

    *Adding New Validations*
      1. Create the validation rule class in validation/rules/custom/sanity/
      2. Import it at the top of this file
      3. Add instances to a category in the validation dict below
      4. Update dependencies to include the datasets providing the data

    Example
    -------
    ```python
    ArrayCardinalityValidation(
        table="grid.egon_etrago_load_timeseries",
        rule_id="SANITY_TIMESERIES_LOAD_P_SET",
        array_column="p_set",
        expected_length=8760,
    ),
    ```
    """

    #:
    name: str = "FinalValidations"
    #: The ``.dev`` suffix exempts this dataset from
    #: :meth:`Dataset.check_version`, so the rules are re-evaluated on
    #: every run. Without it the dataset would be registered as executed
    #: even by a run in which every rule errored out -- rule errors are
    #: turned into failed results, not task failures, so the task always
    #: succeeds -- and the re-run after the fix would skip it, leaving
    #: the report with no cross-cutting results at all.
    version: str = "0.0.1.dev"
    #: Run the cross-cutting rules even when an upstream data task
    #: failed, so that ValidationReport downstream has something to
    #: report on. Without this, `all_success` would mark these tasks
    #: `upstream_failed` and the report would cover only the per-dataset
    #: validations, missing exactly the cross-cutting checks that explain
    #: the failure.
    trigger_rule: str = "all_done"

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(notasks,),  # No data tasks - only validation tasks
            validation={
                "timeseries_length": [
                    # One rule per array column that actually holds arrays.
                    # Columns that are NULL in every row of both reference
                    # databases are omitted -- there is nothing to measure.
                    ArrayCardinalityValidation(
                        table="grid.egon_etrago_generator_timeseries",
                        rule_id="SANITY_TIMESERIES_GENERATOR_P_MAX_PU",
                        array_column="p_max_pu",
                        expected_length=8760,
                    ),
                    ArrayCardinalityValidation(
                        table="grid.egon_etrago_load_timeseries",
                        rule_id="SANITY_TIMESERIES_LOAD_P_SET",
                        array_column="p_set",
                        expected_length=8760,
                    ),
                    ArrayCardinalityValidation(
                        table="grid.egon_etrago_link_timeseries",
                        rule_id="SANITY_TIMESERIES_LINK_P_MIN_PU",
                        array_column="p_min_pu",
                        expected_length=8760,
                    ),
                    ArrayCardinalityValidation(
                        table="grid.egon_etrago_link_timeseries",
                        rule_id="SANITY_TIMESERIES_LINK_P_MAX_PU",
                        array_column="p_max_pu",
                        expected_length=8760,
                    ),
                    ArrayCardinalityValidation(
                        table="grid.egon_etrago_link_timeseries",
                        rule_id="SANITY_TIMESERIES_LINK_EFFICIENCY",
                        array_column="efficiency",
                        expected_length=8760,
                    ),
                    ArrayCardinalityValidation(
                        table="grid.egon_etrago_store_timeseries",
                        rule_id="SANITY_TIMESERIES_STORE_E_MIN_PU",
                        array_column="e_min_pu",
                        expected_length=8760,
                    ),
                    ArrayCardinalityValidation(
                        table="grid.egon_etrago_store_timeseries",
                        rule_id="SANITY_TIMESERIES_STORE_E_MAX_PU",
                        array_column="e_max_pu",
                        expected_length=8760,
                    ),
                ],
                "data-quality": [
                    # grid.egon_etrago_bus
                    TableValidation(
                        table_name="grid.egon_etrago_bus",
                        row_count=resolve_boundary_dependence(
                            {
                                "Schleswig-Holstein": 5952,
                                "Everything": 67594,
                            }
                        ),
                        geometry_columns=[
                            "geom",
                        ],
                        data_type_columns={
                            "bus_id": "bigint",
                            "carrier": "text",
                            "country": "text",
                            "geom": "geometry",
                            "scn_name": "character varying",
                            "type": "text",
                            "v_mag_pu_max": "double precision",
                            "v_mag_pu_min": "double precision",
                            "v_mag_pu_set": "double precision",
                            "v_nom": "double precision",
                            "x": "double precision",
                            "y": "double precision",
                        },
                        not_null_columns=[
                            "bus_id",
                            "carrier",
                            "scn_name",
                            "v_nom",
                            "x",
                            "y",
                        ],
                        value_set_columns={
                            "scn_name": [
                                "eGon2035",
                                "reGon2037",
                                "reGon2045",
                                "status2024",
                            ],
                            "carrier": [
                                "AC",
                                "CH4",
                                "H2",
                                "H2_grid",
                                "H2_saltcavern",
                                "HGV_charger",
                                "Li_ion",
                                "O2",
                                "central_heat",
                                "central_heat_store",
                                "dsm",
                                "rural_heat",
                                "rural_heat_store",
                            ],
                        },
                    ),
                    # grid.egon_etrago_generator
                    TableValidation(
                        table_name="grid.egon_etrago_generator",
                        row_count=resolve_boundary_dependence(
                            {
                                "Schleswig-Holstein": 2991,
                                "Everything": 39564,
                            }
                        ),
                        data_type_columns={
                            "build_year": "bigint",
                            "bus": "bigint",
                            "capital_cost": "double precision",
                            "carrier": "text",
                            "committable": "boolean",
                            "control": "text",
                            "down_time_before": "bigint",
                            "e_nom_max": "double precision",
                            "efficiency": "double precision",
                            "generator_id": "bigint",
                            "lifetime": "double precision",
                            "marginal_cost": "double precision",
                            "min_down_time": "bigint",
                            "min_up_time": "bigint",
                            "p_max_pu": "double precision",
                            "p_min_pu": "double precision",
                            "p_nom": "double precision",
                            "p_nom_extendable": "boolean",
                            "p_nom_max": "double precision",
                            "p_nom_min": "double precision",
                            "p_set": "double precision",
                            "q_set": "double precision",
                            "ramp_limit_down": "double precision",
                            "ramp_limit_shut_down": "double precision",
                            "ramp_limit_start_up": "double precision",
                            "ramp_limit_up": "double precision",
                            "scn_name": "character varying",
                            "shut_down_cost": "double precision",
                            "sign": "double precision",
                            "start_up_cost": "double precision",
                            "type": "text",
                            "up_time_before": "bigint",
                        },
                        not_null_columns=[
                            "build_year",
                            "bus",
                            "capital_cost",
                            "carrier",
                            "committable",
                            "down_time_before",
                            "e_nom_max",
                            "efficiency",
                            "generator_id",
                            "lifetime",
                            "marginal_cost",
                            "min_down_time",
                            "min_up_time",
                            "p_max_pu",
                            "p_min_pu",
                            "p_nom",
                            "p_nom_extendable",
                            "p_nom_max",
                            "p_nom_min",
                            "ramp_limit_down",
                            "ramp_limit_shut_down",
                            "ramp_limit_start_up",
                            "ramp_limit_up",
                            "scn_name",
                            "shut_down_cost",
                            "sign",
                            "start_up_cost",
                            "up_time_before",
                        ],
                        value_set_columns={
                            "scn_name": [
                                "eGon2035",
                                "reGon2037",
                                "reGon2045",
                                "status2024",
                            ],
                            "carrier": [
                                "CH4",
                                "O2",
                                "OCGT",
                                "biomass",
                                "central_biomass_CHP",
                                "central_biomass_CHP_heat",
                                "central_coal_CHP",
                                "central_coal_CHP_heat",
                                "central_lignite_CHP",
                                "central_lignite_CHP_heat",
                                "central_oil_CHP",
                                "central_oil_CHP_heat",
                                "central_others_CHP",
                                "central_others_CHP_heat",
                                "coal",
                                "geo_thermal",
                                "industrial_biomass_CHP",
                                "industrial_coal_CHP",
                                "industrial_lignite_CHP",
                                "industrial_oil_CHP",
                                "industrial_others_CHP",
                                "lignite",
                                "nuclear",
                                "oil",
                                "others",
                                "reservoir",
                                "run_of_river",
                                "solar",
                                "solar_rooftop",
                                "solar_thermal_collector",
                                "wind_offshore",
                                "wind_onshore",
                            ],
                        },
                    ),
                    # grid.egon_etrago_generator_timeseries
                    TableValidation(
                        table_name="grid.egon_etrago_generator_timeseries",
                        row_count=resolve_boundary_dependence(
                            {
                                "Schleswig-Holstein": 2455,
                                "Everything": 21056,
                            }
                        ),
                        data_type_columns={
                            "generator_id": "integer",
                            "marginal_cost": "array",
                            "p_max_pu": "array",
                            "p_min_pu": "array",
                            "p_set": "array",
                            "q_set": "array",
                            "scn_name": "character varying",
                            "temp_id": "integer",
                        },
                        not_null_columns=[
                            "generator_id",
                            "p_max_pu",
                            "scn_name",
                            "temp_id",
                        ],
                        value_set_columns={
                            "scn_name": [
                                "eGon2035",
                                "reGon2037",
                                "reGon2045",
                                "status2024",
                            ],
                        },
                    ),
                    # grid.egon_etrago_line
                    TableValidation(
                        table_name="grid.egon_etrago_line",
                        row_count=resolve_boundary_dependence(
                            {
                                "Schleswig-Holstein": 4852,
                                "Everything": 55426,
                            }
                        ),
                        geometry_columns=[
                            "geom",
                            "topo",
                        ],
                        data_type_columns={
                            "b": "numeric",
                            "build_year": "bigint",
                            "bus0": "bigint",
                            "bus1": "bigint",
                            "cables": "integer",
                            "capital_cost": "double precision",
                            "carrier": "text",
                            "g": "numeric",
                            "geom": "geometry",
                            "length": "double precision",
                            "lifetime": "double precision",
                            "line_id": "bigint",
                            "num_parallel": "double precision",
                            "r": "numeric",
                            "s_max_pu": "double precision",
                            "s_nom": "numeric",
                            "s_nom_extendable": "boolean",
                            "s_nom_max": "double precision",
                            "s_nom_min": "double precision",
                            "scn_name": "character varying",
                            "terrain_factor": "double precision",
                            "topo": "geometry",
                            "type": "text",
                            "v_ang_max": "double precision",
                            "v_ang_min": "double precision",
                            "v_nom": "double precision",
                            "x": "numeric",
                        },
                        not_null_columns=[
                            "b",
                            "build_year",
                            "bus0",
                            "bus1",
                            "capital_cost",
                            "carrier",
                            "g",
                            "length",
                            "lifetime",
                            "line_id",
                            "num_parallel",
                            "r",
                            "s_max_pu",
                            "s_nom",
                            "s_nom_extendable",
                            "s_nom_max",
                            "s_nom_min",
                            "scn_name",
                            "terrain_factor",
                            "v_ang_max",
                            "v_ang_min",
                            "v_nom",
                            "x",
                        ],
                        value_set_columns={
                            "scn_name": [
                                "eGon2035",
                                "reGon2037",
                                "reGon2045",
                                "status2024",
                            ],
                            "carrier": [
                                "AC",
                            ],
                        },
                    ),
                    # grid.egon_etrago_line_timeseries
                    TableValidation(
                        table_name="grid.egon_etrago_line_timeseries",
                        row_count=resolve_boundary_dependence(
                            {
                                "Schleswig-Holstein": 4852,
                                "Everything": 55426,
                            }
                        ),
                        data_type_columns={
                            "line_id": "bigint",
                            "s_max_pu": "array",
                            "scn_name": "character varying",
                            "temp_id": "integer",
                        },
                        not_null_columns=[
                            "line_id",
                            "s_max_pu",
                            "scn_name",
                            "temp_id",
                        ],
                        value_set_columns={
                            "scn_name": [
                                "eGon2035",
                                "reGon2037",
                                "reGon2045",
                                "status2024",
                            ],
                        },
                    ),
                    # grid.egon_etrago_link
                    TableValidation(
                        table_name="grid.egon_etrago_link",
                        row_count=resolve_boundary_dependence(
                            {
                                "Schleswig-Holstein": 5121,
                                "Everything": 94235,
                            }
                        ),
                        geometry_columns=[
                            "geom",
                            "topo",
                        ],
                        data_type_columns={
                            "build_year": "bigint",
                            "bus0": "bigint",
                            "bus1": "bigint",
                            "capital_cost": "double precision",
                            "carrier": "text",
                            "efficiency": "double precision",
                            "geom": "geometry",
                            "length": "double precision",
                            "lifetime": "double precision",
                            "link_id": "bigint",
                            "marginal_cost": "double precision",
                            "p_max_pu": "double precision",
                            "p_min_pu": "double precision",
                            "p_nom": "numeric",
                            "p_nom_extendable": "boolean",
                            "p_nom_max": "double precision",
                            "p_nom_min": "double precision",
                            "p_set": "double precision",
                            "scn_name": "character varying",
                            "terrain_factor": "double precision",
                            "topo": "geometry",
                            "type": "text",
                        },
                        not_null_columns=[
                            "bus0",
                            "bus1",
                            "capital_cost",
                            "carrier",
                            "efficiency",
                            "lifetime",
                            "link_id",
                            "marginal_cost",
                            "p_max_pu",
                            "p_min_pu",
                            "p_nom",
                            "p_nom_extendable",
                            "p_nom_max",
                            "p_nom_min",
                            "scn_name",
                            "terrain_factor",
                        ],
                        value_set_columns={
                            "scn_name": [
                                "eGon2035",
                                "reGon2037",
                                "reGon2045",
                                "status2024",
                            ],
                            "carrier": [
                                "BEV_charger",
                                "CH4",
                                "CH4_to_H2",
                                "DC",
                                "H2_grid",
                                "H2_saltcavern",
                                "H2_to_CH4",
                                "H2_to_power",
                                "HGV_charger",
                                "OCGT",
                                "PtH2_O2",
                                "PtH2_waste_heat",
                                "central_gas_CHP",
                                "central_gas_CHP_heat",
                                "central_gas_boiler",
                                "central_heat_pump",
                                "central_heat_store_charger",
                                "central_heat_store_discharger",
                                "central_resistive_heater",
                                "dsm",
                                "industrial_gas_CHP",
                                "power_to_H2",
                                "rural_gas_boiler",
                                "rural_heat_pump",
                                "rural_heat_store_charger",
                                "rural_heat_store_discharger",
                            ],
                        },
                    ),
                    # grid.egon_etrago_link_timeseries
                    TableValidation(
                        table_name="grid.egon_etrago_link_timeseries",
                        row_count=resolve_boundary_dependence(
                            {
                                "Schleswig-Holstein": 1896,
                                "Everything": 25732,
                            }
                        ),
                        data_type_columns={
                            "efficiency": "array",
                            "link_id": "bigint",
                            "marginal_cost": "array",
                            "p_max_pu": "array",
                            "p_min_pu": "array",
                            "p_set": "array",
                            "scn_name": "character varying",
                            "temp_id": "integer",
                        },
                        not_null_columns=[
                            "link_id",
                            "scn_name",
                            "temp_id",
                        ],
                        value_set_columns={
                            "scn_name": [
                                "eGon2035",
                                "reGon2037",
                                "reGon2045",
                                "status2024",
                            ],
                        },
                    ),
                    # grid.egon_etrago_load
                    TableValidation(
                        table_name="grid.egon_etrago_load",
                        row_count=resolve_boundary_dependence(
                            {
                                "Schleswig-Holstein": 2287,
                                "Everything": 40428,
                            }
                        ),
                        data_type_columns={
                            "bus": "bigint",
                            "carrier": "text",
                            "load_id": "bigint",
                            "p_set": "double precision",
                            "q_set": "double precision",
                            "scn_name": "character varying",
                            "sign": "double precision",
                            "type": "text",
                        },
                        not_null_columns=[
                            "bus",
                            "carrier",
                            "load_id",
                            "scn_name",
                            "sign",
                        ],
                        value_set_columns={
                            "scn_name": [
                                "eGon2035",
                                "eGon2035_lowflex",
                                "reGon2037",
                                "reGon2037_lowflex",
                                "reGon2045",
                                "reGon2045_lowflex",
                                "status2024",
                            ],
                            "carrier": [
                                "AC",
                                "CH4",
                                "CH4_for_industry",
                                "H2_for_industry",
                                "O2",
                                "central_heat",
                                "land_transport_EV",
                                "land_transport_HGV",
                                "land_transport_bus",
                                "rural_gas_boiler",
                                "rural_heat",
                            ],
                        },
                    ),
                    # grid.egon_etrago_load_timeseries
                    TableValidation(
                        table_name="grid.egon_etrago_load_timeseries",
                        row_count=resolve_boundary_dependence(
                            {
                                "Schleswig-Holstein": 2276,
                                "Everything": 40417,
                            }
                        ),
                        data_type_columns={
                            "load_id": "bigint",
                            "p_set": "array",
                            "q_set": "array",
                            "scn_name": "character varying",
                            "temp_id": "integer",
                        },
                        not_null_columns=[
                            "load_id",
                            "p_set",
                            "scn_name",
                            "temp_id",
                        ],
                        value_set_columns={
                            "scn_name": [
                                "eGon2035",
                                "eGon2035_lowflex",
                                "reGon2037",
                                "reGon2037_lowflex",
                                "reGon2045",
                                "reGon2045_lowflex",
                                "status2024",
                            ],
                        },
                    ),
                    # grid.egon_etrago_storage
                    TableValidation(
                        table_name="grid.egon_etrago_storage",
                        row_count=resolve_boundary_dependence(
                            {
                                "Schleswig-Holstein": 1872,
                                "Everything": 18336,
                            }
                        ),
                        data_type_columns={
                            "build_year": "bigint",
                            "bus": "bigint",
                            "capital_cost": "double precision",
                            "carrier": "text",
                            "control": "text",
                            "cyclic_state_of_charge": "boolean",
                            "efficiency_dispatch": "double precision",
                            "efficiency_store": "double precision",
                            "inflow": "double precision",
                            "lifetime": "double precision",
                            "marginal_cost": "double precision",
                            "max_hours": "double precision",
                            "p_max_pu": "double precision",
                            "p_min_pu": "double precision",
                            "p_nom": "double precision",
                            "p_nom_extendable": "boolean",
                            "p_nom_max": "double precision",
                            "p_nom_min": "double precision",
                            "p_set": "double precision",
                            "q_set": "double precision",
                            "scn_name": "character varying",
                            "sign": "double precision",
                            "standing_loss": "double precision",
                            "state_of_charge_initial": "double precision",
                            "state_of_charge_set": "double precision",
                            "storage_id": "bigint",
                            "type": "text",
                        },
                        not_null_columns=[
                            "build_year",
                            "bus",
                            "capital_cost",
                            "carrier",
                            "cyclic_state_of_charge",
                            "efficiency_dispatch",
                            "efficiency_store",
                            "inflow",
                            "lifetime",
                            "marginal_cost",
                            "max_hours",
                            "p_max_pu",
                            "p_min_pu",
                            "p_nom",
                            "p_nom_extendable",
                            "p_nom_max",
                            "p_nom_min",
                            "scn_name",
                            "sign",
                            "standing_loss",
                            "state_of_charge_initial",
                            "storage_id",
                        ],
                        value_set_columns={
                            "scn_name": [
                                "eGon2035",
                                "reGon2037",
                                "reGon2045",
                                "status2024",
                            ],
                            "carrier": [
                                "BESS",
                                "battery",
                                "home_battery",
                                "pumped_hydro",
                            ],
                        },
                    ),
                    # grid.egon_etrago_storage_timeseries
                    TableValidation(
                        table_name="grid.egon_etrago_storage_timeseries",
                        row_count=resolve_boundary_dependence(
                            {
                                "Schleswig-Holstein": 0,  # TODO: update
                                "Everything": 0,  # TODO: update
                            }
                        ),
                        data_type_columns={
                            "inflow": "array",
                            "marginal_cost": "array",
                            "p_max_pu": "array",
                            "p_min_pu": "array",
                            "p_set": "array",
                            "q_set": "array",
                            "scn_name": "character varying",
                            "state_of_charge_set": "array",
                            "storage_id": "bigint",
                            "temp_id": "integer",
                        },
                        not_null_columns=[
                            "inflow",
                            "marginal_cost",
                            "p_max_pu",
                            "p_min_pu",
                            "p_set",
                            "q_set",
                            "scn_name",
                            "state_of_charge_set",
                            "storage_id",
                            "temp_id",
                        ],
                    ),
                    # grid.egon_etrago_store
                    TableValidation(
                        table_name="grid.egon_etrago_store",
                        row_count=resolve_boundary_dependence(
                            {
                                "Schleswig-Holstein": 1739,
                                "Everything": 22967,
                            }
                        ),
                        data_type_columns={
                            "build_year": "bigint",
                            "bus": "bigint",
                            "capital_cost": "double precision",
                            "carrier": "text",
                            "e_cyclic": "boolean",
                            "e_initial": "double precision",
                            "e_max_pu": "double precision",
                            "e_min_pu": "double precision",
                            "e_nom": "double precision",
                            "e_nom_extendable": "boolean",
                            "e_nom_max": "double precision",
                            "e_nom_min": "double precision",
                            "lifetime": "double precision",
                            "marginal_cost": "double precision",
                            "p_set": "double precision",
                            "q_set": "double precision",
                            "scn_name": "character varying",
                            "sign": "double precision",
                            "standing_loss": "double precision",
                            "store_id": "bigint",
                            "type": "text",
                        },
                        not_null_columns=[
                            "build_year",
                            "bus",
                            "capital_cost",
                            "carrier",
                            "e_cyclic",
                            "e_initial",
                            "e_max_pu",
                            "e_min_pu",
                            "e_nom",
                            "e_nom_extendable",
                            "e_nom_max",
                            "e_nom_min",
                            "lifetime",
                            "marginal_cost",
                            "scn_name",
                            "sign",
                            "standing_loss",
                            "store_id",
                        ],
                        value_set_columns={
                            "scn_name": [
                                "eGon2035",
                                "reGon2037",
                                "reGon2045",
                                "status2024",
                            ],
                            "carrier": [
                                "CH4",
                                "H2_overground",
                                "H2_underground",
                                "battery_storage",
                                "central_heat_store",
                                "dsm",
                                "rural_heat_store",
                            ],
                        },
                    ),
                    # grid.egon_etrago_store_timeseries
                    TableValidation(
                        table_name="grid.egon_etrago_store_timeseries",
                        row_count=resolve_boundary_dependence(
                            {
                                "Schleswig-Holstein": 1372,
                                "Everything": 14317,
                            }
                        ),
                        data_type_columns={
                            "e_max_pu": "array",
                            "e_min_pu": "array",
                            "marginal_cost": "array",
                            "p_set": "array",
                            "q_set": "array",
                            "scn_name": "character varying",
                            "store_id": "bigint",
                            "temp_id": "integer",
                        },
                        not_null_columns=[
                            "e_max_pu",
                            "e_min_pu",
                            "scn_name",
                            "store_id",
                            "temp_id",
                        ],
                        value_set_columns={
                            "scn_name": [
                                "eGon2035",
                                "reGon2037",
                                "reGon2045",
                                "status2024",
                            ],
                        },
                    ),
                    # grid.egon_etrago_temp_resolution
                    TableValidation(
                        table_name="grid.egon_etrago_temp_resolution",
                        row_count=resolve_boundary_dependence(
                            {
                                "Schleswig-Holstein": 1,
                                "Everything": 1,
                            }
                        ),
                        data_type_columns={
                            "resolution": "text",
                            "start_time": "timestamp without time zone",
                            "temp_id": "bigint",
                            "timesteps": "bigint",
                        },
                        not_null_columns=[
                            "resolution",
                            "start_time",
                            "temp_id",
                            "timesteps",
                        ],
                    ),
                    # grid.egon_etrago_transformer
                    TableValidation(
                        table_name="grid.egon_etrago_transformer",
                        row_count=resolve_boundary_dependence(
                            {
                                "Schleswig-Holstein": 116,
                                "Everything": 1098,
                            }
                        ),
                        geometry_columns=[
                            "geom",
                            "topo",
                        ],
                        data_type_columns={
                            "b": "numeric",
                            "build_year": "bigint",
                            "bus0": "bigint",
                            "bus1": "bigint",
                            "capital_cost": "double precision",
                            "g": "numeric",
                            "geom": "geometry",
                            "lifetime": "double precision",
                            "model": "text",
                            "num_parallel": "double precision",
                            "phase_shift": "double precision",
                            "r": "numeric",
                            "s_max_pu": "double precision",
                            "s_nom": "double precision",
                            "s_nom_extendable": "boolean",
                            "s_nom_max": "double precision",
                            "s_nom_min": "double precision",
                            "scn_name": "character varying",
                            "tap_position": "bigint",
                            "tap_ratio": "double precision",
                            "tap_side": "bigint",
                            "topo": "geometry",
                            "trafo_id": "bigint",
                            "type": "text",
                            "v_ang_max": "double precision",
                            "v_ang_min": "double precision",
                            "x": "numeric",
                        },
                        not_null_columns=[
                            "b",
                            "build_year",
                            "bus0",
                            "bus1",
                            "capital_cost",
                            "g",
                            "lifetime",
                            "model",
                            "num_parallel",
                            "phase_shift",
                            "r",
                            "s_max_pu",
                            "s_nom",
                            "s_nom_extendable",
                            "s_nom_max",
                            "s_nom_min",
                            "scn_name",
                            "tap_position",
                            "tap_ratio",
                            "tap_side",
                            "trafo_id",
                            "v_ang_max",
                            "v_ang_min",
                            "x",
                        ],
                        value_set_columns={
                            "scn_name": [
                                "eGon2035",
                                "reGon2037",
                                "reGon2045",
                                "status2024",
                            ],
                        },
                    ),
                    # grid.egon_hvmv_substation
                    TableValidation(
                        table_name="grid.egon_hvmv_substation",
                        row_count=resolve_boundary_dependence(
                            {
                                "Schleswig-Holstein": 201,
                                "Everything": 4092,
                            }
                        ),
                        geometry_columns=[
                            "point",
                            "polygon",
                        ],
                        data_type_columns={
                            "bus_id": "integer",
                            "dbahn": "text",
                            "frequency": "text",
                            "lat": "double precision",
                            "lon": "double precision",
                            "operator": "text",
                            "osm_id": "text",
                            "osm_www": "text",
                            "point": "geometry",
                            "polygon": "geometry",
                            "power_type": "text",
                            "ref": "text",
                            "status": "integer",
                            "subst_name": "text",
                            "substation": "text",
                            "voltage": "text",
                        },
                        not_null_columns=[
                            "bus_id",
                            "dbahn",
                            "frequency",
                            "lat",
                            "lon",
                            "operator",
                            "osm_id",
                            "osm_www",
                            "power_type",
                            "ref",
                            "status",
                            "subst_name",
                            "substation",
                            "voltage",
                        ],
                    ),
                    # grid.egon_mv_grid_district
                    TableValidation(
                        table_name="grid.egon_mv_grid_district",
                        row_count=resolve_boundary_dependence(
                            {
                                "Schleswig-Holstein": 201,
                                "Everything": 4092,
                            }
                        ),
                        geometry_columns=[
                            "geom",
                        ],
                        data_type_columns={
                            "area": "double precision",
                            "bus_id": "integer",
                            "geom": "geometry",
                        },
                        not_null_columns=[
                            "area",
                            "bus_id",
                        ],
                    ),
                ],
            },
            # Continue pipeline even if validations fail
            proceed_on_validation_failure=True,
            trigger_rule=self.trigger_rule,
        )
