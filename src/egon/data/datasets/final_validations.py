"""
Dataset for cross-cutting validations that run at the end of the pipeline.

This module provides the FinalValidations dataset which contains validation rules
that check data consistency across multiple datasets. These validations should run
after all data generation is complete, but before the final validation report.
"""

from egon_validation import (
    ArrayCardinalityValidation,
    ElectricalLoadAggregationValidation,
)

from egon.data.datasets import Dataset
from egon.data.validation import TableValidation, resolve_boundary_dependence
from egon.data.validation.rules.custom.sanity import (
    CH4GeneratorsAbroad,
    CH4GridCapacity,
    CH4GridLinksAbroad,
    CH4LoadsAbroad,
    CH4StoresAbroad,
    CH4StoresCapacity,
    DSMTimeseries,
    ElectricalLoadSectorBreakdown,
    ElectricityCapacityComparison,
    EVAllocationCount,
    EVGridDistrictAllocation,
    EVLowflexDrivingLoad,
    EVModelComponentsCreated,
    EVModelEnergyDemand,
    EVModelSoCConstraint,
    EVModelStorageCapacity,
    EVModelTimeseriesLength,
    EVTripChargingDemand,
    EVTripTimeranges,
    GasBusesCount,
    GasBusesIsolated,
    GasBusesIsolatedAbroad,
    GasGeneratorsCapacity,
    GasLinksConnections,
    GasLoadsCapacity,
    GasOnePortConnections,
    H2LoadsAbroad,
    H2SaltcavernStoresCapacity,
    HeatDemandValidation,
    HeatGasLoadPypsaEurComparison,
    PvRooftopBuildingsValidation,
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

    *Dependencies*
      Should depend on all datasets whose data is validated by the rules
      defined here. At minimum:
      * CH4Storages - for CH4 store capacity validation
      * HydrogenStoreEtrago - for H2 saltcavern store validation
      * Add more as you add validation rules

    *Validation Results*
      Results are written to validation_runs/{run_id}/tasks/FinalValidations.validate.*/
      and collected by the ValidationReport dataset

    *Adding New Validations*
      To add new cross-cutting validations:
      1. Create the validation rule class in validation/rules/custom/sanity/
      2. Import it at the top of this file
      3. Add instances to the appropriate category in the validation dict below
      4. Update dependencies to include datasets that provide the data being validated

    Example
    -------
    To add a new gas grid validation:

    ```python
    from egon.data.validation.rules.custom.sanity import CH4GridCapacity

    # In the validation dict:
    "gas_stores": [
        # ... existing rules ...
        CH4GridCapacity(
            table="grid.egon_etrago_link",
            rule_id="SANITY_CH4_GRID_CAPACITY",
            scenario="eGon2035"
        ),
    ]
    ```
    """

    #:
    name: str = "FinalValidations"
    #:
    version: str = "0.0.1"
    #:
    create_finalize_task: bool = True

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(notasks,),  # No data tasks - only validation tasks
            validation={
                # Gas store capacity validations
                # These check that CH4 and H2 store capacities match expected values
                "gas_stores": [
                    # CH4 stores - eGon2035
                    CH4StoresCapacity(
                        table="grid.egon_etrago_store",
                        rule_id="SANITY_CH4_STORES_CAPACITY_EGON2035",
                        scenario="eGon2035",
                        rtol=0.02,
                    ),
                    # CH4 stores - eGon100RE
                    CH4StoresCapacity(
                        table="grid.egon_etrago_store",
                        rule_id="SANITY_CH4_STORES_CAPACITY_EGON100RE",
                        scenario="eGon100RE",
                        rtol=0.02,
                    ),
                    # H2 saltcavern stores - eGon2035
                    H2SaltcavernStoresCapacity(
                        table="grid.egon_etrago_store",
                        rule_id="SANITY_H2_SALTCAVERN_STORES_CAPACITY_EGON2035",
                        scenario="eGon2035",
                        rtol=0.02,
                    ),
                    # H2 saltcavern stores - eGon100RE
                    H2SaltcavernStoresCapacity(
                        table="grid.egon_etrago_store",
                        rule_id="SANITY_H2_SALTCAVERN_STORES_CAPACITY_EGON100RE",
                        scenario="eGon100RE",
                        rtol=0.02,
                    ),
                ],
                # Gas grid bus validations
                # These check that gas buses are properly connected and
                # counts match expectations
                "gas_grid": [
                    # Check for isolated CH4 buses - eGon2035
                    GasBusesIsolated(
                        table="grid.egon_etrago_bus",
                        rule_id="SANITY_GAS_BUSES_ISOLATED_CH4_EGON2035",
                        scenario="eGon2035",
                        carrier="CH4",
                    ),
                    # Check for isolated H2_grid buses - eGon2035
                    GasBusesIsolated(
                        table="grid.egon_etrago_bus",
                        rule_id="SANITY_GAS_BUSES_ISOLATED_H2_GRID_EGON2035",
                        scenario="eGon2035",
                        carrier="H2_grid",
                    ),
                    # Check for isolated H2_saltcavern buses - eGon2035
                    GasBusesIsolated(
                        table="grid.egon_etrago_bus",
                        rule_id="SANITY_GAS_BUSES_ISOLATED_H2_SALTCAVERN_EGON2035",
                        scenario="eGon2035",
                        carrier="H2_saltcavern",
                    ),
                    # NOTE: eGon100RE gas bus isolated checks are commented
                    # out because they are also commented out in the
                    # original sanity_checks.py
                    # (lines 1435-1439). Uncomment when eGon100RE gas bus data is ready.
                    # # Check for isolated CH4 buses - eGon100RE
                    # GasBusesIsolated(
                    #     table="grid.egon_etrago_bus",
                    #     rule_id="SANITY_GAS_BUSES_ISOLATED_CH4_EGON100RE",
                    #     scenario="eGon100RE",
                    #     carrier="CH4"
                    # ),
                    # # Check for isolated H2_grid buses - eGon100RE
                    # GasBusesIsolated(
                    #     table="grid.egon_etrago_bus",
                    #     rule_id="SANITY_GAS_BUSES_ISOLATED_H2_GRID_EGON100RE",
                    #     scenario="eGon100RE",
                    #     carrier="H2_grid"
                    # ),
                    # # Check for isolated H2_saltcavern buses - eGon100RE
                    # GasBusesIsolated(
                    #     table="grid.egon_etrago_bus",
                    #     rule_id="SANITY_GAS_BUSES_ISOLATED_H2_SALTCAVERN_EGON100RE",
                    #     scenario="eGon100RE",
                    #     carrier="H2_saltcavern"
                    # ),
                    # Check CH4 bus count - eGon2035
                    GasBusesCount(
                        table="grid.egon_etrago_bus",
                        rule_id="SANITY_GAS_BUSES_COUNT_CH4_EGON2035",
                        scenario="eGon2035",
                        carrier="CH4",
                        rtol=0.10,
                        expected_count=resolve_boundary_dependence(
                            {"Schleswig-Holstein": 11, "Everything": 619}
                        ),
                    ),
                    # Check H2_grid bus count - eGon2035
                    GasBusesCount(
                        table="grid.egon_etrago_bus",
                        rule_id="SANITY_GAS_BUSES_COUNT_H2_GRID_EGON2035",
                        scenario="eGon2035",
                        carrier="H2_grid",
                        rtol=0.10,
                        expected_count=resolve_boundary_dependence(
                            {"Schleswig-Holstein": 305, "Everything": 631}
                        ),
                    ),
                    # NOTE: eGon100RE gas bus count checks are commented
                    # out because sanity_check_gas_buses() is only called
                    # for eGon2035 (line 1943)
                    # # Check CH4 bus count - eGon100RE
                    # GasBusesCount(
                    #     table="grid.egon_etrago_bus",
                    #     rule_id="SANITY_GAS_BUSES_COUNT_CH4_EGON100RE",
                    #     scenario="eGon100RE",
                    #     carrier="CH4",
                    #     rtol=0.10
                    # ),
                    # # Check H2_grid bus count - eGon100RE
                    # GasBusesCount(
                    #     table="grid.egon_etrago_bus",
                    #     rule_id="SANITY_GAS_BUSES_COUNT_H2_GRID_EGON100RE",
                    #     scenario="eGon100RE",
                    #     carrier="H2_grid",
                    #     rtol=0.10
                    # ),
                    # Check CH4 grid capacity - eGon2035
                    CH4GridCapacity(
                        table="grid.egon_etrago_link",
                        rule_id="SANITY_CH4_GRID_CAPACITY_EGON2035",
                        scenario="eGon2035",
                        rtol=0.10,
                    ),
                    # Check CH4 grid capacity - eGon100RE
                    CH4GridCapacity(
                        table="grid.egon_etrago_link",
                        rule_id="SANITY_CH4_GRID_CAPACITY_EGON100RE",
                        scenario="eGon100RE",
                        rtol=0.10,
                    ),
                ],
                # Gas one-port component connection validations
                # These check that loads, generators, and stores are
                # connected to valid buses
                "gas_one_port": [
                    # LOADS - eGon2035
                    # CH4_for_industry loads in Germany must connect to CH4 buses
                    GasOnePortConnections(
                        table="grid.egon_etrago_load",
                        rule_id="SANITY_GAS_ONE_PORT_LOAD_CH4_FOR_INDUSTRY_DE_EGON2035",
                        scenario="eGon2035",
                        component_type="load",
                        component_carrier="CH4_for_industry",
                        bus_conditions=[("CH4", "= 'DE'")],
                    ),
                    # CH4 loads abroad must connect to CH4 buses outside Germany
                    GasOnePortConnections(
                        table="grid.egon_etrago_load",
                        rule_id="SANITY_GAS_ONE_PORT_LOAD_CH4_ABROAD_EGON2035",
                        scenario="eGon2035",
                        component_type="load",
                        component_carrier="CH4",
                        bus_conditions=[("CH4", "!= 'DE'")],
                    ),
                    # H2_for_industry loads must connect to H2_grid in DE or AC abroad
                    GasOnePortConnections(
                        table="grid.egon_etrago_load",
                        rule_id="SANITY_GAS_ONE_PORT_LOAD_H2_FOR_INDUSTRY_EGON2035",
                        scenario="eGon2035",
                        component_type="load",
                        component_carrier="H2_for_industry",
                        bus_conditions=[
                            ("H2_grid", "= 'DE'"),
                            ("AC", "!= 'DE'"),
                        ],
                    ),
                    # GENERATORS - eGon2035
                    # CH4 generators must connect to CH4 buses (any country)
                    GasOnePortConnections(
                        table="grid.egon_etrago_generator",
                        rule_id="SANITY_GAS_ONE_PORT_GENERATOR_CH4_EGON2035",
                        scenario="eGon2035",
                        component_type="generator",
                        component_carrier="CH4",
                        bus_conditions=[
                            ("CH4", "")
                        ],  # Any CH4 bus, no country filter
                    ),
                    # STORES - eGon2035
                    # CH4 stores must connect to CH4 buses (any country)
                    GasOnePortConnections(
                        table="grid.egon_etrago_store",
                        rule_id="SANITY_GAS_ONE_PORT_STORE_CH4_EGON2035",
                        scenario="eGon2035",
                        component_type="store",
                        component_carrier="CH4",
                        bus_conditions=[
                            ("CH4", "")
                        ],  # Any CH4 bus, no country filter
                    ),
                    # H2_underground stores must connect to H2_saltcavern
                    # buses (any country)
                    GasOnePortConnections(
                        table="grid.egon_etrago_store",
                        rule_id="SANITY_GAS_ONE_PORT_STORE_H2_UNDERGROUND_EGON2035",
                        scenario="eGon2035",
                        component_type="store",
                        component_carrier="H2_underground",
                        bus_conditions=[
                            ("H2_saltcavern", "")
                        ],  # Any H2_saltcavern bus, no country filter
                    ),
                    # H2_overground stores must connect to H2_saltcavern or
                    # H2_grid in DE
                    GasOnePortConnections(
                        table="grid.egon_etrago_store",
                        rule_id="SANITY_GAS_ONE_PORT_STORE_H2_OVERGROUND_EGON2035",
                        scenario="eGon2035",
                        component_type="store",
                        component_carrier="H2_overground",
                        bus_conditions=[
                            ("H2_saltcavern", "= 'DE'"),
                            ("H2_grid", "= 'DE'"),
                        ],
                    ),
                ],
                # Gas link connection validations
                # These check that gas links have both bus0 and bus1
                # connected to existing buses
                "gas_links": [
                    # CH4 links - eGon2035
                    GasLinksConnections(
                        table="grid.egon_etrago_link",
                        rule_id="SANITY_GAS_LINKS_CH4_EGON2035",
                        scenario="eGon2035",
                        carrier="CH4",
                    ),
                    # H2_feedin links - eGon2035
                    GasLinksConnections(
                        table="grid.egon_etrago_link",
                        rule_id="SANITY_GAS_LINKS_H2_FEEDIN_EGON2035",
                        scenario="eGon2035",
                        carrier="H2_feedin",
                    ),
                    # H2_to_CH4 links - eGon2035
                    GasLinksConnections(
                        table="grid.egon_etrago_link",
                        rule_id="SANITY_GAS_LINKS_H2_TO_CH4_EGON2035",
                        scenario="eGon2035",
                        carrier="H2_to_CH4",
                    ),
                    # CH4_to_H2 links - eGon2035
                    GasLinksConnections(
                        table="grid.egon_etrago_link",
                        rule_id="SANITY_GAS_LINKS_CH4_TO_H2_EGON2035",
                        scenario="eGon2035",
                        carrier="CH4_to_H2",
                    ),
                    # H2_to_power links - eGon2035
                    GasLinksConnections(
                        table="grid.egon_etrago_link",
                        rule_id="SANITY_GAS_LINKS_H2_TO_POWER_EGON2035",
                        scenario="eGon2035",
                        carrier="H2_to_power",
                    ),
                    # power_to_H2 links - eGon2035
                    GasLinksConnections(
                        table="grid.egon_etrago_link",
                        rule_id="SANITY_GAS_LINKS_POWER_TO_H2_EGON2035",
                        scenario="eGon2035",
                        carrier="power_to_H2",
                    ),
                    # OCGT links - eGon2035
                    GasLinksConnections(
                        table="grid.egon_etrago_link",
                        rule_id="SANITY_GAS_LINKS_OCGT_EGON2035",
                        scenario="eGon2035",
                        carrier="OCGT",
                    ),
                    # central_gas_boiler links - eGon2035
                    GasLinksConnections(
                        table="grid.egon_etrago_link",
                        rule_id="SANITY_GAS_LINKS_CENTRAL_GAS_BOILER_EGON2035",
                        scenario="eGon2035",
                        carrier="central_gas_boiler",
                    ),
                    # central_gas_CHP links - eGon2035
                    GasLinksConnections(
                        table="grid.egon_etrago_link",
                        rule_id="SANITY_GAS_LINKS_CENTRAL_GAS_CHP_EGON2035",
                        scenario="eGon2035",
                        carrier="central_gas_CHP",
                    ),
                    # central_gas_CHP_heat links - eGon2035
                    GasLinksConnections(
                        table="grid.egon_etrago_link",
                        rule_id="SANITY_GAS_LINKS_CENTRAL_GAS_CHP_HEAT_EGON2035",
                        scenario="eGon2035",
                        carrier="central_gas_CHP_heat",
                    ),
                    # industrial_gas_CHP links - eGon2035
                    GasLinksConnections(
                        table="grid.egon_etrago_link",
                        rule_id="SANITY_GAS_LINKS_INDUSTRIAL_GAS_CHP_EGON2035",
                        scenario="eGon2035",
                        carrier="industrial_gas_CHP",
                    ),
                ],
                # Gas loads and generators capacity validations
                # These check that gas demand and generation capacity
                # match reference data
                "gas_loads_generators": [
                    # CH4_for_industry loads - eGon2035
                    GasLoadsCapacity(
                        table="grid.egon_etrago_load",
                        rule_id="SANITY_GAS_LOADS_CH4_FOR_INDUSTRY_EGON2035",
                        scenario="eGon2035",
                        carrier="CH4_for_industry",
                        rtol=0.10,
                        expected_load=resolve_boundary_dependence(
                            {"Schleswig-Holstein": 5.37, "Everything": 196.01}
                        ),
                    ),
                    # H2_for_industry loads - eGon2035
                    GasLoadsCapacity(
                        table="grid.egon_etrago_load",
                        rule_id="SANITY_GAS_LOADS_H2_FOR_INDUSTRY_EGON2035",
                        scenario="eGon2035",
                        carrier="H2_for_industry",
                        rtol=0.10,
                        expected_load=resolve_boundary_dependence(
                            {"Schleswig-Holstein": 0.33, "Everything": 16.07}
                        ),
                    ),
                    # CH4 generators - eGon2035
                    GasGeneratorsCapacity(
                        table="grid.egon_etrago_generator",
                        rule_id="SANITY_GAS_GENERATORS_CH4_EGON2035",
                        scenario="eGon2035",
                        carrier="CH4",
                        rtol=0.10,
                        expected_capacity=resolve_boundary_dependence(
                            {
                                "Schleswig-Holstein": 35.63,
                                "Everything": 4185700.39,
                            }
                        ),
                    ),
                ],
                # Electricity capacity validations
                # These check that distributed generator and storage
                # capacities match input capacities
                "electricity_capacity": [
                    # GENERATORS - eGon2035
                    # Wind onshore
                    ElectricityCapacityComparison(
                        table="grid.egon_etrago_generator",
                        rule_id="SANITY_ELECTRICITY_GENERATOR_WIND_ONSHORE_EGON2035",
                        scenario="eGon2035",
                        carrier="wind_onshore",
                        component_type="generator",
                        rtol=0.10,
                    ),
                    # Wind offshore
                    ElectricityCapacityComparison(
                        table="grid.egon_etrago_generator",
                        rule_id="SANITY_ELECTRICITY_GENERATOR_WIND_OFFSHORE_EGON2035",
                        scenario="eGon2035",
                        carrier="wind_offshore",
                        component_type="generator",
                        rtol=0.10,
                    ),
                    # Solar
                    ElectricityCapacityComparison(
                        table="grid.egon_etrago_generator",
                        rule_id="SANITY_ELECTRICITY_GENERATOR_SOLAR_EGON2035",
                        scenario="eGon2035",
                        carrier="solar",
                        component_type="generator",
                        rtol=0.10,
                    ),
                    # Solar rooftop
                    ElectricityCapacityComparison(
                        table="grid.egon_etrago_generator",
                        rule_id="SANITY_ELECTRICITY_GENERATOR_SOLAR_ROOFTOP_EGON2035",
                        scenario="eGon2035",
                        carrier="solar_rooftop",
                        component_type="generator",
                        rtol=0.10,
                    ),
                    # Biomass (maps to multiple output carriers: biomass,
                    # industrial_biomass_CHP, central_biomass_CHP)
                    ElectricityCapacityComparison(
                        table="grid.egon_etrago_generator",
                        rule_id="SANITY_ELECTRICITY_GENERATOR_BIOMASS_EGON2035",
                        scenario="eGon2035",
                        carrier="biomass",
                        component_type="generator",
                        output_carriers=[
                            "biomass",
                            "industrial_biomass_CHP",
                            "central_biomass_CHP",
                        ],
                        rtol=0.10,
                    ),
                    # Run of river
                    ElectricityCapacityComparison(
                        table="grid.egon_etrago_generator",
                        rule_id="SANITY_ELECTRICITY_GENERATOR_RUN_OF_RIVER_EGON2035",
                        scenario="eGon2035",
                        carrier="run_of_river",
                        component_type="generator",
                        rtol=0.10,
                    ),
                    # Reservoir
                    ElectricityCapacityComparison(
                        table="grid.egon_etrago_generator",
                        rule_id="SANITY_ELECTRICITY_GENERATOR_RESERVOIR_EGON2035",
                        scenario="eGon2035",
                        carrier="reservoir",
                        component_type="generator",
                        rtol=0.10,
                    ),
                    # Oil
                    ElectricityCapacityComparison(
                        table="grid.egon_etrago_generator",
                        rule_id="SANITY_ELECTRICITY_GENERATOR_OIL_EGON2035",
                        scenario="eGon2035",
                        carrier="oil",
                        component_type="generator",
                        rtol=0.10,
                    ),
                    # Others
                    ElectricityCapacityComparison(
                        table="grid.egon_etrago_generator",
                        rule_id="SANITY_ELECTRICITY_GENERATOR_OTHERS_EGON2035",
                        scenario="eGon2035",
                        carrier="others",
                        component_type="generator",
                        rtol=0.10,
                    ),
                    # STORAGE - eGon2035
                    # Pumped hydro
                    ElectricityCapacityComparison(
                        table="grid.egon_etrago_storage",
                        rule_id="SANITY_ELECTRICITY_STORAGE_PUMPED_HYDRO_EGON2035",
                        scenario="eGon2035",
                        carrier="pumped_hydro",
                        component_type="storage",
                        rtol=0.10,
                    ),
                    # GENERATORS - eGon100RE
                    # Wind onshore
                    ElectricityCapacityComparison(
                        table="grid.egon_etrago_generator",
                        rule_id="SANITY_ELECTRICITY_GENERATOR_WIND_ONSHORE_EGON100RE",
                        scenario="eGon100RE",
                        carrier="wind_onshore",
                        component_type="generator",
                        rtol=0.10,
                    ),
                    # Wind offshore
                    ElectricityCapacityComparison(
                        table="grid.egon_etrago_generator",
                        rule_id="SANITY_ELECTRICITY_GENERATOR_WIND_OFFSHORE_EGON100RE",
                        scenario="eGon100RE",
                        carrier="wind_offshore",
                        component_type="generator",
                        rtol=0.10,
                    ),
                    # Solar
                    ElectricityCapacityComparison(
                        table="grid.egon_etrago_generator",
                        rule_id="SANITY_ELECTRICITY_GENERATOR_SOLAR_EGON100RE",
                        scenario="eGon100RE",
                        carrier="solar",
                        component_type="generator",
                        rtol=0.10,
                    ),
                    # Solar rooftop
                    ElectricityCapacityComparison(
                        table="grid.egon_etrago_generator",
                        rule_id="SANITY_ELECTRICITY_GENERATOR_SOLAR_ROOFTOP_EGON100RE",
                        scenario="eGon100RE",
                        carrier="solar_rooftop",
                        component_type="generator",
                        rtol=0.10,
                    ),
                    # Run of river
                    ElectricityCapacityComparison(
                        table="grid.egon_etrago_generator",
                        rule_id="SANITY_ELECTRICITY_GENERATOR_RUN_OF_RIVER_EGON100RE",
                        scenario="eGon100RE",
                        carrier="run_of_river",
                        component_type="generator",
                        rtol=0.10,
                    ),
                    # Oil
                    ElectricityCapacityComparison(
                        table="grid.egon_etrago_generator",
                        rule_id="SANITY_ELECTRICITY_GENERATOR_OIL_EGON100RE",
                        scenario="eGon100RE",
                        carrier="oil",
                        component_type="generator",
                        rtol=0.10,
                    ),
                    # Lignite
                    ElectricityCapacityComparison(
                        table="grid.egon_etrago_generator",
                        rule_id="SANITY_ELECTRICITY_GENERATOR_LIGNITE_EGON100RE",
                        scenario="eGon100RE",
                        carrier="lignite",
                        component_type="generator",
                        rtol=0.10,
                    ),
                    # Coal
                    ElectricityCapacityComparison(
                        table="grid.egon_etrago_generator",
                        rule_id="SANITY_ELECTRICITY_GENERATOR_COAL_EGON100RE",
                        scenario="eGon100RE",
                        carrier="coal",
                        component_type="generator",
                        rtol=0.10,
                    ),
                    # Solar thermal collector
                    ElectricityCapacityComparison(
                        table="grid.egon_etrago_generator",
                        rule_id="SANITY_ELECTRICITY_GENERATOR_SOLAR_THERMAL_EGON100RE",
                        scenario="eGon100RE",
                        carrier="urban_central_solar_thermal_collector",
                        component_type="generator",
                        output_carriers=["solar_thermal_collector"],
                        rtol=0.10,
                    ),
                    # Geothermal
                    ElectricityCapacityComparison(
                        table="grid.egon_etrago_generator",
                        rule_id="SANITY_ELECTRICITY_GENERATOR_GEO_THERMAL_EGON100RE",
                        scenario="eGon100RE",
                        carrier="urban_central_geo_thermal",
                        component_type="generator",
                        output_carriers=["geo_thermal"],
                        rtol=0.10,
                    ),
                    # Rural solar thermal
                    ElectricityCapacityComparison(
                        table="grid.egon_etrago_generator",
                        rule_id=(
                            "SANITY_ELECTRICITY_GENERATOR_"
                            "RURAL_SOLAR_THERMAL_EGON100RE"
                        ),
                        scenario="eGon100RE",
                        carrier="rural_solar_thermal",
                        component_type="generator",
                        rtol=0.10,
                    ),
                    # Urban central gas CHP
                    ElectricityCapacityComparison(
                        table="grid.egon_etrago_generator",
                        rule_id="SANITY_ELECTRICITY_GENERATOR_URBAN_GAS_CHP_EGON100RE",
                        scenario="eGon100RE",
                        carrier="urban_central_gas_CHP",
                        component_type="generator",
                        rtol=0.10,
                    ),
                    # Urban central solid biomass CHP
                    ElectricityCapacityComparison(
                        table="grid.egon_etrago_generator",
                        rule_id="SANITY_ELECTRICITY_GENERATOR_BIOMASS_CHP_EGON100RE",
                        scenario="eGon100RE",
                        carrier="urban_central_solid_biomass_CHP",
                        component_type="generator",
                        rtol=0.10,
                    ),
                    # LINKS - eGon100RE
                    # Central gas boiler
                    ElectricityCapacityComparison(
                        table="grid.egon_etrago_link",
                        rule_id="SANITY_ELECTRICITY_LINK_CENTRAL_GAS_BOILER_EGON100RE",
                        scenario="eGon100RE",
                        carrier="urban_central_gas_boiler",
                        component_type="link",
                        output_carriers=["central_gas_boiler"],
                        rtol=0.10,
                    ),
                    # Central heat pump
                    ElectricityCapacityComparison(
                        table="grid.egon_etrago_link",
                        rule_id="SANITY_ELECTRICITY_LINK_CENTRAL_HEAT_PUMP_EGON100RE",
                        scenario="eGon100RE",
                        carrier="urban_central_heat_pump",
                        component_type="link",
                        output_carriers=["central_heat_pump"],
                        rtol=0.10,
                    ),
                    # Central resistive heater
                    ElectricityCapacityComparison(
                        table="grid.egon_etrago_link",
                        rule_id=(
                            "SANITY_ELECTRICITY_LINK_"
                            "CENTRAL_RESISTIVE_HEATER_EGON100RE"
                        ),
                        scenario="eGon100RE",
                        carrier="urban_central_resistive_heater",
                        component_type="link",
                        output_carriers=["central_resistive_heater"],
                        rtol=0.10,
                    ),
                    # OCGT (gas)
                    ElectricityCapacityComparison(
                        table="grid.egon_etrago_link",
                        rule_id="SANITY_ELECTRICITY_LINK_OCGT_EGON100RE",
                        scenario="eGon100RE",
                        carrier="gas",
                        component_type="link",
                        output_carriers=["OCGT"],
                        rtol=0.10,
                    ),
                    # Rural biomass boiler
                    ElectricityCapacityComparison(
                        table="grid.egon_etrago_link",
                        rule_id=(
                            "SANITY_ELECTRICITY_LINK_"
                            "RURAL_BIOMASS_BOILER_EGON100RE"
                        ),
                        scenario="eGon100RE",
                        carrier="rural_biomass_boiler",
                        component_type="link",
                        rtol=0.10,
                    ),
                    # Rural gas boiler
                    ElectricityCapacityComparison(
                        table="grid.egon_etrago_link",
                        rule_id="SANITY_ELECTRICITY_LINK_RURAL_GAS_BOILER_EGON100RE",
                        scenario="eGon100RE",
                        carrier="rural_gas_boiler",
                        component_type="link",
                        rtol=0.10,
                    ),
                    # Rural heat pump
                    ElectricityCapacityComparison(
                        table="grid.egon_etrago_link",
                        rule_id="SANITY_ELECTRICITY_LINK_RURAL_HEAT_PUMP_EGON100RE",
                        scenario="eGon100RE",
                        carrier="rural_heat_pump",
                        component_type="link",
                        rtol=0.10,
                    ),
                    # Rural oil boiler
                    ElectricityCapacityComparison(
                        table="grid.egon_etrago_link",
                        rule_id="SANITY_ELECTRICITY_LINK_RURAL_OIL_BOILER_EGON100RE",
                        scenario="eGon100RE",
                        carrier="rural_oil_boiler",
                        component_type="link",
                        rtol=0.10,
                    ),
                    # Rural resistive heater
                    ElectricityCapacityComparison(
                        table="grid.egon_etrago_link",
                        rule_id=(
                            "SANITY_ELECTRICITY_LINK_"
                            "RURAL_RESISTIVE_HEATER_EGON100RE"
                        ),
                        scenario="eGon100RE",
                        carrier="rural_resistive_heater",
                        component_type="link",
                        rtol=0.10,
                    ),
                    # STORAGE - eGon100RE
                    # Pumped hydro
                    ElectricityCapacityComparison(
                        table="grid.egon_etrago_storage",
                        rule_id="SANITY_ELECTRICITY_STORAGE_PUMPED_HYDRO_EGON100RE",
                        scenario="eGon100RE",
                        carrier="pumped_hydro",
                        component_type="storage",
                        rtol=0.10,
                    ),
                ],
                # Heat capacity validations
                # These check that distributed heat supply capacities match
                # input capacities
                "heat_capacity": [
                    # LINKS - eGon2035
                    # Central heat pump
                    ElectricityCapacityComparison(
                        table="grid.egon_etrago_link",
                        rule_id="SANITY_HEAT_LINK_CENTRAL_HEAT_PUMP_EGON2035",
                        scenario="eGon2035",
                        carrier="urban_central_heat_pump",
                        component_type="link",
                        output_carriers=["central_heat_pump"],
                        rtol=0.10,
                    ),
                    # Rural heat pump
                    ElectricityCapacityComparison(
                        table="grid.egon_etrago_link",
                        rule_id="SANITY_HEAT_LINK_RURAL_HEAT_PUMP_EGON2035",
                        scenario="eGon2035",
                        carrier="residential_rural_heat_pump",
                        component_type="link",
                        output_carriers=["rural_heat_pump"],
                        rtol=0.10,
                    ),
                    # Central resistive heater
                    ElectricityCapacityComparison(
                        table="grid.egon_etrago_link",
                        rule_id="SANITY_HEAT_LINK_CENTRAL_RESISTIVE_HEATER_EGON2035",
                        scenario="eGon2035",
                        carrier="urban_central_resistive_heater",
                        component_type="link",
                        output_carriers=["central_resistive_heater"],
                        rtol=0.10,
                    ),
                    # GENERATORS - eGon2035
                    # Solar thermal collector
                    ElectricityCapacityComparison(
                        table="grid.egon_etrago_generator",
                        rule_id="SANITY_HEAT_GENERATOR_SOLAR_THERMAL_EGON2035",
                        scenario="eGon2035",
                        carrier="urban_central_solar_thermal_collector",
                        component_type="generator",
                        output_carriers=["solar_thermal_collector"],
                        rtol=0.10,
                    ),
                    # Geothermal
                    ElectricityCapacityComparison(
                        table="grid.egon_etrago_generator",
                        rule_id="SANITY_HEAT_GENERATOR_GEO_THERMAL_EGON2035",
                        scenario="eGon2035",
                        carrier="urban_central_geo_thermal",
                        component_type="generator",
                        output_carriers=["geo_thermal"],
                        rtol=0.10,
                    ),
                ],
                # Timeseries length validations
                # These check that all timeseries arrays have the expected
                # length (8760 hours)
                # NOTE: All array columns are validated to match original
                # sanity_checks.py which dynamically discovers all array
                # columns (lines 2465-2494)
                "timeseries_length": [
                    # Generator timeseries - all array columns
                    ArrayCardinalityValidation(
                        rule_id="SANITY_TIMESERIES_GENERATOR_P_SET",
                        task="FinalValidations.timeseries_length",
                        table="grid.egon_etrago_generator_timeseries",
                        array_column="p_set",
                        expected_length=8760,
                    ),
                    ArrayCardinalityValidation(
                        rule_id="SANITY_TIMESERIES_GENERATOR_Q_SET",
                        task="FinalValidations.timeseries_length",
                        table="grid.egon_etrago_generator_timeseries",
                        array_column="q_set",
                        expected_length=8760,
                    ),
                    ArrayCardinalityValidation(
                        rule_id="SANITY_TIMESERIES_GENERATOR_P_MIN_PU",
                        task="FinalValidations.timeseries_length",
                        table="grid.egon_etrago_generator_timeseries",
                        array_column="p_min_pu",
                        expected_length=8760,
                    ),
                    ArrayCardinalityValidation(
                        rule_id="SANITY_TIMESERIES_GENERATOR_P_MAX_PU",
                        task="FinalValidations.timeseries_length",
                        table="grid.egon_etrago_generator_timeseries",
                        array_column="p_max_pu",
                        expected_length=8760,
                    ),
                    ArrayCardinalityValidation(
                        rule_id="SANITY_TIMESERIES_GENERATOR_MARGINAL_COST",
                        task="FinalValidations.timeseries_length",
                        table="grid.egon_etrago_generator_timeseries",
                        array_column="marginal_cost",
                        expected_length=8760,
                    ),
                    # Load timeseries - all array columns
                    ArrayCardinalityValidation(
                        rule_id="SANITY_TIMESERIES_LOAD_P_SET",
                        task="FinalValidations.timeseries_length",
                        table="grid.egon_etrago_load_timeseries",
                        array_column="p_set",
                        expected_length=8760,
                    ),
                    ArrayCardinalityValidation(
                        rule_id="SANITY_TIMESERIES_LOAD_Q_SET",
                        task="FinalValidations.timeseries_length",
                        table="grid.egon_etrago_load_timeseries",
                        array_column="q_set",
                        expected_length=8760,
                    ),
                    # Link timeseries - all array columns
                    ArrayCardinalityValidation(
                        rule_id="SANITY_TIMESERIES_LINK_P_SET",
                        task="FinalValidations.timeseries_length",
                        table="grid.egon_etrago_link_timeseries",
                        array_column="p_set",
                        expected_length=8760,
                    ),
                    ArrayCardinalityValidation(
                        rule_id="SANITY_TIMESERIES_LINK_P_MIN_PU",
                        task="FinalValidations.timeseries_length",
                        table="grid.egon_etrago_link_timeseries",
                        array_column="p_min_pu",
                        expected_length=8760,
                    ),
                    ArrayCardinalityValidation(
                        rule_id="SANITY_TIMESERIES_LINK_P_MAX_PU",
                        task="FinalValidations.timeseries_length",
                        table="grid.egon_etrago_link_timeseries",
                        array_column="p_max_pu",
                        expected_length=8760,
                    ),
                    ArrayCardinalityValidation(
                        rule_id="SANITY_TIMESERIES_LINK_EFFICIENCY",
                        task="FinalValidations.timeseries_length",
                        table="grid.egon_etrago_link_timeseries",
                        array_column="efficiency",
                        expected_length=8760,
                    ),
                    ArrayCardinalityValidation(
                        rule_id="SANITY_TIMESERIES_LINK_MARGINAL_COST",
                        task="FinalValidations.timeseries_length",
                        table="grid.egon_etrago_link_timeseries",
                        array_column="marginal_cost",
                        expected_length=8760,
                    ),
                    # Storage timeseries - all array columns
                    ArrayCardinalityValidation(
                        rule_id="SANITY_TIMESERIES_STORAGE_P_SET",
                        task="FinalValidations.timeseries_length",
                        table="grid.egon_etrago_storage_timeseries",
                        array_column="p_set",
                        expected_length=8760,
                    ),
                    ArrayCardinalityValidation(
                        rule_id="SANITY_TIMESERIES_STORAGE_Q_SET",
                        task="FinalValidations.timeseries_length",
                        table="grid.egon_etrago_storage_timeseries",
                        array_column="q_set",
                        expected_length=8760,
                    ),
                    ArrayCardinalityValidation(
                        rule_id="SANITY_TIMESERIES_STORAGE_P_MIN_PU",
                        task="FinalValidations.timeseries_length",
                        table="grid.egon_etrago_storage_timeseries",
                        array_column="p_min_pu",
                        expected_length=8760,
                    ),
                    ArrayCardinalityValidation(
                        rule_id="SANITY_TIMESERIES_STORAGE_P_MAX_PU",
                        task="FinalValidations.timeseries_length",
                        table="grid.egon_etrago_storage_timeseries",
                        array_column="p_max_pu",
                        expected_length=8760,
                    ),
                    ArrayCardinalityValidation(
                        rule_id="SANITY_TIMESERIES_STORAGE_STATE_OF_CHARGE_SET",
                        task="FinalValidations.timeseries_length",
                        table="grid.egon_etrago_storage_timeseries",
                        array_column="state_of_charge_set",
                        expected_length=8760,
                    ),
                    ArrayCardinalityValidation(
                        rule_id="SANITY_TIMESERIES_STORAGE_INFLOW",
                        task="FinalValidations.timeseries_length",
                        table="grid.egon_etrago_storage_timeseries",
                        array_column="inflow",
                        expected_length=8760,
                    ),
                    ArrayCardinalityValidation(
                        rule_id="SANITY_TIMESERIES_STORAGE_MARGINAL_COST",
                        task="FinalValidations.timeseries_length",
                        table="grid.egon_etrago_storage_timeseries",
                        array_column="marginal_cost",
                        expected_length=8760,
                    ),
                    # Store timeseries - all array columns
                    ArrayCardinalityValidation(
                        rule_id="SANITY_TIMESERIES_STORE_P_SET",
                        task="FinalValidations.timeseries_length",
                        table="grid.egon_etrago_store_timeseries",
                        array_column="p_set",
                        expected_length=8760,
                    ),
                    ArrayCardinalityValidation(
                        rule_id="SANITY_TIMESERIES_STORE_Q_SET",
                        task="FinalValidations.timeseries_length",
                        table="grid.egon_etrago_store_timeseries",
                        array_column="q_set",
                        expected_length=8760,
                    ),
                    ArrayCardinalityValidation(
                        rule_id="SANITY_TIMESERIES_STORE_E_MIN_PU",
                        task="FinalValidations.timeseries_length",
                        table="grid.egon_etrago_store_timeseries",
                        array_column="e_min_pu",
                        expected_length=8760,
                    ),
                    ArrayCardinalityValidation(
                        rule_id="SANITY_TIMESERIES_STORE_E_MAX_PU",
                        task="FinalValidations.timeseries_length",
                        table="grid.egon_etrago_store_timeseries",
                        array_column="e_max_pu",
                        expected_length=8760,
                    ),
                    ArrayCardinalityValidation(
                        rule_id="SANITY_TIMESERIES_STORE_MARGINAL_COST",
                        task="FinalValidations.timeseries_length",
                        table="grid.egon_etrago_store_timeseries",
                        array_column="marginal_cost",
                        expected_length=8760,
                    ),
                ],
                # Electrical load demand validations
                # Validates annual electrical load sums against expected values
                "electrical_load": [
                    # Total AC load aggregation for all scenarios
                    # (eGon2035, eGon100RE, etc.)
                    ElectricalLoadAggregationValidation(
                        rule_id="SANITY_ELECTRICAL_LOAD_AGGREGATION",
                        task="FinalValidations.electrical_load",
                        table="grid.egon_etrago_load",
                        tolerance=0.05,  # 5% tolerance
                    ),
                    # Sector breakdown validation for eGon100RE
                    # Validates residential (90.4 TWh), commercial (146.7 TWh),
                    # industrial (382.9 TWh), and total (620.0 TWh) loads
                    ElectricalLoadSectorBreakdown(
                        rule_id="SANITY_ELECTRICAL_LOAD_SECTOR_BREAKDOWN_EGON100RE",
                        task="FinalValidations.electrical_load",
                        table="grid.egon_etrago_load",
                        scenario="eGon100RE",
                        rtol=0.01,  # 1% tolerance as in original
                    ),
                ],
                # Heat demand validations
                # Validates annual heat demand against peta_heat reference values
                "heat_demand": [
                    # Heat demand - eGon2035
                    HeatDemandValidation(
                        table="grid.egon_etrago_load",
                        rule_id="SANITY_HEAT_DEMAND_EGON2035",
                        scenario="eGon2035",
                        rtol=0.02,  # 2% tolerance
                    ),
                ],
                "dsm": [
                    # DSM timeseries validation - eGon2035
                    # Checks that DSM link/store timeseries match
                    # aggregated individual data
                    DSMTimeseries(
                        table="grid.egon_etrago_link",
                        rule_id="SANITY_DSM_TIMESERIES_EGON2035",
                        scenario="eGon2035",
                    ),
                    # DSM timeseries validation - eGon100RE
                    DSMTimeseries(
                        table="grid.egon_etrago_link",
                        rule_id="SANITY_DSM_TIMESERIES_EGON100RE",
                        scenario="eGon100RE",
                    ),
                ],
                "pv_rooftop": [
                    # PV rooftop buildings validation - eGon2035
                    # Checks building assignments and capacity against
                    # scenario_data
                    PvRooftopBuildingsValidation(
                        table="supply.egon_power_plants_pv_roof_building",
                        rule_id="SANITY_PV_ROOFTOP_BUILDINGS_EGON2035",
                        scenario="eGon2035",
                        rtol=0.01,  # 1% tolerance
                    ),
                    # PV rooftop buildings validation - eGon100RE
                    # Checks building assignments and capacity against
                    # scenario_capacities
                    PvRooftopBuildingsValidation(
                        table="supply.egon_power_plants_pv_roof_building",
                        rule_id="SANITY_PV_ROOFTOP_BUILDINGS_EGON100RE",
                        scenario="eGon100RE",
                        rtol=0.01,  # 1% tolerance
                    ),
                ],
                "emobility_mit": [
                    # EV allocation count - eGon2035
                    # Only validates when TESTMODE_OFF (boundary == "Everything")
                    EVAllocationCount(
                        table="demand.egon_ev_count_mv_grid_district",
                        rule_id="SANITY_EV_ALLOCATION_COUNT_EGON2035",
                        scenario="eGon2035",
                    ),
                    # EV allocation count - eGon100RE
                    EVAllocationCount(
                        table="demand.egon_ev_count_mv_grid_district",
                        rule_id="SANITY_EV_ALLOCATION_COUNT_EGON100RE",
                        scenario="eGon100RE",
                    ),
                    # EV grid district allocation - eGon2035
                    EVGridDistrictAllocation(
                        table="demand.egon_ev_mv_grid_district",
                        rule_id="SANITY_EV_GRID_DISTRICT_ALLOCATION_EGON2035",
                        scenario="eGon2035",
                    ),
                    # EV grid district allocation - eGon100RE
                    EVGridDistrictAllocation(
                        table="demand.egon_ev_mv_grid_district",
                        rule_id="SANITY_EV_GRID_DISTRICT_ALLOCATION_EGON100RE",
                        scenario="eGon100RE",
                    ),
                    # EV trip timeranges - eGon2035
                    EVTripTimeranges(
                        table="demand.egon_ev_trip",
                        rule_id="SANITY_EV_TRIP_TIMERANGES_EGON2035",
                        scenario="eGon2035",
                    ),
                    # EV trip timeranges - eGon100RE
                    EVTripTimeranges(
                        table="demand.egon_ev_trip",
                        rule_id="SANITY_EV_TRIP_TIMERANGES_EGON100RE",
                        scenario="eGon100RE",
                    ),
                    # EV trip charging demand - eGon2035
                    EVTripChargingDemand(
                        table="demand.egon_ev_trip",
                        rule_id="SANITY_EV_TRIP_CHARGING_DEMAND_EGON2035",
                        scenario="eGon2035",
                    ),
                    # EV trip charging demand - eGon100RE
                    EVTripChargingDemand(
                        table="demand.egon_ev_trip",
                        rule_id="SANITY_EV_TRIP_CHARGING_DEMAND_EGON100RE",
                        scenario="eGon100RE",
                    ),
                    # EV model components created - eGon2035
                    EVModelComponentsCreated(
                        table="grid.egon_etrago_load",
                        rule_id="SANITY_EV_MODEL_COMPONENTS_EGON2035",
                        scenario="eGon2035",
                    ),
                    # EV model components created - eGon100RE
                    EVModelComponentsCreated(
                        table="grid.egon_etrago_load",
                        rule_id="SANITY_EV_MODEL_COMPONENTS_EGON100RE",
                        scenario="eGon100RE",
                    ),
                    # EV model timeseries length - eGon2035
                    EVModelTimeseriesLength(
                        table="grid.egon_etrago_load_timeseries",
                        rule_id="SANITY_EV_MODEL_TIMESERIES_LENGTH_EGON2035",
                        scenario="eGon2035",
                    ),
                    # EV model timeseries length - eGon100RE
                    EVModelTimeseriesLength(
                        table="grid.egon_etrago_load_timeseries",
                        rule_id="SANITY_EV_MODEL_TIMESERIES_LENGTH_EGON100RE",
                        scenario="eGon100RE",
                    ),
                    # EV model energy demand - eGon2035
                    EVModelEnergyDemand(
                        table="grid.egon_etrago_load",
                        rule_id="SANITY_EV_MODEL_ENERGY_DEMAND_EGON2035",
                        scenario="eGon2035",
                        rtol=0.1,  # 10% tolerance
                    ),
                    # EV model energy demand - eGon100RE
                    EVModelEnergyDemand(
                        table="grid.egon_etrago_load",
                        rule_id="SANITY_EV_MODEL_ENERGY_DEMAND_EGON100RE",
                        scenario="eGon100RE",
                        rtol=0.1,  # 10% tolerance
                    ),
                    # EV model storage capacity - eGon2035
                    EVModelStorageCapacity(
                        table="grid.egon_etrago_store",
                        rule_id="SANITY_EV_MODEL_STORAGE_CAPACITY_EGON2035",
                        scenario="eGon2035",
                        rtol=0.01,  # 1% tolerance
                    ),
                    # EV model storage capacity - eGon100RE
                    EVModelStorageCapacity(
                        table="grid.egon_etrago_store",
                        rule_id="SANITY_EV_MODEL_STORAGE_CAPACITY_EGON100RE",
                        scenario="eGon100RE",
                        rtol=0.01,  # 1% tolerance
                    ),
                    # EV model SoC constraint - eGon2035
                    EVModelSoCConstraint(
                        table="grid.egon_etrago_store_timeseries",
                        rule_id="SANITY_EV_MODEL_SOC_CONSTRAINT_EGON2035",
                        scenario="eGon2035",
                    ),
                    # EV model SoC constraint - eGon100RE
                    EVModelSoCConstraint(
                        table="grid.egon_etrago_store_timeseries",
                        rule_id="SANITY_EV_MODEL_SOC_CONSTRAINT_EGON100RE",
                        scenario="eGon100RE",
                    ),
                    # EV lowflex driving load comparison
                    EVLowflexDrivingLoad(
                        table="grid.egon_etrago_load",
                        rule_id="SANITY_EV_LOWFLEX_DRIVING_LOAD",
                        rtol=0.01,  # 1% tolerance
                    ),
                ],
                "gas_abroad": [
                    # Gas buses isolated abroad - eGon2035
                    # Only runs when TESTMODE_OFF (boundary == "Everything")
                    GasBusesIsolatedAbroad(
                        table="grid.egon_etrago_bus",
                        rule_id="SANITY_GAS_BUSES_ISOLATED_ABROAD_CH4_EGON2035",
                        scenario="eGon2035",
                        carrier="CH4",
                    ),
                    # CH4 loads abroad - eGon2035
                    CH4LoadsAbroad(
                        table="grid.egon_etrago_load",
                        rule_id="SANITY_CH4_LOADS_ABROAD_EGON2035",
                        scenario="eGon2035",
                        rtol=0.10,  # 10% tolerance
                    ),
                    # H2 loads abroad - eGon2035
                    H2LoadsAbroad(
                        table="grid.egon_etrago_load",
                        rule_id="SANITY_H2_LOADS_ABROAD_EGON2035",
                        scenario="eGon2035",
                        rtol=0.10,  # 10% tolerance
                    ),
                    # CH4 generators abroad - eGon2035
                    CH4GeneratorsAbroad(
                        table="grid.egon_etrago_generator",
                        rule_id="SANITY_CH4_GENERATORS_ABROAD_EGON2035",
                        scenario="eGon2035",
                        rtol=0.10,  # 10% tolerance
                    ),
                    # CH4 stores abroad - eGon2035
                    CH4StoresAbroad(
                        table="grid.egon_etrago_store",
                        rule_id="SANITY_CH4_STORES_ABROAD_EGON2035",
                        scenario="eGon2035",
                        rtol=0.10,  # 10% tolerance
                    ),
                    # CH4 grid links abroad - eGon2035
                    CH4GridLinksAbroad(
                        table="grid.egon_etrago_link",
                        rule_id="SANITY_CH4_GRID_LINKS_ABROAD_EGON2035",
                        scenario="eGon2035",
                        rtol=0.10,  # 10% tolerance
                    ),
                ],
                "heat_gas_load_pypsa_eur": [
                    # Heat and gas loads comparison with PyPSA-Eur - eGon100RE
                    # Validates all carriers against PyPSA-Eur network
                    HeatGasLoadPypsaEurComparison(
                        table="grid.egon_etrago_load",
                        rule_id="SANITY_HEAT_GAS_LOAD_PYPSA_EUR_EGON100RE",
                        scenario="eGon100RE",
                        rtol=0.10,  # 10% tolerance
                    ),
                ],
                "data-quality": [
                    # ==========================================
                    # Grid component validations using TableValidation
                    # ==========================================
                    # grid.egon_etrago_bus
                    TableValidation(
                        table_name="grid.egon_etrago_bus",
                        row_count=resolve_boundary_dependence(
                            {"Schleswig-Holstein": 6178, "Everything": 85710}
                        ),
                        geometry_columns=["geom"],
                        data_type_columns={
                            "scn_name": "character varying",
                            "bus_id": "bigint",
                            "v_nom": "double precision",
                            "type": "text",
                            "carrier": "text",
                            "v_mag_pu_set": "double precision",
                            "v_mag_pu_min": "double precision",
                            "v_mag_pu_max": "double precision",
                            "x": "double precision",
                            "y": "double precision",
                            "geom": "geometry",
                            "country": "text",
                        },
                        not_null_columns=[
                            "scn_name",
                            "bus_id",
                            "v_nom",
                            "carrier",
                            "v_mag_pu_min",
                            "v_mag_pu_max",
                            "x",
                            "y",
                            "geom",
                        ],
                        value_set_columns={
                            "scn_name": [
                                "eGon2035",
                                "eGon2035_lowflex",
                                "eGon100RE",
                            ],
                            "carrier": [
                                "rural_heat",
                                "urban_central_water_tanks",
                                "low_voltage",
                                "CH4",
                                "H2_saltcavern",
                                "services_rural_heat",
                                "services_rural_water_tanks",
                                "central_heat_store",
                                "AC",
                                "Li_ion",
                                "H2_grid",
                                "dsm",
                                "urban_central_heat",
                                "residential_rural_heat",
                                "central_heat",
                                "rural_heat_store",
                                "residential_rural_water_tanks",
                                "H2",
                                "O2",
                            ],
                        },
                    ),
                    # grid.egon_etrago_generator
                    TableValidation(
                        table_name="grid.egon_etrago_generator",
                        row_count=resolve_boundary_dependence(
                            {"Schleswig-Holstein": 1976, "Everything": 40577}
                        ),
                        data_type_columns={
                            "scn_name": "character varying",
                            "generator_id": "bigint",
                            "control": "text",
                            "type": "text",
                            "carrier": "text",
                            "p_nom": "double precision",
                            "p_nom_extendable": "boolean",
                            "p_nom_min": "double precision",
                            "p_nom_max": "double precision",
                            "p_min_pu": "double precision",
                            "p_max_pu": "double precision",
                            "p_set": "double precision",
                            "q_set": "double precision",
                            "sign": "double precision",
                            "marginal_cost": "double precision",
                            "build_year": "bigint",
                            "lifetime": "double precision",
                            "capital_cost": "double precision",
                            "efficiency": "double precision",
                            "committable": "boolean",
                            "start_up_cost": "double precision",
                            "shut_down_cost": "double precision",
                            "min_up_time": "bigint",
                            "min_down_time": "bigint",
                            "up_time_before": "bigint",
                            "down_time_before": "bigint",
                            "ramp_limit_up": "double precision",
                            "ramp_limit_down": "double precision",
                            "ramp_limit_start_up": "double precision",
                            "ramp_limit_shut_down": "double precision",
                            "e_nom_max": "double precision",
                        },
                        not_null_columns=[
                            "scn_name",
                            "generator_id",
                            "bus",
                            "control",
                            "type",
                            "carrier",
                            "p_nom",
                            "p_nom_extendable",
                            "p_nom_min",
                            "p_nom_max",
                            "p_min_pu",
                            "p_max_pu",
                            "sign",
                            "marginal_cost",
                            "build_year",
                            "lifetime",
                            "capital_cost",
                            "efficiency",
                            "committable",
                            "start_up_cost",
                            "shut_down_cost",
                            "min_up_time",
                            "min_down_time",
                            "up_time_before",
                            "down_time_before",
                            "ramp_limit_start_up",
                            "ramp_limit_shut_down",
                            "e_nom_max",
                        ],
                        value_set_columns={
                            "scn_name": [
                                "eGon2035",
                                "eGon2035_lowflex",
                                "eGon100RE",
                            ],
                            "carrier": [
                                "CH4",
                                "others",
                                "central_biomass_CHP",
                                "wind_onshore",
                                "lignite",
                                "geo_thermal",
                                "solar",
                                "reservoir",
                                "services_rural_solar_thermal_collector",
                                "residential_rural_solar_thermal_collector",
                                "industrial_biomass_CHP",
                                "biomass",
                                "urban_central_solar_thermal_collector",
                                "run_of_river",
                                "oil",
                                "central_biomass_CHP_heat",
                                "nuclear",
                                "coal",
                                "solar_thermal_collector",
                                "solar_rooftop",
                                "wind_offshore",
                                "central_others_CHP",
                                "central_others_CHP_heat",
                                "industrial_others_CHP",
                                "O2",
                            ],
                        },
                    ),
                    # grid.egon_etrago_generator_timeseries
                    TableValidation(
                        table_name="grid.egon_etrago_generator_timeseries",
                        row_count=resolve_boundary_dependence(
                            {"Schleswig-Holstein": 1732, "Everything": 28651}
                        ),
                        data_type_columns={
                            "scn_name": "character varying",
                            "generator_id": "integer",
                            "temp_id": "integer",
                            "p_set": "array",
                            "q_set": "array",
                            "p_min_pu": "array",
                            "p_max_pu": "array",
                            "marginal_cost": "array",
                        },
                        not_null_columns=[
                            "scn_name",
                            "generator_id",
                            "temp_id",
                            "p_set",
                            "q_set",
                            "p_min_pu",
                            "p_max_pu",
                            "marginal_cost",
                        ],
                        value_set_columns={
                            "scn_name": [
                                "eGon2035",
                                "eGon2035_lowflex",
                                "eGon100RE",
                            ]
                        },
                    ),
                    # grid.egon_etrago_line
                    TableValidation(
                        table_name="grid.egon_etrago_line",
                        row_count=resolve_boundary_dependence(
                            {"Schleswig-Holstein": 2380, "Everything": 69901}
                        ),
                        geometry_columns=["geom", "topo"],
                        data_type_columns={
                            "scn_name": "character varying",
                            "line_id": "bigint",
                            "bus0": "bigint",
                            "bus1": "bigint",
                            "type": "text",
                            "carrier": "text",
                            "x": "numeric",
                            "r": "numeric",
                            "g": "numeric",
                            "b": "numeric",
                            "s_nom": "numeric",
                            "s_nom_extendable": "boolean",
                            "s_nom_min": "double precision",
                            "s_nom_max": "double precision",
                            "s_max_pu": "double precision",
                            "build_year": "bigint",
                            "lifetime": "double precision",
                            "capital_cost": "double precision",
                            "length": "double precision",
                            "cables": "integer",
                            "terrain_factor": "double precision",
                            "num_parallel": "double precision",
                            "v_ang_min": "double precision",
                            "v_ang_max": "double precision",
                            "v_nom": "double precision",
                            "geom": "geometry",
                            "topo": "geometry",
                        },
                        not_null_columns=[
                            "scn_name",
                            "line_id",
                            "bus0",
                            "bus1",
                            "carrier",
                            "x",
                            "r",
                            "g",
                            "b",
                            "s_nom",
                            "s_nom_extendable",
                            "s_nom_min",
                            "s_nom_max",
                            "s_max_pu",
                            "build_year",
                            "lifetime",
                            "capital_cost",
                            "length",
                            "cables",
                            "terrain_factor",
                            "num_parallel",
                            "v_ang_min",
                            "v_ang_max",
                            "v_nom",
                            "geom",
                            "topo",
                        ],
                        value_set_columns={
                            "scn_name": [
                                "eGon2035",
                                "eGon2035_lowflex",
                                "eGon100RE",
                            ],
                            "carrier": ["AC"],
                        },
                    ),
                    # grid.egon_etrago_line_timeseries
                    # Note: Row count does not equal egon_etrago_line because
                    # buses are located outside Germany
                    TableValidation(
                        table_name="grid.egon_etrago_line_timeseries",
                        row_count=resolve_boundary_dependence(
                            {"Schleswig-Holstein": 2380, "Everything": 69714}
                        ),
                        data_type_columns={
                            "scn_name": "character varying",
                            "line_id": "bigint",
                            "temp_id": "integer",
                            "s_max_pu": "array",
                        },
                        not_null_columns=[
                            "scn_name",
                            "line_id",
                            "temp_id",
                            "s_max_pu",
                        ],
                        value_set_columns={
                            "scn_name": [
                                "eGon2035",
                                "eGon2035_lowflex",
                                "eGon100RE",
                            ]
                        },
                    ),
                    # grid.egon_etrago_link
                    TableValidation(
                        table_name="grid.egon_etrago_link",
                        row_count=resolve_boundary_dependence(
                            {"Schleswig-Holstein": 5937, "Everything": 83980}
                        ),
                        geometry_columns=["geom", "topo"],
                        data_type_columns={
                            "scn_name": "character varying",
                            "link_id": "bigint",
                            "bus0": "bigint",
                            "bus1": "bigint",
                            "type": "text",
                            "carrier": "text",
                            "efficiency": "double precision",
                            "build_year": "bigint",
                            "lifetime": "double precision",
                            "p_nom": "numeric",
                            "p_nom_extendable": "boolean",
                            "p_nom_min": "double precision",
                            "p_nom_max": "double precision",
                            "p_min_pu": "double precision",
                            "p_max_pu": "double precision",
                            "p_set": "double precision",
                            "capital_cost": "double precision",
                            "marginal_cost": "double precision",
                            "length": "double precision",
                            "terrain_factor": "double precision",
                            "geom": "geometry",
                            "topo": "geometry",
                        },
                        not_null_columns=[
                            "scn_name",
                            "link_id",
                            "bus0",
                            "bus1",
                            "carrier",
                            "efficiency",
                            "build_year",
                            "p_nom",
                            "p_nom_extendable",
                            "p_nom_min",
                            "p_nom_max",
                            "p_min_pu",
                            "p_max_pu",
                            "p_set",
                            "capital_cost",
                            "marginal_cost",
                            "length",
                            "terrain_factor",
                            "geom",
                            "topo",
                        ],
                        value_set_columns={
                            "scn_name": [
                                "eGon2035",
                                "eGon2035_lowflex",
                                "eGon100RE",
                            ],
                            "carrier": [
                                "industrial_gas_CHP",
                                "residential_rural_water_tanks_discharger",
                                "BEV_charger",
                                "CH4",
                                "power_to_H2",
                                "urban_central_gas_CHP",
                                "rural_heat_store_discharger",
                                "H2_gridextension",
                                "urban_central_gas_CHP_CC",
                                "dsm",
                                "services_rural_water_tanks_charger",
                                "H2_to_CH4",
                                "rural_heat_store_charger",
                                "DC",
                                "central_gas_boiler",
                                "H2_feedin",
                                "H2_retrofit",
                                "OCGT",
                                "central_gas_CHP_heat",
                                "residential_rural_water_tanks_charger",
                                "central_heat_pump",
                                "services_rural_ground_heat_pump",
                                "rural_heat_pump",
                                "CH4_to_H2",
                                "central_resistive_heater",
                                "urban_central_air_heat_pump",
                                "urban_central_water_tanks_discharger",
                                "urban_central_water_tanks_charger",
                                "services_rural_water_tanks_discharger",
                                "electricity_distribution_grid",
                                "central_heat_store_discharger",
                                "H2_to_power",
                                "central_heat_store_charger",
                                "central_gas_CHP",
                                "residential_rural_ground_heat_pump",
                                "H2_grid",
                                "H2_saltcavern",
                                "PtH2_O2",
                                "PtH2_waste_heat",
                                "rural_gas_boiler",
                            ],
                        },
                    ),
                    # grid.egon_etrago_link_timeseries
                    TableValidation(
                        table_name="grid.egon_etrago_link_timeseries",
                        row_count=resolve_boundary_dependence(
                            {"Schleswig-Holstein": 893, "Everything": 25729}
                        ),
                        data_type_columns={
                            "scn_name": "character varying",
                            "link_id": "bigint",
                            "temp_id": "integer",
                            "p_set": "array",
                            "p_min_pu": "array",
                            "p_max_pu": "array",
                            "efficiency": "array",
                            "marginal_cost": "array",
                        },
                        not_null_columns=[
                            "scn_name",
                            "link_id",
                            "temp_id",
                            "p_set",
                            "p_min_pu",
                            "p_max_pu",
                            "efficiency",
                            "marginal_cost",
                        ],
                        value_set_columns={
                            "scn_name": [
                                "eGon2035",
                                "eGon2035_lowflex",
                                "eGon100RE",
                            ]
                        },
                    ),
                    # grid.egon_etrago_load
                    TableValidation(
                        table_name="grid.egon_etrago_load",
                        row_count=resolve_boundary_dependence(
                            {"Schleswig-Holstein": 960, "Everything": 44019}
                        ),
                        data_type_columns={
                            "scn_name": "character varying",
                            "load_id": "bigint",
                            "bus": "bigint",
                            "type": "text",
                            "carrier": "text",
                            "p_set": "double precision",
                            "q_set": "double precision",
                            "sign": "double precision",
                        },
                        not_null_columns=[
                            "scn_name",
                            "load_id",
                            "bus",
                            "type",
                            "carrier",
                            "p_set",
                            "q_set",
                            "sign",
                        ],
                        value_set_columns={
                            "scn_name": [
                                "eGon2035",
                                "eGon2035_lowflex",
                                "eGon100RE",
                            ],
                            "carrier": [
                                "CH4",
                                "H2_for_industry",
                                "services_rural_heat",
                                "H2_system_boundary",
                                "AC",
                                "urban_central_heat",
                                "residential_rural_heat",
                                "low-temperature_heat_for_industry",
                                "CH4_for_industry",
                                "central_heat",
                                "CH4_system_boundary",
                                "land_transport_EV",
                                "H2_hgv_load",
                                "rural_gas_boiler",
                                "rural_heat",
                                "O2",
                            ],
                        },
                    ),
                    # grid.egon_etrago_load_timeseries
                    TableValidation(
                        table_name="grid.egon_etrago_load_timeseries",
                        row_count=resolve_boundary_dependence(
                            {"Schleswig-Holstein": 938, "Everything": 44013}
                        ),
                        data_type_columns={
                            "scn_name": "character varying",
                            "load_id": "bigint",
                            "temp_id": "integer",
                            "p_set": "array",
                            "q_set": "array",
                        },
                        not_null_columns=[
                            "scn_name",
                            "load_id",
                            "temp_id",
                            "p_set",
                            "q_set",
                        ],
                        value_set_columns={
                            "scn_name": [
                                "eGon2035",
                                "eGon2035_lowflex",
                                "eGon100RE",
                            ]
                        },
                    ),
                    # grid.egon_etrago_storage
                    TableValidation(
                        table_name="grid.egon_etrago_storage",
                        row_count=resolve_boundary_dependence(
                            {"Schleswig-Holstein": 496, "Everything": 13044}
                        ),
                        data_type_columns={
                            "scn_name": "character varying",
                            "storage_id": "bigint",
                            "bus": "bigint",
                            "control": "text",
                            "type": "text",
                            "carrier": "text",
                            "p_nom": "double precision",
                            "p_nom_extendable": "boolean",
                            "p_nom_min": "double precision",
                            "p_nom_max": "double precision",
                            "p_min_pu": "double precision",
                            "p_max_pu": "double precision",
                            "p_set": "double precision",
                            "q_set": "double precision",
                            "sign": "double precision",
                            "marginal_cost": "double precision",
                            "capital_cost": "double precision",
                            "build_year": "bigint",
                            "lifetime": "double precision",
                            "state_of_charge_initial": "double precision",
                            "cyclic_state_of_charge": "boolean",
                            "state_of_charge_set": "double precision",
                            "max_hours": "double precision",
                            "efficiency_store": "double precision",
                            "efficiency_dispatch": "double precision",
                            "standing_loss": "double precision",
                            "inflow": "double precision",
                        },
                        not_null_columns=[
                            "scn_name",
                            "storage_id",
                            "bus",
                            "control",
                            "type",
                            "carrier",
                            "p_nom",
                            "p_nom_extendable",
                            "p_nom_min",
                            "p_nom_max",
                            "p_min_pu",
                            "p_max_pu",
                            "p_set",
                            "q_set",
                            "sign",
                            "marginal_cost",
                            "capital_cost",
                            "build_year",
                            "lifetime",
                            "state_of_charge_initial",
                            "cyclic_state_of_charge",
                            "state_of_charge_set",
                            "max_hours",
                            "efficiency_store",
                            "efficiency_dispatch",
                            "standing_loss",
                            "inflow",
                        ],
                        value_set_columns={
                            "scn_name": [
                                "eGon2035",
                                "eGon2035_lowflex",
                                "eGon100RE",
                            ],
                            "carrier": [
                                "battery",
                                "home_battery",
                                "pumped_hydro",
                                "reservoir",
                            ],
                        },
                    ),
                    # grid.egon_etrago_storage_timeseries
                    TableValidation(
                        table_name="grid.egon_etrago_storage_timeseries",
                        row_count=resolve_boundary_dependence(
                            {"Schleswig-Holstein": 0, "Everything": 9}
                        ),
                        data_type_columns={
                            "scn_name": "character varying",
                            "storage_id": "bigint",
                            "temp_id": "integer",
                            "p_set": "double precision[]",
                            "q_set": "double precision[]",
                            "p_min_pu": "double precision[]",
                            "p_max_pu": "double precision[]",
                            "state_of_charge_set": "double precision[]",
                            "inflow": "double precision[]",
                            "marginal_cost": "double precision[]",
                        },
                        not_null_columns=[
                            "scn_name",
                            "storage_id",
                            "temp_id",
                            "inflow",
                            "marginal_cost",
                        ],
                        value_set_columns={"scn_name": ["eGon100RE"]},
                    ),
                    # grid.egon_etrago_store
                    TableValidation(
                        table_name="grid.egon_etrago_store",
                        row_count=resolve_boundary_dependence(
                            {"Schleswig-Holstein": 1652, "Everything": 26520}
                        ),
                        data_type_columns={
                            "scn_name": "character varying",
                            "store_id": "bigint",
                            "bus": "bigint",
                            "type": "text",
                            "carrier": "text",
                            "e_nom": "double precision",
                            "e_nom_extendable": "boolean",
                            "e_nom_min": "double precision",
                            "e_nom_max": "double precision",
                            "e_min_pu": "double precision",
                            "e_max_pu": "double precision",
                            "p_set": "double precision",
                            "q_set": "double precision",
                            "e_initial": "double precision",
                            "e_cyclic": "boolean",
                            "sign": "double precision",
                            "marginal_cost": "double precision",
                            "capital_cost": "double precision",
                            "standing_loss": "double precision",
                            "build_year": "bigint",
                            "lifetime": "double precision",
                        },
                        not_null_columns=[
                            "scn_name",
                            "store_id",
                            "bus",
                            "type",
                            "carrier",
                            "e_nom",
                            "e_nom_extendable",
                            "e_nom_min",
                            "e_nom_max",
                            "e_min_pu",
                            "e_max_pu",
                            "p_set",
                            "q_set",
                            "e_initial",
                            "e_cyclic",
                            "sign",
                            "marginal_cost",
                            "capital_cost",
                            "standing_loss",
                            "build_year",
                            "lifetime",
                        ],
                        value_set_columns={
                            "scn_name": [
                                "eGon2035",
                                "eGon2035_lowflex",
                                "eGon100RE",
                            ]
                        },
                    ),
                    # grid.egon_etrago_store_timeseries
                    TableValidation(
                        table_name="grid.egon_etrago_store_timeseries",
                        row_count=resolve_boundary_dependence(
                            {"Schleswig-Holstein": 387, "Everything": 15281}
                        ),
                        data_type_columns={
                            "scn_name": "character varying",
                            "store_id": "bigint",
                            "temp_id": "integer",
                            "p_set": "array",
                            "q_set": "array",
                            "e_min_pu": "array",
                            "e_max_pu": "array",
                            "marginal_cost": "array",
                        },
                        not_null_columns=[
                            "scn_name",
                            "store_id",
                            "temp_id",
                            "p_set",
                            "q_set",
                            "e_min_pu",
                            "e_max_pu",
                            "marginal_cost",
                        ],
                        value_set_columns={
                            "scn_name": [
                                "eGon2035",
                                "eGon2035_lowflex",
                                "eGon100RE",
                            ]
                        },
                    ),
                    # grid.egon_etrago_temp_resolution
                    TableValidation(
                        table_name="grid.egon_etrago_temp_resolution",
                        row_count=1,
                        data_type_columns={
                            "temp_id": "bigint",
                            "timesteps": "bigint",
                            "resolution": "text",
                            "start_time": "timestamp without time zone",
                        },
                    ),
                    # grid.egon_etrago_transformer
                    TableValidation(
                        table_name="grid.egon_etrago_transformer",
                        row_count=resolve_boundary_dependence(
                            {"Schleswig-Holstein": 62, "Everything": 1545}
                        ),
                        data_type_columns={
                            "scn_name": "character varying",
                            "trafo_id": "bigint",
                            "bus0": "bigint",
                            "bus1": "bigint",
                            "type": "text",
                            "model": "text",
                            "x": "numeric",
                            "r": "numeric",
                            "g": "numeric",
                            "b": "numeric",
                            "s_nom": "double precision",
                            "s_nom_extendable": "boolean",
                            "s_nom_min": "double precision",
                            "s_nom_max": "double precision",
                            "s_max_pu": "double precision",
                            "tap_ratio": "double precision",
                            "tap_side": "bigint",
                            "tap_position": "bigint",
                            "phase_shift": "double precision",
                            "build_year": "bigint",
                            "lifetime": "double precision",
                            "v_ang_min": "double precision",
                            "v_ang_max": "double precision",
                            "capital_cost": "double precision",
                            "num_parallel": "double precision",
                            "geom": "geometry",
                            "topo": "geometry",
                        },
                        not_null_columns=[
                            "scn_name",
                            "trafo_id",
                            "bus0",
                            "bus1",
                            "type",
                            "model",
                            "x",
                            "r",
                            "g",
                            "b",
                            "s_nom",
                            "s_nom_extendable",
                            "s_nom_min",
                            "s_nom_max",
                            "s_max_pu",
                            "tap_ratio",
                            "tap_side",
                            "tap_position",
                            "phase_shift",
                            "build_year",
                            "lifetime",
                            "v_ang_min",
                            "v_ang_max",
                            "capital_cost",
                            "num_parallel",
                            "geom",
                            "topo",
                        ],
                        value_set_columns={
                            "scn_name": [
                                "eGon2035",
                                "eGon2035_lowflex",
                                "eGon100RE",
                            ]
                        },
                    ),
                    # grid.egon_hvmv_substation
                    TableValidation(
                        table_name="grid.egon_hvmv_substation",
                        row_count=resolve_boundary_dependence(
                            {"Schleswig-Holstein": 198, "Everything": 3854}
                        ),
                        geometry_columns=["point", "polygon"],
                        data_type_columns={
                            "bus_id": "integer",
                            "lon": "double precision",
                            "lat": "double precision",
                            "point": "geometry",
                            "polygon": "geometry",
                            "voltage": "text",
                            "power_type": "text",
                            "substation": "text",
                            "osm_id": "text",
                            "osm_www": "text",
                            "frequency": "text",
                            "subst_name": "text",
                            "ref": "text",
                            "operator": "text",
                            "dbahn": "text",
                            "status": "integer",
                        },
                        not_null_columns=[
                            "bus_id",
                            "lon",
                            "lat",
                            "point",
                            "polygon",
                            "voltage",
                            "power_type",
                            "substation",
                            "osm_id",
                            "osm_www",
                            "frequency",
                            "subst_name",
                            "ref",
                            "operator",
                            "dbahn",
                            "status",
                        ],
                    ),
                    # grid.egon_mv_grid_district
                    TableValidation(
                        table_name="grid.egon_mv_grid_district",
                        row_count=resolve_boundary_dependence(
                            {"Schleswig-Holstein": 198, "Everything": 3854}
                        ),
                        geometry_columns=["geom"],
                        data_type_columns={
                            "bus_id": "integer",
                            "geom": "geometry",
                            "area": "double precision",
                        },
                    ),
                ],
            },
            # Continue pipeline even if validations fail
            proceed_on_validation_failure=True,
        )
