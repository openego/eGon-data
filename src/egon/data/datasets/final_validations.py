"""
Dataset for cross-cutting validations that run at the end of the pipeline.

This module provides the FinalValidations dataset which contains validation rules
that check data consistency across multiple datasets. These validations should run
after all data generation is complete, but before the final validation report.
"""

from egon.data.datasets import Dataset
from egon.data.validation.rules.custom.sanity import (
    CH4StoresCapacity,
    H2SaltcavernStoresCapacity,
    GasBusesIsolated,
    GasBusesCount,
    GasOnePortConnections,
    CH4GridCapacity,
    GasLinksConnections,
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
                        rtol=0.02
                    ),
                    # CH4 stores - eGon100RE
                    CH4StoresCapacity(
                        table="grid.egon_etrago_store",
                        rule_id="SANITY_CH4_STORES_CAPACITY_EGON100RE",
                        scenario="eGon100RE",
                        rtol=0.02
                    ),
                    # H2 saltcavern stores - eGon2035
                    H2SaltcavernStoresCapacity(
                        table="grid.egon_etrago_store",
                        rule_id="SANITY_H2_SALTCAVERN_STORES_CAPACITY_EGON2035",
                        scenario="eGon2035",
                        rtol=0.02
                    ),
                    # H2 saltcavern stores - eGon100RE
                    H2SaltcavernStoresCapacity(
                        table="grid.egon_etrago_store",
                        rule_id="SANITY_H2_SALTCAVERN_STORES_CAPACITY_EGON100RE",
                        scenario="eGon100RE",
                        rtol=0.02
                    ),
                ],

                # Gas grid bus validations
                # These check that gas buses are properly connected and counts match expectations
                "gas_grid": [
                    # Check for isolated CH4 buses - eGon2035
                    GasBusesIsolated(
                        table="grid.egon_etrago_bus",
                        rule_id="SANITY_GAS_BUSES_ISOLATED_CH4_EGON2035",
                        scenario="eGon2035",
                        carrier="CH4"
                    ),
                    # Check for isolated H2_grid buses - eGon2035
                    GasBusesIsolated(
                        table="grid.egon_etrago_bus",
                        rule_id="SANITY_GAS_BUSES_ISOLATED_H2_GRID_EGON2035",
                        scenario="eGon2035",
                        carrier="H2_grid"
                    ),
                    # Check for isolated H2_saltcavern buses - eGon2035
                    GasBusesIsolated(
                        table="grid.egon_etrago_bus",
                        rule_id="SANITY_GAS_BUSES_ISOLATED_H2_SALTCAVERN_EGON2035",
                        scenario="eGon2035",
                        carrier="H2_saltcavern"
                    ),
                    # Check for isolated CH4 buses - eGon100RE
                    GasBusesIsolated(
                        table="grid.egon_etrago_bus",
                        rule_id="SANITY_GAS_BUSES_ISOLATED_CH4_EGON100RE",
                        scenario="eGon100RE",
                        carrier="CH4"
                    ),
                    # Check for isolated H2_grid buses - eGon100RE
                    GasBusesIsolated(
                        table="grid.egon_etrago_bus",
                        rule_id="SANITY_GAS_BUSES_ISOLATED_H2_GRID_EGON100RE",
                        scenario="eGon100RE",
                        carrier="H2_grid"
                    ),
                    # Check for isolated H2_saltcavern buses - eGon100RE
                    GasBusesIsolated(
                        table="grid.egon_etrago_bus",
                        rule_id="SANITY_GAS_BUSES_ISOLATED_H2_SALTCAVERN_EGON100RE",
                        scenario="eGon100RE",
                        carrier="H2_saltcavern"
                    ),
                    # Check CH4 bus count - eGon2035
                    GasBusesCount(
                        table="grid.egon_etrago_bus",
                        rule_id="SANITY_GAS_BUSES_COUNT_CH4_EGON2035",
                        scenario="eGon2035",
                        carrier="CH4",
                        rtol=0.10
                    ),
                    # Check H2_grid bus count - eGon2035
                    GasBusesCount(
                        table="grid.egon_etrago_bus",
                        rule_id="SANITY_GAS_BUSES_COUNT_H2_GRID_EGON2035",
                        scenario="eGon2035",
                        carrier="H2_grid",
                        rtol=0.10
                    ),
                    # Check CH4 bus count - eGon100RE
                    GasBusesCount(
                        table="grid.egon_etrago_bus",
                        rule_id="SANITY_GAS_BUSES_COUNT_CH4_EGON100RE",
                        scenario="eGon100RE",
                        carrier="CH4",
                        rtol=0.10
                    ),
                    # Check H2_grid bus count - eGon100RE
                    GasBusesCount(
                        table="grid.egon_etrago_bus",
                        rule_id="SANITY_GAS_BUSES_COUNT_H2_GRID_EGON100RE",
                        scenario="eGon100RE",
                        carrier="H2_grid",
                        rtol=0.10
                    ),
                    # Check CH4 grid capacity - eGon2035
                    CH4GridCapacity(
                        table="grid.egon_etrago_link",
                        rule_id="SANITY_CH4_GRID_CAPACITY_EGON2035",
                        scenario="eGon2035",
                        rtol=0.10
                    ),
                    # Check CH4 grid capacity - eGon100RE
                    CH4GridCapacity(
                        table="grid.egon_etrago_link",
                        rule_id="SANITY_CH4_GRID_CAPACITY_EGON100RE",
                        scenario="eGon100RE",
                        rtol=0.10
                    ),
                ],

                # Gas one-port component connection validations
                # These check that loads, generators, and stores are connected to valid buses
                "gas_one_port": [
                    # LOADS - eGon2035
                    # CH4_for_industry loads in Germany must connect to CH4 buses
                    GasOnePortConnections(
                        table="grid.egon_etrago_load",
                        rule_id="SANITY_GAS_ONE_PORT_LOAD_CH4_FOR_INDUSTRY_DE_EGON2035",
                        scenario="eGon2035",
                        component_type="load",
                        component_carrier="CH4_for_industry",
                        bus_conditions=[("CH4", "= 'DE'")]
                    ),
                    # CH4 loads abroad must connect to CH4 buses outside Germany
                    GasOnePortConnections(
                        table="grid.egon_etrago_load",
                        rule_id="SANITY_GAS_ONE_PORT_LOAD_CH4_ABROAD_EGON2035",
                        scenario="eGon2035",
                        component_type="load",
                        component_carrier="CH4",
                        bus_conditions=[("CH4", "!= 'DE'")]
                    ),
                    # H2_for_industry loads must connect to H2_grid in DE or AC abroad
                    GasOnePortConnections(
                        table="grid.egon_etrago_load",
                        rule_id="SANITY_GAS_ONE_PORT_LOAD_H2_FOR_INDUSTRY_EGON2035",
                        scenario="eGon2035",
                        component_type="load",
                        component_carrier="H2_for_industry",
                        bus_conditions=[("H2_grid", "= 'DE'"), ("AC", "!= 'DE'")]
                    ),

                    # GENERATORS - eGon2035
                    # CH4 generators must connect to CH4 buses
                    GasOnePortConnections(
                        table="grid.egon_etrago_generator",
                        rule_id="SANITY_GAS_ONE_PORT_GENERATOR_CH4_EGON2035",
                        scenario="eGon2035",
                        component_type="generator",
                        component_carrier="CH4",
                        bus_conditions=[("CH4", "IS NOT NULL")]  # Any CH4 bus
                    ),

                    # STORES - eGon2035
                    # CH4 stores must connect to CH4 buses
                    GasOnePortConnections(
                        table="grid.egon_etrago_store",
                        rule_id="SANITY_GAS_ONE_PORT_STORE_CH4_EGON2035",
                        scenario="eGon2035",
                        component_type="store",
                        component_carrier="CH4",
                        bus_conditions=[("CH4", "IS NOT NULL")]
                    ),
                    # H2_underground stores must connect to H2_saltcavern buses
                    GasOnePortConnections(
                        table="grid.egon_etrago_store",
                        rule_id="SANITY_GAS_ONE_PORT_STORE_H2_UNDERGROUND_EGON2035",
                        scenario="eGon2035",
                        component_type="store",
                        component_carrier="H2_underground",
                        bus_conditions=[("H2_saltcavern", "IS NOT NULL")]
                    ),
                    # H2_overground stores must connect to H2_saltcavern or H2_grid in DE
                    GasOnePortConnections(
                        table="grid.egon_etrago_store",
                        rule_id="SANITY_GAS_ONE_PORT_STORE_H2_OVERGROUND_EGON2035",
                        scenario="eGon2035",
                        component_type="store",
                        component_carrier="H2_overground",
                        bus_conditions=[("H2_saltcavern", "= 'DE'"), ("H2_grid", "= 'DE'")]
                    ),
                ],

                # Gas link connection validations
                # These check that gas links have both bus0 and bus1 connected to existing buses
                "gas_links": [
                    # CH4 links - eGon2035
                    GasLinksConnections(
                        table="grid.egon_etrago_link",
                        rule_id="SANITY_GAS_LINKS_CH4_EGON2035",
                        scenario="eGon2035",
                        carrier="CH4"
                    ),
                    # H2_feedin links - eGon2035
                    GasLinksConnections(
                        table="grid.egon_etrago_link",
                        rule_id="SANITY_GAS_LINKS_H2_FEEDIN_EGON2035",
                        scenario="eGon2035",
                        carrier="H2_feedin"
                    ),
                    # H2_to_CH4 links - eGon2035
                    GasLinksConnections(
                        table="grid.egon_etrago_link",
                        rule_id="SANITY_GAS_LINKS_H2_TO_CH4_EGON2035",
                        scenario="eGon2035",
                        carrier="H2_to_CH4"
                    ),
                    # CH4_to_H2 links - eGon2035
                    GasLinksConnections(
                        table="grid.egon_etrago_link",
                        rule_id="SANITY_GAS_LINKS_CH4_TO_H2_EGON2035",
                        scenario="eGon2035",
                        carrier="CH4_to_H2"
                    ),
                    # H2_to_power links - eGon2035
                    GasLinksConnections(
                        table="grid.egon_etrago_link",
                        rule_id="SANITY_GAS_LINKS_H2_TO_POWER_EGON2035",
                        scenario="eGon2035",
                        carrier="H2_to_power"
                    ),
                    # power_to_H2 links - eGon2035
                    GasLinksConnections(
                        table="grid.egon_etrago_link",
                        rule_id="SANITY_GAS_LINKS_POWER_TO_H2_EGON2035",
                        scenario="eGon2035",
                        carrier="power_to_H2"
                    ),
                    # OCGT links - eGon2035
                    GasLinksConnections(
                        table="grid.egon_etrago_link",
                        rule_id="SANITY_GAS_LINKS_OCGT_EGON2035",
                        scenario="eGon2035",
                        carrier="OCGT"
                    ),
                    # central_gas_boiler links - eGon2035
                    GasLinksConnections(
                        table="grid.egon_etrago_link",
                        rule_id="SANITY_GAS_LINKS_CENTRAL_GAS_BOILER_EGON2035",
                        scenario="eGon2035",
                        carrier="central_gas_boiler"
                    ),
                    # central_gas_CHP links - eGon2035
                    GasLinksConnections(
                        table="grid.egon_etrago_link",
                        rule_id="SANITY_GAS_LINKS_CENTRAL_GAS_CHP_EGON2035",
                        scenario="eGon2035",
                        carrier="central_gas_CHP"
                    ),
                    # central_gas_CHP_heat links - eGon2035
                    GasLinksConnections(
                        table="grid.egon_etrago_link",
                        rule_id="SANITY_GAS_LINKS_CENTRAL_GAS_CHP_HEAT_EGON2035",
                        scenario="eGon2035",
                        carrier="central_gas_CHP_heat"
                    ),
                    # industrial_gas_CHP links - eGon2035
                    GasLinksConnections(
                        table="grid.egon_etrago_link",
                        rule_id="SANITY_GAS_LINKS_INDUSTRIAL_GAS_CHP_EGON2035",
                        scenario="eGon2035",
                        carrier="industrial_gas_CHP"
                    ),
                ],

                # Add more validation categories here as you migrate more sanity checks
                # Examples:
                # "timeseries": [ ... ],
                # "capacity_comparison": [ ... ],
            },
            validation_on_failure="continue"  # Continue pipeline even if validations fail
        )
