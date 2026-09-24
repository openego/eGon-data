"""
The central module containing all code dealing with open cycle gas turbine
"""

from egon.data.datasets import Dataset, DatasetSources, DatasetTargets
from egon.data.datasets.power_etrago.match_ocgt import (
    insert_open_cycle_gas_turbines,
)
from egon.data.validation.rules.custom.sanity import (
    OcgtCapacity,
    OcgtNepCapacity,
    OcgtParameters,
    OcgtPositiveCapacity,
    OcgtScenarioCoverage,
)

#: Scenarios the validation rules are built for; a scenario the run does
#: not produce is skipped by the rules themselves.
SCENARIOS = ["status2024", "eGon2035", "reGon2037", "reGon2045"]

#: Scenario tag and capacity column of the NEP list of conventional power
#: plants per future scenario; status2024 has no such reference.
NEP_REFERENCE = {
    "eGon2035": ("eGon2035", "c2035_capacity"),
    "reGon2037": ("reGon", "c2037_capacity"),
    "reGon2045": ("reGon", "c2045_capacity"),
}


class OpenCycleGasTurbineEtrago(Dataset):
    """
    Insert the open cycle gas turbine links into the database

    Insert the open cycle gas turbine links into the database by using
    the function :py:func:`insert_open_cycle_gas_turbines <egon.data.datasets.power_etrago.match_ocgt.insert_open_cycle_gas_turbines>`.

    *Dependencies*
      * :py:class:`GasAreaseGon2035 <egon.data.datasets.gas_areas.GasAreaseGon2035>`
      * :py:class:`GasNodesAndPipes <egon.data.datasets.gas_grid.GasNodesAndPipes>`
      * :py:class:`PowerPlants <egon.data.datasets.power_plants.PowerPlants>`
      * :py:class:`mastr_data_setup <egon.data.datasets.mastr.mastr_data_setup>`

    *Resulting tables*
      * :py:class:`grid.egon_etrago_link <egon.data.datasets.etrago_setup.EgonPfHvLink>` is extended

    """

    #:
    name: str = "OpenCycleGasTurbineEtrago"
    #:
    version: str = "0.0.5"

    sources = DatasetSources(
        tables={
            "power_plants": "supply.egon_power_plants",
            "etrago_bus": "grid.egon_etrago_bus",
            "etrago_link": "grid.egon_etrago_link",
        }
    )

    targets = DatasetTargets(
        tables={
            "etrago_link": "grid.egon_etrago_link",
        }
    )

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(insert_open_cycle_gas_turbines,),
            validation={
                "data_quality": [
                    OcgtScenarioCoverage(
                        table="grid.egon_etrago_link",
                        rule_id="SANITY_OCGT_SCENARIOS",
                    ),
                    *[
                        OcgtCapacity(
                            table="grid.egon_etrago_link",
                            rule_id=f"SANITY_OCGT_CAPACITY.{scn}",
                            scenario=scn,
                        )
                        for scn in SCENARIOS
                    ],
                    *[
                        OcgtPositiveCapacity(
                            table="grid.egon_etrago_link",
                            rule_id=f"SANITY_OCGT_P_NOM.{scn}",
                            scenario=scn,
                        )
                        for scn in SCENARIOS
                    ],
                    *[
                        OcgtParameters(
                            table="grid.egon_etrago_link",
                            rule_id=f"SANITY_OCGT_PARAMETERS.{scn}",
                            scenario=scn,
                        )
                        for scn in SCENARIOS
                    ],
                    *[
                        OcgtNepCapacity(
                            table="grid.egon_etrago_link",
                            rule_id=f"SANITY_OCGT_NEP_CAPACITY.{scn}",
                            scenario=scn,
                            nep_scenario=nep_scenario,
                            capacity_column=capacity_column,
                        )
                        for scn, (
                            nep_scenario,
                            capacity_column,
                        ) in NEP_REFERENCE.items()
                    ],
                ]
            },
            proceed_on_validation_failure=True,
        )
