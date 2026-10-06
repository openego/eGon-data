"""
The central module containing definition of the datasets dealing with gas neighbours
"""

from pathlib import Path
from urllib.request import urlretrieve

from egon.data import config
from egon.data.datasets import Dataset, DatasetSources, DatasetTargets
from egon.data.datasets.gas_neighbours.gas_scenarios import (
    grid,
    insert_ocgt_abroad,
    tyndp_gas_demand,
    tyndp_gas_generation,
)


def download_tyndp2024_gas_data():
    """Download the TYNDP 2024 data of the gas sector abroad

    The gas Annexes C1 and E of ENTSOG and the demand scenarios of the
    TYNDP 2024. The files are only downloaded if they do not exist yet.

    Returns
    -------
    None

    """
    for key in ["tyndp2024_annex_c1", "tyndp2024_annex_e", "tyndp2024_demand"]:
        target_file = Path(GasNeighbours.sources.files[key])
        if not target_file.is_file():
            target_file.parent.mkdir(parents=True, exist_ok=True)
            urlretrieve(GasNeighbours.sources.urls[key], target_file)


def no_gas_neighbours_required():
    print(
        """
          None of the required scenarios need the creation of
          foreign gas buses
          """
    )
    return None


def insert_gas_neighbours():
    """
    Insert the gas data abroad of all configured scenarios

    The status quo scenarios have no gas buses abroad and are skipped.

    Returns
    -------
    None

    """
    scenarios = [
        scn_name
        for scn_name in config.settings()["egon-data"]["--scenarios"]
        if "status" not in scn_name
    ]

    if not scenarios:
        no_gas_neighbours_required()
        return

    for scn_name in scenarios:
        tyndp_gas_generation(scn_name)
        tyndp_gas_demand(scn_name)
        grid(scn_name)
        insert_ocgt_abroad(scn_name)


class GasNeighbours(Dataset):

    sources = DatasetSources(
        files={
            # TYNDP 2024 gas Annexes (ENTSOG, 18 March 2026) and demand
            # scenarios, see download_tyndp2024_gas_data
            "tyndp2024_annex_c1": "datasets/gas_data/TYNDP_2024_Annex_C1.xlsx",
            "tyndp2024_annex_e": "datasets/gas_data/TYNDP_2024_Annex_E.xlsx",
            "tyndp2024_demand": (
                "datasets/gas_data/"
                "Demand_Scenarios_TYNDP_2024_After_Public_Consultation.xlsb.zip"
            ),
        },
        urls={
            "tyndp2024_annex_c1": (
                "https://www.entsog.eu/sites/default/files/2026-03/"
                "TYNDP%202024_Annex%20C1_Natural%20Gas%20Infrastructure"
                "%20Capacities.xlsx"
            ),
            "tyndp2024_annex_e": (
                "https://www.entsog.eu/sites/default/files/2026-03/"
                "TYNDP%202024%20Annex%20E%20-%20Analysis%20tables.xlsx"
            ),
            "tyndp2024_demand": (
                "https://2024-data.entsos-tyndp-scenarios.eu/files/"
                "scenarios-inputs/"
                "Demand_Scenarios_TYNDP_2024_After_Public_Consultation.xlsb.zip"
            ),
        },
        tables={
            "buses": "grid.egon_etrago_bus",
            "links": "grid.egon_etrago_link",
        },
    )
    targets = DatasetTargets(
        tables={
            "generators": "grid.egon_etrago_generator",
            "loads": "grid.egon_etrago_load",
            "load_timeseries": "grid.egon_etrago_load_timeseries",
            "stores": "grid.egon_etrago_store",
            "links": "grid.egon_etrago_link",
        }
    )
    """
    Insert the missing gas data abroad.

    Insert the missing gas data into the database for every scenario
    except the status scenarios by executing successively the functions
    :py:func:`tyndp_gas_generation <egon.data.datasets.gas_neighbours.gas_scenarios.tyndp_gas_generation>`,
    :py:func:`tyndp_gas_demand <egon.data.datasets.gas_neighbours.gas_scenarios.tyndp_gas_demand>`,
    :py:func:`grid <egon.data.datasets.gas_neighbours.gas_scenarios.grid>` and
    :py:func:`insert_ocgt_abroad <egon.data.datasets.gas_neighbours.gas_scenarios.insert_ocgt_abroad>`.

    *Dependencies*
      * :py:class:`RunPypsaEur <egon.data.datasets.pypsaeur.RunPypsaEur>`
      * :py:class:`GasNodesAndPipes <egon.data.datasets.gas_grid.GasNodesAndPipes>`
      * :py:class:`ElectricalNeighbours <egon.data.datasets.electrical_neighbours.ElectricalNeighbours>`
      * :py:class:`HydrogenBusEtrago <egon.data.datasets.hydrogen_etrago.HydrogenBusEtrago>`
      * :py:class:`HydrogenGridEtrago <egon.data.datasets.hydrogen_etrago.HydrogenGridEtrago>`

    *Resulting tables*
      * :py:class:`grid.egon_etrago_link <egon.data.datasets.etrago_setup.EgonPfHvLink>` is extended
      * :py:class:`grid.egon_etrago_generator <egon.data.datasets.etrago_setup.EgonPfHvGenerator>` is extended
      * :py:class:`grid.egon_etrago_load <egon.data.datasets.etrago_setup.EgonPfHvLoad>` is extended
      * :py:class:`grid.egon_etrago_store <egon.data.datasets.etrago_setup.EgonPfHvStore>` is extended

    """

    #:
    name: str = "GasNeighbours"
    #:
    version: str = "0.0.9"

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(download_tyndp2024_gas_data, insert_gas_neighbours),
            # Gas validation rules (#1526)
            validation=self.validation_rules(),
            proceed_on_validation_failure=True,
        )

    @staticmethod
    def validation_rules():
        """
        Return the validation rules of the dataset

        Returns
        -------
        dict
            Validation rules per validation task

        """
        from egon.data.validation.rules.custom.sanity.gas import (
            TARGET_SCENARIOS,
            GasBusReferences,
            GasComponentTotal,
            GasTimeseriesComplete,
            for_each_scenario,
        )

        # No gas data abroad in status scenarios; same values for both
        # dataset boundaries
        totals = [
            # rule id, table, carriers, location, measure, expected
            (
                "SANITY_GAS_CH4_LOADS_ABROAD",
                "grid.egon_etrago_load",
                ["CH4"],
                "abroad",
                "annual_energy",
                {
                    "status2024": 0,
                    "eGon2035": 909306938.314,
                    "reGon2037": 829480082.288,
                    "reGon2045": 555563135.530,
                },
            ),
            # Demand of the electrolysers abroad
            (
                "SANITY_GAS_H2_LOADS_ABROAD",
                "grid.egon_etrago_load",
                ["H2_for_industry"],
                "abroad",
                "annual_energy",
                {
                    "status2024": 0,
                    "eGon2035": 258886478.000,
                    "reGon2037": 388685542.400,
                    "reGon2045": 760872307.500,
                },
            ),
            (
                "SANITY_GAS_CH4_GENERATORS_ABROAD",
                "grid.egon_etrago_generator",
                ["CH4"],
                "abroad",
                ("sum", "p_nom"),
                {
                    "status2024": 0,
                    "eGon2035": 399754.008,
                    "reGon2037": 392043.861,
                    "reGon2045": 52702.742,
                },
            ),
            (
                "SANITY_GAS_CH4_STORES_ABROAD",
                "grid.egon_etrago_store",
                ["CH4"],
                "abroad",
                ("sum", "e_nom"),
                {
                    "status2024": 0,
                    "eGon2035": 430271072.959,
                    "reGon2037": 430271072.959,
                    "reGon2045": 430271072.959,
                },
            ),
            (
                "SANITY_GAS_CH4_BORDER_LINKS",
                "grid.egon_etrago_link",
                ["CH4"],
                "cross-border",
                "count",
                {
                    "status2024": 0,
                    "eGon2035": 2,
                    "reGon2037": 2,
                    "reGon2045": 2,
                },
            ),
            (
                "SANITY_GAS_CH4_BORDER_LINKS_P_NOM",
                "grid.egon_etrago_link",
                ["CH4"],
                "cross-border",
                ("sum", "p_nom"),
                {
                    "status2024": 0,
                    "eGon2035": 4107.949,
                    "reGon2037": 4107.949,
                    "reGon2045": 4107.949,
                },
            ),
            (
                "SANITY_GAS_OCGT_ABROAD",
                "grid.egon_etrago_link",
                ["OCGT"],
                "abroad",
                ("sum", "p_nom"),
                {
                    "status2024": 0,
                    "eGon2035": 129323.810,
                    "reGon2037": 117439.048,
                    "reGon2045": 85509.524,
                },
            ),
        ]
        return {
            "sanity": [
                *[
                    rule
                    for rule_id, table, carriers, location, measure, expected in totals
                    for rule in for_each_scenario(
                        GasComponentTotal,
                        rule_id,
                        table=table,
                        carriers=carriers,
                        location=location,
                        measure=measure,
                        expected=expected,
                    )
                ],
                # The demand of the electrolysers abroad (H2_for_industry)
                # is a constant p_set without a time series
                *for_each_scenario(
                    GasTimeseriesComplete,
                    "SANITY_GAS_CH4_LOADS_ABROAD_TIMESERIES",
                    scenarios=TARGET_SCENARIOS,
                    table="grid.egon_etrago_load",
                    carriers=["CH4"],
                    location="abroad",
                ),
                *[
                    rule
                    for rule_id, table, carriers in [
                        (
                            "SANITY_GAS_NEIGHBOURS_LOADS_BUSES",
                            "grid.egon_etrago_load",
                            ["CH4", "H2_for_industry"],
                        ),
                        (
                            "SANITY_GAS_NEIGHBOURS_GENERATORS_BUSES",
                            "grid.egon_etrago_generator",
                            ["CH4"],
                        ),
                        (
                            "SANITY_GAS_NEIGHBOURS_STORES_BUSES",
                            "grid.egon_etrago_store",
                            ["CH4"],
                        ),
                        (
                            "SANITY_GAS_NEIGHBOURS_LINKS_BUSES",
                            "grid.egon_etrago_link",
                            ["CH4", "OCGT"],
                        ),
                    ]
                    for rule in for_each_scenario(
                        GasBusReferences,
                        rule_id,
                        table=table,
                        carriers=carriers,
                    )
                ],
            ],
        }
