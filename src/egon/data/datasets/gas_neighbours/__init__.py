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
        )
