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
from egon.data.datasets.emobility.public_bus_charging.spatial_assignment import (  # noqa: E501
    spatial_assignment,
)


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
      * :py:class:`SubstationVoronoi <egon.data.datasets.substation_voronoi.SubstationVoronoi>`
      * :py:class:`EtragoSetup <egon.data.datasets.etrago_setup.EtragoSetup>`
      * :py:class:`ScenarioParameters <egon.data.datasets.scenario_parameters.ScenarioParameters>`
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
    version: str = "0.0.1"

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
        )
