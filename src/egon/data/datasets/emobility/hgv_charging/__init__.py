"""
HGV charging integration for egon-data.

Reads precomputed HGV charging demand input files (sites, charging points,
events, profiles) and writes:
  - demand.egon_hgv_charging_site      (Table 1)
  - demand.egon_hgv_charging_point     (Table 2)
  - demand.egon_hgv_charging_event     (Table 3)
  - demand.egon_hgv_profile            (Table 4)
  - grid.egon_etrago_load              (extended)
  - grid.egon_etrago_load_timeseries   (extended)

Scenarios:
  reGon2037 → input data "C 2037"
  reGon2045 → input data "C 2045"

Only scenarios that are both in SCENARIO_MAP and in the pipeline's configured
--scenarios are processed (see active_scenario_map()).
"""

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
            "charging_site": "demand.egon_hgv_charging_site",
            "charging_point": "demand.egon_hgv_charging_point",
            "charging_event": "demand.egon_hgv_charging_event",
            "profile": "demand.egon_hgv_profile",
            "etrago_load": "grid.egon_etrago_load",
            "etrago_load_timeseries": "grid.egon_etrago_load_timeseries",
        },
    )

    name: str = "HGVCharging"
    version: str = "0.0.1"

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
        )
