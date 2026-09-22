"""
Write public bus charging demand into the eTraGo load tables.

Depots are aggregated **per eTraGo bus**: one ``grid.egon_etrago_load`` row
and one 8760-step ``grid.egon_etrago_load_timeseries`` row per bus per
scenario, carrier ``land_transport_bus``. The per-depot detail stays in
``demand.egon_ev_bus_charging_depot`` for eDisGo (``docs/adr/0002``).

Depots sharing a bus carry no flexibility to distinguish them, so N loads on
one bus are mathematically identical to one summed load for every power-flow
result eTraGo computes -- writing them separately would cost N times the
rows of 8760 floats for no modelling gain.

No flex model (bus + link + store) and no lowflex variant are written: this
dataset has no flexibility (``docs/adr/0003``).
"""

from loguru import logger
import numpy as np
import pandas as pd

from egon.data import db
from egon.data.datasets.emobility.public_bus_charging.scenarios import (
    active_scenarios,
)
from egon.data.datasets.etrago_setup import (
    EgonPfHvLoad,
    EgonPfHvLoadTimeseries,
)

#: eTraGo carrier for public bus charging. Distinct from MIT's
#: ``land_transport_EV`` and HGV's ``land_transport_HGV`` so that each
#: dataset's carrier-scoped delete only removes its own rows.
CARRIER = "land_transport_bus"

N_TIMESTEPS = 8760


def _delete_old_entries(scenario: str):
    """Remove this dataset's eTraGo rows for one scenario.

    Scoped by carrier **and** scenario, so a run configured for one scenario
    leaves another scenario's bus loads alone.
    """
    db.execute_sql(
        f"""
        DELETE FROM grid.egon_etrago_load_timeseries
        WHERE scn_name = '{scenario}'
          AND load_id IN (
              SELECT load_id FROM grid.egon_etrago_load
              WHERE scn_name = '{scenario}' AND carrier = '{CARRIER}'
          );
        DELETE FROM grid.egon_etrago_load
        WHERE scn_name = '{scenario}' AND carrier = '{CARRIER}';
        """
    )


def write_etrago():
    """Aggregate depots per bus and write eTraGo loads for all scenarios."""
    scenarios = active_scenarios()
    if not scenarios:
        logger.warning("No active scenario carries public bus data.")
        return

    for scenario in scenarios:
        depots = db.select_dataframe(
            f"""
            SELECT bus_id, p_set
            FROM demand.egon_ev_bus_charging_depot
            WHERE scenario = '{scenario}' AND bus_id IS NOT NULL
            """
        )
        if depots.empty:
            logger.warning(
                f"  {scenario}: no depots with a bus, nothing to write"
            )
            continue

        per_bus = {}
        for row in depots.itertuples():
            series = np.asarray(row.p_set, dtype="float64")
            if series.size != N_TIMESTEPS:
                raise ValueError(
                    f"{scenario}: p_set for bus {row.bus_id} has "
                    f"{series.size} values, expected {N_TIMESTEPS}"
                )
            bus = int(row.bus_id)
            if bus in per_bus:
                per_bus[bus] = per_bus[bus] + series
            else:
                per_bus[bus] = series

        _delete_old_entries(scenario)

        bus_ids = sorted(per_bus)
        load_ids = db.next_etrago_id("load", len(bus_ids))

        loads = pd.DataFrame(
            {
                "scn_name": scenario,
                "load_id": load_ids,
                "bus": bus_ids,
                "carrier": CARRIER,
                "sign": -1,
            }
        )
        timeseries = pd.DataFrame(
            {
                "scn_name": scenario,
                "load_id": load_ids,
                "temp_id": 1,
                "p_set": [per_bus[bus].tolist() for bus in bus_ids],
            }
        )

        loads.to_sql(
            EgonPfHvLoad.__tablename__,
            db.engine(),
            schema="grid",
            if_exists="append",
            index=False,
        )
        timeseries.to_sql(
            EgonPfHvLoadTimeseries.__tablename__,
            db.engine(),
            schema="grid",
            if_exists="append",
            index=False,
        )

        total_mwh = sum(s.sum() for s in per_bus.values())
        logger.info(
            f"  {scenario}: wrote {len(bus_ids)} eTraGo loads "
            f"({len(depots)} depots), {total_mwh / 1e6:.3f} TWh"
        )
