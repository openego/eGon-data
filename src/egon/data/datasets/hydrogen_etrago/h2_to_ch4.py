# -*- coding: utf-8 -*-
"""
Module containing the definition of the CH4 grid to H2 links
"""

from geoalchemy2.types import Geometry

from egon.data import db
from egon.data.datasets.etrago_helpers import copy_and_modify_links
from egon.data.datasets.etrago_setup import link_geom_from_buses
from egon.data.datasets.scenario_parameters import get_sector_parameters


def insert_h2_to_ch4_to_h2():
    """
    Define methanisation, feed in and SMR capacities and insert in etrago_link.

    The potentials for methanisation and SMR are created between CH4 and H2
    buses of the CH4 grid.
    """
    # Connect to local database
    engine = db.engine()

    scn_name = "eGon2035"
    # Select CH4 and corresponding H2 buses
    # No geometry required in this case!
    buses = db.select_dataframe(
        f"""
        SELECT * FROM grid.egon_etrago_ch4_h2 WHERE scn_name = '{scn_name}'
        """
    )

    methanation = buses.copy().rename(
        columns={"bus_H2": "bus0", "bus_CH4": "bus1"}
    )
    SMR = buses.copy().rename(columns={"bus_H2": "bus1", "bus_CH4": "bus0"})
    feed_in = methanation.copy()

    # Delete old entries
    db.execute_sql(
        f"""
            DELETE FROM grid.egon_etrago_link WHERE "carrier" IN
            ('H2_to_CH4', 'H2_feedin', 'CH4_to_H2') AND scn_name = '{scn_name}'
            AND bus0 NOT IN (
               SELECT bus_id FROM grid.egon_etrago_bus
               WHERE scn_name = '{scn_name}' AND country != 'DE'
            ) AND bus1 NOT IN (
               SELECT bus_id FROM grid.egon_etrago_bus
               WHERE scn_name = '{scn_name}' AND country != 'DE'
            );
        """
    )

    scn_params = get_sector_parameters("gas", scn_name)

    # Write new entries
    for table, carrier in zip(
        [methanation, SMR, feed_in], ["H2_to_CH4", "CH4_to_H2", "H2_feedin"]
    ):

        # set parameters according to carrier name
        table["carrier"] = carrier
        table["efficiency"] = scn_params["efficiency"][carrier]
        if carrier == "H2_feedin":
            table["p_nom_extendable"] = False
            table["p_nom"] = 1e9
        else:
            table["p_nom_extendable"] = True
            table["capital_cost"] = scn_params["capital_cost"][carrier]
            table["lifetime"] = scn_params["lifetime"][carrier]
        new_id = db.next_etrago_id("link")
        table["link_id"] = range(new_id, new_id + len(table))

        table = link_geom_from_buses(table, scn_name)

        table.to_postgis(
            "egon_etrago_link",
            engine,
            schema="grid",
            index=False,
            if_exists="append",
            dtype={"topo": Geometry()},
        )


def insert_h2_to_ch4_eGon100RE():
    """Copy H2/CH4 links from the eGon2035 to the eGon100RE scenario."""
    copy_and_modify_links(
        "eGon2035", "eGon100RE", ["H2_to_CH4", "CH4_to_H2"], "gas"
    )
