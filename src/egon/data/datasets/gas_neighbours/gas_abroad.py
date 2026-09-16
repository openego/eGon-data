"""
Module containing functions to insert the gas sector abroad

In this module, functions used to insert the gas components (H2 and
CH4) abroad for scenarios are defined.

"""

from geoalchemy2.types import Geometry

from egon.data import config, db
from egon.data.datasets import load_sources_and_targets


def insert_gas_grid_capacities(Neighbouring_pipe_capacities_list, scn_name):
    """Insert crossbordering gas pipelines into the database

    This function inserts a list of crossbordering gas pipelines after
    cleaning the database.
    For scenarios, all the CH4 crossbordering pipelines are inserted
    (no H2 grid in this scenario).

    This function inserts data in the database and has no return.

    Parameters
    ----------
    Neighbouring_pipe_capacities_list : pandas.DataFrame
        List of the crossbordering gas pipelines
    scn_name : str
        Name of the scenario

    """
    sources, targets = load_sources_and_targets("GasNeighbours")

    # Delete existing data
    if scn_name in ["eGon2035", "reGon2037", "reGon2045"]:
        carrier_link = "CH4"
        carrier_bus = "CH4"

        db.execute_sql(f"""
            DELETE FROM 
            {targets.tables['links']}
            WHERE "bus0" IN (
                SELECT bus_id FROM 
                {sources.tables['buses']}
                    WHERE country != 'DE'
                    AND carrier = '{carrier_bus}'
                    AND scn_name = '{scn_name}')
            OR "bus1" IN (
                SELECT bus_id FROM 
                {sources.tables['buses']}
                    WHERE country != 'DE'
                    AND carrier = '{carrier_bus}' 
                    AND scn_name = '{scn_name}')
            AND scn_name = '{scn_name}'
            AND carrier = '{carrier_link}'            
            ;
            """)

    # Insert data to db
    Neighbouring_pipe_capacities_list.set_geometry(
        "geom", crs=4326
    ).to_postgis(
        "egon_etrago_gas_link",
        db.engine(),
        schema="grid",
        index=False,
        if_exists="replace",
        dtype={"geom": Geometry(), "topo": Geometry()},
    )

    db.execute_sql(f"""
    select UpdateGeometrySRID('grid', 'egon_etrago_gas_link', 'topo', 4326) ;

    INSERT INTO {targets.tables['links']} (
        scn_name, link_id, carrier,
        bus0, bus1, p_nom, p_min_pu, length, geom, topo)
    
    SELECT scn_name, link_id, carrier, bus0, bus1, p_nom, p_min_pu, length, geom, topo

    FROM grid.egon_etrago_gas_link;

    DROP TABLE grid.egon_etrago_gas_link;
        """)
