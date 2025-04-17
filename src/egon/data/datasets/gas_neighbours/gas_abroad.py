"""
Module containing functions to insert the gas sector abroad

In this module, functions used to insert the gas components (H2 and
CH4) abroad for eGon2035 and eGon100RE are defined.

"""

from geoalchemy2.types import Geometry

from egon.data import config, db


def insert_gas_grid_capacities(Neighbouring_pipe_capacities_list, scn_name):
    """Insert crossbordering gas pipelines into the database

    This function inserts a list of crossbordering gas pipelines after
    cleaning the database.
    For eGon2035, all the CH4 crossbordering pipelines are inserted
    (no H2 grid in this scenario).
    For eGon100RE, only the crossbordering pipelines with Germany
    are inserted (the other ones are inserted in PypsaEurSec),
    but in this scenario there are H2 and CH4 pipelines.
    This function inserts data in the database and has no return.

    Parameters
    ----------
    Neighbouring_pipe_capacities_list : pandas.DataFrame
        List of the crossbordering gas pipelines
    scn_name : str
        Name of the scenario

    """
    sources = config.datasets()["gas_neighbours"]["sources"]
    targets = config.datasets()["gas_neighbours"]["targets"]

    # Delete existing data
    if scn_name == "eGon2035":
        carrier_link = "CH4"
        carrier_bus = "CH4"

        db.execute_sql(
            f"""
            DELETE FROM 
            {sources['links']['schema']}.{sources['links']['table']}
            WHERE "bus0" IN (
                SELECT bus_id FROM 
                {sources['buses']['schema']}.{sources['buses']['table']}
                    WHERE country != 'DE'
                    AND carrier = '{carrier_bus}'
                    AND scn_name = '{scn_name}')
            OR "bus1" IN (
                SELECT bus_id FROM 
                {sources['buses']['schema']}.{sources['buses']['table']}
                    WHERE country != 'DE'
                    AND carrier = '{carrier_bus}' 
                    AND scn_name = '{scn_name}')
            AND scn_name = '{scn_name}'
            AND carrier = '{carrier_link}'            
            ;
            """
        )

    carriers = {
        "CH4": {"bus_inDE": "CH4", "bus_abroad": "CH4"},
        "H2_retrofit": {"bus_inDE": "H2", "bus_abroad": "H2"},
    }

    if scn_name == "eGon100RE":
        for c in carriers:
            db.execute_sql(
                f"""
                DELETE FROM
                {sources['links']['schema']}.{sources['links']['table']}
                WHERE ("bus0" IN (
                        SELECT bus_id FROM 
                        {sources['buses']['schema']}.{sources['buses']['table']}
                        WHERE country != 'DE'
                        AND carrier = '{carriers[c]["bus_abroad"]}'
                        AND scn_name = '{scn_name}')
                    AND "bus1" IN (SELECT bus_id FROM 
                        {sources['buses']['schema']}.{sources['buses']['table']}
                        WHERE country = 'DE'
                        AND carrier = '{carriers[c]["bus_inDE"]}' 
                        AND scn_name = '{scn_name}'))
                OR ("bus0" IN (
                        SELECT bus_id FROM 
                        {sources['buses']['schema']}.{sources['buses']['table']}
                        WHERE country = 'DE'
                        AND carrier = '{carriers[c]["bus_inDE"]}'
                        AND scn_name = '{scn_name}')
                AND "bus1" IN (
                        SELECT bus_id FROM 
                        {sources['buses']['schema']}.{sources['buses']['table']}
                        WHERE country != 'DE'
                        AND carrier = '{carriers[c]["bus_abroad"]}' 
                        AND scn_name = '{scn_name}'))
                AND scn_name = '{scn_name}'
                AND carrier = '{carriers[c]["bus_abroad"]}'
                ;
                """
            )

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

    db.execute_sql(
        f"""
    select UpdateGeometrySRID('grid', 'egon_etrago_gas_link', 'topo', 4326) ;

    INSERT INTO {targets['links']['schema']}.{targets['links']['table']} (
        scn_name, link_id, carrier,
        bus0, bus1, p_nom, p_min_pu, length, geom, topo)
    
    SELECT scn_name, link_id, carrier, bus0, bus1, p_nom, p_min_pu, length, geom, topo

    FROM grid.egon_etrago_gas_link;

    DROP TABLE grid.egon_etrago_gas_link;
        """
    )
