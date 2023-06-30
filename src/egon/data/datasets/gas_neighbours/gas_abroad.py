"""Module containing functions to insert gas abroad

In this module, functions useful to insert the gas components (H2 and
CH4) abroad for eGon2035 and eGon100RE are defined.

"""

from geoalchemy2.types import Geometry

from egon.data import config, db


def insert_gas_grid_capacities(Neighbouring_pipe_capacities_list, scn_name):
    """Insert crossbordering gas pipelines in the database

    This function insert a list of crossbordering gas pipelines after
    cleaning the database.
    For eGon2035, all the CH4 crossbordering pipelines are inserted
    there (no H2 grid in this scenario).
    For eGon100RE, only the the crossbordering pipelines with Germany
    are inserted there (the other ones are inserted in PypsaEurSec),
    but in this scenario there are H2 and CH4 pipelines.

    Parameters
    ----------
    Neighbouring_pipe_capacities_list : pandas.DataFrame
        List of the crossbordering gas pipelines
    scn_name : str
        Name of the scenario

    Returns
    -------
    None

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

    carriers = {"CH4": "CH4", "H2_retrofit": "H2_grid"}

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
                        AND carrier = '{carriers[c]}'
                        AND scn_name = '{scn_name}')
                    AND "bus1" IN (SELECT bus_id FROM 
                        {sources['buses']['schema']}.{sources['buses']['table']}
                        WHERE country = 'DE'
                        AND carrier = '{carriers[c]}'
                        AND scn_name = '{scn_name}'))
                OR ("bus0" IN (
                        SELECT bus_id FROM 
                        {sources['buses']['schema']}.{sources['buses']['table']}
                        WHERE country = 'DE'
                        AND carrier = '{carriers[c]}'
                        AND scn_name = '{scn_name}')
                    AND "bus1" IN (
                        SELECT bus_id FROM 
                        {sources['buses']['schema']}.{sources['buses']['table']}
                        WHERE country != 'DE'
                        AND carrier = '{carriers[c]}'
                        AND scn_name = '{scn_name}'))
                AND scn_name = '{scn_name}'
                AND carrier = '{c}'
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
