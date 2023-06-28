"""Module containing functions to insert gas abroad

In this module, functions useful to insert the gas components (H2 and
CH4) abroad for eGon2035 and eGon100RE are defined.

"""
import zipfile

from geoalchemy2.types import Geometry
import geopandas as gpd
import pandas as pd

from egon.data import config, db
from egon.data.datasets.electrical_neighbours import get_map_buses
from egon.data.datasets.scenario_parameters import get_sector_parameters


def insert_generators(gen, scn_name):
    """Insert gas generators for foreign countries in database

    This function inserts the gas generators for foreign countries
    with the following steps:
      * Clean the database
      * Receive the gas production capacities per foreign node
      * For eGon2035, put them in the righ format
      * Add missing columns (generator_id and carrier)
      * Insert the table into the database

    Parameters
    ----------
    gen : pandas.DataFrame
        Gas production capacities per foreign node
    scn_name : str
        Name of the scenario

    Returns
    -------
    None

    """
    carrier = "CH4"
    sources = config.datasets()["gas_neighbours"]["sources"]
    targets = config.datasets()["gas_neighbours"]["targets"]

    # Delete existing data
    db.execute_sql(
        f"""
        DELETE FROM
        {targets['generators']['schema']}.{targets['generators']['table']}
        WHERE bus IN (
            SELECT bus_id FROM
            {sources['buses']['schema']}.{sources['buses']['table']}
            WHERE country != 'DE'
            AND scn_name = '{scn_name}')
        AND scn_name = '{scn_name}'
        AND carrier = '{carrier}';
        """
    )

    if scn_name == "eGon2035":
        map_buses = get_map_buses()
        scn_params = get_sector_parameters("gas", scn_name)

        # Set bus_id
        gen.loc[
            gen[gen["index"].isin(map_buses.keys())].index, "index"
        ] = gen.loc[
            gen[gen["index"].isin(map_buses.keys())].index, "index"
        ].map(
            map_buses
        )
        gen.loc[:, "bus"] = (
            get_foreign_gas_bus_id().loc[gen.loc[:, "index"]].values
        )

        gen["p_nom"] = gen["cap_2035"]
        gen["marginal_cost"] = (
        gen["share_LNG_2035"] * scn_params["marginal_cost"]["CH4"] * 1.3
        + gen["share_conv_pipe_2035"] * scn_params["marginal_cost"]["CH4"]
        + gen["share_bio_2035"] * scn_params["marginal_cost"]["biogas"]
        )
        gen["scn_name"] = scn_name

        # Remove useless columns
        gen = gen.drop(
            columns=[
                "index",
                "share_LNG_2035",
                "share_conv_pipe_2035",
                "share_bio_2035",
                "cap_2035",
            ]
    )

    # Add missing columns
    new_id = db.next_etrago_id("generator")
    gen["generator_id"] = range(new_id, new_id + len(gen))

    gen["carrier"] = carrier

    # Insert data to db
    gen.to_sql(
        targets["generators"]["table"],
        db.engine(),
        schema=targets["generators"]["schema"],
        index=False,
        if_exists="append",
    )


def insert_gas_grid_capacities(Neighbouring_pipe_capacities_list, scn_name):
    """Insert crossbordering gas pipelines in the database

    This function insert a list of crossbordering gas pipelines after
    cleaning the database.
    For eGon2035, all the CH4 crossbordering pipelines are inserted
    there (no H2 grid in this scenario).
    For eGon100RE, only the the crossbordering pipelines with Germany
    are inserted there (the other ones are inerted in PypsaEurSec),
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

    carriers = {
        "CH4": {"bus_inDE": "CH4", "bus_abroad": "CH4"},
        "H2_retrofit": {"bus_inDE": "H2_grid", "bus_abroad": "H2"},
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
                AND carrier = '{c.index}'            
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


def get_foreign_gas_bus_id(scn_name="eGon2035", carrier="CH4"):
    """Calculate the etrago bus id based on the geometry for eGon2035

    Map node_ids from TYNDP and etragos bus_id

    Parameters
    ----------
    scn_name : str
        Name of the scenario
    carrier : str
        Name of the carrier

    Returns
    -------
    pandas.Series
        List of mapped node_ids from TYNDP and etragos bus_id

    """
    sources = config.datasets()["gas_neighbours"]["sources"]

    bus_id = db.select_geodataframe(
        f"""
        SELECT bus_id, ST_Buffer(geom, 1) as geom, country
        FROM grid.egon_etrago_bus
        WHERE scn_name = '{scn_name}'
        AND carrier = '{carrier}'
        AND country != 'DE'
        """,
        epsg=3035,
    )

    # insert installed capacities
    file = zipfile.ZipFile(f"tyndp/{sources['tyndp_capacities']}")

    # Select buses in neighbouring countries as geodataframe
    buses = pd.read_excel(
        file.open("TYNDP-2020-Scenario-Datafile.xlsx").read(),
        sheet_name="Nodes - Dict",
    ).query("longitude==longitude")
    buses = gpd.GeoDataFrame(
        buses,
        crs=4326,
        geometry=gpd.points_from_xy(buses.longitude, buses.latitude),
    ).to_crs(3035)

    buses["bus_id"] = 0

    # Select bus_id from etrago with shortest distance to TYNDP node
    for i, row in buses.iterrows():
        distance = bus_id.set_index("bus_id").geom.distance(row.geometry)
        buses.loc[i, "bus_id"] = distance[
            distance == distance.min()
        ].index.values[0]

    return buses.set_index("node_id").bus_id
