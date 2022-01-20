"""The central module containing all code dealing with heat sector in etrago
"""
from geoalchemy2.types import Geometry
import geopandas as gpd

from egon.data import config, db
from egon.data.datasets.etrago_setup import link_geom_from_buses
from egon.data.datasets.gas_prod import assign_bus_id
from egon.data.datasets.scenario_parameters import get_sector_parameters


def insert_grid():
    """ Insert hydrogen grid to etrago table

    Parameters
    ----------
    scn_name : str, optional
        Name of the scenario The default is 'eGon2035'.

    """
    H2_buses = db.select_geodataframe(
        f"""
        SELECT * FROM grid.egon_etrago_bus WHERE scn_name = 'eGon2035' AND
        carrier IN ('H2_grid', 'H2_saltcavern') and country = 'DE'
        """,
        epsg=4326,
    )

    H2_buses["scn_name"] = "eGon100RE"

    pipelines = db.select_geodataframe(
        f"""
        SELECT * FROM grid.egon_etrago_link
        WHERE scn_name = 'eGon2035' AND carrier = 'CH4'
        AND bus0 IN (
            SELECT bus_id FROM grid.egon_etrago_bus
            WHERE scn_name = 'eGon2035' AND country = 'DE'
        ) AND bus1 IN (
            SELECT bus_id FROM grid.egon_etrago_bus
            WHERE scn_name = 'eGon2035' AND country = 'DE'
        );
        """
    )

    CH4_H2_busmap = db.select_dataframe(
        f"""
        SELECT * FROM grid.egon_etrago_ch4_h2 WHERE scn_name = 'eGon2035'
        """,
        index_col="bus_CH4",
    )

    pipelines["carrier"] = "H2_retrofit"
    pipelines["p_nom"] *= 0.6  # capacity retrofitting factor
    # map pipeline buses
    pipelines["bus0"] = CH4_H2_busmap.loc[pipelines["bus0"], "bus_H2"].values
    pipelines["bus1"] = CH4_H2_busmap.loc[pipelines["bus1"], "bus_H2"].values
    pipelines["scn_name"] = "eGon100RE"

    # create new pipelines between grid and saltcaverns

    new_pipelines = gpd.GeoDataFrame()
    new_pipelines["bus0"] = H2_buses.loc[
        H2_buses["carrier"] == "H2_saltcavern", "bus_id"
    ].values
    new_pipelines["geometry"] = H2_buses.loc[
        H2_buses["carrier"] == "H2_saltcavern", "geom"
    ].values
    new_pipelines.set_crs(epsg=4326, inplace=True)

    # find bus in H2_grid voronoi
    new_pipelines = assign_bus_id(new_pipelines, "eGon2035", "H2_grid")
    new_pipelines = new_pipelines.rename(columns={"bus_id": "bus1"}).drop(
        columns=["bus"]
    )

    # create link geometries
    new_pipelines = link_geom_from_buses(
        new_pipelines[["bus0", "bus1"]], "eGon2035"
    )
    new_pipelines["carrier"] = "H2_gridextension"
    new_pipelines["scn_name"] = "eGon100RE"
    new_pipelines["p_nom_extendable"] = True
    new_pipelines["distance"] = new_pipelines.to_crs(epsg=3035).geometry.length

    scn_params = get_sector_parameters("gas", "eGon100RE")
    new_pipelines["capital_cost"] = (
        1
        # scn_params["capital_cost"]["H2_pipeline"]  (data not yet entered)
        * new_pipelines["distance"]
        / 1e3
    )
    new_pipelines.drop(columns=["distance"], inplace=True)

    new_id = db.next_etrago_id("link")
    new_pipelines["link_id"] = range(new_id, new_id + len(new_pipelines))

    engine = db.engine()

    # Delete old entries
    db.execute_sql(
        f"""
            DELETE FROM grid.egon_etrago_link WHERE "carrier" IN
            ('H2_retrofit', 'H2_gridextension') AND scn_name = 'eGon100RE'
            AND bus0 IN (
               SELECT bus_id FROM grid.egon_etrago_bus
               WHERE scn_name = 'eGon100RE' AND country = 'DE'
            ) AND bus1 IN (
               SELECT bus_id FROM grid.egon_etrago_bus
               WHERE scn_name = 'eGon100RE' AND country = 'DE'
            );
        """
    )

    # Delete old entries
    db.execute_sql(
        f"""
            DELETE FROM grid.egon_etrago_bus WHERE "carrier" IN
            ('H2_grid', 'H2_saltcavern') AND scn_name = 'eGon100RE'
            AND country = 'DE'
        """
    )

    pipelines.to_crs(epsg=4326).to_postgis(
        "egon_etrago_link",
        engine,
        schema="grid",
        index=False,
        if_exists="append",
        dtype={"topo": Geometry()},
    )
    new_pipelines.to_crs(epsg=4326).to_postgis(
        "egon_etrago_link",
        engine,
        schema="grid",
        index=False,
        if_exists="append",
        dtype={"topo": Geometry()},
    )
    H2_buses.to_crs(epsg=4326).to_postgis(
        "egon_etrago_bus",
        engine,
        schema="grid",
        index=False,
        if_exists="append",
        dtype={"geom": Geometry()},
    )

    ##### GRID RELATED
    # 1. Select eGon2035 H2 buses (DE), CH4 pipelines
    # 2. Rename scenario
    # 3. Change pipeline carrier
    # 4. Adjust pipeline Links capacities
    # 5. Change pipeline bus0 and bus1 to respective H2 buses (h2_ch4 busmap)
    # 6. Build new pipelines between H2 cavern and H2 grid (H2 cavern in H2 grid voronoi)
    # -> p_nom_extendable = True
    # 7. Update scn_name
    # 8. Write stuff to db

    ##### INTERCONNECTIONS H2-AC
    # 1. Select eGon2035 AC-H2 related links (DE)
    # 2. Update scn_name
    # -> bus_id does not need change, since all AC and H2 buses should have the same ids
    # 3. Write stuff to db

    ##### INTERCONNECTIONS H2-CH4
    # 1. Select CH4 loads
    # 2. Allocate loads to H2 voronoi
    # 3. Build new methanation links
