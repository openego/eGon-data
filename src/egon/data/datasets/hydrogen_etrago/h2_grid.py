"""
The central module containing all code dealing with the H2 grid in eGon100RE

The H2 grid, present only in eGon100RE, is composed of two parts:
  * a fixed part with the same topology as the CH4 grid and with
    carrier 'H2_retrofit' corresponding to the retrofitted share of
    the CH4 grid into a hydrogen grid,
  * an extendable part with carrier 'H2_gridextension', linking each
    H2_salcavern bus to the closest H2 bus: this part as no
    capacity (p_nom = 0) but it can be extended.
As for the CH4 grid, the H2 pipelines are modelled by PyPSA links.

"""
from geoalchemy2.types import Geometry
from shapely.geometry import MultiLineString
import geopandas as gpd

from egon.data import db
from egon.data.datasets.etrago_setup import link_geom_from_buses
from egon.data.datasets.scenario_parameters import get_sector_parameters


def insert_h2_pipelines():
    """
    Insert hydrogen grid (H2 links) into the database for eGon100RE.

    Insert the H2 grid by executing the following steps:
      * Copy the CH4 links in Germany from eGon100RE
      * Overwrite the followings columns:
          * bus0 and bus1 using the grid.egon_etrago_ch4_h2 table
          * carrier, scn_name
          * p_nom: the value attributed there corresponds to the share
            of p_nom of the specific pipeline that could be retrofited into
            H2 pipeline. This share is the same for every pipeline and is
            calculated in the PyPSA-eur-sec run.
      * Create new extendable pipelines to link the existing grid to the
        H2_saltcavern buses
      * Clean database
      * Attribute link_id to the links
      * Insert into the database

    Returns
    -------
    None
     

    """
    H2_buses = db.select_geodataframe(
        f"""
        SELECT * FROM grid.egon_etrago_bus WHERE scn_name = 'eGon100RE' AND
        carrier IN ('H2', 'H2_saltcavern') and country = 'DE'
        """,
        epsg=4326,
    )

    pipelines = db.select_geodataframe(
        f"""
        SELECT * FROM grid.egon_etrago_link
        WHERE scn_name = 'eGon100RE' AND carrier = 'CH4'
        AND bus0 IN (
            SELECT bus_id FROM grid.egon_etrago_bus
            WHERE scn_name = 'eGon100RE' AND country = 'DE'
        ) AND bus1 IN (
            SELECT bus_id FROM grid.egon_etrago_bus
            WHERE scn_name = 'eGon100RE' AND country = 'DE'
        );
        """
    )

    CH4_H2_busmap = db.select_dataframe(
        f"""
        SELECT * FROM grid.egon_etrago_ch4_h2 WHERE scn_name = 'eGon100RE'
        """,
        index_col="bus_CH4",
    )

    scn_params = get_sector_parameters("gas", "eGon100RE")

    pipelines["carrier"] = "H2_retrofit"
    pipelines["p_nom"] *= (
        scn_params["retrofitted_CH4pipeline-to-H2pipeline_share"]
    )
    # map pipeline buses
    pipelines["bus0"] = CH4_H2_busmap.loc[pipelines["bus0"], "bus_H2"].values
    pipelines["bus1"] = CH4_H2_busmap.loc[pipelines["bus1"], "bus_H2"].values
    pipelines["scn_name"] = "eGon100RE"
    pipelines["p_min_pu"] = -1.0
    pipelines["capital_cost"] = (
        scn_params["capital_cost"]["H2_pipeline_retrofit"]
        * pipelines["length"]
        / 1e3
    )

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
    new_pipelines = db.assign_gas_bus_id(new_pipelines, "eGon100RE", "H2")
    new_pipelines = new_pipelines.rename(columns={"bus_id": "bus1"}).drop(
        columns=["bus"]
    )

    # create link geometries
    new_pipelines = link_geom_from_buses(
        new_pipelines[["bus0", "bus1"]], "eGon100RE"
    )
    new_pipelines["geom"] = new_pipelines.apply(
        lambda row: MultiLineString([row["topo"]]), axis=1
    )
    new_pipelines = new_pipelines.set_geometry("geom", crs=4326)
    new_pipelines["carrier"] = "H2_gridextension"
    new_pipelines["scn_name"] = "eGon100RE"
    new_pipelines["p_min_pu"] = -1.0
    new_pipelines["p_nom_extendable"] = True
    new_pipelines["length"] = new_pipelines.to_crs(epsg=3035).geometry.length

    # Insert capital cost data
    new_pipelines["capital_cost"] = (
        scn_params["capital_cost"]["H2_pipeline"]
        * new_pipelines["length"]
        / 1e3
    )

    # Delete old entries
    db.execute_sql(
        f"""
            DELETE FROM grid.egon_etrago_link WHERE "carrier" IN
            ('H2_retrofit', 'H2_gridextension') AND scn_name = 'eGon100RE'
            AND bus0 NOT IN (
               SELECT bus_id FROM grid.egon_etrago_bus
               WHERE scn_name = 'eGon100RE' AND country != 'DE'
            ) AND bus1 NOT IN (
               SELECT bus_id FROM grid.egon_etrago_bus
               WHERE scn_name = 'eGon100RE' AND country != 'DE'
            );
        """
    )

    engine = db.engine()

    new_id = db.next_etrago_id("link")
    pipelines["link_id"] = range(new_id, new_id + len(pipelines))

    pipelines.to_crs(epsg=4326).to_postgis(
        "egon_etrago_link",
        engine,
        schema="grid",
        index=False,
        if_exists="append",
        dtype={"topo": Geometry()},
    )

    new_id = db.next_etrago_id("link")
    new_pipelines["link_id"] = range(new_id, new_id + len(new_pipelines))

    new_pipelines.to_postgis(
        "egon_etrago_h2_link",
        engine,
        schema="grid",
        index=False,
        if_exists="replace",
        dtype={"geom": Geometry(), "topo": Geometry()},
    )

    db.execute_sql(
    """
    select UpdateGeometrySRID('grid', 'egon_etrago_h2_link', 'topo', 4326) ;

    INSERT INTO grid.egon_etrago_link (scn_name, capital_cost,
                                              link_id, carrier,
                                              bus0, bus1, p_min_pu,
                                              p_nom_extendable, length,
                                              geom, topo)
    SELECT scn_name, capital_cost,
                link_id, carrier,
                bus0, bus1, p_min_pu,
                p_nom_extendable, length,
                geom, topo

    FROM grid.egon_etrago_h2_link;

    DROP TABLE grid.egon_etrago_h2_link;
    """
    )
