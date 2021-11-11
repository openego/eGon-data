"""The central module containing all code dealing with heat sector in etrago
"""
from geoalchemy2 import Geometry
import geopandas as gpd
import pandas as pd

from egon.data import config, db
from egon.data.datasets import Dataset


def insert_H2_overground_storage():
    """Insert H2 steel tank storage for every H2 bus."""
    # The targets of etrago_hydrogen also serve as source here ಠ_ಠ
    sources = config.datasets()["etrago_hydrogen"]["sources"]
    targets = config.datasets()["etrago_hydrogen"]["targets"]

    # Place storage at every H2 bus
    storages = db.select_geodataframe(
        f"""
        SELECT bus_id, scn_name, geom
        FROM {sources['buses']['schema']}.
        {sources['buses']['table']} WHERE carrier LIKE 'H2%%'""",
        index_col="bus_id",
    )

    carrier = "H2_overground"
    # Add missing column
    storages["bus"] = storages.index
    storages["carrier"] = carrier

    # Does e_nom_extenable = True render e_nom useless?
    storages["e_nom"] = 0
    storages["e_nom_extendable"] = True

    # "Synergies of sector coupling and transmission reinforcement in a cost-optimised, highly renewable European energy system", p.4
    storages["capital_cost"] = 8.4 * 1e3

    # Remove useless columns
    storages.drop(columns=["geom"], inplace=True)

    # Clean table
    db.execute_sql(
        f"""
        DELETE FROM grid.egon_etrago_store WHERE "carrier" = '{carrier}';
        """
    )

    # Select next id value
    new_id = db.next_etrago_id("store")
    storages["store_id"] = range(new_id, new_id + len(storages))
    storages = storages.reset_index(drop=True)

    # Insert data to db
    storages.to_sql(
        targets["hydrogen_stores"]["table"],
        db.engine(),
        schema=targets["hydrogen_stores"]["schema"],
        index=False,
        if_exists="append",
    )


def insert_H2_saltcavern_storage():
    """Insert H2 saltcavern storage for every H2_saltcavern bus in the table."""

    # Datatables sources and targets
    sources = config.datasets()["etrago_hydrogen"]["sources"]
    targets = config.datasets()["etrago_hydrogen"]["targets"]

    # Place storage at every H2 bus
    storage_potentials = db.select_geodataframe(
        f"""
        SELECT *
        FROM {sources['saltcavern_data']['schema']}.
        {sources['saltcavern_data']['table']}""",
        geom_col="geometry"
    )

    H2_AC_bus_map = db.select_dataframe(
        f"""
        SELECT *
        FROM {sources['H2_AC_map']['schema']}.
        {sources['H2_AC_map']['table']}""",
    )

    storage_potentials["storage_potential"] = (
        storage_potentials["area_fraction"] * storage_potentials["potential"]
    )

    # print(storage_potentials["storage_potential"])
    storage_potentials["summed_potential_per_bus"] = storage_potentials.groupby("bus_id")["storage_potential"].transform("sum")

    storages = storage_potentials[["summed_potential_per_bus", "bus_id"]].copy()
    storages.drop_duplicates('bus_id', keep='last', inplace=True)

    # map AC buses in potetial data to respective H2 buses
    storages = storages.merge(
        H2_AC_bus_map, left_on='bus_id', right_on='bus_AC'
    ).reindex(columns=['bus_H2', 'summed_potential_per_bus', 'scn_name'])

    # rename columns
    storages.rename(
        columns={'bus_H2': 'bus', 'summed_potential_per_bus': 'e_nom_max'},
        inplace=True
    )

    # add missing columns
    carrier = "H2_underground"
    storages["carrier"] = carrier
    storages["e_nom"] = 0
    storages["e_nom_extendable"] = True

    # capital cost needs update, also see respective redmine issue
    storages["capital_cost"] = 8.4 * 1e3

    # Clean table
    db.execute_sql(
        f"""
        DELETE FROM grid.egon_etrago_store WHERE "carrier" = '{carrier}';
        """
    )

    # Select next id value
    new_id = db.next_etrago_id("store")
    storages["store_id"] = range(new_id, new_id + len(storages))
    storages = storages.reset_index(drop=True)

    # # Insert data to db
    storages.to_sql(
        targets['hydrogen_stores']['table'],
        db.engine(),
        schema=targets['hydrogen_stores']['schema'],
        index=False,
        if_exists="append",
    )


def calculate_and_map_saltcavern_storage_potential():
    """Calculate site specific storage potential based on InSpEE-DS report."""

    # select onshore vg250 data
    sources = config.datasets()["bgr"]["sources"]
    targets = config.datasets()["bgr"]["targets"]
    vg250_data = db.select_geodataframe(
        f"""SELECT * FROM
                {sources['vg250_federal_states']['schema']}.
                {sources['vg250_federal_states']['table']}
            WHERE gf = '4'""",
        index_col="id",
        geom_col="geometry",
    )

    # hydrogen storage potential data from InSpEE-DS report
    hydrogen_storage_potential = pd.DataFrame(
        columns=["federal_state", "INSPEEDS", "INSPEE"]
    )

    # values in MWh, modified to fit the saltstructure data
    hydrogen_storage_potential.loc[0] = ["Brandenburg", 353e6, 159e6]
    hydrogen_storage_potential.loc[1] = ["Niedersachsen", 253e6, 702e6]
    hydrogen_storage_potential.loc[2] = ["Schleswig-Holstein", 0, 413e6]
    hydrogen_storage_potential.loc[3] = ["Mecklenburg-Vorpommern", 25e6, 193e6]
    hydrogen_storage_potential.loc[4] = ["Nordrhein-Westfalen", 168e6, 0]
    hydrogen_storage_potential.loc[5] = ["Sachsen-Anhalt", 318e6, 147e6]
    hydrogen_storage_potential.loc[6] = ["Thüringen", 595e6, 0]

    hydrogen_storage_potential["total"] = (
        # currently only InSpEE saltstructure shapefiles are available
        # hydrogen_storage_potential["INSPEEDS"]
        hydrogen_storage_potential["INSPEE"]
    )

    # get saltcavern shapes
    saltcavern_data = db.select_geodataframe(
        f"""SELECT * FROM
                {sources['saltcaverns']['schema']}.
                {sources['saltcaverns']['table']}
            """,
        geom_col="geometry",
    )

    saltcaverns_in_fed_state = gpd.GeoDataFrame()

    # intersection of saltstructures with federal state
    for row in hydrogen_storage_potential.index:
        federal_state = hydrogen_storage_potential.loc[row, "federal_state"]
        federal_state_data = vg250_data[vg250_data["gen"] == federal_state]

        # skip if federal state not available (e.g. local testing)
        if federal_state_data.size > 0:
            saltcaverns_in_fed_state = saltcaverns_in_fed_state.append(
                saltcavern_data.overlay(federal_state_data, how="intersection")
            )
            # write total potential in column, will be overwritten by actual
            # value later
            saltcaverns_in_fed_state[
                "potential"
            ] = hydrogen_storage_potential.loc[row, "total"]

    # drop all federal state data columns except name of the state
    saltcaverns_in_fed_state.drop(
        columns=[
            col
            for col in federal_state_data.columns
            if col not in ["gen", "geometry"]
        ],
        inplace=True,
    )

    # this is required for the first loop as no geometry has been set
    # prior to this, also set crs to match original saltcavern_data crs
    saltcaverns_in_fed_state.set_geometry("geometry")
    saltcaverns_in_fed_state.set_crs(saltcavern_data.crs, inplace=True)
    saltcaverns_in_fed_state.to_crs(epsg=4326, inplace=True)

    # recalculate area in case structures have been split at federal
    # state borders in original data epsg
    # mapping of potential to individual H2 storage is in
    # hydrogen_etrago/storage.py
    saltcaverns_in_fed_state["shape_star"] = saltcaverns_in_fed_state.to_crs(
        epsg=25832
    ).area

    # get substation voronois
    substation_voronoi = db.select_geodataframe(
        f"""
        SELECT * FROM grid.egon_hvmv_substation_voronoi
        """,
        index_col="bus_id",
    ).to_crs(4326)

    # get substations
    substations = db.select_geodataframe(
        f"""
        SELECT * FROM grid.egon_hvmv_substation""",
        geom_col="point",
        index_col="bus_id",
    ).to_crs(4326)

    # create 500 m radius around substations as storage potential area
    # epsg for buffer in line with original saltstructre data
    substations_inflation = gpd.GeoDataFrame(
        geometry=substations.to_crs(25832).buffer(500).to_crs(4326)
    )

    # !!row wise!! intersection between the substations inflation and the
    # respective voronoi (overlay only allows for intersection to all
    # voronois)
    voroni_buffer_intersect = substations_inflation["geometry"].intersection(
        substation_voronoi["geom"]
    )

    # make intersection a dataframe to kepp bus_id column in potential area
    # overlay
    voroni_buffer_intersect = gpd.GeoDataFrame(
        {
            "bus_id": voroni_buffer_intersect.index.tolist(),
            "geometry": voroni_buffer_intersect.geometry.tolist(),
        }
    ).set_crs(epsg=4326)

    # overlay saltstructures with substation buffer
    potential_areas = saltcaverns_in_fed_state.overlay(
        voroni_buffer_intersect, how="intersection"
    ).set_crs(epsg=4326)

    # calculate area fraction of individual site over total area within
    # the same federal state
    potential_areas["area_fraction"] = potential_areas.to_crs(
        epsg=25832
    ).area / potential_areas.groupby("gen")["shape_star"].transform("sum")

    # write information to saltcavern data
    potential_areas.to_crs(epsg=4326).to_postgis(
        targets["storage_potential"]["table"],
        db.engine(),
        schema=targets["storage_potential"]["schema"],
        index=True,
        if_exists="replace",
        dtype={"geometry": Geometry()},
    )