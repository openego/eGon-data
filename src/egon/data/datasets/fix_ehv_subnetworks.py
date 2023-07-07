"""The central module containing all code dealing with fixing ehv subnetworks
"""
import geopandas as gpd
import numpy as np
import pandas as pd

from egon.data import db, config
from egon.data.config import settings
from egon.data.datasets import Dataset
from egon.data.datasets.etrago_setup import link_geom_from_buses


class FixEhvSubnetworks(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="FixEhvSubnetworks",
            version="0.0.2",
            dependencies=dependencies,
            tasks=run,
        )


def select_bus_id(x, y, v_nom, scn_name, carrier, find_closest=False):
    bus_id = db.select_dataframe(
        f"""
        SELECT bus_id
        FROM grid.egon_etrago_bus
        WHERE x = {x}
        AND y = {y}
        AND v_nom = {v_nom}
        AND scn_name = '{scn_name}'
        AND carrier = '{carrier}'
        """
    )

    if bus_id.empty:
        if find_closest:
            bus_id = db.select_dataframe(
            f"""
            SELECT bus_id, st_distance(geom, 'SRID=4326;POINT({x} {y})'::geometry)
            FROM grid.egon_etrago_bus
            WHERE v_nom = {v_nom}
            AND scn_name = '{scn_name}'
            AND carrier = '{carrier}'
            ORDER BY st_distance
            Limit 1
            """)
            return bus_id.bus_id[0]
        else:
            return None
    else:
        return bus_id.bus_id[0]


def add_bus(x, y, v_nom, scn_name):
    df = pd.DataFrame(
        index=[db.next_etrago_id("bus")],
        data={
            "scn_name": scn_name,
            "v_nom": v_nom,
            "x": x,
            "y": y,
            "carrier": "AC",
        },
    )
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df.x, df.y, crs=4326)
    ).rename_geometry("geom")

    gdf.index.name = "bus_id"

    gdf.reset_index().to_postgis(
        "egon_etrago_bus", schema="grid", con=db.engine(), if_exists="append"
    )


def drop_bus(x, y, v_nom, scn_name):
    bus = select_bus_id(x, y, v_nom, scn_name, carrier="AC")

    if bus is not None:
        db.execute_sql(
            f"""
            DELETE FROM grid.egon_etrago_bus
            WHERE
            scn_name = '{scn_name}'
            AND bus_id = {bus}
            AND v_nom = {v_nom}
            AND carrier = 'AC'
            """
        )


def add_line(x0, y0, x1, y1, v_nom, scn_name, cables):
    bus0 = select_bus_id(x0, y0, v_nom, scn_name, carrier="AC", find_closest= True)
    bus1 = select_bus_id(x1, y1, v_nom, scn_name, carrier="AC", find_closest= True)

    df = pd.DataFrame(
        index=[db.next_etrago_id("line")],
        data={
            "bus0": bus0,
            "bus1": bus1,
            "scn_name": scn_name,
            "v_nom": v_nom,
            "cables": cables,
            "carrier": "AC",
        },
    )

    gdf = link_geom_from_buses(df, scn_name)

    gdf["length"] = gdf.to_crs(3035).topo.length.mul(1e-3)

    if v_nom == 220:
        s_nom = 520
        x_per_km = 0.001 * 2 * np.pi * 50

    elif v_nom == 380:
        s_nom = 1790
        x_per_km = 0.0008 * 2 * np.pi * 50

    gdf["s_nom"] = s_nom * gdf["cables"] / 3

    gdf["x"] = (x_per_km * gdf["length"]) / (gdf["cables"] / 3)

    gdf.index.name = "line_id"

    gdf.reset_index().to_postgis(
        "egon_etrago_line", schema="grid", con=db.engine(), if_exists="append"
    )


def drop_line(x0, y0, x1, y1, v_nom, scn_name):
    bus0 = select_bus_id(x0, y0, v_nom, scn_name, carrier="AC")
    bus1 = select_bus_id(x1, y1, v_nom, scn_name, carrier="AC")

    if (bus0 is not None) and (bus1 is not None):
        db.execute_sql(
            f"""
            DELETE FROM grid.egon_etrago_line
            WHERE
            scn_name = '{scn_name}'
            AND bus0 = {bus0}
            AND bus1 = {bus1}
            AND v_nom = {v_nom}
            """
        )


def add_trafo(x, y, v_nom0, v_nom1, scn_name, n=1):
    bus0 = select_bus_id(x, y, v_nom0, scn_name, carrier="AC", find_closest= True)
    bus1 = select_bus_id(x, y, v_nom1, scn_name, carrier="AC", find_closest= True)

    df = pd.DataFrame(
        index=[db.next_etrago_id("line")],
        data={
            "bus0": bus0,
            "bus1": bus1,
            "scn_name": scn_name,
        },
    )

    gdf = link_geom_from_buses(df, scn_name)

    if (v_nom0 == 220) & (v_nom1 == 380):
        s_nom = 600
        x = 0.0002

    gdf["s_nom"] = s_nom * n

    gdf["x"] = x / n

    gdf.index.name = "trafo_id"

    gdf.reset_index().to_postgis(
        "egon_etrago_transformer",
        schema="grid",
        con=db.engine(),
        if_exists="append",
    )


def drop_trafo(x, y, v_nom0, v_nom1, scn_name):
    bus0 = select_bus_id(x, y, v_nom0, scn_name, carrier="AC")
    bus1 = select_bus_id(x, y, v_nom1, scn_name, carrier="AC")

    if (bus0 is not None) and (bus1 is not None):
        db.execute_sql(
            f"""
            DELETE FROM grid.egon_etrago_transformer
            WHERE
            scn_name = '{scn_name}'
            AND bus0 = {bus0}
            AND bus1 = {bus1}
            """
        )


def fix_subnetworks(scn_name):
    # Missing 220kV line to Lübeck Siems
    # add 220kV bus at substation Lübeck Siems
    add_bus(10.760835327266625, 53.90974536547805, 220, scn_name)
    # add 220/380kV transformer at substation Lübeck Siems
    add_trafo(10.760835327266625, 53.90974536547805, 220, 380, scn_name)

    # add 220kV line from Umspannwerk Lübeck to Lübeck Siems
    add_line(
        10.760835327266625,  # Lübeck Siems
        53.90974536547805,
        10.641467804496818,  # Umspannwerk Lübeck
        53.91916269128779,
        220,
        scn_name,
        3,
    )

    if settings()["egon-data"]["--dataset-boundary"] == "Everything":
        # Missing line from USW Uchtelfangen to 'Kraftwerk Weiher'
        add_line(
            7.032657738999395,  # Kraftwerk Weiher
            49.33473737285781,
            6.996454674906,  # Umspannwerk Uchtelfangen
            49.3754149606116,
            220,
            scn_name,
            6,
        )
        if (
            select_bus_id(
                12.85381530378627, 48.764209444817745, 380, scn_name, "AC"
            )
            != None
        ):
            # Missing line from Umspannwerk Plottling to Gänsdorf UW
            add_line(
                12.85381530378627,  # Umspannwerk Plottling
                48.764209444817745,
                12.769768646403532,  # Gänsdorf UW
                48.80533685376445,
                380,
                scn_name,
                3,
            )

        # Missing line inside Ottstedt
        add_line(
            11.4295305,  # Ottstedt
            50.9115176,
            11.4299277,  # Ottstedt
            50.911449600000005,
            380,
            scn_name,
            3,
        )


def run():
    for scenario in config.settings()["egon-data"]["--scenarios"]:
        fix_subnetworks(scenario)
