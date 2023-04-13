"""The central module containing all code dealing with fixing ehv subnetworks
"""
import geopandas as gpd
import numpy as np
import pandas as pd

from egon.data import db
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


def select_bus_id(x, y, v_nom, scn_name, carrier):

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

    if bus != None:
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
    bus0 = select_bus_id(x0, y0, v_nom, scn_name, carrier="AC")
    bus1 = select_bus_id(x1, y1, v_nom, scn_name, carrier="AC")

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

    if (bus0 != None) and (bus1 != None):
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

    bus0 = select_bus_id(x, y, v_nom0, scn_name, carrier="AC")
    bus1 = select_bus_id(x, y, v_nom1, scn_name, carrier="AC")

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

    if (bus0 != None) and (bus1 != None):
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
        10.640952461335745,  # Umspannwerk Lübeck
        53.91944427801032,
        220,
        scn_name,
        3,
    )

    # Missing 220kV line from Audorf to Kiel
    add_line(
        # Audorf
        9.726992766257577,
        54.291420962253234,
        # Kiel
        9.9572075,
        54.321589,
        220,
        scn_name,
        6,
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

        # Missing 380kV line near Elsfleth
        add_line(
            # Line
            8.419326700000001,
            53.229867000000006,
            # Schaltanlage Elsfleth/West
            8.402976949446648,
            53.2371468322213,
            380,
            scn_name,
            6,
        )

        # Missing 380kV line near Magdala
        add_line(
            # Line north south
            11.4298432,
            50.9117467,
            # Line east
            11.4295305,
            50.9115176,
            380,
            scn_name,
            3,
        )

        # Missing 220kV line near Frimmersdorf
        add_line(
            # Line west
            6.585418000000001,
            51.0495723,
            # Line east
            6.5867616,
            51.0520915,
            220,
            scn_name,
            6,
        )

        # Missing 220kV line from Wolmirstedt to Stendal
        add_line(
            # Wolmirstedt
            11.637225336209951,
            52.26707328151311,
            # Stendal
            11.7689,
            52.505533,
            220,
            scn_name,
            6,
        )

        # Plattling
        # Update way for osmTGmod in
        # 'LINESTRING (12.85328076018362 48.76616932172957,
        # 12.85221826521118 48.76597882857125,
        # 12.85092755963579 48.76451816626182,
        # 12.85081583430311 48.76336597271223,
        # 12.85089191559093 48.76309793961921,
        # 12.85171674549663 48.76313124988151,
        # 12.85233496021983 48.76290980724934,
        # 12.85257485139349 48.76326650768988,
        # 12.85238077788078 48.76354965879587,
        # 12.85335698387775 48.76399030383004,
        # 12.85444925633996 48.76422235417385,
        # 12.853289544662 48.76616304929393)'

        # Lamspringe 380kV lines
        drop_line(
            9.988215035677026,
            51.954230057487926,
            9.991477300000001,
            51.939711,
            380,
            scn_name,
        )

        drop_line(
            9.995589,
            51.969716000000005,
            9.988215035677026,
            51.954230057487926,
            380,
            scn_name,
        )

        drop_line(
            9.982829,
            51.985980000000005,
            9.995589,
            51.969716000000005,
            380,
            scn_name,
        )

        drop_line(
            10.004865,
            51.999120000000005,
            9.982829,
            51.985980000000005,
            380,
            scn_name,
        )

        drop_line(
            10.174395,
            52.036448,
            9.988215035677026,
            51.954230057487926,
            380,
            scn_name,
        )

        drop_line(
            10.195144702845797,
            52.079851837273964,
            10.174395,
            52.036448,
            380,
            scn_name,
        )

        drop_trafo(9.988215035677026, 51.954230057487926, 110, 380, scn_name)

        drop_bus(9.988215035677026, 51.954230057487926, 380, scn_name)
        drop_bus(9.991477300000001, 51.939711, 380, scn_name)
        drop_bus(9.995589, 51.969716000000005, 380, scn_name)
        drop_bus(9.982829, 51.985980000000005, 380, scn_name)
        drop_bus(10.174395, 52.036448, 380, scn_name)
        drop_bus(10.195144702845797, 52.079851837273964, 380, scn_name)

        drop_bus(10.004865, 51.999120000000005, 380, scn_name)

        # Umspannwerk Vieselbach
        # delete isolated bus and trafo
        drop_trafo(11.121774798935334, 51.00038603925895, 220, 380, scn_name)
        drop_bus(11.121774798935334, 51.00038603925895, 380, scn_name)


        # Umspannwerk Waldlaubersheim
        # delete isolated bus and trafo
        drop_trafo(7.815993836091339, 49.92211102637183, 110, 380, scn_name)
        drop_bus(7.815993836091339, 49.92211102637183, 380, scn_name)



def run():
    fix_subnetworks("eGon2035")
    fix_subnetworks("eGon100RE")
    fix_subnetworks("status2019")
