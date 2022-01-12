"""The central module containing code to create CH4 and H2 voronoi polygons

"""
from geoalchemy2.types import Geometry

from egon.data import db
from egon.data.datasets import Dataset
from egon.data.datasets.generate_voronoi import get_voronoi_geodataframe


class GasAreas(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="GasAreas",
            version="0.0.1",
            dependencies=dependencies,
            tasks=(create_voronoi),
        )


def create_voronoi(scn_name="eGon2035"):
    """
    Creates voronoi polygons for all gas carriers

    Returns
    -------
    None.
    """
    boundary = db.select_geodataframe(
        f"""
            SELECT id, geometry
            FROM boundaries.vg250_sta_union;
        """,
        geom_col="geometry",
    ).to_crs(epsg=4326)

    engine = db.engine()

    for carrier in ["CH4", "H2_grid", "H2_saltcavern"]:
        db.execute_sql(
            f"""
            DROP TABLE IF EXISTS grid.egon_voronoi_{carrier.lower()} CASCADE;
            """
        )

        buses = db.select_geodataframe(
            f"""
                SELECT bus_id, geom
                FROM grid.egon_etrago_bus
                WHERE scn_name = '{scn_name}'
                AND country = 'DE'
                AND carrier = '{carrier}';
            """,
        ).to_crs(epsg=4326)

        buses["x"] = buses.geometry.x
        buses["y"] = buses.geometry.y
        # generate voronois
        gdf = get_voronoi_geodataframe(buses, boundary.geometry.iloc[0])
        # set scn_name
        gdf["scn_name"] = scn_name

        # Insert data to db
        gdf.set_crs(epsg=4326).to_postgis(
            f"egon_voronoi_{carrier.lower()}",
            engine,
            schema="grid",
            index=False,
            if_exists="append",
            dtype={"geometry": Geometry},
        )
