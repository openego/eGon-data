"""The central module containing code to create CH4 and H2 voronoi polygons

"""
from geoalchemy2.types import Geometry
from sqlalchemy import BigInteger, Column, Text
from sqlalchemy.ext.declarative import declarative_base

from egon.data import db
from egon.data.datasets import Dataset
from egon.data.datasets.generate_voronoi import get_voronoi_geodataframe


class GasAreaseGon2035(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="GasAreaseGon2035",
            version="0.0.2",
            dependencies=dependencies,
            tasks=(create_gas_voronoi_table, voronoi_egon2035),
        )


class GasAreaseGon100RE(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="GasAreaseGon100RE",
            version="0.0.1",
            dependencies=dependencies,
            tasks=(voronoi_egon100RE),
        )


Base = declarative_base()


class EgonPfHvGasVoronoi(Base):
    __tablename__ = "egon_gas_voronoi"
    __table_args__ = {"schema": "grid"}

    scn_name = Column(Text, primary_key=True, nullable=False)
    bus_id = Column(BigInteger, primary_key=True, nullable=False)
    carrier = Column(Text)
    geom = Column(Geometry("GEOMETRY", 4326))


def create_gas_voronoi_table():
    engine = db.engine()
    EgonPfHvGasVoronoi.__table__.drop(bind=engine, checkfirst=True)
    EgonPfHvGasVoronoi.__table__.create(bind=engine, checkfirst=True)


def voronoi_egon2035():
    """
    Create voronoi polygons for all gas carriers in eGon2035 scenario
    """
    for carrier in ["CH4", "H2_grid", "H2_saltcavern"]:
        create_voronoi("eGon2035", carrier)


def voronoi_egon100RE():
    """
    Create voronoi polygons for all gas carriers in eGon100RE scenario
    """
    for carrier in ["CH4", "H2_grid", "H2_saltcavern"]:
        create_voronoi("eGon100RE", carrier)


def create_voronoi(scn_name, carrier):
    """
    Create voronoi polygons for specified carrier in specified scenario.

    Parameters
    ----------
    scn_name : str
        Name of the scenario
    carrier : str
        Name of the carrier
    """
    boundary = db.select_geodataframe(
        f"""
            SELECT id, geometry
            FROM boundaries.vg250_sta_union;
        """,
        geom_col="geometry",
    ).to_crs(epsg=4326)

    engine = db.engine()

    db.execute_sql(
        f"""
        DELETE FROM grid.egon_gas_voronoi
        WHERE "carrier" = '{carrier}' and "scn_name" = '{scn_name}';
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

    if len(buses) == 0:
        return

    buses["x"] = buses.geometry.x
    buses["y"] = buses.geometry.y
    # generate voronois
    gdf = get_voronoi_geodataframe(buses, boundary.geometry.iloc[0])
    # set scn_name
    gdf["scn_name"] = scn_name
    gdf["carrier"] = carrier

    gdf.rename_geometry("geom", inplace=True)
    gdf.drop(columns=["id"], inplace=True)
    # Insert data to db
    gdf.set_crs(epsg=4326).to_postgis(
        f"egon_gas_voronoi",
        engine,
        schema="grid",
        index=False,
        if_exists="append",
        dtype={"geom": Geometry},
    )
