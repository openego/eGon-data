"""
The central module containing code to create CH4 and H2 voronoi polygons

"""
from geoalchemy2.types import Geometry
from sqlalchemy import BigInteger, Column, Text
from sqlalchemy.ext.declarative import declarative_base

from egon.data import db
from egon.data.datasets import Dataset
from egon.data.datasets.generate_voronoi import get_voronoi_geodataframe


class GasAreaseGon2035(Dataset):
    """Create the gas voronoi table and the gas voronoi areas for eGon2035

    *Dependencies*
      * :py:class:`EtragoSetup <egon.data.datasets.etrago_setup.EtragoSetup>`
      * :py:class:`HydrogenBusEtrago <egon.data.datasets.hydrogen_etrago.HydrogenBusEtrago>`
      * :py:class:`Vg250 <egon.data.datasets.vg250.Vg250>`
      * :py:class:`GasNodesAndPipes <egon.data.datasets.gas_grid.GasNodesAndPipes>`

    *Resulting tables*
      * :py:class:`EgonPfHvGasVoronoi <EgonPfHvGasVoronoi>`

    """

    #:
    name: str = "GasAreaseGon2035"
    #:
    version: str = "0.0.2"

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(create_gas_voronoi_table, voronoi_egon2035),
        )


class GasAreaseGon100RE(Dataset):
    """Create the gas voronoi table and the gas voronoi areas for eGon100RE

    *Dependencies*
      * :py:class:`EtragoSetup <egon.data.datasets.etrago_setup.EtragoSetup>`
      * :py:class:`HydrogenBusEtrago <egon.data.datasets.hydrogen_etrago.HydrogenBusEtrago>`
      * :py:class:`HydrogenGridEtrago <egon.data.datasets.hydrogen_etrago.HydrogenGridEtrago>`
      * :py:class:`Vg250 <egon.data.datasets.vg250.Vg250>`
      * :py:class:`GasNodesAndPipes <egon.data.datasets.gas_grid.GasNodesAndPipes>`
      * :py:class:`GasAreaseGon2035 <GasAreaseGon2035>`

    *Resulting tables*
      * :py:class:`EgonPfHvGasVoronoi <EgonPfHvGasVoronoi>`

    """

    #:
    name: str = "GasAreaseGon100RE"
    #:
    version: str = "0.0.1"

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(voronoi_egon100RE),
        )


class GasAreasstatus2019(Dataset):
    """Create the gas voronoi table and the gas voronoi areas for status2019

    *Dependencies*
      * :py:class:`EtragoSetup <egon.data.datasets.etrago_setup.EtragoSetup>`
      * :py:class:`Vg250 <egon.data.datasets.vg250.Vg250>`
      * :py:class:`GasNodesAndPipes <egon.data.datasets.gas_grid.GasNodesAndPipes>`

    *Resulting tables*
      * :py:class:`EgonPfHvGasVoronoi <EgonPfHvGasVoronoi>`

    """

    #:
    name: str = "GasAreasstatus2019"
    #:
    version: str = "0.0.1"

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(create_gas_voronoi_table, voronoi_status2019),
        )


Base = declarative_base()


class EgonPfHvGasVoronoi(Base):
    """
    Class definition of table grid.egon_gas_voronoi
    """

    __tablename__ = "egon_gas_voronoi"
    __table_args__ = {"schema": "grid"}

    #: Name of the scenario
    scn_name = Column(Text, primary_key=True, nullable=False)
    #: Bus of the corresponding area
    bus_id = Column(BigInteger, primary_key=True, nullable=False)
    #: Gas carrier of the voronoi area ("CH4", "H2" or "H2_saltcavern")
    carrier = Column(Text)
    #: Geometry of the corresponding area
    geom = Column(Geometry("GEOMETRY", 4326))


def create_gas_voronoi_table():
    """
    Create voronoi gas voronoi table
    """
    engine = db.engine()
    EgonPfHvGasVoronoi.__table__.drop(bind=engine, checkfirst=True)
    EgonPfHvGasVoronoi.__table__.create(bind=engine, checkfirst=True)


def voronoi_egon2035():
    """
    Create voronoi polygons for all gas carriers in eGon2035 scenario
    """
    for carrier in ["CH4", "H2", "H2_saltcavern"]:
        create_voronoi("eGon2035", carrier)


def voronoi_egon100RE():
    """
    Create voronoi polygons for all gas carriers in eGon100RE scenario
    """
    for carrier in ["CH4", "H2", "H2_saltcavern"]:
        create_voronoi("eGon100RE", carrier)


def voronoi_status2019():
    """
    Create voronoi polygons for all gas carriers in status2019 scenario
    """
    for carrier in ["CH4"]:
        create_voronoi("status2019", carrier)


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

    # generate voronois
    # For some scenarios it is defined that there is only 1 bus (e.g. gas). It
    # means that there will be just 1 voronoi covering the entire german
    # territory. For other scenarios with more buses, voronois are calculated.
    if len(buses) == 1:
        gdf = buses.copy()
        gdf.at[0, "geom"] = boundary.at[0, "geometry"]
    else:
        buses["x"] = buses.geometry.x
        buses["y"] = buses.geometry.y
        gdf = get_voronoi_geodataframe(buses, boundary.geometry.iloc[0])
        gdf.rename_geometry("geom", inplace=True)
        gdf.drop(columns=["id"], inplace=True)

    # set scn_name and carrier
    gdf["scn_name"] = scn_name
    gdf["carrier"] = carrier

    # Insert data to db
    gdf.set_crs(epsg=4326).to_postgis(
        f"egon_gas_voronoi",
        engine,
        schema="grid",
        index=False,
        if_exists="append",
        dtype={"geom": Geometry},
    )
