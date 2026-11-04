"""
The central module containing code to create CH4 and H2 voronoi polygons

"""

from geoalchemy2.types import Geometry
from sqlalchemy import BigInteger, Column, Text
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd

from egon.data import config, db
from egon.data.datasets import (
    Dataset,
    DatasetSources,
    DatasetTargets,
    wrapped_partial,
)
from egon.data.datasets.generate_voronoi import get_voronoi_geodataframe


class GasAreaseGon2035(Dataset):
    """
    Create the gas voronoi table and the gas voronoi areas for eGon2035

    Create the gas voronoi table by executing the function :py:func:`create_gas_voronoi_table`
    and inserts the gas voronoi areas for the eGon2035 scenario with the
    :py:func:`voronoi_egon2035` function.

    *Dependencies*
      * :py:class:`EtragoSetup <egon.data.datasets.etrago_setup.EtragoSetup>`
      * :py:class:`HydrogenBusEtrago <egon.data.datasets.hydrogen_etrago.HydrogenBusEtrago>`
      * :py:class:`Vg250 <egon.data.datasets.vg250.Vg250>`
      * :py:class:`GasNodesAndPipes <egon.data.datasets.gas_grid.GasNodesAndPipes>`

    *Resulting tables*
      * :py:class:`EgonPfHvGasVoronoi <EgonPfHvGasVoronoi>`

    """  # noqa: E501

    #:
    name: str = "GasAreaseGon2035"
    #:
    version: str = "0.0.5"

    # Dataset sources (input tables)
    sources = DatasetSources(
        tables={
            "vg250_sta_union": "boundaries.vg250_sta_union",
            "egon_etrago_bus": "grid.egon_etrago_bus",
        }
    )

    targets = DatasetTargets(
        tables={
            "ch4_voronoi": "grid.egon_gas_voronoi",
        }
    )

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(create_gas_voronoi_table, voronoi_egon2035),
        )


class GasAreaseGon100RE(Dataset):
    """Insert the gas voronoi areas for eGon100RE

    Inserts the gas voronoi areas for the eGon100RE scenario with the
    :py:func:`voronoi_egon100RE` function.

    *Dependencies*
      * :py:class:
        `EtragoSetup <egon.data.datasets.etrago_setup.EtragoSetup>`
      * :py:class:
        `HydrogenBusEtrago <egon.data.datasets.hydrogen_etrago.HydrogenBusEtrago>`
      * :py:class:
        `HydrogenGridEtrago <egon.data.datasets.hydrogen_etrago.HydrogenGridEtrago>`
      * :py:class:
        `Vg250 <egon.data.datasets.vg250.Vg250>`
      * :py:class:
        `GasNodesAndPipes <egon.data.datasets.gas_grid.GasNodesAndPipes>`
      * :py:class:
        `GasAreaseGon2035 <GasAreaseGon2035>`

    *Resulting tables*
      * :py:class:`EgonPfHvGasVoronoi <EgonPfHvGasVoronoi>`

    """  # noqa: E501

    #:
    name: str = "GasAreaseGon100RE"
    #:
    version: str = "0.0.5"

    # Same sources as GasAreaseGon2035
    sources = DatasetSources(
        tables={
            "vg250_sta_union": "boundaries.vg250_sta_union",
            "egon_etrago_bus": "grid.egon_etrago_bus",
        }
    )

    targets = DatasetTargets(
        tables={
            "ch4_voronoi": "grid.egon_gas_voronoi",
        }
    )

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(voronoi_egon100RE,),
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
    Create gas voronoi table
    """
    engine = db.engine()
    EgonPfHvGasVoronoi.__table__.drop(bind=engine, checkfirst=True)
    EgonPfHvGasVoronoi.__table__.create(bind=engine, checkfirst=True)


def voronoi_egon2035():
    """
    Insert the gas voronoi polygons in eGon2035 into the database

    This function insert the voronoi polygons for CH4, H2_grid and
    H2_saltcavern into the database for the scenario eGon2035, making
    use of the function :py:func:`create_voronoi`.

    """
    for carrier in ["CH4", "H2", "H2_saltcavern"]:
        create_voronoi("eGon2035", carrier)


def voronoi_egon100RE():
    """
    Insert the gas voronoi polygons in eGon100RE into the database

    This function insert the voronoi polygons for CH4, H2_grid and
    H2_saltcavern into the database for the scenario eGon100RE, making
    use of the function :py:func:`create_voronoi`.

    """
    for carrier in ["CH4", "H2", "H2_saltcavern"]:
        create_voronoi("eGon100RE", carrier)


def voronoi_status(scn_name):
    """
    Create voronoi polygons for all gas carriers in status_x scenario
    """
    for carrier in ["CH4"]:
        create_voronoi(scn_name, carrier)


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
    targets = GasAreaseGon2035.targets
    sources = GasAreaseGon2035.sources
    engine = db.engine()

    table_exist = (
        len(
            pd.read_sql(
                f"""
    SELECT *
    FROM information_schema.tables
    WHERE table_schema = '{targets.get_table_schema("ch4_voronoi")}'
        AND table_name = '{targets.get_table_name("ch4_voronoi")}'
    LIMIT 1;
        """,
                engine,
            )
        )
        > 0
    )

    if not table_exist:
        create_gas_voronoi_table()

    boundary = db.select_geodataframe(
        f"""
            SELECT id, geometry
            FROM {sources.tables["vg250_sta_union"]};
        """,
        geom_col="geometry",
    ).to_crs(epsg=4326)

    if isinstance(carrier, str):
        if carrier == "H2":
            carriers = ["H2", "H2_grid"]
        else:
            carriers = [carrier]
    else:
        carriers = carrier

    carrier_strings = "', '".join(carriers)

    db.execute_sql(
        f"""
        DELETE FROM {targets.tables["ch4_voronoi"]}
        WHERE "carrier" IN ('{carrier_strings}') and "scn_name" = '{scn_name}';
        """
    )

    buses = db.select_geodataframe(
        f"""
            SELECT bus_id, geom
            FROM {sources.tables["egon_etrago_bus"]}
            WHERE scn_name = '{scn_name}'
            AND country = 'DE'
            AND carrier IN ('{carrier_strings}');
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
        targets.get_table_name("ch4_voronoi"),
        engine,
        schema=targets.get_table_schema("ch4_voronoi"),
        index=False,
        if_exists="append",
        dtype={"geom": Geometry},
    )


class GasAreas(Dataset):
    """Create the gas voronoi table and the gas voronoi areas

    *Dependencies*
      * :py:class:`EtragoSetup <egon.data.datasets.etrago_setup.EtragoSetup>`
      * :py:class:`HydrogenBusEtrago <egon.data.datasets.hydrogen_etrago.HydrogenBusEtrago>`
      * :py:class:`Vg250 <egon.data.datasets.vg250.Vg250>`
      * :py:class:`GasNodesAndPipes <egon.data.datasets.gas_grid.GasNodesAndPipes>`

    *Resulting tables*
      * :py:class:`EgonPfHvGasVoronoi <EgonPfHvGasVoronoi>`

    """  # noqa: E501

    #:
    name: str = "GasAreas"
    #:
    version: str = "0.0.5"

    tasks = (create_gas_voronoi_table,)
    extra_dependencies = ()

    if "eGon2035" in config.settings()["egon-data"]["--scenarios"]:
        tasks = tasks + (voronoi_egon2035,)

    if "eGon100RE" in config.settings()["egon-data"]["--scenarios"]:
        tasks = tasks + (voronoi_egon100RE,)
        extra_dependencies = extra_dependencies + ("insert_h2_grid",)

    for scn_name in config.settings()["egon-data"]["--scenarios"]:
        if "status" in scn_name:
            tasks += (
                wrapped_partial(
                    voronoi_status,
                    scn_name=scn_name,
                    postfix=f"_{scn_name[-4:]}",
                ),
            )

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=self.tasks,
        )
