"""
The central module containing code to create CH4 and H2 voronoi polygons

"""
import datetime
import json
import pandas as pd

from geoalchemy2.types import Geometry
from sqlalchemy import BigInteger, Column, Text
from sqlalchemy.ext.declarative import declarative_base

from egon.data import db, config
from egon.data.datasets import Dataset, wrapped_partial
from egon.data.datasets.generate_voronoi import get_voronoi_geodataframe
from egon.data.metadata import (
    context,
    contributors,
    license_egon_data_odbl,
    meta_metadata,
    sources,
)


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
    """Insert the gas voronoi areas for eGon100RE

    Inserts the gas voronoi areas for the eGon100RE scenario with the
    :py:func:`voronoi_egon100RE` function.

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


Base = declarative_base()


class EgonPfHvGasVoronoi(Base):
    """
    Class definition of table grid.egon_gas_voronoi
    """


    source_list = [
        sources()["openstreetmap"],
        sources()["SciGRID_gas"],
        sources()["bgr_inspeeds_data_bundle"],
    ]
    meta = {
        "name": "grid.egon_gas_voronoi",
        "title": "Gas voronoi areas",
        "id": "WILL_BE_SET_AT_PUBLICATION",
        "description": "H2 and CH4 voronoi cells",
        "language": ["en-EN"],
        "publicationDate": datetime.date.today().isoformat(),
        "context": context(),
        "spatial": {
            "location": None,
            "extent": "Germany",
            "resolution": None,
        },
        "sources": source_list,
        "licenses": [license_egon_data_odbl()],
        "contributors": contributors(["fw"]),
        "resources": [
            {
                "profile": "tabular-data-resource",
                "name": "grid.egon_gas_voronoi",
                "path": None,
                "format": "PostgreSQL",
                "encoding": "UTF-8",
                "schema": {
                    "fields": [
                        {
                            "name": "scn_name",
                            "description": "Name of the scenario",
                            "type": "str",
                            "unit": None,
                        },
                        {
                            "name": "bus_id",
                            "description": "Unique identifier",
                            "type": "integer",
                            "unit": None,
                        },
                        {
                            "name": "carrier",
                            "description": "Carrier of the voronoi cell",
                            "type": "str",
                            "unit": None,
                        },
                        {
                            "name": "geom",
                            "description": "Voronoi cell geometry",
                            "type": "Geometry(Polygon, 4326)",
                            "unit": None,
                        },
                    ],
                    "primaryKey": ["scn_name", "bus_id"],
                    "foreignKeys": [],
                },
                "dialect": {"delimiter": None, "decimalSeparator": "."},
            }
        ],
        "metaMetadata": meta_metadata(),
    }
    # Create json dump
    meta_json = "'" + json.dumps(meta, indent=4, ensure_ascii=False) + "'"

    __tablename__ = "egon_gas_voronoi"
    __table_args__ = {
        "schema": "grid",
        "comment": meta_json,
    }

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

    engine = db.engine()

    table_exist = (
        len(
            pd.read_sql(
                """
    SELECT *
    FROM information_schema.tables
    WHERE table_schema = 'grid'
        AND table_name = 'egon_gas_voronoi'
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
        """
            SELECT id, geometry
            FROM boundaries.vg250_sta_union;
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
        DELETE FROM grid.egon_gas_voronoi
        WHERE "carrier" IN ('{carrier_strings}') and "scn_name" = '{scn_name}';
        """
    )

    buses = db.select_geodataframe(
        f"""
            SELECT bus_id, geom
            FROM grid.egon_etrago_bus
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
        f"egon_gas_voronoi",
        engine,
        schema="grid",
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

    """

    #:
    name: str = "GasAreas"
    #:
    version: str = "0.0.3"

    tasks = (create_gas_voronoi_table,)
    extra_dependencies = ()

    if "eGon2035" in config.settings()["egon-data"]["--scenarios"]:
        tasks = tasks + (voronoi_egon2035,)

    if "eGon100RE" in config.settings()["egon-data"]["--scenarios"]:
        tasks = tasks + (voronoi_egon100RE,)
        extra_dependencies = extra_dependencies + ("insert_h2_grid",)

    for scn_name in config.settings()["egon-data"]["--scenarios"]:
        if "status" in scn_name:
            tasks += (wrapped_partial(
                voronoi_status, scn_name=scn_name, postfix=f"_{scn_name[-4:]}"
            ),)

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=self.tasks,
        )
