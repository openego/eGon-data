"""The central module containing code to create CH4 and H2 voronoi polygons

"""
import datetime
import json

from geoalchemy2.types import Geometry
from sqlalchemy import BigInteger, Column, Text
from sqlalchemy.ext.declarative import declarative_base

from egon.data import db
from egon.data.datasets import Dataset
from egon.data.datasets.generate_voronoi import get_voronoi_geodataframe
from egon.data.metadata import (
    context,
    contributors,
    license_ccby,
    meta_metadata,
)


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
        "sources": None,
        "licenses": [license_ccby("eGon development team")],
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
