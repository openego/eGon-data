
"""
The central module containing all code dealing with chp.
"""

from egon.data import db, config
from egon.data.datasets import Dataset
from egon.data.datasets.chp.match_nep import insert_large_chp
from egon.data.datasets.chp.small_chp import existing_chp_smaller_10mw
from sqlalchemy import Column, String, Float, Integer, Sequence
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import JSONB
from geoalchemy2 import Geometry
Base = declarative_base()

class EgonChp(Base):
    __tablename__ = "egon_chp"
    __table_args__ = {"schema": "supply"}
    id = Column(Integer, Sequence("chp_seq"), primary_key=True)
    sources = Column(JSONB)
    source_id = Column(JSONB)
    carrier = Column(String)
    use_case = Column(String)
    el_capacity = Column(Float)
    th_capacity = Column(Float)
    electrical_bus_id = Column(Integer)
    heat_bus_id = Column(Integer)
    gas_bus_id = Column(Integer)
    voltage_level = Column(Integer)
    scenario = Column(String)
    geom = Column(Geometry("POINT", 4326))

class Chp(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="Chp",
            version="0.0.0.dev",
            dependencies=dependencies,
            tasks=(create_tables, insert_chp_egon2035,
                   assign_heat_bus),
        )

def create_tables():
    """Create tables for chp data
    Returns
    -------
    None.
    """

    db.execute_sql("CREATE SCHEMA IF NOT EXISTS supply;")
    engine = db.engine()
    EgonChp.__table__.drop(bind=engine, checkfirst=True)
    EgonChp.__table__.create(bind=engine, checkfirst=True)

def nearest(row, geom_union, df1, df2,
            geom1_col='geometry', geom2_col='geometry', src_column=None):
    """Find the nearest point and return the corresponding value from specified column."""
    from shapely.ops import nearest_points
    # Find the geometry that is closest
    nearest = df2[geom2_col] == nearest_points(row[geom1_col], geom_union)[1]
    # Get the corresponding value from df2 (matching is based on the geometry)
    value = df2[nearest][src_column].values[0]
    return value

def assign_heat_bus(scenario='eGon2035'):
    # Select CHP with use_case = 'district_heating'
    chp = db.select_geodataframe(
        f"""
        SELECT * FROM
        supply.egon_chp
        WHERE scenario = '{scenario}'
        AND use_case = 'district_heating'
        """,
        index_col='id',
        epsg=4326)

    # Select district heating nodes
    district_heating = db.select_geodataframe(
        """
        SELECT bus_id, geom
        FROM grid.egon_pf_hv_bus
        WHERE scn_name = 'eGon2035'
        AND carrier = 'central_heat'
        """,
        epsg=4326)

    # Assign district heating area_id to district_heating_chp
    # According to nearest centroid of district heating area
    chp['heat_bus_id'] = chp.apply(
        nearest, geom_union=district_heating.centroid.unary_union,
        df1=chp, df2=district_heating, geom1_col='geom', geom2_col='geom',
        src_column='bus_id', axis=1)

    # Drop district heating CHP without heat_bus_id
    db.execute_sql(
        f"""
        DELETE FROM supply.egon_chp
        WHERE scenario = '{scenario}'
        AND use_case = 'district_heating'
        """)

    # Insert district heating CHP with heat_bus_id
    session = sessionmaker(bind=db.engine())()
    for i, row in chp.iterrows():
        entry = EgonChp(
                id = i,
                sources=row.sources,
                source_id=row.source_id,
                carrier=row.carrier,
                el_capacity=row.el_capacity,
                th_capacity= row.th_capacity,
                electrical_bus_id = row.electrical_bus_id,
                gas_bus_id = row.gas_bus_id,
                heat_bus_id = row.heat_bus_id,
                use_case=row.use_case,
                scenario=scenario,
                geom=f"SRID=4326;POINT({row.geom.x} {row.geom.y})",
            )
        session.add(entry)
    session.commit()

def insert_chp_egon2035():
    """ Insert large CHP plants for eGon2035 considering NEP and MaStR

    Returns
    -------
    None.

    """

    create_tables()

    target = config.datasets()["chp_location"]["targets"]["power_plants"]

    # Insert large CHPs based on NEP's list of conventional power plants
    MaStR_konv = insert_large_chp(target, EgonChp)

    # Insert smaller CHPs (< 10MW) based on existing locations from MaStR
    additional_capacitiy = existing_chp_smaller_10mw(MaStR_konv, EgonChp)


