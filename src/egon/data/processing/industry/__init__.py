"""The central module containing all code dealing with the spatial
    distribution of industrial electricity demands.
    Industrial demands from DemandRegio are distributed from nuts3 level down
    to osm landuse polygons and/or industrial sites identified earlier in the
    workflow.

"""
import egon.data.config
from egon.data import db
from sqlalchemy import Column, String, Float, Integer, ForeignKey, Sequence
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class EgonDemandRegioIndustryDistributed(Base):
    __tablename__ = 'egon_demandregio_industry_distributed'
    __table_args__ = {'schema':'demand'}
    id = Column(Integer,
        Sequence('egon_demandregio_industry_distributed_id_seq', schema='demand'),
        server_default=
            Sequence('egon_demandregio_industry_distributed_id_seq', schema='demand').next_value(),
        primary_key=True)
    scenario = Column(String(20))
    wz = Column(Integer)
    demand = Column(Float)
    osm_landuse_id = Column(Integer)
    industrial_sites_id = Column(Integer)


def create_tables():
    """Create tables for ditributed industrial demands
    Returns
    -------
    None.
    """
    cfg = egon.data.config.datasets()['distributed_industrial_demand']

    # Drop table
    db.execute_sql(
        f"""DROP TABLE IF EXISTS
            {cfg['target']['schema']}.
            {cfg['target']['table']} CASCADE;"""
    )

    engine = db.engine()
    EgonDemandRegioIndustryDistributed.__table__.create(bind=engine, checkfirst=True)


def distribute_industrial_demands():
    """ Distribute electrical demands for industry to osm landuse polygons
    and/or industrial sites, identified earlier in the process.
    The demands per subsector on nuts3-level from demandregio are distributed
    linearly to the area of the corresponding landuse polygons or evenly to
    identified industrial sites.

    Returns
    -------
    None.
    """
