from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import Boolean

from egon.data import config, db

Base = declarative_base()


class EgonMapZensusMvgdBuildings(Base):
    __tablename__ = "egon_map_zensus_mvgd_buildings"
    __table_args__ = {"schema": "boundaries"}

    building_id = Column(Integer, primary_key=True)
    sector = Column(String, primary_key=True)
    zensus_population_id = Column(Integer, index=True)
    bus_id = Column(Integer, index=True)
    osm = Column(Boolean, index=True)
    electricity = Column(Boolean, index=True)
    heat = Column(Boolean, index=True)

scenarios = config.settings()["egon-data"]["--scenarios"]

def map_all_used_buildings():
    """This function maps all used buildings from OSM and synthetic ones."""

    EgonMapZensusMvgdBuildings.__table__.drop(
        bind=db.engine(), checkfirst=True
    )
    EgonMapZensusMvgdBuildings.__table__.create(bind=db.engine())

    db.execute_sql(
        sql_string=f"""
        INSERT INTO {EgonMapZensusMvgdBuildings.__table_args__["schema"]}.
        {EgonMapZensusMvgdBuildings.__tablename__}
            SELECT
                bld.id as building_id,
                peak.sector,
                zensus.id as zensus_population_id,
                mvgd.bus_id::integer
            FROM (
                SELECT "id"::integer, geom_point
                FROM openstreetmap.osm_buildings_synthetic
                UNION
                SELECT "id"::integer, geom_point
                FROM openstreetmap.osm_buildings_filtered
            ) AS bld,
                demand.egon_building_electricity_peak_loads AS peak,
                society.destatis_zensus_population_per_ha
                AS zensus,
                boundaries.egon_map_zensus_grid_districts AS mvgd
            WHERE bld.id = peak.building_id
-- Buildings do not change in the scenarios
                AND peak.scenario = '{scenarios[0]}'
                AND ST_Within(bld.geom_point, zensus.geom)
                AND mvgd.zensus_population_id = zensus.id;

        UPDATE boundaries.egon_map_zensus_mvgd_buildings
        SET     "osm" = TRUE;

        UPDATE boundaries.egon_map_zensus_mvgd_buildings as bld
        SET     "osm" = FALSE
        FROM (
            SELECT "id"::integer
            FROM openstreetmap.osm_buildings_synthetic
            ) as synth
        WHERE bld.building_id = synth.id;

        UPDATE boundaries.egon_map_zensus_mvgd_buildings
        SET     "electricity" = TRUE;

        UPDATE boundaries.egon_map_zensus_mvgd_buildings
        SET     "heat" = FALSE;

-- Only residentials
        UPDATE boundaries.egon_map_zensus_mvgd_buildings as bld
        SET     "heat" = TRUE
        FROM (
            SELECT distinct(building_id)
            FROM demand.egon_heat_timeseries_selected_profiles
            ) as heat
        WHERE bld.building_id = heat.building_id
         AND bld.sector = 'residential';

-- All electricity cts also are heat cts also
        UPDATE boundaries.egon_map_zensus_mvgd_buildings as bld
        SET     "heat" = TRUE
        WHERE bld.sector = 'cts' AND electricity = TRUE;



        """
    )
