from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import Boolean

from egon.data import config, db
from egon.data.datasets import load_sources_and_targets

Base = declarative_base()


class EgonMapZensusMvgdBuildings(Base):
    """
    A final mapping table including all buildings used for residential and
    cts, heat and electricity timeseries. Including census cells, mvgd bus_id,
    building type (osm or synthetic)
    """

    __tablename__ = "egon_map_zensus_mvgd_buildings"
    __table_args__ = {"schema": "boundaries"}

    building_id = Column(Integer, primary_key=True)
    sector = Column(String, primary_key=True)
    zensus_population_id = Column(Integer, index=True)
    bus_id = Column(Integer, index=True)
    osm = Column(Boolean, index=True)
    electricity = Column(Boolean, index=True)
    heat = Column(Boolean, index=True)


def map_all_used_buildings():
    """This function maps all used buildings from OSM and synthetic ones."""
    scenarios = config.settings()["egon-data"]["--scenarios"]

    cts_s, cts_t = load_sources_and_targets("CtsDemandBuildings")
    hh_s, hh_t = load_sources_and_targets("Household Demands")
    hts_s, hts_t = load_sources_and_targets("HeatTimeSeries")

    EgonMapZensusMvgdBuildings.__table__.drop(
        bind=db.engine(), checkfirst=True
    )
    EgonMapZensusMvgdBuildings.__table__.create(bind=db.engine())

    db.execute_sql(sql_string=f"""
        INSERT INTO {EgonMapZensusMvgdBuildings.__table_args__["schema"]}.
        {EgonMapZensusMvgdBuildings.__tablename__}
            SELECT
                bld.id as building_id,
                peak.sector,
                zensus.id as zensus_population_id,
                mvgd.bus_id::integer
            FROM (
                SELECT "id"::integer, geom_point
                FROM {cts_s.tables["osm_buildings_synthetic"]}
                UNION
                SELECT "id"::integer, geom_point
                FROM {cts_s.tables["osm_buildings_filtered"]}
            ) AS bld,
                {cts_t.tables["building_electricity_peak_loads"]} AS peak,
                {hh_s.tables["destatis_zensus_population_per_ha"]} AS zensus,
                {hts_s.tables["map_zensus_grid_districts"]} AS mvgd
            WHERE bld.id = peak.building_id
-- Buildings do not change in the scenarios
                AND peak.scenario = '{scenarios[0]}'
                AND ST_Within(bld.geom_point, zensus.geom)
                AND mvgd.zensus_population_id = zensus.id;

        UPDATE {cts_t.tables["map_zensus_mvgd_buildings"]}
        SET     "osm" = TRUE;

        UPDATE {cts_t.tables["map_zensus_mvgd_buildings"]} as bld
        SET     "osm" = FALSE
        FROM (
            SELECT "id"::integer
            FROM {cts_s.tables["osm_buildings_synthetic"]}
            ) as synth
        WHERE bld.building_id = synth.id;

        UPDATE {cts_t.tables["map_zensus_mvgd_buildings"]}
        SET     "electricity" = TRUE;

        UPDATE {cts_t.tables["map_zensus_mvgd_buildings"]}
        SET     "heat" = FALSE;

-- Only residentials
        UPDATE {cts_t.tables["map_zensus_mvgd_buildings"]} as bld
        SET     "heat" = TRUE
        FROM (
            SELECT distinct(building_id)
            FROM {hts_s.tables["selected_profiles"]}
            ) as heat
        WHERE bld.building_id = heat.building_id
         AND bld.sector = 'residential';

-- All electricity cts also are heat cts also
        UPDATE {cts_t.tables["map_zensus_mvgd_buildings"]} as bld
        SET     "heat" = TRUE
        WHERE bld.sector = 'cts' AND electricity = TRUE;



        """)
