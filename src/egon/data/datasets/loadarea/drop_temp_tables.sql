/*
Drop temp tables, views and sequences

__copyright__   = "Reiner Lemoine Institut"
__license__     = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__         = "https://github.com/openego/eGon-data/blob/main/LICENSE"
__author__      = "nesnoj"
*/

-- From script: osm_landuse_melt.sql
DROP SEQUENCE IF EXISTS openstreetmap.osm_landuse_buffer100_mview_id CASCADE;
DROP MATERIALIZED VIEW IF EXISTS openstreetmap.osm_landuse_buffer100_mview CASCADE;
DROP TABLE IF EXISTS openstreetmap.osm_landuse_melted CASCADE;

-- From script: census_cells_melt.sql
DROP TABLE IF EXISTS society.egon_destatis_zensus_cells_melted CASCADE;
DROP TABLE IF EXISTS society.egon_destatis_zensus_cells_melted_cluster CASCADE;
DROP MATERIALIZED VIEW IF EXISTS openstreetmap.egon_society_zensus_per_la_mview CASCADE;

-- From script: osm_landuse_census_cells_melt.sql
DROP TABLE IF EXISTS demand.egon_loadarea_load_collect CASCADE;
DROP TABLE IF EXISTS demand.egon_loadarea_load_collect_buffer100 CASCADE;
DROP TABLE IF EXISTS demand.egon_loadarea_load_melt CASCADE;
DROP MATERIALIZED VIEW IF EXISTS demand.egon_loadarea_load_melt_error_geom_mview CASCADE;
DROP MATERIALIZED VIEW IF EXISTS demand.egon_loadarea_load_melt_error_geom_fix_mview CASCADE;
DROP MATERIALIZED VIEW IF EXISTS demand.egon_loadarea_load_melt_error_2_geom_mview CASCADE;

-- From script: loadareas_create.sql
DROP MATERIALIZED VIEW IF EXISTS demand.egon_loadarea_smaller100m2_mview CASCADE;
DROP TABLE IF EXISTS openstreetmap.egon_osm_sector_per_griddistrict_1_residential CASCADE;
DROP TABLE IF EXISTS openstreetmap.egon_osm_sector_per_griddistrict_2_retail CASCADE;
DROP MATERIALIZED VIEW IF EXISTS openstreetmap.osm_polygon_urban_sector_3_industrial_nolargescale_mview CASCADE;
DROP TABLE IF EXISTS openstreetmap.egon_osm_sector_per_griddistrict_3_industrial CASCADE;
DROP TABLE IF EXISTS openstreetmap.egon_osm_sector_per_griddistrict_4_agricultural CASCADE;
DROP MATERIALIZED VIEW IF EXISTS demand.egon_loadarea_error_noags_mview CASCADE;
