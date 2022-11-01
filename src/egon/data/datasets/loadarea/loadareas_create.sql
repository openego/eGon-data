/*
Cut Loadarea with MV Griddistrict
Identify and exclude Loadarea smaller than 100m².
Generate Centre of Loadareas with Centroid and PointOnSurface.
Calculate population from Census 2011.
Cut all 4 OSM sectors with MV Griddistricts.
Calculate statistics like NUTS and AGS code.
Check for Loadareas without AGS code.

__copyright__   = "Reiner Lemoine Institut"
__license__     = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__         = "https://github.com/openego/data_processing/blob/master/LICENSE"
__author__      = "Ludee, nesnoj"
*/

-- loadareas per mv-griddistrict
DROP TABLE IF EXISTS    demand.egon_loadarea CASCADE;
CREATE TABLE            demand.egon_loadarea (
    id SERIAL NOT NULL,
    bus_id integer,
    area_ha numeric,
    nuts varchar(5),
    rs_0 varchar(12),
    ags_0 varchar(12),
    zensus_sum integer,
    zensus_count integer,
    zensus_density numeric,
    sector_area_residential numeric,
    sector_area_retail numeric,
    sector_area_industrial numeric,
    sector_area_agricultural numeric,
    sector_area_sum numeric,
    sector_share_residential numeric,
    sector_share_retail numeric,
    sector_share_industrial numeric,
    sector_share_agricultural numeric,
    sector_share_sum numeric,
    sector_count_residential integer,
    sector_count_retail integer,
    sector_count_industrial integer,
    sector_count_agricultural integer,
    sector_count_sum integer,
    sector_consumption_residential double precision,
    sector_consumption_retail double precision,
    sector_consumption_industrial double precision,
    sector_consumption_agricultural double precision,
    sector_consumption_sum double precision,
    sector_peakload_retail double precision,
    sector_peakload_residential double precision,
    sector_peakload_industrial double precision,
    sector_peakload_agricultural double precision,
    geom_centroid geometry(POINT,3035),
    geom_surfacepoint geometry(POINT,3035),
    geom_centre geometry(POINT,3035),
    geom geometry(Polygon,3035),
    CONSTRAINT egon_loadarea_pkey PRIMARY KEY (id));

-- insert cutted load melt
INSERT INTO     demand.egon_loadarea (geom)
    SELECT  c.geom ::geometry(Polygon,3035)
    FROM (
        SELECT (ST_DUMP(ST_INTERSECTION(a.geom,b.geom))).geom AS geom
        FROM    demand.egon_loadarea_load_melt AS a,
                grid.egon_mv_grid_district AS b
        WHERE   a.geom && b.geom
        ) AS c
    WHERE   ST_GeometryType(c.geom) = 'ST_Polygon';

-- index GIST (geom_centroid)
CREATE INDEX egon_loadarea_geom_centroid_idx
    ON demand.egon_loadarea USING GIST (geom_centroid);

-- index GIST (geom_surfacepoint)
CREATE INDEX egon_loadarea_geom_surfacepoint_idx
    ON demand.egon_loadarea USING GIST (geom_surfacepoint);

-- index GIST (geom_centre)
CREATE INDEX egon_loadarea_geom_centre_idx
    ON demand.egon_loadarea USING gist (geom_centre);

-- index GIST (geom)
CREATE INDEX egon_loadarea_geom_idx
    ON demand.egon_loadarea USING gist (geom);

-- update area (area_ha)
UPDATE 	demand.egon_loadarea AS t1
	SET  	area_ha = t2.area
	FROM    (SELECT	loads.id,
			ST_AREA(ST_TRANSFORM(loads.geom,3035))/10000 AS area
		FROM	demand.egon_loadarea AS loads
		) AS t2
	WHERE  	t1.id = t2.id;

-- Identify Loadarea smaller than 100m²
DROP MATERIALIZED VIEW IF EXISTS    demand.egon_loadarea_smaller100m2_mview CASCADE;
CREATE MATERIALIZED VIEW            demand.egon_loadarea_smaller100m2_mview AS
    SELECT  loads.id AS id,
            loads.area_ha AS area_ha,
            loads.geom AS geom
    FROM    demand.egon_loadarea AS loads
    WHERE   loads.area_ha < 0.001;

-- index (id)
CREATE UNIQUE INDEX egon_loadarea_smaller100m2_mview_id_idx
    ON demand.egon_loadarea_smaller100m2_mview (id);

-- index GIST (geom)
CREATE INDEX egon_loadarea_smaller100m2_mview_geom_idx
    ON demand.egon_loadarea_smaller100m2_mview USING GIST (geom);

-- Exclude Loadarea smaller than 100m²
DELETE FROM demand.egon_loadarea AS loads
    WHERE loads.area_ha < 0.001;


-- Generate Centre of Loadareas with Centroid and PointOnSurface
-- centroid
UPDATE demand.egon_loadarea AS t1
    SET geom_centroid = t2.geom_centroid
    FROM (
        SELECT  loads.id AS id,
                ST_Centroid(loads.geom) AS geom_centroid
        FROM    demand.egon_loadarea AS loads
        ) AS t2
    WHERE   t1.id = t2.id;

-- surfacepoint
UPDATE demand.egon_loadarea AS t1
    SET geom_surfacepoint = t2.geom_surfacepoint
    FROM (
        SELECT  loads.id AS id,
                ST_PointOnSurface(loads.geom) AS geom_surfacepoint
        FROM    demand.egon_loadarea AS loads
        ) AS t2
    WHERE   t1.id = t2.id;

-- centre with centroid if inside loadarea
UPDATE demand.egon_loadarea AS t1
    SET geom_centre = t2.geom_centre
    FROM (
        SELECT  loads.id AS id,
                loads.geom_centroid AS geom_centre
        FROM    demand.egon_loadarea AS loads
        WHERE   loads.geom && loads.geom_centroid AND
                ST_CONTAINS(loads.geom,loads.geom_centroid)
        )AS t2
    WHERE   t1.id = t2.id;

-- centre with surfacepoint if outside area
UPDATE demand.egon_loadarea AS t1
    SET geom_centre = t2.geom_centre
    FROM (
        SELECT  loads.id AS id,
                loads.geom_surfacepoint AS geom_centre
        FROM    demand.egon_loadarea AS loads
        WHERE   loads.geom_centre IS NULL
        )AS t2
    WHERE   t1.id = t2.id;

/* -- validate geom_centre
SELECT  loads.id AS id
FROM    demand.egon_loadarea AS loads
WHERE   NOT ST_CONTAINS(loads.geom,loads.geom_centre); */


-- zensus 2011 population
UPDATE demand.egon_loadarea AS t1
    SET zensus_sum = t2.zensus_sum,
        zensus_count = t2.zensus_count,
        zensus_density = t2.zensus_density
    FROM (
        SELECT  a.id AS id,
                SUM(b.population)::integer AS zensus_sum,
                COUNT(b.geom_point)::integer AS zensus_count,
                (SUM(b.population)/COUNT(b.geom_point))::numeric AS zensus_density
        FROM    demand.egon_loadarea AS a,
                society.destatis_zensus_population_per_ha_inside_germany AS b
        WHERE   a.geom && b.geom_point AND
                ST_CONTAINS(a.geom,b.geom_point)
        GROUP BY a.id
        )AS t2
    WHERE   t1.id = t2.id;

-- Cut all 4 OSM sectors with MV Griddistricts
-- 1. Residential sector
DROP TABLE IF EXISTS    openstreetmap.ego_osm_sector_per_griddistrict_1_residential CASCADE;
CREATE TABLE            openstreetmap.ego_osm_sector_per_griddistrict_1_residential (
    id SERIAL NOT NULL,
    geom geometry(Polygon,3035),
    CONSTRAINT urban_sector_per_grid_district_1_residential_pkey PRIMARY KEY (id));

-- intersect sector with mv-griddistrict
INSERT INTO     openstreetmap.ego_osm_sector_per_griddistrict_1_residential (geom)
    SELECT  loads.geom ::geometry(Polygon,3035)
    FROM (
        SELECT (ST_DUMP(ST_INTERSECTION(loads.geom,dis.geom))).geom AS geom
        FROM    openstreetmap.osm_polygon_urban_sector_1_residential_mview AS loads,
                grid.egon_mv_grid_district AS dis
        WHERE   loads.geom && dis.geom
        ) AS loads
    WHERE   ST_GeometryType(loads.geom) = 'ST_Polygon';

-- index GIST (geom)
CREATE INDEX urban_sector_per_grid_district_1_residential_geom_idx
    ON openstreetmap.ego_osm_sector_per_griddistrict_1_residential USING GIST (geom);

-- sector stats
UPDATE demand.egon_loadarea AS t1
    SET sector_area_residential = t2.sector_area,
        sector_count_residential = t2.sector_count,
        sector_share_residential = t2.sector_area / t2.area_ha
    FROM (
        SELECT  loads.id AS id,
                SUM(ST_AREA(sector.geom)/10000) AS sector_area,
                COUNT(sector.geom) AS sector_count,
                loads.area_ha AS area_ha
        FROM    openstreetmap.ego_osm_sector_per_griddistrict_1_residential AS sector,
                demand.egon_loadarea AS loads
        WHERE   loads.geom && sector.geom AND
                ST_INTERSECTS(loads.geom,ST_BUFFER(sector.geom,-1))
        GROUP BY loads.id
        ) AS t2
    WHERE   t1.id = t2.id;

-- 2. Retail sector
DROP TABLE IF EXISTS    openstreetmap.ego_osm_sector_per_griddistrict_2_retail CASCADE;
CREATE TABLE            openstreetmap.ego_osm_sector_per_griddistrict_2_retail (
    id SERIAL NOT NULL,
    geom geometry(Polygon,3035),
    CONSTRAINT urban_sector_per_grid_district_2_retail_pkey PRIMARY KEY (id));

-- intersect sector with mv-griddistrict
INSERT INTO     openstreetmap.ego_osm_sector_per_griddistrict_2_retail (geom)
    SELECT  loads.geom ::geometry(Polygon,3035)
    FROM (
        SELECT (ST_DUMP(ST_INTERSECTION(loads.geom,dis.geom))).geom AS geom
        FROM    openstreetmap.osm_polygon_urban_sector_2_retail_mview AS loads,
                grid.egon_mv_grid_district AS dis
        WHERE   loads.geom && dis.geom
        ) AS loads
    WHERE   ST_GeometryType(loads.geom) = 'ST_Polygon';

-- index GIST (geom)
CREATE INDEX    urban_sector_per_grid_district_2_retail_geom_idx
    ON          openstreetmap.ego_osm_sector_per_griddistrict_2_retail USING GIST (geom);

-- sector stats
UPDATE demand.egon_loadarea AS t1
    SET sector_area_retail = t2.sector_area,
        sector_count_retail = t2.sector_count,
        sector_share_retail = t2.sector_area / t2.area_ha
    FROM (
        SELECT  loads.id AS id,
                SUM(ST_AREA(sector.geom)/10000) AS sector_area,
                COUNT(sector.geom) AS sector_count,
                loads.area_ha AS area_ha
        FROM    openstreetmap.ego_osm_sector_per_griddistrict_2_retail AS sector,
                demand.egon_loadarea AS loads
        WHERE   loads.geom && sector.geom AND
                ST_INTERSECTS(loads.geom,ST_BUFFER(sector.geom,-1))
        GROUP BY loads.id
        ) AS t2
    WHERE   t1.id = t2.id;

-- filter Industrial without largescale
DROP MATERIALIZED VIEW IF EXISTS	openstreetmap.osm_polygon_urban_sector_3_industrial_nolargescale_mview CASCADE;
CREATE MATERIALIZED VIEW		openstreetmap.osm_polygon_urban_sector_3_industrial_nolargescale_mview AS
	SELECT	osm.*
	FROM	openstreetmap.osm_polygon_urban AS osm
	WHERE	sector = '3' --AND gid NOT IN (SELECT polygon_id FROM model_draft.ego_demand_hv_largescaleconsumer) -- 31.10.22: COMMENTED OUT! CHECK IF NECESSARY!!!
ORDER BY	osm.id;

-- index (id)
CREATE UNIQUE INDEX osm_polygon_urban_sector_3_industrial_nolargescale_mview_gid_idx
    ON openstreetmap.osm_polygon_urban_sector_3_industrial_nolargescale_mview (id);

-- index GIST (geom)
CREATE INDEX    osm_polygon_urban_sector_3_industrial_nolargescale_mview_geom_idx
    ON openstreetmap.osm_polygon_urban_sector_3_industrial_nolargescale_mview USING GIST (geom);


/* -- check
SELECT  'industrial' AS name,
        count(ind.*) AS cnt
FROM    openstreetmap.osm_deu_polygon_urban_sector_3_industrial_mview ind
UNION ALL
SELECT  'largescale' AS name,
        count(ls.*) AS cnt
FROM    model_draft.ego_demand_hv_largescaleconsumer ls
UNION ALL
SELECT  'nolargescale' AS name,
        count(nols.*) AS cnt
FROM    openstreetmap.osm_deu_polygon_urban_sector_3_industrial_nolargescale_mview nols;
*/


-- 3. industrial sector
DROP TABLE IF EXISTS    openstreetmap.ego_osm_sector_per_griddistrict_3_industrial CASCADE;
CREATE TABLE            openstreetmap.ego_osm_sector_per_griddistrict_3_industrial	 (
    id SERIAL NOT NULL,
    geom geometry(Polygon,3035),
    CONSTRAINT urban_sector_per_grid_district_3_industrial_pkey PRIMARY KEY (id));

-- intersect sector with mv-griddistrict
INSERT INTO     openstreetmap.ego_osm_sector_per_griddistrict_3_industrial (geom)
    SELECT  loads.geom ::geometry(Polygon,3035)
    FROM (
        SELECT (ST_DUMP(ST_INTERSECTION(loads.geom,dis.geom))).geom AS geom
        FROM    openstreetmap.osm_polygon_urban_sector_3_industrial_nolargescale_mview AS loads,
                grid.egon_mv_grid_district AS dis
        WHERE   loads.geom && dis.geom
        ) AS loads
    WHERE   ST_GeometryType(loads.geom) = 'ST_Polygon';

-- index GIST (geom)
CREATE INDEX    urban_sector_per_grid_district_3_industrial_geom_idx
    ON openstreetmap.ego_osm_sector_per_griddistrict_3_industrial USING GIST (geom);

-- sector stats
UPDATE demand.egon_loadarea AS t1
    SET sector_area_industrial = t2.sector_area,
        sector_count_industrial = t2.sector_count,
        sector_share_industrial = t2.sector_area / t2.area_ha
    FROM (
        SELECT  loads.id AS id,
                SUM(ST_AREA(sector.geom)/10000) AS sector_area,
                COUNT(sector.geom) AS sector_count,
                loads.area_ha AS area_ha
        FROM    openstreetmap.ego_osm_sector_per_griddistrict_3_industrial AS sector,
                demand.egon_loadarea AS loads
        WHERE   loads.geom && sector.geom AND
                ST_INTERSECTS(loads.geom,ST_BUFFER(sector.geom,-1))
        GROUP BY loads.id
        ) AS t2
    WHERE   t1.id = t2.id;

-- 4. agricultural sector
DROP TABLE IF EXISTS    openstreetmap.ego_osm_sector_per_griddistrict_4_agricultural CASCADE;
CREATE TABLE            openstreetmap.ego_osm_sector_per_griddistrict_4_agricultural	 (
    id SERIAL NOT NULL,
    geom geometry(Polygon,3035),
    CONSTRAINT 	urban_sector_per_grid_district_4_agricultural_pkey PRIMARY KEY (id));

-- intersect sector with mv-griddistrict
INSERT INTO openstreetmap.ego_osm_sector_per_griddistrict_4_agricultural (geom)
    SELECT  loads.geom ::geometry(Polygon,3035)
    FROM (
        SELECT (ST_DUMP(ST_INTERSECTION(loads.geom,dis.geom))).geom AS geom
        FROM    openstreetmap.osm_polygon_urban_sector_4_agricultural_mview AS loads,
                grid.egon_mv_grid_district AS dis
        WHERE   loads.geom && dis.geom
        ) AS loads
    WHERE   ST_GeometryType(loads.geom) = 'ST_Polygon';

-- index GIST (geom)
CREATE INDEX urban_sector_per_grid_district_4_agricultural_geom_idx
    ON openstreetmap.ego_osm_sector_per_griddistrict_4_agricultural USING GIST (geom);

-- sector stats
UPDATE demand.egon_loadarea AS t1
    SET sector_area_agricultural = t2.sector_area,
        sector_count_agricultural = t2.sector_count,
        sector_share_agricultural = t2.sector_area / t2.area_ha
    FROM (
        SELECT  loads.id AS id,
                SUM(ST_AREA(sector.geom)/10000) AS sector_area,
                COUNT(sector.geom) AS sector_count,
                loads.area_ha AS area_ha
        FROM    openstreetmap.ego_osm_sector_per_griddistrict_4_agricultural AS sector,
                demand.egon_loadarea AS loads
        WHERE   loads.geom && sector.geom AND
                ST_INTERSECTS(loads.geom,ST_BUFFER(sector.geom,-1))
        GROUP BY loads.id
        ) AS t2
    WHERE   t1.id = t2.id;

-- sector stats
UPDATE demand.egon_loadarea AS t1
    SET sector_area_sum = t2.sector_area_sum,
        sector_share_sum = t2.sector_share_sum,
        sector_count_sum = t2.sector_count_sum
    FROM (
        SELECT  id,
                coalesce(load.sector_area_residential,0) +
                    coalesce(load.sector_area_retail,0) +
                    coalesce(load.sector_area_industrial,0) +
                    coalesce(load.sector_area_agricultural,0) AS sector_area_sum,
                coalesce(load.sector_share_residential,0) +
                    coalesce(load.sector_share_retail,0) +
                    coalesce(load.sector_share_industrial,0) +
                    coalesce(load.sector_share_agricultural,0) AS sector_share_sum,
                coalesce(load.sector_count_residential,0) +
                    coalesce(load.sector_count_retail,0) +
                    coalesce(load.sector_count_industrial,0) +
                    coalesce(load.sector_count_agricultural,0) AS sector_count_sum
        FROM    demand.egon_loadarea AS load
        ) AS t2
    WHERE   t1.id = t2.id;



-- Calculate statistics like NUTS and AGS code
-- nuts code (nuts)
UPDATE demand.egon_loadarea AS t1
    SET nuts = t2.nuts
    FROM (
        SELECT  loads.id AS id,
                vg.nuts AS nuts
        FROM    demand.egon_loadarea AS loads,
                boundaries.vg250_gem_clean AS vg
        WHERE   vg.geometry && loads.geom_centre AND
                ST_CONTAINS(vg.geometry,loads.geom_centre)
        ) AS t2
    WHERE   t1.id = t2.id;

-- regionalschlüssel (rs_0)
UPDATE  demand.egon_loadarea AS t1
    SET rs_0 = t2.rs_0
    FROM (
        SELECT  loads.id,
                vg.rs_0
        FROM    demand.egon_loadarea AS loads,
                boundaries.vg250_gem_clean AS vg
        WHERE   vg.geometry && loads.geom_centre AND
                ST_CONTAINS(vg.geometry,loads.geom_centre)
        ) AS t2
    WHERE   t1.id = t2.id;

-- gemeindeschlüssel (ags_0)
UPDATE  demand.egon_loadarea AS t1
    SET ags_0 = t2.ags_0
    FROM (
        SELECT  loads.id AS id,
                vg.ags_0 AS ags_0
        FROM    demand.egon_loadarea AS loads,
                boundaries.vg250_gem_clean AS vg
        WHERE   vg.geometry && loads.geom_centre AND
                ST_CONTAINS(vg.geometry,loads.geom_centre)
        ) AS t2
    WHERE   t1.id = t2.id;

-- substation id
UPDATE demand.egon_loadarea AS t1
    SET bus_id = t2.bus_id
    FROM (
        SELECT  a.id AS id,
                b.bus_id AS bus_id
        FROM    demand.egon_loadarea AS a,
                grid.egon_mv_grid_district AS b
        WHERE   b.geom && a.geom_centre AND
                ST_CONTAINS(b.geom,a.geom_centre)
        ) AS t2
    WHERE   t1.id = t2.id;


-- Check for Loadareas without AGS code
DROP MATERIALIZED VIEW IF EXISTS    demand.egon_loadarea_error_noags_mview CASCADE;
CREATE MATERIALIZED VIEW            demand.egon_loadarea_error_noags_mview AS
    SELECT  loads.id,
            loads.geom
    FROM    demand.egon_loadarea AS loads
    WHERE   loads.ags_0 IS NULL;

-- index (id)
CREATE UNIQUE INDEX egon_loadarea_error_noags_mview_id_idx
    ON demand.egon_loadarea_error_noags_mview (id);

-- index GIST (geom)
CREATE INDEX egon_loadarea_error_noags_mview_geom_idx
    ON demand.egon_loadarea_error_noags_mview USING GIST (geom);
