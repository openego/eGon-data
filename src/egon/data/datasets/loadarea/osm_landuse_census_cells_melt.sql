/*
Melt loads from OSM landuse and Census 2011
Collect loads from both sources.
Buffer collected loads with with 100m.
Unbuffer the collection with 100m.
Validate the melted geometries.
Fix geometries with error.
Check again for errors.

__copyright__   = "Reiner Lemoine Institut"
__license__     = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__         = "https://github.com/openego/data_processing/blob/master/LICENSE"
__author__      = "Ludee, nesnoj"
*/

-- collect loads
DROP TABLE IF EXISTS	demand.egon_loadarea_load_collect CASCADE;
CREATE TABLE		demand.egon_loadarea_load_collect (
	id SERIAL,
	geom geometry(Polygon,3035),
	CONSTRAINT egon_loadarea_load_collect_pkey PRIMARY KEY (id));

-- insert loads OSM
INSERT INTO	demand.egon_loadarea_load_collect (geom)
	SELECT	geom
	FROM	openstreetmap.osm_landuse_melted;
--    ORDER BY gid;

-- insert loads zensus cluster
INSERT INTO	demand.egon_loadarea_load_collect (geom)
	SELECT	geom
	FROM	society.egon_destatis_zensus_cells_melted_cluster;
--    ORDER BY cid;

-- index GIST (geom)
CREATE INDEX egon_loadarea_load_collect_geom_idx
    ON demand.egon_loadarea_load_collect USING GIST (geom);

-- buffer with 100m
DROP TABLE IF EXISTS	demand.egon_loadarea_load_collect_buffer100 CASCADE;
CREATE TABLE		demand.egon_loadarea_load_collect_buffer100 (
	id SERIAL,
	geom geometry(Polygon,3035),
	CONSTRAINT egon_loadarea_load_collect_buffer100_pkey PRIMARY KEY (id));

-- insert buffer
INSERT INTO	demand.egon_loadarea_load_collect_buffer100 (geom)
	SELECT	(ST_DUMP(ST_MULTI(ST_UNION(
			ST_BUFFER(geom, 100)
		)))).geom ::geometry(Polygon,3035) AS geom
	FROM	demand.egon_loadarea_load_collect;
--    ORDER BY id;

-- index GIST (geom)
CREATE INDEX egon_loadarea_load_collect_buffer100_geom_idx
    ON demand.egon_loadarea_load_collect_buffer100 USING GIST (geom);

-- unbuffer with 99m
DROP TABLE IF EXISTS	demand.egon_loadarea_load_melt CASCADE;
CREATE TABLE		demand.egon_loadarea_load_melt (
	id SERIAL,
	geom geometry(Polygon,3035),
	CONSTRAINT egon_loadarea_load_melt_pkey PRIMARY KEY (id));

-- insert buffer
INSERT INTO	demand.egon_loadarea_load_melt (geom)
	SELECT	(ST_DUMP(ST_MULTI(ST_UNION(
			ST_BUFFER(buffer.geom, -99)
		)))).geom ::geometry(Polygon,3035) AS geom
	FROM	demand.egon_loadarea_load_collect_buffer100 AS buffer
	GROUP BY buffer.id
	ORDER BY buffer.id;

-- index GIST (geom)
CREATE INDEX egon_loadarea_load_melt_geom_idx
    ON demand.egon_loadarea_load_melt USING GIST (geom);


-- Validate the melted geometries
DROP MATERIALIZED VIEW IF EXISTS    demand.egon_loadarea_load_melt_error_geom_mview CASCADE;
CREATE MATERIALIZED VIEW            demand.egon_loadarea_load_melt_error_geom_mview AS
    SELECT  test.id,
            test.error,
            reason(ST_IsValidDetail(test.geom)) AS error_reason,
            ST_SetSRID(location(ST_IsValidDetail(test.geom)),3035) ::geometry(Point,3035) AS error_location,
            test.geom ::geometry(Polygon,3035) AS geom
    FROM (
        SELECT  source.id AS id,    -- PK
                ST_IsValid(source.geom) AS error,
                source.geom AS geom
        FROM    demand.egon_loadarea_load_melt AS source  -- Table
        ) AS test
    WHERE   test.error = FALSE;

-- index (id)
CREATE UNIQUE INDEX egon_loadarea_load_melt_error_geom_mview_id_idx
    ON demand.egon_loadarea_load_melt_error_geom_mview (id);

-- index GIST (geom)
CREATE INDEX egon_loadarea_load_melt_error_geom_mview_geom_idx
    ON demand.egon_loadarea_load_melt_error_geom_mview USING GIST (geom);

-- Fix geometries with error
DROP MATERIALIZED VIEW IF EXISTS    demand.egon_loadarea_load_melt_error_geom_fix_mview CASCADE;
CREATE MATERIALIZED VIEW            demand.egon_loadarea_load_melt_error_geom_fix_mview AS
    SELECT  fix.id AS id,
            ST_IsValid(fix.geom) AS error,
            GeometryType(fix.geom) AS geom_type,
            ST_AREA(fix.geom) AS area,
            fix.geom_buffer ::geometry(POLYGON,3035) AS geom_buffer,
            fix.geom ::geometry(POLYGON,3035) AS geom
    FROM (
        SELECT  fehler.id AS id,
                ST_BUFFER(fehler.geom, -1) AS geom_buffer,
                (ST_DUMP(ST_BUFFER(ST_BUFFER(fehler.geom, -1), 1))).geom AS geom
        FROM    demand.egon_loadarea_load_melt_error_geom_mview AS fehler
        ) AS fix
    ORDER BY fix.id;

-- index (id)
CREATE UNIQUE INDEX egon_loadarea_load_melt_error_geom_fix_mview_id_idx
    ON demand.egon_loadarea_load_melt_error_geom_fix_mview (id);

-- index GIST (geom)
CREATE INDEX egon_loadarea_load_melt_error_geom_fix_mview_geom_idx
    ON demand.egon_loadarea_load_melt_error_geom_fix_mview USING GIST (geom);

-- update fixed geoms
UPDATE demand.egon_loadarea_load_melt AS t1
    SET geom = t2.geom
    FROM (
        SELECT  fix.id AS id,
                fix.geom AS geom
        FROM    demand.egon_loadarea_load_melt_error_geom_fix_mview AS fix
        ) AS t2
    WHERE   t1.id = t2.id;

-- Check again for errors.
DROP MATERIALIZED VIEW IF EXISTS    demand.egon_loadarea_load_melt_error_2_geom_mview CASCADE;
CREATE MATERIALIZED VIEW            demand.egon_loadarea_load_melt_error_2_geom_mview AS
    SELECT  test.id AS id,
            test.error AS error,
            reason(ST_IsValidDetail(test.geom)) AS error_reason,
            ST_SetSRID(location(ST_IsValidDetail(test.geom)),3035) ::geometry(Point,3035) AS error_location,
            ST_TRANSFORM(test.geom,3035) ::geometry(Polygon,3035) AS geom
    FROM (
        SELECT  source.id AS id,
                ST_IsValid(source.geom) AS error,
                source.geom ::geometry(Polygon,3035) AS geom
        FROM    demand.egon_loadarea_load_melt AS source
        ) AS test
    WHERE   test.error = FALSE;

-- index (id)
CREATE UNIQUE INDEX egon_loadarea_load_melt_error_2_geom_mview_id_idx
    ON demand.egon_loadarea_load_melt_error_2_geom_mview (id);

-- index GIST (geom)
CREATE INDEX egon_loadarea_load_melt_error_2_geom_mview_geom_idx
    ON demand.egon_loadarea_load_melt_error_2_geom_mview USING GIST (geom);


/* -- drop temp
DROP TABLE IF EXISTS                demand.egon_loadarea_load_collect CASCADE;
DROP TABLE IF EXISTS                demand.egon_loadarea_load_collect_buffer100 CASCADE;
DROP TABLE IF EXISTS                demand.egon_loadarea_load_melt CASCADE;
DROP MATERIALIZED VIEW IF EXISTS    demand.egon_loadarea_load_melt_error_geom_mview CASCADE;
DROP MATERIALIZED VIEW IF EXISTS    demand.egon_loadarea_load_melt_error_geom_fix_mview CASCADE;
DROP MATERIALIZED VIEW IF EXISTS    demand.egon_loadarea_load_melt_error_2_geom_mview CASCADE;
 */
