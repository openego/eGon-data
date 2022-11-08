/*
OSM landuse sectors
Extract landuse areas from OpenStreetMap.
Cut the landuse with German boders (vg250) and make valid geometries.
Divide into 4 landuse sectors:
1. Residential
2. Retail
3. Industrial
4. Agricultural
__copyright__   = "Reiner Lemoine Institut"
__license__     = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__         = "https://github.com/openego/eGon-data/blob/main/LICENSE"
__author__      = "Ludee, IlkaCu, nesnoj"
*/


---------- ---------- ----------
-- Filter OSM Urban Landuse
---------- ---------- ----------

-- Polygons with tags related to settlements are extracted from the original OSM data set and stored in table 'openstreetmap.osm_landuse.

DELETE FROM openstreetmap.osm_landuse;

-- filter urban

INSERT INTO            openstreetmap.osm_landuse
    SELECT  osm.id ::integer AS id,
            osm.osm_id ::integer AS osm_id,
            osm.name ::text AS name,
            '0' ::integer AS sector,
            'undefined'::text AS sector_name,
            ST_AREA(ST_TRANSFORM(osm.geom,3035))/10000 ::double precision AS area_ha,
            osm.tags ::hstore AS tags,
            'outside' ::text AS vg250,
            ST_MULTI(ST_TRANSFORM(osm.geom,3035)) ::geometry(MultiPolygon,3035) AS geom
    FROM    openstreetmap.osm_polygon AS osm
    WHERE
        tags @> '"landuse"=>"residential"'::hstore OR
        tags @> '"landuse"=>"commercial"'::hstore OR
        tags @> '"landuse"=>"retail"'::hstore OR
        tags @> '"landuse"=>"industrial;retail"'::hstore OR
        tags @> '"landuse"=>"industrial"'::hstore OR
        tags @> '"landuse"=>"port"'::hstore OR
        tags @> '"man_made"=>"wastewater_plant"'::hstore OR
        tags @> '"aeroway"=>"terminal"'::hstore OR
        tags @> '"aeroway"=>"gate"'::hstore OR
        tags @> '"man_made"=>"works"'::hstore OR
        tags @> '"landuse"=>"farmyard"'::hstore OR
        tags @> '"landuse"=>"greenhouse_horticulture"'::hstore
    ORDER BY    osm.id;

-- Create index using GIST (geom)

DROP INDEX IF EXISTS openstreetmap.osm_landuse_geom_idx;

CREATE INDEX osm_landuse_geom_idx
    ON openstreetmap.osm_landuse USING GIST (geom);

-------------------------------------------------------------------------------------------------------------------------------
-- Identify the intersection between OSM polygons and the (German) external borders as defined in 'boundaries.vg250_sta_union'.
-------------------------------------------------------------------------------------------------------------------------------

-- Identify polygons which are completely inside the defined boundaries
UPDATE 	openstreetmap.osm_landuse AS t1
	SET  	vg250 = t2.vg250
	FROM    (
		SELECT	osm.id AS id,
			'inside' ::text AS vg250
		FROM	boundaries.vg250_sta_union AS vg,
			openstreetmap.osm_landuse AS osm
		WHERE  	vg.geometry && osm.geom AND
			ST_CONTAINS(vg.geometry,osm.geom)
		) AS t2
	WHERE  	t1.id = t2.id;

-- Identify polygons which are spatially overlapping with the defined boundaries
UPDATE 	openstreetmap.osm_landuse AS t1
	SET  	vg250 = t2.vg250
	FROM    (
		SELECT	osm.id AS id,
			'crossing' ::text AS vg250
		FROM	boundaries.vg250_sta_union AS vg,
			openstreetmap.osm_landuse AS osm
		WHERE  	osm.vg250 = 'outside' AND
			vg.geometry && osm.geom AND
			ST_Overlaps(vg.geometry,osm.geom)
		) AS t2
	WHERE  	t1.id = t2.id;

-- Move all polygons which are overlapping the external borders or lying outside of these to a materialized view

DROP MATERIALIZED VIEW IF EXISTS	openstreetmap.osm_landuse_error_geom_vg250 CASCADE;
CREATE MATERIALIZED VIEW		openstreetmap.osm_landuse_error_geom_vg250 AS
	SELECT	osm.*
	FROM	openstreetmap.osm_landuse AS osm
	WHERE	osm.vg250 = 'outside' OR osm.vg250 = 'crossing';

-- Create index
CREATE UNIQUE INDEX  	osm_landuse_error_geom_vg250_id_idx
		ON	openstreetmap.osm_landuse_error_geom_vg250 (id);

-- index GIST (geom)
CREATE INDEX  	osm_landuse_error_geom_vg250_geom_idx
	ON	openstreetmap.osm_landuse_error_geom_vg250
	USING	GIST (geom);

-- Sequence
DROP SEQUENCE IF EXISTS 	openstreetmap.osm_landuse_vg250_cut_id CASCADE;
CREATE SEQUENCE 		openstreetmap.osm_landuse_vg250_cut_id;


-- Create materialized views to identify and store intersecting parts of polygons overlapping the external borders
DROP MATERIALIZED VIEW IF EXISTS	openstreetmap.osm_landuse_vg250_cut;
CREATE MATERIALIZED VIEW		openstreetmap.osm_landuse_vg250_cut AS
	SELECT	nextval('openstreetmap.osm_landuse_vg250_cut_id') ::integer AS cut_id,
		cut.id ::integer AS id,
		cut.osm_id ::integer AS osm_id,
		cut.name ::text AS name,
		cut.sector ::integer AS sector,
		cut.sector_name::text AS sector_name,
		cut.area_ha ::double precision AS area_ha,
		cut.tags ::hstore AS tags,
		cut.vg250 ::text AS vg250,
		GeometryType(cut.geom_new) ::text AS geom_type,
		ST_MULTI(ST_TRANSFORM(cut.geom_new,3035)) ::geometry(MultiPolygon,3035) AS geom
	FROM	(SELECT	poly.*,
			ST_INTERSECTION(poly.geom, cut.geometry) AS geom_new
		FROM	openstreetmap.osm_landuse_error_geom_vg250 AS poly,
			boundaries.vg250_sta_union AS cut
		WHERE	poly.vg250 = 'crossing'
		) AS cut
	ORDER BY 	cut.id;

-- index (id)
CREATE UNIQUE INDEX  	osm_landuse_vg250_cut_id_idx
		ON	openstreetmap.osm_landuse_vg250_cut (id);

-- index GIST (geom)
CREATE INDEX  	osm_landuse_vg250_cut_geom_idx
	ON	openstreetmap.osm_landuse_vg250_cut
	USING	GIST (geom);

-- Store polygons in a materialized view
DROP MATERIALIZED VIEW IF EXISTS	openstreetmap.osm_landuse_vg250_clean_cut_multi;
CREATE MATERIALIZED VIEW		openstreetmap.osm_landuse_vg250_clean_cut_multi AS
	SELECT	nextval('openstreetmap.osm_polygon_id_seq'::regclass) ::integer AS id,
		cut.osm_id ::integer AS osm_id,
		cut.name ::text AS name,
		cut.sector ::integer AS sector,
		cut.sector_name::text AS sector_name,
		cut.area_ha ::double precision AS area_ha,
		cut.tags ::hstore AS tags,
		cut.vg250 ::text AS vg250,
		ST_MULTI(cut.geom) ::geometry(MultiPolygon,3035) AS geom
	FROM	openstreetmap.osm_landuse_vg250_cut AS cut
	WHERE	cut.geom_type = 'MULTIPOLYGON';

-- index (id)
CREATE UNIQUE INDEX  	osm_landuse_vg250_clean_cut_multi_id_idx
		ON	openstreetmap.osm_landuse_vg250_clean_cut_multi (id);

-- index GIST (geom)
CREATE INDEX  	osm_landuse_vg250_clean_cut_multi_geom_idx
	ON	openstreetmap.osm_landuse_vg250_clean_cut_multi
	USING	GIST (geom);

-- Store multipolygons in a materialized view
DROP MATERIALIZED VIEW IF EXISTS	openstreetmap.osm_landuse_vg250_clean_cut;
CREATE MATERIALIZED VIEW		openstreetmap.osm_landuse_vg250_clean_cut AS
	SELECT	nextval('openstreetmap.osm_polygon_id_seq'::regclass) ::integer AS id,
		cut.osm_id ::integer AS osm_id,
		cut.name ::text AS name,
		cut.sector ::integer AS sector,
		cut.sector_name::text AS sector_name,
		cut.area_ha ::double precision AS area_ha,
		cut.tags ::hstore AS tags,
		cut.vg250 ::text AS vg250,
		cut.geom ::geometry(MultiPolygon,3035) AS geom
	FROM	openstreetmap.osm_landuse_vg250_cut AS cut
	WHERE	cut.geom_type = 'POLYGON';

-- index (id)
CREATE UNIQUE INDEX  	osm_landuse_vg250_clean_cut_id_idx
		ON	openstreetmap.osm_landuse_vg250_clean_cut (id);

----------------------------------------------------------------------------
-- Remove all faulty entries from table openstreetmap.osm_landuse
-- and insert those parts of the polygons lying within the (German) borders
----------------------------------------------------------------------------

-- Remove polygons 'outside' vg250 boundaries
DELETE FROM	openstreetmap.osm_landuse AS osm
	WHERE	osm.vg250 = 'outside';

-- Remove polygons overlapping with vg250 boundaries
DELETE FROM	openstreetmap.osm_landuse AS osm
	WHERE	osm.vg250 = 'crossing';

-- Insert polygon fragments lying inside vg250 boundaries
INSERT INTO	openstreetmap.osm_landuse
	SELECT	clean.*
	FROM	openstreetmap.osm_landuse_vg250_clean_cut AS clean
	ORDER BY 	clean.id;

-- Insert multi polygon fragments lying inside vg250 boundaries
INSERT INTO	openstreetmap.osm_landuse
	SELECT	clean.*
	FROM	openstreetmap.osm_landuse_vg250_clean_cut_multi AS clean
	ORDER BY 	clean.id;


---------- ---------- ----------
-- Update Sector Information
---------- ---------- ----------

-- Sector 1. Residential
-- update sector
UPDATE 	openstreetmap.osm_landuse
SET  	sector = '1',
	sector_name = 'residential'
WHERE	tags @> '"landuse"=>"residential"'::hstore;

-- Sector 2. Retail
-- update sector
UPDATE 	openstreetmap.osm_landuse
SET  	sector = '2',
	sector_name = 'retail'
WHERE	tags @> '"landuse"=>"commercial"'::hstore OR
		tags @> '"landuse"=>"retail"'::hstore OR
		tags @> '"landuse"=>"industrial;retail"'::hstore;

-- Sector 3. Industrial
-- update sector
UPDATE 	openstreetmap.osm_landuse
SET  	sector = '3',
	sector_name = 'industrial'
WHERE	tags @> '"landuse"=>"industrial"'::hstore OR
		tags @> '"landuse"=>"port"'::hstore OR
		tags @> '"man_made"=>"wastewater_plant"'::hstore OR
		tags @> '"aeroway"=>"terminal"'::hstore OR
		tags @> '"aeroway"=>"gate"'::hstore OR
		tags @> '"man_made"=>"works"'::hstore;

-- Sector 4. Agricultural
-- update sector
UPDATE 	openstreetmap.osm_landuse
	SET  	sector = '4',
		sector_name = 'agricultural'
	WHERE	tags @> '"landuse"=>"farmyard"'::hstore OR
		tags @> '"landuse"=>"greenhouse_horticulture"'::hstore;

-- Drop MViews which are not of special interest for downstream tasks
DROP MATERIALIZED VIEW IF EXISTS	openstreetmap.osm_landuse_error_geom_vg250 CASCADE;
DROP MATERIALIZED VIEW IF EXISTS	openstreetmap.osm_landuse_vg250_clean_cut_multi CASCADE;
DROP MATERIALIZED VIEW IF EXISTS	openstreetmap.osm_landuse_vg250_cut CASCADE;
