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
__url__         = "https://github.com/openego/data_processing/blob/master/LICENSE"
__author__      = "Ludee, IlkaCu"
*/


---------- ---------- ----------
-- Filter OSM Urban Landuse
---------- ---------- ----------

DELETE FROM openstreetmap.osm_polygon_urban; 

-- filter urban

INSERT INTO            openstreetmap.osm_polygon_urban
    SELECT  osm.gid ::integer AS gid,
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
    ORDER BY    osm.gid;
    
-- index GIST (geom)

DROP INDEX IF EXISTS osm_deu_polygon_urban_geom_idx; 

CREATE INDEX osm_deu_polygon_urban_geom_idx
    ON openstreetmap.osm_deu_polygon_urban USING GIST (geom);


-- OSM Urban Landuse Inside vg250

-- Calculate 'inside' vg250
UPDATE 	openstreetmap.osm_polygon_urban AS t1
	SET  	vg250 = t2.vg250
	FROM    (
		SELECT	osm.gid AS gid,
			'inside' ::text AS vg250
		FROM	boundaries.vg250_sta_union AS vg,
			openstreetmap.osm_polygon_urban AS osm
		WHERE  	vg.geometry && osm.geom AND
			ST_CONTAINS(vg.geometry,osm.geom)
		) AS t2
	WHERE  	t1.gid = t2.gid;

-- Calculate 'crossing' vg250
UPDATE 	openstreetmap.osm_polygon_urban AS t1
	SET  	vg250 = t2.vg250
	FROM    (
		SELECT	osm.gid AS gid,
			'crossing' ::text AS vg250
		FROM	boundaries.vg250_sta_union AS vg,
			openstreetmap.osm_polygon_urban AS osm
		WHERE  	osm.vg250 = 'outside' AND
			vg.geometry && osm.geom AND
			ST_Overlaps(vg.geometry,osm.geom)
		) AS t2
	WHERE  	t1.gid = t2.gid;


-- OSM outside of vg250

-- OSM error vg250
DROP MATERIALIZED VIEW IF EXISTS	openstreetmap.osm_polygon_urban_error_geom_vg250 CASCADE;
CREATE MATERIALIZED VIEW		openstreetmap.osm_polygon_urban_error_geom_vg250 AS
	SELECT	osm.*
	FROM	openstreetmap.osm_polygon_urban AS osm
	WHERE	osm.vg250 = 'outside' OR osm.vg250 = 'crossing';

-- index (id)
CREATE UNIQUE INDEX  	osm_polygon_urban_error_geom_vg250_gid_idx
		ON	openstreetmap.osm_polygon_urban_error_geom_vg250 (gid);

-- index GIST (geom)
CREATE INDEX  	osm_polygon_urban_error_geom_vg250_geom_idx
	ON	openstreetmap.osm_polygon_urban_error_geom_vg250
	USING	GIST (geom);

-- Sequence
DROP SEQUENCE IF EXISTS 	openstreetmap.osm_polygon_urban_vg250_cut_id CASCADE;
CREATE SEQUENCE 		openstreetmap.osm_polygon_urban_vg250_cut_id;

-- Cutting 'crossing' with vg250
DROP MATERIALIZED VIEW IF EXISTS	openstreetmap.osm_polygon_urban_vg250_cut;
CREATE MATERIALIZED VIEW		openstreetmap.osm_polygon_urban_vg250_cut AS
	SELECT	nextval('openstreetmap.osm_polygon_urban_vg250_cut_id') ::integer AS id,
		cut.gid ::integer AS gid,
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
		FROM	openstreetmap.osm_polygon_urban_error_geom_vg250 AS poly,
			boundaries.vg250_sta_union AS cut
		WHERE	poly.vg250 = 'crossing'
		) AS cut
	ORDER BY 	cut.gid;

-- index (id)
CREATE UNIQUE INDEX  	osm_polygon_urban_vg250_cut_gid_idx
		ON	openstreetmap.osm_polygon_urban_vg250_cut (gid);

-- index GIST (geom)
CREATE INDEX  	osm_polygon_urban_vg250_cut_geom_idx
	ON	openstreetmap.osm_polygon_urban_vg250_cut
	USING	GIST (geom);

-- 'crossing' Polygon to MultiPolygon
DROP MATERIALIZED VIEW IF EXISTS	openstreetmap.osm_polygon_urban_vg250_clean_cut_multi;
CREATE MATERIALIZED VIEW		openstreetmap.osm_polygon_urban_vg250_clean_cut_multi AS
	SELECT	nextval('openstreetmap.osm_polygon_gid_seq'::regclass) ::integer AS gid,
		cut.osm_id ::integer AS osm_id,
		cut.name ::text AS name,
		cut.sector ::integer AS sector,
		cut.sector_name::text AS sector_name,
		cut.area_ha ::double precision AS area_ha,
		cut.tags ::hstore AS tags,
		cut.vg250 ::text AS vg250,
		ST_MULTI(cut.geom) ::geometry(MultiPolygon,3035) AS geom
	FROM	openstreetmap.osm_polygon_urban_vg250_cut AS cut
	WHERE	cut.geom_type = 'POLYGON';

-- index (id)
CREATE UNIQUE INDEX  	osm_polygon_urban_vg250_clean_cut_multi_gid_idx
		ON	openstreetmap.osm_polygon_urban_vg250_clean_cut_multi (gid);

-- index GIST (geom)
CREATE INDEX  	osm_polygon_urban_vg250_clean_cut_multi_geom_idx
	ON	openstreetmap.osm_polygon_urban_vg250_clean_cut_multi
	USING	GIST (geom);

---------- ---------- ----------

-- clean cut
DROP MATERIALIZED VIEW IF EXISTS	openstreetmap.osm_polygon_urban_vg250_clean_cut;
CREATE MATERIALIZED VIEW		openstreetmap.osm_polygon_urban_vg250_clean_cut AS
	SELECT	nextval('openstreetmap.osm_polygon_gid_seq'::regclass) ::integer AS gid,
		cut.osm_id ::integer AS osm_id,
		cut.name ::text AS name,
		cut.sector ::integer AS sector,
		cut.sector_name::text AS sector_name, 
		cut.area_ha ::double precision AS area_ha,
		cut.tags ::hstore AS tags,
		cut.vg250 ::text AS vg250,
		cut.geom ::geometry(MultiPolygon,3035) AS geom
	FROM	openstreetmap.osm_polygon_urban_vg250_cut AS cut
	WHERE	cut.geom_type = 'MULTIPOLYGON';

-- index (id)
CREATE UNIQUE INDEX  	osm_polygon_urban_vg250_clean_cut_gid_idx
		ON	openstreetmap.osm_polygon_urban_vg250_clean_cut (gid);


-- remove 'outside' vg250
DELETE FROM	openstreetmap.osm_polygon_urban AS osm
	WHERE	osm.vg250 = 'outside';

-- remove 'outside' vg250
DELETE FROM	openstreetmap.osm_polygon_urban AS osm
	WHERE	osm.vg250 = 'crossing';

-- insert cut
INSERT INTO	openstreetmap.osm_polygon_urban
	SELECT	clean.*
	FROM	openstreetmap.osm_polygon_urban_vg250_clean_cut AS clean
	ORDER BY 	clean.gid;

-- insert cut multi
INSERT INTO	openstreetmap.osm_polygon_urban
	SELECT	clean.*
	FROM	openstreetmap.osm_polygon_urban_vg250_clean_cut_multi AS clean
	ORDER BY 	clean.gid;


---------- ---------- ----------
-- Update Sector Information 
---------- ---------- ----------

-- Sector 1. Residential
-- update sector
UPDATE 	openstreetmap.osm_polygon_urban
SET  	sector = '1', 
	sector_name = 'residential'
WHERE	tags @> '"landuse"=>"residential"'::hstore;

-- Sector 2. Retail
-- update sector
UPDATE 	openstreetmap.osm_polygon_urban
SET  	sector = '2', 
	sector_name = 'retail'
WHERE	tags @> '"landuse"=>"commercial"'::hstore OR 
		tags @> '"landuse"=>"retail"'::hstore OR 
		tags @> '"landuse"=>"industrial;retail"'::hstore;
	
-- Sector 3. Industrial
-- update sector
UPDATE 	openstreetmap.osm_polygon_urban
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
UPDATE 	openstreetmap.osm_polygon_urban
	SET  	sector = '4', 
		sector_name = 'agricultural'
	WHERE	tags @> '"landuse"=>"farmyard"'::hstore OR 
		tags @> '"landuse"=>"greenhouse_horticulture"'::hstore;

-- Drop MViews which are not of special interest for downstream tasks 	
DROP MATERIALIZED VIEW IF EXISTS	openstreetmap.osm_polygon_urban_error_geom_vg250 CASCADE;
DROP MATERIALIZED VIEW IF EXISTS	openstreetmap.osm_polygon_urban_vg250_clean_cut_multi CASCADE;
DROP MATERIALIZED VIEW IF EXISTS	openstreetmap.osm_polygon_urban_vg250_cut CASCADE;

