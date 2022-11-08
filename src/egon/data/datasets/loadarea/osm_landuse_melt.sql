/*
OSM Loads from landuse
Excludes large scale consumer.
Buffer OSM urban sectors with 100m
Unbuffer buffer with -100m

__copyright__   = "Reiner Lemoine Institut"
__license__     = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__         = "https://github.com/openego/data_processing/blob/master/LICENSE"
__author__      = "Ludee, nesnoj"
*/


-- 28.10.22: CHECK IF NECESSARY!!!
-- exclude large scale consumer
--DELETE FROM openstreetmap.osm_landuse
--	WHERE gid IN (SELECT polygon_id FROM model_draft.egon_demand_hv_largescaleconsumer);


-- sequence
DROP SEQUENCE IF EXISTS 	openstreetmap.osm_landuse_buffer100_mview_id CASCADE;
CREATE SEQUENCE 		openstreetmap.osm_landuse_buffer100_mview_id;

-- buffer with 100m
DROP MATERIALIZED VIEW IF EXISTS	openstreetmap.osm_landuse_buffer100_mview CASCADE;
CREATE MATERIALIZED VIEW		openstreetmap.osm_landuse_buffer100_mview AS
	SELECT	 nextval('openstreetmap.osm_landuse_buffer100_mview_id') ::integer AS id,
		(ST_DUMP(ST_MULTI(ST_UNION(
			ST_BUFFER(geom, 100)
		)))).geom ::geometry(Polygon,3035) AS geom
	FROM	openstreetmap.osm_landuse;

-- index (id)
CREATE UNIQUE INDEX  	osm_landuse_buffer100_mview_gid_idx
	ON	openstreetmap.osm_landuse_buffer100_mview (id);

-- index GIST (geom)
CREATE INDEX  	osm_landuse_buffer100_mview_geom_idx
	ON	openstreetmap.osm_landuse_buffer100_mview USING GIST (geom);


-- unbuffer with 100m
DROP TABLE IF EXISTS  	openstreetmap.osm_landuse_melted CASCADE;
CREATE TABLE         	openstreetmap.osm_landuse_melted (
	id SERIAL NOT NULL,
	area_ha double precision,
	geom geometry(Polygon,3035),
	CONSTRAINT osm_landuse_melted_pkey PRIMARY KEY (id));

-- insert buffer
INSERT INTO     openstreetmap.osm_landuse_melted(area_ha,geom)
    SELECT  ST_AREA(buffer.geom)/10000 ::double precision AS area_ha,
            buffer.geom ::geometry(Polygon,3035) AS geom
    FROM    (SELECT (ST_DUMP(ST_MULTI(ST_UNION(
                        ST_BUFFER(osm.geom, -100)
                    )))).geom ::geometry(Polygon,3035) AS geom
            FROM    openstreetmap.osm_landuse_buffer100_mview AS osm
--            ORDER BY id
            ) AS buffer;

-- index GIST (geom)
CREATE INDEX  	osm_landuse_melted_geom_idx
	ON    	openstreetmap.osm_landuse_melted USING GIST (geom);


DROP MATERIALIZED VIEW IF EXISTS openstreetmap.osm_landuse_buffer100_mview CASCADE;
