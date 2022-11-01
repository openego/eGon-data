/*
Loads from Census 2011
Include Census 2011 population per ha.
Identify population in OSM loads.
Include Census cells with CTS demand.

__copyright__   = "Reiner Lemoine Institut"
__license__     = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__         = "https://github.com/openego/data_processing/blob/master/LICENSE"
__author__      = "Ludee, nesnoj"
*/


-- zensus load
DROP TABLE IF EXISTS  	society.egon_destatis_zensus_cells_melted CASCADE;
CREATE TABLE         	society.egon_destatis_zensus_cells_melted (
	id 		SERIAL NOT NULL,
	gid 		integer,
	population 	integer,
	inside_la 	boolean,
	geom_point 	geometry(Point,3035),
	geom 		geometry(Polygon,3035),
	CONSTRAINT egon_destatis_zensus_cells_melted_pkey PRIMARY KEY (id));

-- insert zensus loads
INSERT INTO	society.egon_destatis_zensus_cells_melted (gid,population,inside_la,geom_point,geom)
	SELECT	id ::integer AS gid,
		population ::integer,
		'FALSE' ::boolean AS inside_la,
		geom_point ::geometry(Point,3035),
		geom ::geometry(Polygon,3035)
	--FROM	model_draft.destatis_zensus_population_per_ha_invg_mview
	FROM	society.destatis_zensus_population_per_ha_inside_germany
    ORDER BY gid;

-- zensus cells with CTS loads
INSERT INTO	society.egon_destatis_zensus_cells_melted (gid,population,inside_la,geom_point,geom)
    SELECT
        id ::integer AS gid,
        0 AS population,
        'FALSE' ::boolean AS inside_la,
        geom_point ::geometry(Point,3035),
        geom ::geometry(Polygon,3035)
    FROM society.destatis_zensus_population_per_ha
    WHERE id in (
        SELECT DISTINCT zensus_population_id
        FROM demand.egon_demandregio_zensus_electricity
        WHERE scenario = 'eGon2035' AND sector = 'service'
    )
    ORDER BY gid;

-- index gist (geom_point)
CREATE INDEX  	egon_destatis_zensus_cells_melted_geom_point_idx
	ON	society.egon_destatis_zensus_cells_melted USING GIST (geom_point);

-- index gist (geom)
CREATE INDEX  	egon_destatis_zensus_cells_melted_geom_idx
	ON	society.egon_destatis_zensus_cells_melted USING GIST (geom);

-- population in osm loads
UPDATE 	society.egon_destatis_zensus_cells_melted AS t1
	SET  	inside_la = t2.inside_la
	FROM    (
		SELECT	zensus.id AS id,
			'TRUE' ::boolean AS inside_la
		FROM	society.egon_destatis_zensus_cells_melted AS zensus,
			openstreetmap.osm_landuse_melted AS osm
		WHERE  	osm.geom && zensus.geom_point AND
			ST_CONTAINS(osm.geom,zensus.geom_point)
		) AS t2
	WHERE  	t1.id = t2.id;

-- remove identified population
DELETE FROM	society.egon_destatis_zensus_cells_melted AS lp
	WHERE	lp.inside_la IS TRUE;



-- cluster from zensus load lattice
DROP TABLE IF EXISTS	society.egon_destatis_zensus_cells_melted_cluster CASCADE;
CREATE TABLE         	society.egon_destatis_zensus_cells_melted_cluster (
	cid serial,
	zensus_sum INT,
	area_ha INT,
	geom geometry(Polygon,3035),
	geom_buffer geometry(Polygon,3035),
	geom_centroid geometry(Point,3035),
	geom_surfacepoint geometry(Point,3035),
	CONSTRAINT egon_destatis_zensus_cells_melted_cluster_pkey PRIMARY KEY (cid));

-- insert cluster
INSERT INTO	society.egon_destatis_zensus_cells_melted_cluster(geom)
	SELECT	(ST_DUMP(ST_MULTI(ST_UNION(geom)))).geom ::geometry(Polygon,3035)
	FROM    society.egon_destatis_zensus_cells_melted;
--    ORDER BY gid;

-- index gist (geom)
CREATE INDEX egon_destatis_zensus_cells_melted_cluster_geom_idx
    ON society.egon_destatis_zensus_cells_melted_cluster USING GIST (geom);

-- index gist (geom_centroid)
CREATE INDEX egon_destatis_zensus_cells_melted_cluster_geom_centroid_idx
    ON society.egon_destatis_zensus_cells_melted_cluster USING GIST (geom_centroid);

-- index gist (geom_surfacepoint)
CREATE INDEX egon_destatis_zensus_cells_melted_cluster_geom_surfacepoint_idx
    ON society.egon_destatis_zensus_cells_melted_cluster USING GIST (geom_surfacepoint);

-- insert cluster
INSERT INTO society.egon_destatis_zensus_cells_melted_cluster(geom)
    SELECT  (ST_DUMP(ST_MULTI(ST_UNION(grid.geom)))).geom ::geometry(Polygon,3035) AS geom
    FROM    society.egon_destatis_zensus_cells_melted AS grid;

-- cluster data
UPDATE society.egon_destatis_zensus_cells_melted_cluster AS t1
    SET zensus_sum = t2.zensus_sum,
        area_ha = t2.area_ha,
        geom_buffer = t2.geom_buffer,
        geom_centroid = t2.geom_centroid,
        geom_surfacepoint = t2.geom_surfacepoint
    FROM    (
        SELECT  cl.cid AS cid,
                SUM(lp.population) AS zensus_sum,
                COUNT(lp.geom) AS area_ha,
                ST_BUFFER(cl.geom, 100) AS geom_buffer,
                ST_Centroid(cl.geom) AS geom_centroid,
                ST_PointOnSurface(cl.geom) AS geom_surfacepoint
        FROM    society.egon_destatis_zensus_cells_melted AS lp,
                society.egon_destatis_zensus_cells_melted_cluster AS cl
        WHERE   cl.geom && lp.geom AND
                ST_CONTAINS(cl.geom,lp.geom)
        GROUP BY cl.cid
        ORDER BY cl.cid
        ) AS t2
    WHERE   t1.cid = t2.cid;


-- zensus stats
DROP MATERIALIZED VIEW IF EXISTS	openstreetmap.ego_society_zensus_per_la_mview CASCADE;
CREATE MATERIALIZED VIEW         	openstreetmap.ego_society_zensus_per_la_mview AS
-- 	SELECT 	'destatis_zensus_population_per_ha_mview' AS name,
-- 		sum(population),
-- 		count(geom) AS census_count
-- 	FROM	openstreetmap.destatis_zensus_population_per_ha_mview
-- 	UNION ALL
	SELECT 	'destatis_zensus_population_per_ha_inside_germany' AS name,
		sum(population),
		count(geom) AS census_count
	FROM	society.destatis_zensus_population_per_ha_inside_germany
	UNION ALL
	SELECT 	'egon_destatis_zensus_cells_melted' AS name,
		sum(population),
		count(geom) AS census_count
	FROM	society.egon_destatis_zensus_cells_melted
	UNION ALL
	SELECT 	'egon_destatis_zensus_cells_melted_cluster' AS name,
		sum(zensus_sum),
		count(geom) AS census_count
	FROM	society.egon_destatis_zensus_cells_melted_cluster;
