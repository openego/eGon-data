/*
HVMV Substation
Abstract HVMV Substations of the high voltage level from OSM.
This script abstracts substations of the high voltage level from openstreetmap data.
All substations that are relevant transition points between the transmission and distribution grid are identified, irrelevant ones are disregarded.

__copyright__   = "DLR Institute for Networked Energy Systems"
__license__     = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__         = "https://github.com/openego/data_processing/blob/master/LICENSE"
__author__      = "lukasol, C. Matke, Ludee, ilkacu"
*/


--> WAY: create view of way substations:
DROP VIEW IF EXISTS 	grid.egon_way_substations CASCADE;
CREATE VIEW 		grid.egon_way_substations AS
	SELECT 	openstreetmap.osm_ways.id, openstreetmap.osm_ways.tags, openstreetmap.osm_polygon.geom
	FROM 	openstreetmap.osm_ways JOIN openstreetmap.osm_polygon ON openstreetmap.osm_ways.id = openstreetmap.osm_polygon.osm_id
	WHERE 	hstore(openstreetmap.osm_ways.tags)->'power' in ('substation','sub_station','station')
	UNION 
	SELECT 	openstreetmap.osm_ways.id, openstreetmap.osm_ways.tags, openstreetmap.osm_line.geom
	FROM 	openstreetmap.osm_ways JOIN openstreetmap.osm_line ON openstreetmap.osm_ways.id = openstreetmap.osm_line.osm_id
	WHERE 	hstore(openstreetmap.osm_ways.tags)->'power' in ('substation','sub_station','station');


--> WAY: create view of way substations with 110kV:
DROP VIEW IF EXISTS	grid.egon_way_substations_with_110kV CASCADE;
CREATE VIEW 		grid.egon_way_substations_with_110kV AS
	SELECT 	* 
	FROM 	grid.egon_way_substations
	WHERE 	'110000' = ANY( string_to_array(hstore(grid.egon_way_substations.tags)->'voltage',';')) 
		OR '60000' = ANY( string_to_array(hstore(grid.egon_way_substations.tags)->'voltage',';'));


--> WAY: create view of substations without 110kV
DROP VIEW IF EXISTS 	grid.egon_way_substations_without_110kV CASCADE;
CREATE VIEW 		grid.egon_way_substations_without_110kV AS
	SELECT 	* 
	FROM 	grid.egon_way_substations
	WHERE 	not '110000' = ANY( string_to_array(hstore(grid.egon_way_substations.tags)->'voltage',';')) 
		AND not '60000' = ANY( string_to_array(hstore(grid.egon_way_substations.tags)->'voltage',';')) 
		OR not exist(hstore(grid.egon_way_substations.tags),'voltage');


--> NODE: create view of 110kV node substations:
DROP VIEW IF EXISTS 	grid.egon_node_substations_with_110kV CASCADE;
CREATE VIEW 		grid.egon_node_substations_with_110kV AS
	SELECT 	openstreetmap.osm_nodes.id, openstreetmap.osm_point.tags, openstreetmap.osm_point.geom
	FROM 	openstreetmap.osm_nodes JOIN openstreetmap.osm_point ON openstreetmap.osm_nodes.id = openstreetmap.osm_point.osm_id
	WHERE 	'110000' = ANY( string_to_array(hstore(openstreetmap.osm_point.tags)->'voltage',';')) 
		and hstore(openstreetmap.osm_point.tags)->'power' in ('substation','sub_station','station') 
		OR '60000' = ANY( string_to_array(hstore(openstreetmap.osm_point.tags)->'voltage',';')) 
		and hstore(openstreetmap.osm_point.tags)->'power' in ('substation','sub_station','station');


--> LINES 110kV: create view of 110kV lines
DROP VIEW IF EXISTS 	grid.egon_way_lines_110kV CASCADE;
CREATE VIEW 		grid.egon_way_lines_110kV AS
	SELECT 	openstreetmap.osm_ways.id, openstreetmap.osm_ways.tags, openstreetmap.osm_line.geom 
	FROM 	openstreetmap.osm_ways JOIN openstreetmap.osm_line ON openstreetmap.osm_ways.id = openstreetmap.osm_line.osm_id
	WHERE 	'110000' = ANY( string_to_array(hstore(openstreetmap.osm_ways.tags)->'voltage',';')) 
		AND NOT hstore(openstreetmap.osm_ways.tags)->'power' in ('minor_line','razed','dismantled:line','historic:line','construction','planned','proposed','abandoned:line','sub_station','abandoned','substation') 
		OR '60000' = ANY( string_to_array(hstore(openstreetmap.osm_ways.tags)->'voltage',';')) 
		AND NOT hstore(openstreetmap.osm_ways.tags)->'power' in ('minor_line','razed','dismantled:line','historic:line','construction','planned','proposed','abandoned:line','sub_station','abandoned','substation');


-- INTERSECTION: create view from substations without 110kV tag that contain 110kV line
DROP VIEW IF EXISTS 	grid.egon_way_substations_without_110kV_intersected_by_110kV_line CASCADE;
CREATE VIEW 		grid.egon_way_substations_without_110kV_intersected_by_110kV_line AS
	SELECT DISTINCT grid.egon_way_substations_without_110kV.* 
	FROM 	grid.egon_way_substations_without_110kV, grid.egon_way_lines_110kV
	WHERE 	ST_Contains(grid.egon_way_substations_without_110kV.geom,ST_StartPoint(grid.egon_way_lines_110kV.geom)) 
		or ST_Contains(grid.egon_way_substations_without_110kV.geom,ST_EndPoint(grid.egon_way_lines_110kV.geom));


-- 
DROP VIEW IF EXISTS 	grid.egon_substation_110kV CASCADE;
CREATE VIEW 		grid.egon_substation_110kV AS
	SELECT 	*,
		'http://www.osm.org/way/'|| grid.egon_way_substations_with_110kV.id as osm_www,
		'w'|| grid.egon_way_substations_with_110kV.id as osm_id,
		'1'::smallint as status
	FROM grid.egon_way_substations_with_110kV
	UNION 
	SELECT *,
		'http://www.osm.org/way/'|| grid.egon_way_substations_without_110kV_intersected_by_110kV_line.id as osm_www,
		'w'|| grid.egon_way_substations_without_110kV_intersected_by_110kV_line.id as osm_id,
		'2'::smallint as status
	FROM grid.egon_way_substations_without_110kV_intersected_by_110kV_line
	UNION 
	SELECT id, hstore_to_array(tags), geom,
		'http://www.osm.org/node/'|| grid.egon_node_substations_with_110kV.id as osm_www,
		'n'|| grid.egon_node_substations_with_110kV.id as osm_id,
		'3'::smallint as status
	FROM grid.egon_node_substations_with_110kV
	;


-- create view summary_total that contains substations without any filter
DROP VIEW IF EXISTS 	grid.egon_summary_total CASCADE;
CREATE VIEW 		grid.egon_summary_total AS
	SELECT  ST_X(ST_Centroid(ST_Transform(substation.geom,4326))) as lon,
		ST_Y(ST_Centroid(ST_Transform(substation.geom,4326))) as lat,
		ST_Centroid(ST_Transform(substation.geom,4326)) as point,
		ST_Transform(substation.geom,4326) as polygon,
		(CASE WHEN hstore(substation.tags)->'voltage' <> '' THEN hstore(substation.tags)->'voltage' ELSE '110000' END) as voltage, 
		hstore(substation.tags)->'power' as power_type, 
		(CASE WHEN hstore(substation.tags)->'substation' <> '' THEN hstore(substation.tags)->'substation' ELSE 'NA' END) as substation, 
		substation.osm_id as osm_id,
		osm_www,
		(CASE WHEN hstore(substation.tags)->'frequency' <> '' THEN hstore(substation.tags)->'frequency' ELSE 'NA' END) as frequency,
		(CASE WHEN hstore(substation.tags)->'name' <> '' THEN hstore(substation.tags)->'name' ELSE 'NA' END) as subst_name, 
		(CASE WHEN hstore(substation.tags)->'ref' <> '' THEN hstore(substation.tags)->'ref' ELSE 'NA' END) as ref, 
		(CASE WHEN hstore(substation.tags)->'operator' <> '' THEN hstore(substation.tags)->'operator' ELSE 'NA' END) as operator, 
		(CASE WHEN hstore(substation.tags)->'operator' in ('DB_Energie','DB Netz AG','DB Energie GmbH','DB Netz')
		      THEN 'see operator' 
		      ELSE (CASE WHEN '16.7' = ANY( string_to_array(hstore(substation.tags)->'frequency',';')) or '16.67' = ANY( string_to_array(hstore(substation.tags)->'frequency',';')) 
				 THEN 'see frequency' 
				 ELSE 'no' 
				 END)
		      END) as dbahn,
		status
	FROM grid.egon_substation_110kV substation ORDER BY osm_www;

	
-- create view that filters irrelevant tags
DROP MATERIALIZED VIEW IF EXISTS 	grid.egon_summary CASCADE;
CREATE MATERIALIZED VIEW 		grid.egon_summary AS
	SELECT 	*
	FROM 	grid.egon_summary_total
	WHERE 	dbahn = 'no' AND substation NOT IN ('traction','transition');

-- index gist (geom)
CREATE INDEX summary_gix ON grid.egon_summary USING GIST (polygon);


-- eliminate substation that are not within VG250
DROP VIEW IF EXISTS 	grid.egon_summary_de CASCADE;
CREATE VIEW 		grid.egon_summary_de AS
	SELECT 	*
	FROM	grid.egon_summary, boundaries.vg250_sta_union as vg
	WHERE 	ST_Transform(vg.geometry,4326) && grid.egon_summary.polygon 
	AND 	ST_CONTAINS(ST_Transform(vg.geometry,4326),grid.egon_summary.polygon);


-- create view with buffer of 75m around polygons
DROP MATERIALIZED VIEW IF EXISTS 	grid.egon_buffer_75 CASCADE;
CREATE MATERIALIZED VIEW 		grid.egon_buffer_75 AS
	SELECT osm_id, ST_Area(ST_Transform(grid.egon_summary_de.polygon,4326)) as area, ST_Buffer_Meters(ST_Transform(grid.egon_summary_de.polygon,4326), 75) as buffer_75
	FROM grid.egon_summary_de;


-- create second view with same data to compare
DROP MATERIALIZED VIEW IF EXISTS 	grid.egon_buffer_75_a CASCADE;
CREATE MATERIALIZED VIEW 		grid.egon_buffer_75_a AS
	SELECT osm_id, ST_Area(ST_Transform(grid.egon_summary_de.polygon,4326)) as area_a, ST_Buffer_Meters(ST_Transform(grid.egon_summary_de.polygon,4326), 75) as buffer_75_a
	FROM grid.egon_summary_de;


-- create view to eliminate smaller substations where buffers intersect
DROP MATERIALIZED VIEW IF EXISTS 	grid.egon_substations_to_drop CASCADE;
CREATE MATERIALIZED VIEW 		grid.egon_substations_to_drop AS
	SELECT DISTINCT
	(CASE WHEN grid.egon_buffer_75.area < grid.egon_buffer_75_a.area_a THEN grid.egon_buffer_75.osm_id ELSE grid.egon_buffer_75_a.osm_id END) as osm_id,
	(CASE WHEN grid.egon_buffer_75.area < grid.egon_buffer_75_a.area_a THEN grid.egon_buffer_75.area ELSE grid.egon_buffer_75_a.area_a END) as area,
	(CASE WHEN grid.egon_buffer_75.area < grid.egon_buffer_75_a.area_a THEN grid.egon_buffer_75.buffer_75 ELSE grid.egon_buffer_75_a.buffer_75_a END) as buffer
	FROM grid.egon_buffer_75, grid.egon_buffer_75_a
	WHERE ST_Intersects(grid.egon_buffer_75.buffer_75, grid.egon_buffer_75_a.buffer_75_a)
	AND NOT grid.egon_buffer_75.osm_id = grid.egon_buffer_75_a.osm_id;


-- filter those substations and create final_result
DROP VIEW IF EXISTS 	grid.egon_final_result CASCADE;
CREATE VIEW 		grid.egon_final_result AS
	SELECT * 
	FROM grid.egon_summary_de
	WHERE grid.egon_summary_de.osm_id NOT IN ( SELECT grid.egon_substations_to_drop.osm_id FROM grid.egon_substations_to_drop);


-- insert results
INSERT INTO grid.egon_hvmv_transfer_buses (lon, lat, point, polygon, voltage, power_type, substation, osm_id, osm_www, frequency, subst_name, ref, operator, dbahn, status)
	SELECT lon, lat, point, polygon, voltage, power_type, substation, osm_id, osm_www, frequency, subst_name, ref, operator, dbahn, status
	FROM grid.egon_final_result;

-- update voltage level if split by '/' instead of ';' or contains '--'
UPDATE grid.egon_hvmv_transfer_buses
SET voltage = (SELECT REPLACE (voltage, '/', ';'))
WHERE voltage LIKE '%/%';

UPDATE grid.egon_hvmv_transfer_buses
SET voltage = (SELECT REPLACE (voltage, '-', ''))
WHERE voltage LIKE '%-%';

-- drop
DROP VIEW IF EXISTS grid.egon_final_result CASCADE;
DROP MATERIALIZED VIEW IF EXISTS grid.egon_substations_to_drop CASCADE;
DROP MATERIALIZED VIEW IF EXISTS grid.egon_buffer_75 CASCADE;
DROP MATERIALIZED VIEW IF EXISTS grid.egon_buffer_75_a CASCADE;
DROP VIEW IF EXISTS grid.egon_summary_de CASCADE;
DROP MATERIALIZED VIEW IF EXISTS grid.egon_summary CASCADE;
DROP VIEW IF EXISTS grid.egon_summary_total CASCADE;
DROP VIEW IF EXISTS grid.egon_substation_110kV CASCADE;
DROP VIEW IF EXISTS grid.egon_way_substations_without_110kV_intersected_by_110kV_line CASCADE;
DROP VIEW IF EXISTS grid.egon_way_lines_110kV CASCADE;
DROP VIEW IF EXISTS grid.egon_node_substations_with_110kV CASCADE;
DROP VIEW IF EXISTS grid.egon_way_substations_without_110kV CASCADE;
DROP VIEW IF EXISTS grid.egon_way_substations_with_110kV CASCADE;
DROP VIEW IF EXISTS grid.egon_way_substations CASCADE;
