/*
EHV Substation
Abstract EHV Substations of the extra high voltage level from OSM.
This script abstracts substations of the extra high voltage level from openstreetmap data. 

__copyright__   = "DLR Institute for Networked Energy Systems"
__license__     = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__         = "https://github.com/openego/data_processing/blob/master/LICENSE"
__author__      = "lukasol, C. Matke, Ludee, IlkaCu"
*/


--> WAY: Erzeuge einen VIEW aus OSM way substations:
DROP VIEW IF EXISTS	grid.egon_way_substations CASCADE;
CREATE VIEW 		grid.egon_way_substations AS
	SELECT 	openstreetmap.osm_ways.id, openstreetmap.osm_ways.tags, openstreetmap.osm_polygon.geom
	FROM 	openstreetmap.osm_ways JOIN openstreetmap.osm_polygon ON openstreetmap.osm_ways.id = openstreetmap.osm_polygon.osm_id
	WHERE 	hstore(openstreetmap.osm_ways.tags)->'power' in ('substation','sub_station','station')
	UNION
	SELECT 	openstreetmap.osm_ways.id, openstreetmap.osm_ways.tags, openstreetmap.osm_line.geom
	FROM 	openstreetmap.osm_ways JOIN openstreetmap.osm_line ON openstreetmap.osm_ways.id = openstreetmap.osm_line.osm_id
	WHERE 	hstore(openstreetmap.osm_ways.tags)->'power' in ('substation','sub_station','station');

-- 
DROP VIEW IF EXISTS 	grid.egon_way_substations_with_hoes CASCADE;
CREATE VIEW 		grid.egon_way_substations_with_hoes AS
	SELECT 	* 
	FROM 	grid.egon_way_substations
	WHERE 	'220000' = ANY( string_to_array(hstore(grid.egon_way_substations.tags)->'voltage',';')) 
		OR '380000' = ANY( string_to_array(hstore(grid.egon_way_substations.tags)->'voltage',';')); 

--> NODE: Erzeuge einen VIEW aus OSM node substations:
DROP VIEW IF EXISTS 	grid.egon_node_substations_with_hoes CASCADE;
CREATE VIEW 		grid.egon_node_substations_with_hoes AS
	SELECT 	openstreetmap.osm_nodes.id, openstreetmap.osm_point.tags, openstreetmap.osm_point.geom
	FROM 	openstreetmap.osm_nodes JOIN openstreetmap.osm_point ON openstreetmap.osm_nodes.id = openstreetmap.osm_point.osm_id
	WHERE 	'220000' = ANY( string_to_array(hstore(openstreetmap.osm_point.tags)->'voltage',';')) 
		and hstore(openstreetmap.osm_point.tags)->'power' in ('substation','sub_station','station') 
		OR '380000' = ANY( string_to_array(hstore(openstreetmap.osm_point.tags)->'voltage',';')) 
		and hstore(openstreetmap.osm_point.tags)->'power' in ('substation','sub_station','station');


--> RELATION: Erzeuge einen VIEW aus OSM relation substations:
-- Da die relations keine geometry besitzen wird mit folgender funktion der geometrische mittelpunkt der relation ermittelt, 
-- wobei hier der Mittelpunkt aus dem Rechteck ermittelt wird welches entsteht, wenn man die äußersten Korrdinaten für
-- longitude und latitude wählt
-- needs st_relation_geometry
DROP VIEW IF EXISTS 	grid.egon_relation_substations_with_hoes CASCADE;
CREATE VIEW 		grid.egon_relation_substations_with_hoes AS
	SELECT 	openstreetmap.osm_rels.id, openstreetmap.osm_rels.tags, relation_geometry(openstreetmap.osm_rels.members) as way
	FROM 	openstreetmap.osm_rels 
	WHERE 	'220000' = ANY( string_to_array(hstore(openstreetmap.osm_rels.tags)->'voltage',';')) 
		and hstore(openstreetmap.osm_rels.tags)->'power' in ('substation','sub_station','station') 
		OR '380000' = ANY( string_to_array(hstore(openstreetmap.osm_rels.tags)->'voltage',';')) 
		and hstore(openstreetmap.osm_rels.tags)->'power' in ('substation','sub_station','station');

-- 
DROP VIEW IF EXISTS 	grid.egon_substation_hoes CASCADE;
CREATE VIEW 		grid.egon_substation_hoes AS
	SELECT  *,
		'http://www.osm.org/relation/'|| grid.egon_relation_substations_with_hoes.id as osm_www,
		'r'|| grid.egon_relation_substations_with_hoes.id as osm_id,
		'1'::smallint as status
	FROM grid.egon_relation_substations_with_hoes
	UNION
	SELECT 	*,
		'http://www.osm.org/way/'|| grid.egon_way_substations_with_hoes.id as osm_www,
		'w'|| grid.egon_way_substations_with_hoes.id as osm_id,
		'2'::smallint as status
	FROM grid.egon_way_substations_with_hoes
	UNION 
	SELECT id, hstore_to_array(tags), geom,
		'http://www.osm.org/node/'|| grid.egon_node_substations_with_hoes.id as osm_www,
		'n'|| grid.egon_node_substations_with_hoes.id as osm_id,
		'4'::smallint as status
	FROM grid.egon_node_substations_with_hoes
	;


-- create view summary_total_hoes that contains substations without any filter
DROP VIEW IF EXISTS 	grid.egon_summary_total_hoes CASCADE;
CREATE VIEW 		grid.egon_summary_total_hoes AS
	SELECT  ST_X(ST_Centroid(ST_Transform(substation.way,4326))) as lon,
		ST_Y(ST_Centroid(ST_Transform(substation.way,4326))) as lat,
		ST_Centroid(ST_Transform(substation.way,4326)) as point,
		ST_Transform(substation.way,4326) as polygon,
		(CASE WHEN hstore(substation.tags)->'voltage' <> '' THEN hstore(substation.tags)->'voltage' ELSE 'hoes' END) as voltage, 
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
	FROM grid.egon_substation_hoes substation ORDER BY osm_www;


-- create view that filters irrelevant tags
DROP MATERIALIZED VIEW IF EXISTS 	grid.egon_summary_hoes CASCADE;
CREATE MATERIALIZED VIEW 		grid.egon_summary_hoes AS
	SELECT 	*
	FROM 	grid.egon_summary_total_hoes
	WHERE 	dbahn = 'no' AND substation NOT IN ('traction','transition');


-- index gist (geom)
CREATE INDEX summary_hoes_gix ON grid.egon_summary_hoes USING GIST (polygon);

-- eliminate substation that are not within VG250
DROP VIEW IF EXISTS 	grid.egon_summary_de_hoes CASCADE;
CREATE VIEW		grid.egon_summary_de_hoes AS
	SELECT 	*
	FROM 	grid.egon_summary_hoes, boundaries.vg250_sta_union as vg
	WHERE 	ST_Transform(vg.geometry,4326) && grid.egon_summary_hoes.polygon 
	AND 	ST_CONTAINS(ST_Transform(vg.geometry,4326),grid.egon_summary_hoes.polygon);

-- create view with buffer of 75m around polygons
DROP MATERIALIZED VIEW IF EXISTS 	grid.egon_buffer_75_hoes CASCADE;
CREATE MATERIALIZED VIEW 		grid.egon_buffer_75_hoes AS
	SELECT 	osm_id, ST_Area(ST_Transform(grid.egon_summary_de_hoes.polygon,4326)) as area, ST_Buffer_Meters(ST_Transform(grid.egon_summary_de_hoes.polygon,4326), 75) as buffer_75
	FROM 	grid.egon_summary_de_hoes;

-- create second view with same data to compare
DROP MATERIALIZED VIEW IF EXISTS 	grid.egon_buffer_75_a_hoes CASCADE;
CREATE MATERIALIZED VIEW 		grid.egon_buffer_75_a_hoes AS
	SELECT 	osm_id, ST_Area(ST_Transform(grid.egon_summary_de_hoes.polygon,4326)) as area_a, ST_Buffer_Meters(ST_Transform(grid.egon_summary_de_hoes.polygon,4326), 75) as buffer_75_a
	FROM 	grid.egon_summary_de_hoes;

-- create view to eliminate smaller substations where buffers intersect
DROP MATERIALIZED VIEW IF EXISTS 	grid.egon_substations_to_drop_hoes CASCADE;
CREATE MATERIALIZED VIEW 		grid.egon_substations_to_drop_hoes AS
	SELECT DISTINCT
		(CASE WHEN grid.egon_buffer_75_hoes.area < grid.egon_buffer_75_a_hoes.area_a THEN grid.egon_buffer_75_hoes.osm_id ELSE grid.egon_buffer_75_a_hoes.osm_id END) as osm_id,
		(CASE WHEN grid.egon_buffer_75_hoes.area < grid.egon_buffer_75_a_hoes.area_a THEN grid.egon_buffer_75_hoes.area ELSE grid.egon_buffer_75_a_hoes.area_a END) as area,
		(CASE WHEN grid.egon_buffer_75_hoes.area < grid.egon_buffer_75_a_hoes.area_a THEN grid.egon_buffer_75_hoes.buffer_75 ELSE grid.egon_buffer_75_a_hoes.buffer_75_a END) as buffer
	FROM 	grid.egon_buffer_75_hoes, grid.egon_buffer_75_a_hoes
	WHERE 	ST_Intersects(grid.egon_buffer_75_hoes.buffer_75, grid.egon_buffer_75_a_hoes.buffer_75_a)
		AND NOT grid.egon_buffer_75_hoes.osm_id = grid.egon_buffer_75_a_hoes.osm_id;

-- filter those substations and create final_result_hoes
DROP VIEW IF EXISTS 	grid.egon_final_result_hoes CASCADE;
CREATE VIEW 		grid.egon_final_result_hoes AS
	SELECT 	* 
	FROM 	grid.egon_summary_de_hoes
	WHERE 	grid.egon_summary_de_hoes.osm_id NOT IN ( SELECT grid.egon_substations_to_drop_hoes.osm_id FROM grid.egon_substations_to_drop_hoes);

-- insert results
INSERT INTO grid.egon_ehv_transfer_buses (lon, lat, point, polygon, voltage, power_type, substation, osm_id, osm_www, frequency, subst_name, ref, operator, dbahn, status)
	SELECT 	lon, lat, point, polygon, voltage, power_type, substation, osm_id, osm_www, frequency, subst_name, ref, operator, dbahn, status
	FROM 	grid.egon_final_result_hoes;


-- drop
DROP VIEW IF EXISTS grid.egon_final_result_hoes CASCADE;
DROP MATERIALIZED VIEW IF EXISTS grid.egon_substations_to_drop_hoes CASCADE;
DROP MATERIALIZED VIEW IF EXISTS grid.egon_buffer_75_hoes CASCADE;
DROP MATERIALIZED VIEW IF EXISTS grid.egon_buffer_75_a_hoes CASCADE;
DROP VIEW IF EXISTS grid.egon_summary_de_hoes CASCADE;
DROP MATERIALIZED VIEW IF EXISTS grid.egon_summary_hoes CASCADE;
DROP VIEW IF EXISTS grid.egon_summary_total_hoes CASCADE;
DROP VIEW IF EXISTS grid.egon_substation_hoes CASCADE;
DROP VIEW IF EXISTS grid.egon_relation_substations_with_hoes CASCADE;
DROP VIEW IF EXISTS grid.egon_node_substations_with_hoes CASCADE;
DROP VIEW IF EXISTS grid.egon_way_substations_with_hoes CASCADE;
DROP VIEW IF EXISTS grid.egon_way_substations CASCADE;
