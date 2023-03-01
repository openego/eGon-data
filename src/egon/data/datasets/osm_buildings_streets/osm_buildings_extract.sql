/*
 * Original Autor: IsGut (johnrobert@t-online.de)
 * Adapted by: nesnoj (jonathan.amme@rl-institut.de)
*/

-------------------------------------------------------------
-- extract all buildings, calculate area, create centroids --
-------------------------------------------------------------
DROP TABLE IF EXISTS openstreetmap.osm_buildings;
CREATE TABLE openstreetmap.osm_buildings AS
    SELECT
        poly.osm_id,
        poly.amenity,
        poly.building,
        poly.name,
        ST_TRANSFORM(poly.geom, 3035) AS geom_building,
        ST_AREA(ST_TRANSFORM(poly.geom, 3035)) AS area,
        ST_TRANSFORM(ST_CENTROID(poly.geom), 3035) AS geom_point,
        poly.tags
    FROM openstreetmap.osm_polygon poly
    WHERE poly.building IS NOT NULL;

-- add PK as some osm ids are not unique
ALTER TABLE openstreetmap.osm_buildings ADD COLUMN id SERIAL PRIMARY KEY;

CREATE INDEX ON openstreetmap.osm_buildings USING gist (geom_building);
CREATE INDEX ON openstreetmap.osm_buildings USING gist (geom_point);
