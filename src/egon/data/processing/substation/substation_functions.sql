-- Function: utmzone(geometry)
-- source: http://www.gistutor.com/postgresqlpostgis/6-advanced-postgresqlpostgis-tutorials/58-postgis-buffer-latlong-and-other-projections-using-meters-units-custom-stbuffermeters-function.html
-- Usage: SELECT ST_Transform(the_geom, utmzone(ST_Centroid(the_geom))) FROM sometable;
DROP FUNCTION IF EXISTS utmzone(geometry);
CREATE OR REPLACE FUNCTION utmzone(geometry)
RETURNS integer AS
$BODY$
DECLARE
geomgeog geometry;
zone int;
pref int;
 
BEGIN
geomgeog:= ST_Transform($1,4326);
 
IF (ST_Y(geomgeog))>0 THEN
pref:=32600;
ELSE
pref:=32700;
END IF;
 
zone:=floor((ST_X(geomgeog)+180)/6)+1;
 
RETURN zone+pref;
END;
$BODY$ LANGUAGE 'plpgsql' IMMUTABLE
COST 100;


-- Function: relation_geometry
-- creates a geometry point from relation parts of type way
DROP FUNCTION IF EXISTS relation_geometry (members text[]) CASCADE;
CREATE OR REPLACE FUNCTION relation_geometry (members text[]) 
RETURNS geometry 
AS $$
DECLARE 
way  geometry;
BEGIN
   way = (SELECT ST_SetSRID(ST_MakePoint((max(lon) + min(lon))/200.0,(max(lat) + min(lat))/200.0),900913) 
	  FROM openstreetmap.osm_deu_nodes 
	  WHERE id in (SELECT unnest(nodes) 
             FROM openstreetmap.osm_deu_ways 
             WHERE id in (SELECT trim(leading 'w' from member)::bigint 
			  FROM (SELECT unnest(members) as member) t
	                  WHERE member~E'[w,1,2,3,4,5,6,7,8,9,0]')));        
RETURN way;
END;
$$ LANGUAGE plpgsql;

-- Function: ST_Buffer_Meters(geometry, double precision)
-- Usage: SELECT ST_Buffer_Meters(the_geom, num_meters) FROM sometable; 
DROP FUNCTION IF EXISTS ST_Buffer_Meters(geometry, double precision);
CREATE OR REPLACE FUNCTION ST_Buffer_Meters(geometry, double precision)
RETURNS geometry AS
$BODY$
DECLARE
orig_srid int;
utm_srid int;
 
BEGIN
orig_srid:= ST_SRID($1);
utm_srid:= utmzone(ST_Centroid($1));
 
RETURN ST_transform(ST_Buffer(ST_transform($1, utm_srid), $2), orig_srid);
END;
$BODY$ LANGUAGE 'plpgsql' IMMUTABLE
COST 100;
