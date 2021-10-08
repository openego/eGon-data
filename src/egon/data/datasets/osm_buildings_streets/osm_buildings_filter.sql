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
        ST_TRANSFORM(poly.geom, 3035) AS geom,
        ST_AREA(ST_TRANSFORM(poly.geom, 3035)) AS area,
        ST_TRANSFORM(ST_CENTROID(poly.geom), 3035) AS geom_point,
        poly.tags
    FROM openstreetmap.osm_polygon poly
    WHERE poly.building IS NOT NULL;

CREATE INDEX ON openstreetmap.osm_buildings USING gist (geom);
CREATE INDEX ON openstreetmap.osm_buildings USING gist (geom_point);

-----------------------------------------------------------------------
-- extract specific buildings only, calculate area, create centroids --
-----------------------------------------------------------------------
DROP TABLE if exists openstreetmap.osm_buildings_filtered;
CREATE TABLE openstreetmap.osm_buildings_filtered as
    select *
    from openstreetmap.osm_buildings bld
    where
        bld.building like 'yes'
        or bld.building like 'apartments'
        or bld.building like 'detached'
        or bld.building like 'dormitory'
        or bld.building like 'farm'
        or bld.building like 'hotel'
        or bld.building like 'house'
        or bld.building like 'residential'
        or bld.building like 'semidetached_house'
        or bld.building like 'static_caravan'
        or bld.building like 'terrace'
        or bld.building like 'commercial'
        or bld.building like 'industrial'
        or bld.building like 'kiosk'
        or bld.building like 'office'
        or bld.building like 'retail'
        or bld.building like 'supermarket'
        or bld.building like 'warehouse'
        or bld.building like 'cathedral'
        or bld.building like 'chapel'
        or bld.building like 'church'
        or bld.building like 'monastery'
        or bld.building like 'mosque'
        or bld.building like 'presbytery'
        or bld.building like 'religious'
        or bld.building like 'shrine'
        or bld.building like 'synagogue'
        or bld.building like 'temple'
        or bld.building like 'bakehouse'
        or bld.building like 'civic'
        or bld.building like 'fire_station'
        or bld.building like 'government'
        or bld.building like 'hospital'
        or bld.building like 'public'
        or bld.building like 'train_station'
        or bld.building like 'transportation'
        or bld.building like 'kindergarten'
        or bld.building like 'school'
        or bld.building like 'university'
        or bld.building like 'college'
        or bld.building like 'barn'
        or bld.building like 'conservatory'
        or bld.building like 'farm_auxiliary'
        or bld.building like 'stable'
        or bld.building like 'pavilion'
        or bld.building like 'riding_hall'
        or bld.building like 'sports_hall'
        or bld.building like 'stadium'
        or bld.building like 'digester'
        or bld.building like 'service'
        or bld.building like 'transformer_tower'
        or bld.building like 'military'
        or bld.building like 'gatehouse'
        or bld.amenity like 'bar'
        or bld.amenity like 'biergarten'
        or bld.amenity like 'cafe'
        or bld.amenity like 'fast_food'
        or bld.amenity like 'food_court'
        or bld.amenity like 'ice_cream'
        or bld.amenity like 'pub'
        or bld.amenity like 'restaurant'
        or bld.amenity like 'college'
        or bld.amenity like 'driving_school'
        or bld.amenity like 'kindergarten'
        or bld.amenity like 'language_school'
        or bld.amenity like 'library'
        or bld.amenity like 'toy_library'
        or bld.amenity like 'music_school'
        or bld.amenity like 'school'
        or bld.amenity like 'university'
        or bld.amenity like 'car_wash'
        or bld.amenity like 'vehicle_inspection'
        or bld.amenity like 'charging_station'
        or bld.amenity like 'fuel'
        or bld.amenity like 'bank'
        or bld.amenity like 'clinic'
        or bld.amenity like 'dentist'
        or bld.amenity like 'doctors'
        or bld.amenity like 'hospital'
        or bld.amenity like 'nursing_home'
        or bld.amenity like 'pharmacy'
        or bld.amenity like 'social_facility'
        or bld.amenity like 'veterinary'
        or bld.amenity like 'arts_centre'
        or bld.amenity like 'brothel'
        or bld.amenity like 'casino'
        or bld.amenity like 'cinema'
        or bld.amenity like 'community_centre'
        or bld.amenity like 'conference_centre'
        or bld.amenity like 'events_venue'
        or bld.amenity like 'gambling'
        or bld.amenity like 'love_hotel'
        or bld.amenity like 'nightclub'
        or bld.amenity like 'planetarium'
        or bld.amenity like 'social_centre'
        or bld.amenity like 'stripclub'
        or bld.amenity like 'studio'
        or bld.amenity like 'swingerclub'
        or bld.amenity like 'exhibition_centre'
        or bld.amenity like 'theatre'
        or bld.amenity like 'courthouse'
        or bld.amenity like 'embassy'
        or bld.amenity like 'fire_station'
        or bld.amenity like 'police'
        or bld.amenity like 'post_depot'
        or bld.amenity like 'post_office'
        or bld.amenity like 'prison'
        or bld.amenity like 'ranger_station'
        or bld.amenity like 'townhall'
        or bld.amenity like 'animal_boarding'
        or bld.amenity like 'childcare'
        or bld.amenity like 'dive_centre'
        or bld.amenity like 'funeral_hall'
        or bld.amenity like 'gym'
        or bld.amenity like 'internet_cafe'
        or bld.amenity like 'kitchen'
        or bld.amenity like 'monastery'
        or bld.amenity like 'place_of_mourning'
        or bld.amenity like 'place_of_worship';

CREATE INDEX ON openstreetmap.osm_buildings_filtered USING gist (geom);
CREATE INDEX ON openstreetmap.osm_buildings_filtered USING gist (geom_point);
