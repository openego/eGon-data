/*
 * Original Autor: IsGut (johnrobert@t-online.de)
 * Adapted by: nesnoj (jonathan.amme@rl-institut.de)
 *
 * extract specific amenities and all shops
 * preserve relevant columns
*/

DROP TABLE if exists openstreetmap.osm_amenities_shops_filtered;
CREATE TABLE openstreetmap.osm_amenities_shops_filtered AS
    select
        pnt.osm_id,
        pnt.amenity,
        pnt.name,
        ST_TRANSFORM(pnt.geom, 3035) AS geom_amenity,
        pnt.tags
    FROM openstreetmap.osm_point pnt
    where
        pnt.amenity like 'bar'
        or pnt.amenity like 'biergarten'
        or pnt.amenity like 'cafe'
        or pnt.amenity like 'fast_food'
        or pnt.amenity like 'food_court'
        or pnt.amenity like 'ice_cream'
        or pnt.amenity like 'pub'
        or pnt.amenity like 'restaurant'
        or pnt.amenity like 'college'
        or pnt.amenity like 'driving_school'
        or pnt.amenity like 'kindergarten'
        or pnt.amenity like 'language_school'
        or pnt.amenity like 'library'
        or pnt.amenity like 'toy_library'
        or pnt.amenity like 'music_school'
        or pnt.amenity like 'school'
        or pnt.amenity like 'university'
        or pnt.amenity like 'car_wash'
        or pnt.amenity like 'vehicle_inspection'
        or pnt.amenity like 'charging_station'
        or pnt.amenity like 'fuel'
        or pnt.amenity like 'bank'
        or pnt.amenity like 'clinic'
        or pnt.amenity like 'dentist'
        or pnt.amenity like 'doctors'
        or pnt.amenity like 'hospital'
        or pnt.amenity like 'nursing_home'
        or pnt.amenity like 'pharmacy'
        or pnt.amenity like 'social_facility'
        or pnt.amenity like 'veterinary'
        or pnt.amenity like 'arts_centre'
        or pnt.amenity like 'brothel'
        or pnt.amenity like 'casino'
        or pnt.amenity like 'cinema'
        or pnt.amenity like 'community_centre'
        or pnt.amenity like 'conference_centre'
        or pnt.amenity like 'events_venue'
        or pnt.amenity like 'gambling'
        or pnt.amenity like 'love_hotel'
        or pnt.amenity like 'nightclub'
        or pnt.amenity like 'planetarium'
        or pnt.amenity like 'social_centre'
        or pnt.amenity like 'stripclub'
        or pnt.amenity like 'studio'
        or pnt.amenity like 'swingerclub'
        or pnt.amenity like 'exhibition_centre'
        or pnt.amenity like 'theatre'
        or pnt.amenity like 'courthouse'
        or pnt.amenity like 'embassy'
        or pnt.amenity like 'fire_station'
        or pnt.amenity like 'police'
        or pnt.amenity like 'post_depot'
        or pnt.amenity like 'post_office'
        or pnt.amenity like 'prison'
        or pnt.amenity like 'ranger_station'
        or pnt.amenity like 'townhall'
        or pnt.amenity like 'animal_boarding'
        or pnt.amenity like 'childcare'
        or pnt.amenity like 'dive_centre'
        or pnt.amenity like 'funeral_hall'
        or pnt.amenity like 'gym'
        or pnt.amenity like 'internet_cafe'
        or pnt.amenity like 'kitchen'
        or pnt.amenity like 'monastery'
        or pnt.amenity like 'place_of_mourning'
        or pnt.amenity like 'place_of_worship'
        or pnt.amenity like 'shop'
        or pnt.shop IS NOT NULL;

-- add PK as some osm ids are not unique
ALTER TABLE openstreetmap.osm_amenities_shops_filtered
    ADD COLUMN egon_amenity_id SERIAL PRIMARY KEY;

CREATE INDEX idx_osm_amenities_shops_filtered_geom
    ON openstreetmap.osm_amenities_shops_filtered USING gist (geom_amenity);
