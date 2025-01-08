/*
 * Original Autor: IsGut (johnrobert@t-online.de)
 * Adapted by: nesnoj (jonathan.amme@rl-institut.de)
*/

--------------------------------------------------------------------------
-- extract residential buildings only, calculate area, create centroids --
--------------------------------------------------------------------------
DROP TABLE if exists openstreetmap.osm_buildings_residential;
CREATE TABLE openstreetmap.osm_buildings_residential as
    select *
    from openstreetmap.osm_buildings bld
    where
        bld.building like 'yes'
        or bld.building like 'apartments'
        or bld.building like 'detached'
        or bld.building like 'farm'
        or bld.building like 'house'
        or bld.building like 'residential'
        or bld.building like 'semidetached_house'
        or bld.building like 'terrace'
        or bld.building like 'dormitory'
        or bld.building like 'terraced_house'

        -- retirement and assisted homes
        or bld.amenity like 'retirement_home'
        or (
            bld.amenity like 'social_facility'
			and tags::hstore -> 'social_facility' in ('nursing_home', 'assisted_living', 'group_home')
        )
        or bld.amenity like 'nursing_home'
        or bld.amenity like 'assisted_living'
        or bld.amenity like 'group_home';

ALTER TABLE openstreetmap.osm_buildings_residential
    ADD CONSTRAINT osm_buildings_residential_id_pkey PRIMARY KEY (id);

CREATE INDEX ON openstreetmap.osm_buildings_residential USING gist (geom_building);
CREATE INDEX ON openstreetmap.osm_buildings_residential USING gist (geom_point);
