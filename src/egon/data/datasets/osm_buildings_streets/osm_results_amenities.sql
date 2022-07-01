/*
 * Original Autor: IsGut (johnrobert@t-online.de)
 * Adapted by: nesnoj (jonathan.amme@rl-institut.de)
*/

-- amenities not located in a building
drop table if exists openstreetmap.osm_amenities_not_in_buildings;
CREATE TABLE openstreetmap.osm_amenities_not_in_buildings as
    select *
    from openstreetmap.osm_amenities_shops_filtered af
    where af.osm_id not in (
        select aib.osm_id_amenity
        from openstreetmap.osm_amenities_in_buildings_tmp aib
    );

ALTER TABLE openstreetmap.osm_amenities_not_in_buildings
    ADD CONSTRAINT pk_osm_amenities_not_in_buildings PRIMARY KEY (egon_amenity_id);

CREATE INDEX idx_osm_amenities_not_in_buildings_osm_id
    ON openstreetmap.osm_amenities_not_in_buildings using btree (osm_id);

CREATE INDEX idx_osm_amenities_not_in_buildings_geom
    ON openstreetmap.osm_amenities_not_in_buildings USING gist (geom_amenity);
