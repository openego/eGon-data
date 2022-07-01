/*
 * Original Autor: IsGut (johnrobert@t-online.de)
 * Adapted by: nesnoj (jonathan.amme@rl-institut.de)
*/

-- buildings containing amenities
drop table if exists openstreetmap.osm_buildings_with_amenities;
CREATE TABLE openstreetmap.osm_buildings_with_amenities as
    select
        bwa.osm_id_amenity,
        bwa.osm_id_building,
        bwa.id,
        bwa.building,
        bwa.area,
        bwa.geom_building,
        bwa.geom_amenity,
    CASE
       WHEN (ST_Contains(bwa.geom_building, ST_Centroid(bwa.geom_building))) IS TRUE
       THEN ST_Centroid(bwa.geom_building)
       ELSE ST_PointOnSurface(bwa.geom_building)
    END AS geom_point,
    bwa."name",
    bwa.tags_building,
    bwa.tags_amenity,
    bwa.n_amenities_inside,
    case
        when apartment_count > 0
        then bwa.apartment_count / bwa.n_apartments_in_n_buildings
        else 0
    end as apartment_count
    from (
        select
            bwa.osm_id_amenity,
            bwa.osm_id_building,
            bwa.id,
            bwa.building,
            bwa.area,
            bwa.geom_building,
            bwa.geom_amenity,
            bwa.name,
            bwa.tags_building,
            bwa.tags_amenity,
            bwa.n_amenities_inside,
            SUM(bwa.apartment_count) as apartment_count,
            SUM(bwa.n_apartments_in_n_buildings) as n_apartments_in_n_buildings
        from (
            select
                b.osm_id_amenity,
                b.osm_id_building,
                b.id,
                coalesce(b.amenity, b.building) as building,
                b.area,
                b.geom_building,
                b.geom_amenity,
                b.name,
                b.tags_building,
                b.tags_amenity,
                coalesce(b.apartment_count, 0) as apartment_count,
                coalesce(b.n_apartments_in_n_buildings, 0) as n_apartments_in_n_buildings,
                ainb.n_amenities_inside
            from openstreetmap.osm_amenities_in_buildings_tmp b
            left join (
                select
                    ainb.osm_id_building,
                    count(*) as n_amenities_inside
                    from openstreetmap.osm_amenities_in_buildings_tmp ainb
                group by ainb.osm_id_building ) ainb
            on b.osm_id_building = ainb.osm_id_building
        ) bwa
        group by
            bwa.osm_id_amenity,
            bwa.osm_id_building,
            bwa.id,
            bwa.building,
            bwa.area,
            bwa.geom_building,
            bwa.geom_amenity,
            bwa.name,
            bwa.tags_building,
            bwa.tags_amenity,
            bwa.n_amenities_inside
    ) bwa;


-- osm_id_amenity, osm_id_building and id are no unique values

CREATE INDEX idx_osm_buildings_with_amenities_osm_id_amenity
    ON openstreetmap.osm_buildings_with_amenities USING btree (osm_id_amenity);

CREATE INDEX idx_osm_buildings_with_amenities_id
    ON openstreetmap.osm_buildings_with_amenities USING btree (id);

CREATE INDEX idx_osm_buildings_with_amenities_osm_id_building
    ON openstreetmap.osm_buildings_with_amenities USING btree (osm_id_building);

CREATE INDEX idx_osm_buildings_with_amenities_geom_building
    ON openstreetmap.osm_buildings_with_amenities USING gist (geom_building);
