/*
 * Original Autor: IsGut (johnrobert@t-online.de)
 * Adapted by: nesnoj (jonathan.amme@rl-institut.de)
*/

-- merge n_apartments representing households to buildings_filtered
-- first a temp table is needed due to wanna add a count to share n_apartments for n_buildings
-- all buildings within a radius of ~77.7 meters

-- create temporary table of buildings containing zensus data (residentials)
drop table if exists openstreetmap.osm_buildings_with_res_tmp;
CREATE TABLE openstreetmap.osm_buildings_with_res_tmp as
    select * from (
        select
            bld.osm_id,
            bld.id,
            bld.amenity,
            bld.building,
            bld.name,
            bld.geom_building,
            bld.area,
            bld.tags,
            zensus_apartments.apartment_count,
            zensus_apartments.grid_id
        from openstreetmap.osm_buildings_filtered bld
        left join society.egon_destatis_zensus_apartment_building_population_per_ha zensus_apartments
        on ST_DWithin(zensus_apartments.geom, bld.geom_building, 0.0007) -- radius is around 77.7 meters
    ) buildings_with_apartments
    where buildings_with_apartments.osm_id is not null;

-- ^- this table will be droppped at a later point due to the need is only for preprocessing steps

drop table if exists openstreetmap.osm_buildings_with_res_tmp2;
create table openstreetmap.osm_buildings_with_res_tmp2 as
    select
        bld.osm_id,
        bld.id,
        bld.amenity,
        bld.building,
        bld.name,
        bld.geom_building,
        bld.area,
        bld.tags,
        bld.apartment_count,
        bld.grid_id,
        bwa.n_apartments_in_n_buildings -- number of buildings the apartments are shared by
    from openstreetmap.osm_buildings_with_res_tmp as bld
    left join (
        select * from (
            select
                b_res.grid_id,
                count(b_res.*) as n_apartments_in_n_buildings
            from openstreetmap.osm_buildings_with_res_tmp as b_res
            group by b_res.grid_id
        ) bwa ) bwa
    on bld.grid_id = bwa.grid_id;
CREATE INDEX ON openstreetmap.osm_buildings_with_res_tmp2 USING gist (geom_building);


drop table if exists openstreetmap.osm_amenities_in_buildings_tmp;
CREATE TABLE openstreetmap.osm_amenities_in_buildings_tmp as
    with amenity as (select * from openstreetmap.osm_amenities_shops_filtered af)
    select
        bf.osm_id as osm_id_building,
        bf.id,
        bf.building,
        bf.area,
        bf.geom_building,
        amenity.osm_id as osm_id_amenity,
        amenity.egon_amenity_id,
        amenity.amenity,
        amenity.name,
        amenity.geom_amenity,
        bf.tags as tags_building,
        amenity.tags as tags_amenity,
        bf.apartment_count,
        bf.grid_id,
        bf.n_apartments_in_n_buildings
    from amenity, openstreetmap.osm_buildings_with_res_tmp2 bf
    where st_intersects(bf.geom_building, amenity.geom_amenity);

CREATE INDEX idx_osm_amenities_in_buildings_tmp_geom_building
    ON openstreetmap.osm_amenities_in_buildings_tmp USING gist (geom_building);
