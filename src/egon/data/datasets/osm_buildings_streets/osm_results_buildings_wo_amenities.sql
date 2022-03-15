/*
 * Original Autor: IsGut (johnrobert@t-online.de)
 * Adapted by: nesnoj (jonathan.amme@rl-institut.de)
*/

-- buildings containing no amenities
drop table if exists openstreetmap.osm_buildings_without_amenities;
CREATE TABLE openstreetmap.osm_buildings_without_amenities as
    select
        bwa.osm_id,
        bwa.building,
        bwa.area,
        bwa.geom,
    CASE
       WHEN (ST_Contains(bwa.geom, ST_Centroid(bwa.geom))) IS TRUE
       THEN ST_Centroid(bwa.geom)
       ELSE ST_PointOnSurface(bwa.geom)
    END AS geom_point,
    bwa.name,
    bwa.tags,
    CASE
        WHEN apartment_count > 0
        THEN bwa.apartment_count / bwa.n_apartments_in_n_buildings
        ELSE 0
    END AS apartment_count
    from (
        select
            bwa.osm_id,
            bwa.building,
            bwa.area,
            bwa.geom,
            bwa.name,
            bwa.tags,
            SUM(bwa.apartment_count) as apartment_count,
            SUM(bwa.n_apartments_in_n_buildings) as n_apartments_in_n_buildings
        from (
            select
                bf.osm_id,
                coalesce(bf.amenity, bf.building) as building,
                bf.name,
                bf.area,
                bf.geom,
                bf.tags,
                coalesce(bf.apartment_count, 0) as apartment_count,
                coalesce(bf.n_apartments_in_n_buildings, 0) as n_apartments_in_n_buildings
            from openstreetmap.osm_buildings_with_res_tmp2 bf
            -- NOT IN replaced by JOIN DUE TO performance problems
            -- cf. https://github.com/openego/eGon-data/issues/693
            LEFT JOIN openstreetmap.osm_amenities_in_buildings_tmp aib
            ON bf.osm_id = aib.osm_id_building
            WHERE aib.osm_id_building IS NULL
        ) bwa
        group by
            bwa.osm_id,
            bwa.building,
            bwa.area, bwa.geom,
            bwa.name,
        bwa.tags
    ) bwa;
CREATE INDEX ON openstreetmap.osm_buildings_without_amenities USING gist (geom);
