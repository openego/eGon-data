/*
 * Original Autor: IsGut (johnrobert@t-online.de)
 * Adapted by: nesnoj (jonathan.amme@rl-institut.de)
*/

-------------------------------------------------------
-- Create mapping table of buildings to zensus cells --
-------------------------------------------------------
drop table if exists boundaries.egon_map_zensus_buildings_filtered;
CREATE TABLE boundaries.egon_map_zensus_buildings_filtered as
    select * from (
        select
			bld.osm_id,
			zensus.grid_id,
			zensus.zensus_population_id as cell_id
        from openstreetmap.osm_buildings_filtered bld
        left join society.egon_destatis_zensus_apartment_building_population_per_ha zensus
        on ST_Within(bld.geom_point, zensus.geom)
    ) bld2
    where bld2.osm_id is not null and bld2.grid_id is not null;
