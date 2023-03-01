/*
 * Original Autor: IsGut (johnrobert@t-online.de)
 * Adapted by: nesnoj (jonathan.amme@rl-institut.de)
*/

-------------------------------------------------------------------
-- Create mapping table of residential buildings to zensus cells --
-------------------------------------------------------------------
-- Only selecting buildings wihtin the purged census cells
-- purged of cells with unpopulated areas
-- https://github.com/openego/eGon-data/blob/59195926e41c8bd6d1ca8426957b97f33ef27bcc/src/egon/data/importing/zensus/__init__.py#L418-L449
drop table if exists boundaries.egon_map_zensus_buildings_residential;
CREATE TABLE boundaries.egon_map_zensus_buildings_residential as
    select * from (
        select
			bld.id,
			zensus.grid_id,
			zensus.zensus_population_id as cell_id
        from openstreetmap.osm_buildings_residential bld
        left join society.egon_destatis_zensus_apartment_building_population_per_ha zensus
        on ST_Within(bld.geom_point, zensus.geom)
    ) bld2
    where bld2.id is not null and bld2.grid_id is not null;
