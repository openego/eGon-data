/*
 * Original Autor: nesnoj (jonathan.amme@rl-institut.de)
*/

--------------------------------------------------------------------------------
-- Extend residential buildings by finding census cells with population but   --
-- no residential buildings before in osm_buildings_filter_residential.sql .  --
-- Mark commercial and retail buildings as residential in those cells.        --
--------------------------------------------------------------------------------

INSERT INTO openstreetmap.osm_buildings_residential
	SELECT *
	FROM openstreetmap.osm_buildings_filtered
	WHERE id IN (
		SELECT id FROM (
			-- get buildings from filtered table in census cells (by centroid)
			SELECT
				bld.id,
				zensus.grid_id,
				zensus.zensus_population_id AS cell_id
			FROM openstreetmap.osm_buildings_filtered bld
			LEFT JOIN society.egon_destatis_zensus_apartment_building_population_per_ha zensus
			ON ST_Within(bld.geom_point, zensus.geom)
			WHERE building in ('commercial', 'retail')
			AND zensus.zensus_population_id in (
				-- census cell ids which have population but no res. buildings
				SELECT zensus.zensus_population_id
				FROM society.egon_destatis_zensus_apartment_building_population_per_ha zensus
				LEFT OUTER JOIN openstreetmap.osm_buildings_residential bld
				ON ST_Intersects(bld.geom_building, zensus.geom)
				WHERE bld.id IS NULL
			)
		) bld2
		WHERE bld2.id IS NOT NULL AND bld2.grid_id IS NOT NULL
	)
;