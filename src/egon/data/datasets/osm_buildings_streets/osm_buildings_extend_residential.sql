/*
 * Original Autor: nesnoj (jonathan.amme@rl-institut.de)
*/

--------------------------------------------------------------------------------
-- Extend residential buildings by finding census cells with population but   --
-- no residential buildings before in osm_buildings_filter_residential.sql .  --
-- Mark commercial, retail, office, hotel buildings as residential in those   --
-- cells.                                                                     --
--------------------------------------------------------------------------------
/*
 * This safety net predates the ETHOS.BUILDA intersection and is kept: ETHOS
 * covers residential buildings, so census cells with population but without a
 * matched building can still occur. Those buildings carry
 * source = 'census_gap_fill' and no ETHOS attributes, which makes their share
 * measurable against the intersection.
 *
 * The column list is explicit because the target table is wider than
 * osm_buildings_filtered since #1310; SELECT * would break here.
 */

INSERT INTO openstreetmap.osm_buildings_residential (
	id, osm_id, amenity, building, name,
	geom_building, area, geom_point, tags,
	source, ethos_id, match_distance,
	construction_year, size_class, refurbishment_state, tabula_type
)
	SELECT
		id, osm_id, amenity, building, name,
		geom_building, area, geom_point, tags,
		'census_gap_fill'      AS source,
		NULL::text             AS ethos_id,
		NULL::double precision AS match_distance,
		NULL::int              AS construction_year,
		NULL::text             AS size_class,
		NULL::text             AS refurbishment_state,
		NULL::text             AS tabula_type
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
			WHERE building in ('commercial', 'retail', 'office', 'hotel')
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
