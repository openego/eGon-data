/*
 * Original Autor: nailend (julian.endres@rl-institut.de)
*/

----------------------------------------------------------------
-- Create mapping table of all filtered buildings to zensus cells --
----------------------------------------------------------------
-- Cells with unpopulated areas are NOT purged
-- A few buildings get lost in SH as they are outside of SH boundaries
-- Effect at DE not yet tested

DROP TABLE IF EXISTS boundaries.egon_map_zensus_buildings_filtered_all;
CREATE TABLE boundaries.egon_map_zensus_buildings_filtered_all AS
    SELECT * FROM (
    SELECT
        filtered.id,
        zensus.grid_id,
        zensus.id AS zensus_population_id
    FROM openstreetmap.osm_buildings_filtered AS filtered
    LEFT JOIN society.destatis_zensus_population_per_ha AS zensus
    ON ST_Within(filtered.geom_point, zensus.geom)
    ) AS t_1
    WHERE t_1.zensus_population_id  IS NOT null;

ALTER TABLE ONLY boundaries.egon_map_zensus_buildings_filtered_all
    ADD CONSTRAINT pk_egon_map_zensus_buildings_filtered_all PRIMARY KEY (id);

CREATE INDEX idx_egon_map_zensus_buildings_filtered_all_zensus_population_id
    ON boundaries.egon_map_zensus_buildings_filtered_all USING btree (zensus_population_id);
