/*
 * Original Autor: nailend (julian.endres@rl-institut.de)
*/
-- Match amenities to buildings filtered

DROP TABLE IF EXISTS openstreetmap.osm_amenities_in_buildings_filtered;
CREATE TABLE openstreetmap.osm_amenities_in_buildings_filtered as
SELECT table_2.*, zensus.id as zensus_population_id FROM (
    SELECT * FROM (
            SELECT
                amenity.egon_amenity_id,
                amenity.amenity,
                amenity.name,
    --             amenity.osm_id as osm_id_amenity,
    --             amenity.tags as tags_amenity
                amenity.geom_amenity,
                filtered.osm_id as osm_id_building,
                filtered.id,
    --             filtered.building,
                filtered.area,
    --             filtered.tags as tags_building,
                filtered.geom_building


            FROM openstreetmap.osm_amenities_shops_filtered as amenity,
                 openstreetmap.osm_buildings_filtered as filtered
            WHERE ST_INTERSECTS(filtered.geom_building, amenity.geom_amenity)
    --         WHERE ST_DWithin(filtered.geom_building, amenity.geom_amenity, 0)-- 0.00002 2,22 Meter radius
            ) table_1
        WHERE table_1.id IS NOT NULL) as table_2,
        society.destatis_zensus_population_per_ha as zensus
        WHERE ST_INTERSECTS(table_2.geom_amenity, zensus.geom);



-- Amenities and Buildings are not unique probably due to overlay buildings
-- Remove building with smaller area surface to have unique Amenities

DELETE FROM openstreetmap.osm_amenities_in_buildings_filtered
WHERE id IN
    ( SELECT id
    FROM
        (
            SELECT id, egon_amenity_id, area, ROW_NUMBER() OVER( PARTITION BY egon_amenity_id
        ORDER BY  area DESC ) AS row_num
        FROM openstreetmap.osm_amenities_in_buildings_filtered ) AS t_1
        WHERE t_1.row_num > 1
        ) ;


ALTER TABLE openstreetmap.osm_amenities_in_buildings_filtered
    ADD CONSTRAINT pk_osm_amenities_in_buildings_filtered PRIMARY KEY (egon_amenity_id);

CREATE INDEX idx_osm_amenities_in_buildings_filtered_id
    ON openstreetmap.osm_amenities_in_buildings_filtered USING btree (id);

CREATE INDEX idx_osm_amenities_in_buildings_filtered_geom_building
    ON openstreetmap.osm_amenities_in_buildings_filtered USING gist (geom_building);

CREATE INDEX idx_osm_amenities_in_buildings_filtered_geom_amenity
    ON openstreetmap.osm_amenities_in_buildings_filtered USING gist (geom_amenity);



-- buildings filtered containing amenities
DROP TABLE IF EXISTS openstreetmap.osm_buildings_filtered_with_amenities;
CREATE TABLE openstreetmap.osm_buildings_filtered_with_amenities AS
SELECT
    amenities.id,
    amenities.n_amenities_inside,
    filtered.geom_building,
--     CASE
--        WHEN (ST_Contains(geom_building, ST_Centroid(geom_building))) IS TRUE
--        THEN ST_Centroid(geom_building)
--        ELSE ST_PointOnSurface(geom_building)
--     END AS geom_point
    filtered.geom_point,
    filtered.area
FROM (
    SELECT
        id,
        COUNT(*) AS n_amenities_inside
    FROM openstreetmap.osm_amenities_in_buildings_filtered
    GROUP BY id, area) AS amenities
LEFT JOIN
    openstreetmap.osm_buildings_filtered as filtered
    ON  amenities.id = filtered.id;

ALTER TABLE openstreetmap.osm_buildings_filtered_with_amenities
ADD COLUMN building VARCHAR(3);

UPDATE openstreetmap.osm_buildings_filtered_with_amenities SET building = 'cts';

ALTER TABLE openstreetmap.osm_buildings_filtered_with_amenities
    ADD CONSTRAINT pk_osm_buildings_filtered_with_amenities PRIMARY KEY (id);

CREATE INDEX idx_osm_buildings_filtered_with_amenities_geom_building
    ON openstreetmap.osm_buildings_filtered_with_amenities USING gist (geom_building);

-- amenities not located in a building
DROP TABLE IF EXISTS openstreetmap.osm_amenities_not_in_buildings_filtered;

CREATE TABLE openstreetmap.osm_amenities_not_in_buildings_filtered AS
    SELECT *
    FROM openstreetmap.osm_amenities_shops_filtered AS amenities_filtered
    WHERE amenities_filtered.egon_amenity_id NOT IN (
        SELECT amenities_in_buildings.egon_amenity_id
        FROM openstreetmap.osm_amenities_in_buildings_filtered AS amenities_in_buildings
    );

ALTER TABLE openstreetmap.osm_amenities_not_in_buildings_filtered
    ADD CONSTRAINT pk_osm_amenities_not_in_buildings_filtered PRIMARY KEY (egon_amenity_id);

CREATE INDEX idx_osm_amenities_not_in_buildings_filtered_osm_id
    ON openstreetmap.osm_amenities_not_in_buildings_filtered using btree (osm_id);

CREATE INDEX idx_osm_amenities_not_in_buildings_filtered_geom_amenity
    ON openstreetmap.osm_amenities_not_in_buildings_filtered USING gist (geom_amenity);
