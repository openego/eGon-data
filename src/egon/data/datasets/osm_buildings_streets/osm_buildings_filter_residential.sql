/*
 * Original Autor: IsGut (johnrobert@t-online.de)
 * Adapted by: nesnoj (jonathan.amme@rl-institut.de)
 * Rewritten for the ETHOS.BUILDA intersection, see
 *   https://github.com/openego/eGon-data/issues/1310
*/

--------------------------------------------------------------------------
-- Extract residential buildings from the OSM x ETHOS.BUILDA            --
-- intersection, keeping the ETHOS attributes and the match provenance. --
--------------------------------------------------------------------------
/*
 * Before, residential buildings were selected by their OSM tags alone, which
 * overestimated the stock by roughly a third against Zensus 2022. ETHOS.BUILDA
 * supplies one point per residential building; a building is residential if
 * such a point falls on it.
 *
 * Method after Till Krebber (RLI), a three step cascade:
 *   1. ETHOS point inside the OSM polygon        -> source = ethos_intersect
 *   2. Remainder: nearest neighbour within 10 m
 *      of the polygon's representative point     -> source = ethos_nearest
 *   3. Remainder: building tag on the
 *      residential whitelist                     -> source = osm_tagging
 *
 * The 10 m threshold is his, calibrated over radii of 1 to 30 m; recomputed in
 * EPSG:3035 without his per-state decomposition, the knee stays at 9 m.
 *
 * Deviations from his reference implementation, all measured on
 * Schleswig-Holstein:
 *   - Duplicate rule: one building at most once, one ETHOS point at most once,
 *     resolved geometrically and deterministically. He has none; without a
 *     rule the primary key below breaks. Affects 180 buildings (0.031 %) and
 *     9 points (0.002 %).
 *   - Stage 2 measures to ST_PointOnSurface (= geopandas
 *     representative_point()), not to geom_point (= ST_Centroid), to stay
 *     faithful to his code. The centroid would match 342 points more.
 *   - Stage 3 omits building='yes'. With it, 531,674 buildings return and the
 *     result lands at 1.33 x Zensus 2022, back in the overestimation the
 *     intersection is meant to remove.
 *   - Obvious ancillary buildings (garage, shed, carport, ...) are kept when
 *     ETHOS hits them (~500 in SH, 0.08 %): the point belongs to a real
 *     dwelling and merely sits on the wrong polygon, so dropping it would
 *     lose the building altogether.
 *
 * Result on Schleswig-Holstein: 618,208 buildings = 0.713 x Zensus 2022,
 * against 607,490 = 0.700 for his reference run.
 */

DROP TABLE IF EXISTS openstreetmap.osm_buildings_residential;
CREATE TABLE openstreetmap.osm_buildings_residential AS

WITH
-- ---------------------------------------------------------------- Stage 1 ---
s1_raw AS (
    SELECT b.id AS building_id, e.id AS ethos_id, b.area,
           ST_Distance(ST_PointOnSurface(b.geom_building), e.geom_point) AS d
    FROM openstreetmap.osm_buildings b
    JOIN society.egon_ethos_builda_buildings e
      ON ST_Contains(b.geom_building, e.geom_point)
),
-- an ETHOS point claims only ONE building: smallest area wins (the more
-- specific geometry where building and building:part are nested)
s1_one_b AS (
    SELECT DISTINCT ON (ethos_id) building_id, ethos_id, d
    FROM s1_raw ORDER BY ethos_id, area ASC, building_id ASC
),
-- a building appears exactly ONCE: nearest point to its representative point
s1 AS (
    SELECT DISTINCT ON (building_id) building_id, ethos_id,
           0::double precision AS match_distance
    FROM s1_one_b ORDER BY building_id, d ASC, ethos_id ASC
),

-- ---------------------------------------------------------------- Stage 2 ---
rest_e AS (
    SELECT e.* FROM society.egon_ethos_builda_buildings e
    WHERE NOT EXISTS (SELECT 1 FROM s1 WHERE s1.ethos_id = e.id)
),
rest_b AS (
    SELECT b.* FROM openstreetmap.osm_buildings b
    WHERE NOT EXISTS (SELECT 1 FROM s1 WHERE s1.building_id = b.id)
),
-- prefilter candidates through the INDEXED polygon. Admissible because the
-- representative point lies inside the polygon, so dist(pt, repr) >=
-- dist(pt, polygon) -- the polygon candidates are a superset. Then compute
-- the exact distance.
s2_cand AS (
    SELECT e.id AS ethos_id, b.id AS building_id,
           ST_Distance(ST_PointOnSurface(b.geom_building), e.geom_point) AS d
    FROM rest_e e
    JOIN rest_b b ON ST_DWithin(b.geom_building, e.geom_point, 10)
),
s2_one_b AS (
    SELECT DISTINCT ON (ethos_id) ethos_id, building_id, d
    FROM s2_cand WHERE d <= 10 ORDER BY ethos_id, d ASC, building_id ASC
),
s2 AS (
    SELECT DISTINCT ON (building_id) building_id, ethos_id,
           d AS match_distance
    FROM s2_one_b ORDER BY building_id, d ASC, ethos_id ASC
),

-- ---------------------------------------------------------------- Stage 3 ---
s3 AS (
    SELECT b.id AS building_id,
           NULL::text             AS ethos_id,
           NULL::double precision AS match_distance
    FROM openstreetmap.osm_buildings b
    WHERE NOT EXISTS (SELECT 1 FROM s1 WHERE s1.building_id = b.id)
      AND NOT EXISTS (SELECT 1 FROM s2 WHERE s2.building_id = b.id)
      -- the reference implementation's residential whitelist, checked against
      -- the building column as it does. The last three are amenity values in
      -- OSM, so they contribute almost nothing -- exactly 5 buildings in SH
      -- (nursing_home 4, retirement_home 1). Kept for fidelity; without them
      -- the result is 618,203 instead of 618,208.
      AND b.building IN ('residential','house','apartments','detached',
                         'semidetached_house','dormitory','terraced_house',
                         'terrace','bungalow','farm','farmhouse',
                         'assisted_living','nursing_home','retirement_home')
),

matched AS (
    SELECT building_id, ethos_id, match_distance, 'ethos_intersect'::text AS source FROM s1
    UNION ALL
    SELECT building_id, ethos_id, match_distance, 'ethos_nearest'         AS source FROM s2
    UNION ALL
    SELECT building_id, ethos_id, match_distance, 'osm_tagging'           AS source FROM s3
)

SELECT
    b.id, b.osm_id, b.amenity, b.building, b.name,
    b.geom_building, b.area, b.geom_point, b.tags,
    m.source,
    m.ethos_id,
    m.match_distance,
    e.construction_year,
    e.size_class,
    e.refurbishment_state,
    e.tabula_type
FROM matched m
JOIN openstreetmap.osm_buildings b ON b.id = m.building_id
LEFT JOIN society.egon_ethos_builda_buildings e ON e.id = m.ethos_id;

ALTER TABLE openstreetmap.osm_buildings_residential
    ADD CONSTRAINT osm_buildings_residential_id_pkey PRIMARY KEY (id);

CREATE INDEX ON openstreetmap.osm_buildings_residential USING gist (geom_building);
CREATE INDEX ON openstreetmap.osm_buildings_residential USING gist (geom_point);
CREATE INDEX ON openstreetmap.osm_buildings_residential (source);
