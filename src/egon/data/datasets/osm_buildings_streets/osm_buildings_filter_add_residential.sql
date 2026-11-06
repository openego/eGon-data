/*
 * Add residential buildings that are missing from osm_buildings_filtered.
 *
 * Since the ETHOS.BUILDA intersection (#1310), osm_buildings_residential is
 * no longer a subset of osm_buildings_filtered: ETHOS also matches buildings
 * whose tag is not on the filter list in osm_buildings_filter.sql (mainly
 * building=bungalow) and ancillary buildings (garage, shed, ...) that are
 * kept on purpose. Downstream tools such as ding0 rely on the subset relation
 * and silently drop these buildings, see
 *   https://github.com/openego/eGon-data/issues/1574
 *
 * Inserting them here restores the relation. It runs before all tables that
 * are derived from osm_buildings_filtered, so these stay consistent.
 * Measured on Schleswig-Holstein: 2,165 buildings (0.35 % of residential).
 */

INSERT INTO openstreetmap.osm_buildings_filtered (
    osm_id, amenity, building, name,
    geom_building, area, geom_point, tags, id
)
    SELECT
        r.osm_id, r.amenity, r.building, r.name,
        r.geom_building, r.area, r.geom_point, r.tags, r.id
    FROM openstreetmap.osm_buildings_residential r
    WHERE NOT EXISTS (
        SELECT 1
        FROM openstreetmap.osm_buildings_filtered f
        WHERE f.id = r.id
    );
