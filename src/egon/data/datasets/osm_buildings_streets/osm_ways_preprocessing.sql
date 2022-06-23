/*
 * Original Autor: IsGut (johnrobert@t-online.de)
 * Adapted by: nesnoj (jonathan.amme@rl-institut.de)
 *
 * Create table for merged ways and lines to combine way (geo-info) with nodes
 * EPSG is transformed from 3857 to 4326
*/

-- create table containing merged mandatory ways
DROP TABLE if exists openstreetmap.osm_ways_preprocessed;
CREATE TABLE openstreetmap.osm_ways_preprocessed AS
    select
        ways.osm_id,
        ways.highway,
        ways.geom,
        ways.nodes,
        ST_Length(ways.geom::geography) as length
    from (
        select
              ways.osm_id,
            ways.highway,
            ways.geom,
            pow.id,
            pow.nodes
        from (
            select
                l.osm_id as osm_id,
                l.highway as highway,
                st_transform(l.geom, 4326) as geom
            from openstreetmap.osm_line l
            where
                l.highway like 'service'
                or l.highway like 'residential'
                or l.highway like 'secondary'
                or l.highway like 'tertiary'
                or l.highway like 'unclassified'
                or l.highway like 'primary'
                or l.highway like 'living_street'
                or l.highway like 'motorway'
                or l.highway like 'pedestrian'
                or l.highway like 'primary_link'
                or l.highway like 'motorway_link'
                or l.highway like 'trunk_link'
                or l.highway like 'secondary_link'
                or l.highway like 'tertiary_link'
                or l.highway like 'disused'
                or l.highway like 'trunk'
        ) ways
        left join openstreetmap.osm_ways pow
        on ways.osm_id = pow.id
    ) ways;
CREATE INDEX ON openstreetmap.osm_ways_preprocessed USING gist (geom);


/*
 * 1st ST_DumpPoints to separate linestring into points
 * 2nd ST_MakeLine to make lines from linestring segments
 * 3rd group by osm_id and aggregate w. array_agg()
 *
 * with linestring segments:
 *
 * SELECT way_w_segments.osm_id, way_w_segments.highway, way_w_segments.nodes, way_w_segments.geometry, array_agg(way_w_segments.linestring_segment) as linestring_segments, array_agg(ST_Length(way_w_segments.linestring_segment::geography)) as length_segments

 *
 */
drop table if exists openstreetmap.osm_ways_with_segments;
CREATE TABLE openstreetmap.osm_ways_with_segments as
    select
        ways.osm_id,
        ways.nodes,
        ways.highway,
        st_transform(ways.geom, 3035) as geom,
        ways.length_segments
    from (
        with way as (
            SELECT
                wp.osm_id,
                wp.highway,
                wp.nodes,
                wp.geom,
                ST_DumpPoints(wp.geom) as geo_dump
            FROM openstreetmap.osm_ways_preprocessed wp
        )
        SELECT
            way_w_segments.osm_id,
            way_w_segments.nodes,
            way_w_segments.highway,
            way_w_segments.geom,
            array_agg(ST_Length(way_w_segments.linestring_segment::geography)) as length_segments
        FROM (
            SELECT
                way.osm_id,
                way.highway,
                way.nodes,
                way.geom,
                ST_AsText(ST_MakeLine(lag((geo_dump).geom, 1, NULL)
                                      OVER
                                      (PARTITION BY way.osm_id ORDER BY way.osm_id, (geo_dump).path),
                                      (geo_dump).geom)) AS linestring_segment
            FROM way) way_w_segments
        WHERE way_w_segments.linestring_segment IS NOT null
        GROUP BY
            way_w_segments.osm_id,
            way_w_segments.highway,
            way_w_segments.nodes,
            way_w_segments.geom
    ) ways
    where ways.nodes is not null;

ALTER TABLE openstreetmap.osm_ways_with_segments
    ADD CONSTRAINT osm_ways_with_segments_osm_id_pkey PRIMARY KEY (osm_id);

CREATE INDEX ON openstreetmap.osm_ways_with_segments USING gist (geom);

