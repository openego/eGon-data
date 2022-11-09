/*
Add consumption and peak loads to load areas for sector: industry

__copyright__   = "Reiner Lemoine Institut"
__license__     = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__         = "https://github.com/openego/eGon-data/blob/main/LICENSE"
__author__      = "nesnoj"
*/

------------------------
-- Scenario: eGon2035 --
------------------------

-- Add industrial consumption and peak load
-- 1) Industry from OSM landuse areas
UPDATE demand.egon_loadarea AS t1
    SET
        sector_peakload_industrial_2035 = t2.peak_load,
        sector_consumption_industrial_2035 = t2.demand
    FROM (
        SELECT  a.id AS id,
                SUM(b.demand)::float AS demand,
                SUM(b.peak_load)::float AS peak_load
        FROM    demand.egon_loadarea AS a,
                (
                    SELECT
                        sum(ind_osm.demand) as demand,
                        sum(ind_osm.peak_load) as peak_load,
                        ST_PointOnSurface(landuse.geom) AS geom_surfacepoint
                    FROM
                        openstreetmap.osm_landuse as landuse,
                        demand.egon_osm_ind_load_curves_individual as ind_osm
                    WHERE
                        ind_osm.scn_name = 'eGon2035' AND
                        ind_osm.osm_id = landuse.id
                    GROUP BY landuse.id
                ) AS b
        WHERE   a.geom && b.geom_surfacepoint AND
                ST_CONTAINS(a.geom, b.geom_surfacepoint)
        GROUP BY a.id
        ) AS t2
    WHERE   t1.id = t2.id;

-- 2) Industry from industrial sites
UPDATE demand.egon_loadarea AS t1
    SET
        sector_peakload_industrial_2035 = sector_peakload_industrial_2035 + t2.peak_load,
        sector_consumption_industrial_2035 = sector_consumption_industrial_2035 + t2.demand
    FROM (
        SELECT  a.id AS id,
                SUM(b.demand)::float AS demand,
                SUM(b.peak_load)::float AS peak_load
        FROM    demand.egon_loadarea AS a,
                (
                    SELECT
                        ind_sites.id,
                        ind_loads.demand,
                        ind_loads.peak_load,
                        ind_sites.geom
                    FROM
                        demand.egon_industrial_sites as ind_sites,
                        demand.egon_sites_ind_load_curves_individual as ind_loads
                    WHERE
                        ind_loads.scn_name = 'eGon2035' AND
                        ind_loads.site_id = ind_sites.id
                ) AS b
        WHERE   a.geom && b.geom AND
                ST_CONTAINS(a.geom, b.geom)
        GROUP BY a.id
        ) AS t2
    WHERE   t1.id = t2.id;

-------------------------
-- Scenario: eGon100RE --
-------------------------

-- Add industrial consumption and peak load
-- 1) Industry from OSM landuse areas
UPDATE demand.egon_loadarea AS t1
    SET
        sector_peakload_industrial_2050 = t2.peak_load,
        sector_consumption_industrial_2050 = t2.demand
    FROM (
        SELECT  a.id AS id,
                SUM(b.demand)::float AS demand,
                SUM(b.peak_load)::float AS peak_load
        FROM    demand.egon_loadarea AS a,
                (
                    SELECT
                        sum(ind_osm.demand) as demand,
                        sum(ind_osm.peak_load) as peak_load,
                        ST_PointOnSurface(landuse.geom) AS geom_surfacepoint
                    FROM
                        openstreetmap.osm_landuse as landuse,
                        demand.egon_osm_ind_load_curves_individual as ind_osm
                    WHERE
                        ind_osm.scn_name = 'eGon100RE' AND
                        ind_osm.osm_id = landuse.id
                    GROUP BY landuse.id
                ) AS b
        WHERE   a.geom && b.geom_surfacepoint AND
                ST_CONTAINS(a.geom, b.geom_surfacepoint)
        GROUP BY a.id
        ) AS t2
    WHERE   t1.id = t2.id;

-- 2) Industry from industrial sites
UPDATE demand.egon_loadarea AS t1
    SET
        sector_peakload_industrial_2050 = sector_peakload_industrial_2050 + t2.peak_load,
        sector_consumption_industrial_2050 = sector_consumption_industrial_2050 + t2.demand
    FROM (
        SELECT  a.id AS id,
                SUM(b.demand)::float AS demand,
                SUM(b.peak_load)::float AS peak_load
        FROM    demand.egon_loadarea AS a,
                (
                    SELECT
                        ind_sites.id,
                        ind_loads.demand,
                        ind_loads.peak_load,
                        ind_sites.geom
                    FROM
                        demand.egon_industrial_sites as ind_sites,
                        demand.egon_sites_ind_load_curves_individual as ind_loads
                    WHERE
                        ind_loads.scn_name = 'eGon100RE' AND
                        ind_loads.site_id = ind_sites.id
                ) AS b
        WHERE   a.geom && b.geom AND
                ST_CONTAINS(a.geom, b.geom)
        GROUP BY a.id
        ) AS t2
    WHERE   t1.id = t2.id;
