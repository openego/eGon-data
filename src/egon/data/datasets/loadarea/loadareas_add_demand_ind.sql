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

------------------------
-- Scenario: eGon2021 --
------------------------
-- Update values for status quo scenario

UPDATE demand.egon_loadarea AS t1
    SET
        sector_consumption_industrial = sector_consumption_industrial_2035 * t2.scaling_factor,
        sector_peakload_industrial = sector_peakload_industrial_2035 * t2.scaling_factor
    FROM (
        SELECT
            la.id,
            dr.scaling_factor
        FROM
            demand.egon_loadarea as la
        LEFT JOIN
            (
                SELECT
                    a.nuts3 as nuts,
                    a.demand / b.demand as scaling_factor
                FROM (
                        SELECT
                            nuts3,
                            sum(demand) as demand
                        FROM demand.egon_demandregio_cts_ind
                        WHERE
                            wz in (
                                SELECT wz FROM demand.egon_demandregio_wz WHERE sector = 'industry'
                            ) AND
                            scenario = 'eGon2021'
                        GROUP BY nuts3
                        ORDER BY nuts3
                    ) AS a,
                    (
                        SELECT
                            nuts3,
                            sum(demand) as demand
                        FROM demand.egon_demandregio_cts_ind
                        WHERE
                            wz in (
                                SELECT wz FROM demand.egon_demandregio_wz WHERE sector = 'industry'
                            ) AND
                            scenario = 'eGon2035'
                        GROUP BY nuts3
                        ORDER BY nuts3
                    ) AS b
                WHERE a.nuts3 = b.nuts3
            ) as dr
        ON la.nuts = dr.nuts
    ) as t2
    WHERE t1.id = t2.id;
