/*
Add consumption and peak loads to load areas for sector: households

__copyright__   = "Reiner Lemoine Institut"
__license__     = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__         = "https://github.com/openego/eGon-data/blob/main/LICENSE"
__author__      = "nesnoj"
*/

------------------------
-- Scenario: eGon2035 --
------------------------

-- Add residential consumption
UPDATE demand.egon_loadarea AS t1
    SET sector_consumption_residential_2035 = t2.demand
    FROM (
        SELECT  a.id AS id,
                SUM(b.demand)::float AS demand
        FROM    demand.egon_loadarea AS a,
                (
                    SELECT
                        dem.demand AS demand,
                        census.geom_point AS geom_point
                    FROM
                        demand.egon_demandregio_zensus_electricity as dem,
                        society.destatis_zensus_population_per_ha AS census
                    WHERE
                        dem.scenario = 'eGon2035' AND
                        dem.sector = 'residential' AND
                        dem.zensus_population_id = census.id
                ) AS b
        WHERE   a.geom && b.geom_point AND
                ST_CONTAINS(a.geom, b.geom_point)
        GROUP BY a.id
        ) AS t2
    WHERE   t1.id = t2.id;

-- Add residential peak load
UPDATE demand.egon_loadarea AS t1
    SET sector_peakload_residential_2035 = t2.peak_load_in_mw
    FROM (
        SELECT  a.id AS id,
                SUM(b.peak_load_in_w)/1000000::float AS peak_load_in_mw
        FROM    demand.egon_loadarea AS a,
                (
                    SELECT
                        peak.peak_load_in_w AS peak_load_in_w,
                        bld.geom_point AS geom_point
                    FROM
                        demand.egon_building_electricity_peak_loads as peak,
                        (
                            SELECT "id"::integer, geom_point
                            FROM openstreetmap.osm_buildings_synthetic
                            UNION
                            SELECT "id"::integer, geom_point
                            FROM openstreetmap.osm_buildings_filtered
                        ) AS bld
                    WHERE
                        peak.scenario = 'eGon2035' AND
                        peak.sector = 'residential' AND
                        peak.building_id = bld.id
                ) AS b
        WHERE   a.geom && b.geom_point AND
                ST_CONTAINS(a.geom, b.geom_point)
        GROUP BY a.id
        ) AS t2
    WHERE   t1.id = t2.id;

----------------------------
-- Scenario: nep2037_2025 --
----------------------------

-- Add residential consumption
UPDATE demand.egon_loadarea AS t1
    SET sector_consumption_residential_2037_2025 = t2.demand
    FROM (
        SELECT  a.id AS id,
                SUM(b.demand)::float AS demand
        FROM    demand.egon_loadarea AS a,
                (
                    SELECT
                        dem.demand AS demand,
                        census.geom_point AS geom_point
                    FROM
                        demand.egon_demandregio_zensus_electricity as dem,
                        society.destatis_zensus_population_per_ha AS census
                    WHERE
                        dem.scenario = 'nep2037_2025' AND
                        dem.sector = 'residential' AND
                        dem.zensus_population_id = census.id
                ) AS b
        WHERE   a.geom && b.geom_point AND
                ST_CONTAINS(a.geom, b.geom_point)
        GROUP BY a.id
        ) AS t2
    WHERE   t1.id = t2.id;

-- Add residential peak load
UPDATE demand.egon_loadarea AS t1
    SET sector_peakload_residential_2037_2025 = t2.peak_load_in_mw
    FROM (
        SELECT  a.id AS id,
                SUM(b.peak_load_in_w)/1000000::float AS peak_load_in_mw
        FROM    demand.egon_loadarea AS a,
                (
                    SELECT
                        peak.peak_load_in_w AS peak_load_in_w,
                        bld.geom_point AS geom_point
                    FROM
                        demand.egon_building_electricity_peak_loads as peak,
                        (
                            SELECT "id"::integer, geom_point
                            FROM openstreetmap.osm_buildings_synthetic
                            UNION
                            SELECT "id"::integer, geom_point
                            FROM openstreetmap.osm_buildings_filtered
                        ) AS bld
                    WHERE
                        peak.scenario = 'nep2037_2025' AND
                        peak.sector = 'residential' AND
                        peak.building_id = bld.id
                ) AS b
        WHERE   a.geom && b.geom_point AND
                ST_CONTAINS(a.geom, b.geom_point)
        GROUP BY a.id
        ) AS t2
    WHERE   t1.id = t2.id;

-------------------------
-- Scenario: eGon100RE --
-------------------------

-- Add residential consumption
UPDATE demand.egon_loadarea AS t1
    SET sector_consumption_residential_2050 = t2.demand
    FROM (
        SELECT  a.id AS id,
                SUM(b.demand)::float AS demand
        FROM    demand.egon_loadarea AS a,
                (
                    SELECT
                        dem.demand AS demand,
                        census.geom_point AS geom_point
                    FROM
                        demand.egon_demandregio_zensus_electricity as dem,
                        society.destatis_zensus_population_per_ha AS census
                    WHERE
                        dem.scenario = 'eGon100RE' AND
                        dem.sector = 'residential' AND
                        dem.zensus_population_id = census.id
                ) AS b
        WHERE   a.geom && b.geom_point AND
                ST_CONTAINS(a.geom, b.geom_point)
        GROUP BY a.id
        ) AS t2
    WHERE   t1.id = t2.id;

-- Add residential peak load
UPDATE demand.egon_loadarea AS t1
    SET sector_peakload_residential_2050 = t2.peak_load_in_mw
    FROM (
        SELECT  a.id AS id,
                SUM(b.peak_load_in_w)/1000000::float AS peak_load_in_mw
        FROM    demand.egon_loadarea AS a,
                (
                    SELECT
                        peak.peak_load_in_w AS peak_load_in_w,
                        bld.geom_point AS geom_point
                    FROM
                        demand.egon_building_electricity_peak_loads as peak,
                        (
                            SELECT "id"::integer, geom_point
                            FROM openstreetmap.osm_buildings_synthetic
                            UNION
                            SELECT "id"::integer, geom_point
                            FROM openstreetmap.osm_buildings_filtered
                        ) AS bld
                    WHERE
                        peak.scenario = 'eGon100RE' AND
                        peak.sector = 'residential' AND
                        peak.building_id = bld.id
                ) AS b
        WHERE   a.geom && b.geom_point AND
                ST_CONTAINS(a.geom, b.geom_point)
        GROUP BY a.id
        ) AS t2
    WHERE   t1.id = t2.id;

------------------------
-- Scenario: eGon2021 --
------------------------
-- Update values for status quo scenario

UPDATE demand.egon_loadarea AS t1
    SET
        sector_consumption_residential = sector_consumption_residential_2035 * t2.scaling_factor,
        sector_peakload_residential = sector_peakload_residential_2035 * t2.scaling_factor
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
                        FROM demand.egon_demandregio_hh
                        WHERE
                            scenario = 'eGon2021'
                        GROUP BY nuts3
                        ORDER BY nuts3
                    ) AS a,
                    (
                        SELECT
                            nuts3,
                            sum(demand) as demand
                        FROM demand.egon_demandregio_hh
                        WHERE
                            scenario = 'eGon2035'
                        GROUP BY nuts3
                        ORDER BY nuts3
                    ) AS b
                WHERE a.nuts3 = b.nuts3
            ) as dr
        ON la.nuts = dr.nuts
    ) as t2
    WHERE t1.id = t2.id;
