/*
    Join building, apartment and population data from Zensus.
    If there's no data on buildings or apartments for a certain cell,
    the value for building_count resp. apartment_count contains NULL.
*/

DROP TABLE IF EXISTS society.egon_destatis_zensus_apartment_building_population_per_ha;

-- Join tables and filter data
CREATE TABLE society.egon_destatis_zensus_apartment_building_population_per_ha AS
SELECT GREATEST(bld_apt.grid_id, ze.grid_id) AS grid_id,
       GREATEST(bld_apt.zensus_population_id, ze.id) AS zensus_population_id,
       bld_apt.building_count,
       bld_apt.apartment_count,
       ze.population,
       ze.geom,
       ze.geom_point
    FROM
        (SELECT GREATEST(bld.zensus_population_id, apt.zensus_population_id) AS zensus_population_id,
                GREATEST(bld.grid_id, apt.grid_id) AS grid_id,
                bld.quantity as building_count,
                apt.quantity as apartment_count
            FROM
                (SELECT *
                    FROM society.egon_destatis_zensus_building_per_ha
                    WHERE attribute = 'INSGESAMT' AND quantity_q < 2
                ) bld
            FULL JOIN
                (SELECT *
                    FROM society.egon_destatis_zensus_apartment_per_ha
                    WHERE attribute = 'INSGESAMT' AND quantity_q < 2
                ) apt
            USING (grid_id)
        ) bld_apt
    FULL JOIN
        (SELECT *
             FROM society.destatis_zensus_population_per_ha_inside_germany
        ) ze
    USING (grid_id)
    ORDER BY zensus_population_id;

-- Set empty geoms from raw Zensus dataset
-- This is needed as table `society.destatis_zensus_population_per_ha_inside_germany`
-- holds geoms on cells with population > 0
UPDATE society.egon_destatis_zensus_apartment_building_population_per_ha AS t
    SET
        geom = t2.geom,
        geom_point = t2.geom_point
    FROM society.destatis_zensus_population_per_ha AS t2
    WHERE t.geom IS NULL AND t.grid_id = t2.grid_id;

-- Create index
CREATE INDEX ON society.egon_destatis_zensus_apartment_building_population_per_ha USING gist (geom);
