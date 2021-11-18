CREATE INDEX IF NOT EXISTS egon_household_electricity_profile_of_buildings_id_idx ON demand.egon_household_electricity_profile_of_buildings (cell_id);

INSERT INTO demand.egon_building_peak_loads (building_ids, building_peak_load_in_wh_2035,
                                             building_peak_load_in_wh_2050)

SELECT d.building_ids,
-- 		max(d.building_load_in_kWh) as building_peak_load_in_kWh,
       cast(max(d.building_load_in_wh) * d.factor_2035 as REAL) as "building_peak_load_in_wh_2035",
       cast(max(d.building_load_in_wh) * d.factor_2050 as REAL) as "building_peak_load_in_wh_2050"
FROM (
         SELECT SUM(demand) as building_load_in_wh, timestep, b.building_ids, b.factor_2035, b.factor_2050
         FROM (
                  SELECT t.building_ids, demand, timestep, t.factor_2035, t.factor_2050
                  FROM (
                           SELECT buildings.building_ids, profiles.load_in_wh, census.factor_2035, census.factor_2050
                           FROM demand.egon_household_electricity_profile_in_census_cell AS census,
                                demand.egon_household_electricity_profile_of_buildings AS buildings

                                    LEFT OUTER JOIN demand.iee_household_load_profiles AS profiles
                                                    ON profiles.type = buildings.cell_profile_ids

                           WHERE buildings.cell_id = census.cell_id
--                            limit 10000
                       ) as t,

                       UNNEST(t.load_in_wh) WITH ORDINALITY x(demand, timestep)
              ) as b

         GROUP BY building_ids, timestep, factor_2035, factor_2050
         ORDER BY building_ids, timestep
     ) as d
GROUP BY d.building_ids, d.factor_2035, d.factor_2050;
