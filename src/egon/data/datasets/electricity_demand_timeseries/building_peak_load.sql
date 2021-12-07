INSERT INTO demand.egon_building_peak_loads (building_id,
                                             building_peak_load_in_w_2035,
                                             building_peak_load_in_w_2050)

SELECT d.building_id,
-- 		max(d.building_load_in_kWh) as building_peak_load_in_kWh,
       cast(max(d.building_load_in_wh) * d.factor_2035 as REAL) as "building_peak_load_in_w_2035",
       cast(max(d.building_load_in_wh) * d.factor_2050 as REAL) as "building_peak_load_in_w_2050"
FROM (
         SELECT SUM(demand) as building_load_in_wh, timestep, b.building_id, b.factor_2035, b.factor_2050
         FROM (
                  SELECT t.building_id, demand, timestep, t.factor_2035, t.factor_2050
                  FROM (
                           SELECT buildings.building_id, profiles.load_in_wh, census.factor_2035, census.factor_2050
                           FROM demand.egon_household_electricity_profile_in_census_cell AS census,
                                demand.egon_household_electricity_profile_of_buildings AS buildings

                                    LEFT OUTER JOIN demand.iee_household_load_profiles AS profiles
                                                    ON profiles.type = buildings.profile_id

                           WHERE buildings.cell_id = census.cell_id
--                            limit 10000
                       ) as t,

                       UNNEST(t.load_in_wh) WITH ORDINALITY x(demand, timestep)
              ) as b

         GROUP BY building_id, timestep, factor_2035, factor_2050
         ORDER BY building_id, timestep
     ) as d
GROUP BY d.building_id, d.factor_2035, d.factor_2050;
