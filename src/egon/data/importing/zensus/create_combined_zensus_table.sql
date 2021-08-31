/*
	Join building, apartment and population data from Zensus.
	If there's no data on buildings or apartments for a certain cell,
	the value for building_count resp. apartment_count contains NULL.
*/
DROP TABLE IF EXISTS society.egon_destatis_zensus_apartment_building_population_per_ha;
CREATE TABLE society.egon_destatis_zensus_apartment_building_population_per_ha AS
SELECT GREATEST(bld_apt.zensus_population_id, ze.gid) AS zensus_population_id,
	   bld_apt.building_count,
	   bld_apt.apartment_count,
	   ze.population,
	   ze.geom
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
CREATE INDEX ON society.egon_destatis_zensus_apartment_building_population_per_ha USING gist (geom);
