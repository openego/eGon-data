--Drops buses with carriers 'dsm', 'rural_heat_store',
--'central_heat_store' and 'H2_saltcavern' from grid.egon_etrago_bus.
INSERT INTO grid.egon_etrago_bus
    SELECT 'eGon2035_lowflex' as scn_name, bus_id, v_nom, type, carrier, v_mag_pu_set, v_mag_pu_min, v_mag_pu_max,
	x, y, geom, country
    FROM grid.egon_etrago_bus WHERE scn_name='eGon2035';
DELETE FROM grid.egon_etrago_bus
WHERE scn_name = 'eGon2035_lowflex' AND (carrier='dsm' OR carrier='rural_heat_store' 
	   OR carrier='central_heat_store'
	  OR carrier='H2_saltcavern');
	  
	  
--Drops stores with carriers 'dsm', 'rural_heat_store',
--'central_heat_store' and 'H2_saltcavern' from grid.egon_etrago_store.
INSERT INTO grid.egon_etrago_store
    SELECT 'eGon2035_lowflex' as scn_name, store_id, bus, type, carrier, e_nom, e_nom_extendable, 
	e_nom_min, e_nom_max, e_min_pu, e_max_pu, p_set, q_set, e_initial, e_cyclic, 
	sign, marginal_cost, capital_cost, standing_loss, build_year, lifetime
	
    FROM grid.egon_etrago_store WHERE scn_name='eGon2035';
DELETE FROM grid.egon_etrago_store
WHERE scn_name = 'eGon2035_lowflex' AND (carrier='dsm' OR carrier='rural_heat_store' 
	   OR carrier='central_heat_store'
	  OR carrier='H2_saltcavern');


--Drops links with carriers 'dsm', 'rural_heat_store_charger',
--'rural_heat_store_discharger','H2_to_power' and 'power_to_H2' from grid.egon_etrago_link.
INSERT INTO grid.egon_etrago_link
    SELECT 'eGon2035_lowflex' as scn_name, link_id, bus0, bus1, type, carrier, efficiency, 
	build_year, lifetime, p_nom, p_nom_extendable, p_nom_min, p_nom_max, p_min_pu, p_max_pu, 
	p_set, capital_cost, marginal_cost, length, terrain_factor, geom, topo
	
    FROM grid.egon_etrago_link WHERE scn_name='eGon2035';
DELETE FROM grid.egon_etrago_link
WHERE scn_name = 'eGon2035_lowflex' AND (carrier='dsm' OR carrier='rural_heat_store_charger' 
	   OR carrier='rural_heat_store_discharger' OR carrier='central_heat_store_charger'
		OR carrier='central_heat_store_discharger'
	  OR carrier='H2_to_power' OR carrier='power_to_H2');
	  
	  
--Drops stores with carriers 'dsm' from grid.egon_etrago_store_timeseries. Needs check
INSERT INTO grid.egon_etrago_store_timeseries
    SELECT 'eGon2035_lowflex' as scn_name, store_id, temp_id, p_set, q_set, 
	e_min_pu
    FROM grid.egon_etrago_store_timeseries WHERE scn_name='eGon2035';
	DELETE FROM grid.egon_etrago_store_timeseries
	WHERE NOT EXISTS 
	(SELECT store_id 
	 FROM grid.egon_etrago_store
	 WHERE store_id=store_id
	);
	
--Drops links with carriers 'dsm' from grid.egon_etrago_links_timeseries. Needs check
INSERT INTO grid.egon_etrago_link_timeseries
    SELECT 'eGon2035_lowflex' as scn_name, link_id, temp_id, p_set, p_min_pu
    FROM grid.egon_etrago_link_timeseries WHERE scn_name='eGon2035';
	DELETE FROM grid.egon_etrago_link_timeseries
	WHERE NOT EXISTS 
	(SELECT link_id 
	 FROM grid.egon_etrago_link
	 WHERE link_id=link_id
	);
	
--Changes scenario name eGon2035 to eGon2035_lowflex 
--from grid.egon_etrago_generator.
INSERT INTO grid.egon_etrago_generator
    SELECT 'eGon2035_lowflex' as scn_name, generator_id, bus, control, type, carrier, p_nom, p_nom_extendable,
	p_nom_min, p_nom_max, p_min_pu, p_max_pu, p_set, q_set, sign, marginal_cost, build_year,
	lifetime, capital_cost, efficiency, committable, start_up_cost, shut_down_cost, min_up_time, min_down_time,
	up_time_before, down_time_before, ramp_limit_up, ramp_limit_down, ramp_limit_start_up, ramp_limit_shut_down,
	e_nom_max
    FROM grid.egon_etrago_generator WHERE scn_name='eGon2035';
	
--Changes scenario name eGon2035 to eGon2035_lowflex ERROR RELATION DOES NOT EXIST
--from grid.egon_etrago_line.
INSERT INTO grid.egon_etrago_line
    SELECT 'eGon2035_lowflex' as scn_name, line_id, bus0, bus1, type, carrier, x, r,
	g, b, s_nom, s_nom_extendable, s_nom_min, s_nom_max, s_max_pu, build_year, lifetime,
    capital_cost, lenght, cables, terrain_factor, num_parallel, v_ang_min,
	v_ang_max, v_nom, geom
    FROM grid.egon_etrago_generator WHERE scn_name='eGon2035';
	
	
--Changes scenario name eGon2035 to eGon2035_lowflex 
--from grid.egon_etrago_storage.
INSERT INTO grid.egon_etrago_storage
    SELECT 'eGon2035_lowflex' as scn_name, storage_id, bus, control, type, carrier, p_nom,
	p_nom_extendable, p_nom_min, p_nom_max, p_min_pu, s_nom_min, s_nom_max, s_max_pu, build_year, lifetime,
    capital_cost, lenght, cables, terrain_factor, num_parallel, v_ang_min,
	v_ang_max, v_nom, geom
    FROM grid.egon_etrago_generator WHERE scn_name='eGon2035';
	
	
--Changes scenario name eGon2035 to eGon2035_lowflex 
--from grid.egon_etrago_transformer.
INSERT INTO grid.egon_etrago_transformer
    SELECT 'eGon2035_lowflex' as scn_name, trafo_id, bus0, bus1, type, model, x,
	r, g, b, s_nom_extendable, s_nom_min, s_nom_max, s_max_pu, tap_ratio, tap_side,
    tap_position, phase_shift, build_year, lifetime, v_ang_min,
	v_ang_max, capital_cost, num_parallel, geom, topo
    FROM grid.egon_etrago_transformer WHERE scn_name='eGon2035';
