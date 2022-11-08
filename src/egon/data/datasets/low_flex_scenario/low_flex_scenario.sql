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