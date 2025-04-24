/*
Low flex scenario
Create a low flex scenario based on scenario 'nep2037_2025' by copying this scenario and
neglecting all flexibilities provided by non-electrical sectors.
__copyright__   = "Hochschule Flensburg"
__license__     = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__         = "https://github.com/openego/data_processing/blob/master/LICENSE"
__author__      = "Alonsoju95, IlkaCu"
*/

-- Copy relevant buses and bus time series
DELETE FROM grid.egon_etrago_bus WHERE scn_name='nep2037_2025_lowflex';
DELETE FROM grid.egon_etrago_bus_timeseries WHERE scn_name='nep2037_2025_lowflex';

INSERT INTO grid.egon_etrago_bus
    SELECT
	'nep2037_2025_lowflex' as scn_name,
	bus_id,
	v_nom,
	type,
	carrier,
	v_mag_pu_set,
	v_mag_pu_min,
	v_mag_pu_max,
	x,
	y,
	geom,
	country
    FROM grid.egon_etrago_bus
	WHERE scn_name='nep2037_2025'
	AND carrier NOT IN
		('dsm',
		 'rural_heat_store',
		 'central_heat_store',
	  	 'H2_saltcavern',
		 'Li_ion');

INSERT INTO grid.egon_etrago_bus_timeseries
	SELECT
		'nep2037_2025_lowflex' as scn_name,
		bus_id,
		v_mag_pu_set
	FROM grid.egon_etrago_bus_timeseries
	WHERE scn_name='nep2037_2025'
	AND bus_id IN
		(SELECT bus_id
		 	FROM grid.egon_etrago_bus
		 	WHERE scn_name = 'nep2037_2025_lowflex');

-- Copy relevant generators including time series
DELETE FROM grid.egon_etrago_generator WHERE scn_name='nep2037_2025_lowflex';
DELETE FROM grid.egon_etrago_generator_timeseries WHERE scn_name='nep2037_2025_lowflex';

INSERT INTO grid.egon_etrago_generator
    SELECT
		'nep2037_2025_lowflex' as scn_name,
		generator_id,
		bus,
		control,
		type,
		carrier,
		p_nom,
		p_nom_extendable,
		p_nom_min,
		p_nom_max,
		p_min_pu,
		p_max_pu,
		p_set,
		q_set,
		sign,
		marginal_cost,
		build_year,
		lifetime,
		capital_cost,
		efficiency,
		committable,
		start_up_cost,
		shut_down_cost,
		min_up_time,
		min_down_time,
		up_time_before,
		down_time_before,
		ramp_limit_up,
		ramp_limit_down,
		ramp_limit_start_up,
		ramp_limit_shut_down,
		e_nom_max
    FROM grid.egon_etrago_generator
	WHERE scn_name='nep2037_2025';

INSERT INTO grid.egon_etrago_generator_timeseries
    SELECT
		'nep2037_2025_lowflex' as scn_name,
		generator_id,
		temp_id,
		p_set,
		q_set,
		p_min_pu,
		p_max_pu
    FROM grid.egon_etrago_generator_timeseries
	WHERE scn_name='nep2037_2025';

-- Copy lines including time series without copying s_max_pu
DELETE FROM grid.egon_etrago_line WHERE scn_name='nep2037_2025_lowflex';
DELETE FROM grid.egon_etrago_line_timeseries WHERE scn_name='nep2037_2025_lowflex';

INSERT INTO grid.egon_etrago_line
    SELECT
		'nep2037_2025_lowflex' as scn_name,
		line_id,
		bus0,
		bus1,
		type,
		carrier,
		x,
		r,
		g,
		b,
		s_nom,
		s_nom_extendable,
		s_nom_min,
		s_nom_max,
		s_max_pu,
		build_year,
		lifetime,
		capital_cost,
		length,
		cables,
		terrain_factor,
		num_parallel,
		v_ang_min,
		v_ang_max,
		v_nom,
		geom,
		topo
    FROM grid.egon_etrago_line
	WHERE scn_name='nep2037_2025';

INSERT INTO grid.egon_etrago_line_timeseries
    SELECT
		'nep2037_2025_lowflex' as scn_name,
		line_id,
		temp_id,
		NULL as s_max_pu
    FROM grid.egon_etrago_line_timeseries
	WHERE scn_name='nep2037_2025';

-- Copy relevant link components including time series
DELETE FROM grid.egon_etrago_link WHERE scn_name='nep2037_2025_lowflex';
DELETE FROM grid.egon_etrago_link_timeseries WHERE scn_name='nep2037_2025_lowflex';

INSERT INTO grid.egon_etrago_link
    SELECT
		'nep2037_2025_lowflex' as scn_name,
		link_id,
		bus0,
		bus1,
		type,
		carrier,
		efficiency,
		build_year,
		lifetime,
		p_nom,
		p_nom_extendable,
		p_nom_min,
		p_nom_max,
		p_min_pu,
		p_max_pu,
		p_set,
		capital_cost,
		marginal_cost,
		length,
		terrain_factor,
		geom,
		topo
    FROM grid.egon_etrago_link
	WHERE scn_name='nep2037_2025'
	AND carrier NOT IN
	(
		'dsm',
		'rural_heat_store_charger',
		'rural_heat_store_discharger',
		'central_heat_store_charger',
		'central_heat_store_discharger',
		'BEV_charger'
	)
    AND link_id NOT IN (
        SELECT link_id FROM grid.egon_etrago_link
        WHERE scn_name = 'nep2037_2025'
        AND carrier IN ('H2_to_power', 'power_to_H2')
        AND (bus0 IN (SELECT bus_id
                        FROM grid.egon_etrago_bus
                        WHERE scn_name='nep2037_2025'
                        AND carrier= 'H2_saltcavern')
             OR bus1 IN (SELECT bus_id
                            FROM grid.egon_etrago_bus
                            WHERE scn_name='nep2037_2025'
                            AND carrier= 'H2_saltcavern')));

INSERT INTO grid.egon_etrago_link_timeseries
    SELECT
		'nep2037_2025_lowflex' as scn_name,
		link_id,
		temp_id,
		p_set,
		p_min_pu,
		p_max_pu,
		efficiency,
		marginal_cost
    FROM grid.egon_etrago_link_timeseries
	WHERE scn_name='nep2037_2025'
	AND link_id IN
	(
		SELECT link_id
	 	FROM grid.egon_etrago_link
	 	WHERE scn_name='nep2037_2025_lowflex'
	);

-- Copy relevant load components including time series except for emobility (MIT) which
-- have been created in egon.data.datasets.emobility.motorized_individual_travel.model_timeseries
DELETE FROM grid.egon_etrago_load_timeseries WHERE scn_name='nep2037_2025_lowflex'
AND load_id NOT IN (
 SELECT load_id FROM grid.egon_etrago_load WHERE scn_name='nep2037_2025_lowflex' AND carrier = 'land_transport_EV'
);
DELETE FROM grid.egon_etrago_load WHERE scn_name='nep2037_2025_lowflex' AND carrier != 'land_transport_EV';

INSERT INTO grid.egon_etrago_load
    SELECT
		'nep2037_2025_lowflex' as scn_name,
		load_id,
		bus,
		type,
		carrier,
		p_set,
		q_set,
		sign
    FROM grid.egon_etrago_load
	WHERE scn_name='nep2037_2025'
	AND carrier != 'land_transport_EV';

INSERT INTO grid.egon_etrago_load_timeseries
    SELECT
		'nep2037_2025_lowflex' as scn_name,
		load_id,
		temp_id,
		p_set,
		q_set
    FROM grid.egon_etrago_load_timeseries
	WHERE scn_name='nep2037_2025'
	AND load_id IN (
	SELECT load_id
	FROM grid.egon_etrago_load
	WHERE scn_name = 'nep2037_2025_lowflex'
	AND carrier != 'land_transport_EV')
	;

-- Copy relevant storage components including time series
DELETE FROM grid.egon_etrago_storage WHERE scn_name='nep2037_2025_lowflex';
DELETE FROM grid.egon_etrago_storage_timeseries WHERE scn_name='nep2037_2025_lowflex';

INSERT INTO grid.egon_etrago_storage
    SELECT
		'nep2037_2025_lowflex' as scn_name,
		storage_id,
		bus,
		control,
		type,
		carrier,
		p_nom,
		p_nom_extendable,
		p_nom_min,
		p_nom_max,
		p_min_pu,
		p_max_pu,
		p_set,
		q_set,
		sign,
		marginal_cost,
		capital_cost,
		build_year,
		lifetime,
		state_of_charge_initial,
		cyclic_state_of_charge,
		state_of_charge_set,
		max_hours,
		efficiency_store,
		efficiency_dispatch,
		standing_loss,
		inflow
    FROM grid.egon_etrago_storage
	WHERE scn_name='nep2037_2025';

INSERT INTO grid.egon_etrago_storage_timeseries
    SELECT
		'nep2037_2025_lowflex' as scn_name,
		storage_id,
		temp_id,
		p_set,
		q_set,
		p_min_pu,
		p_max_pu,
		state_of_charge_set,
		inflow,
		marginal_cost
    FROM grid.egon_etrago_storage_timeseries
	WHERE scn_name='nep2037_2025';

 -- Copy relevant store components including time series
DELETE FROM grid.egon_etrago_store WHERE scn_name='nep2037_2025_lowflex';
DELETE FROM grid.egon_etrago_store_timeseries WHERE scn_name='nep2037_2025_lowflex';

INSERT INTO grid.egon_etrago_store
    SELECT
		'nep2037_2025_lowflex' as scn_name,
		store_id,
		bus,
		type,
		carrier,
		e_nom,
		e_nom_extendable,
		e_nom_min,
		e_nom_max,
		e_min_pu,
		e_max_pu,
		p_set,
		q_set,
		e_initial,
		e_cyclic,
		sign,
		marginal_cost,
		capital_cost,
		standing_loss,
		build_year,
		lifetime
    FROM grid.egon_etrago_store
	WHERE scn_name='nep2037_2025'
	AND carrier NOT IN
		('dsm',
		 'rural_heat_store',
	   	 'central_heat_store',
	     'H2_underground',
		 'battery_storage');

INSERT INTO grid.egon_etrago_store_timeseries
    SELECT
	'nep2037_2025_lowflex' as scn_name,
	store_id,
	temp_id,
	p_set,
	q_set,
	e_min_pu,
	e_max_pu,
	marginal_cost
    FROM grid.egon_etrago_store_timeseries
	WHERE scn_name='nep2037_2025'
	AND store_id IN
		(SELECT store_id
		 FROM grid.egon_etrago_store
		 WHERE scn_name='nep2037_2025_lowflex'
		);


-- Copy relevant transformers including time series
DELETE FROM grid.egon_etrago_transformer WHERE scn_name='nep2037_2025_lowflex';
DELETE FROM grid.egon_etrago_transformer_timeseries WHERE scn_name='nep2037_2025_lowflex';

INSERT INTO grid.egon_etrago_transformer
    SELECT
		'nep2037_2025_lowflex' as scn_name,
		trafo_id,
		bus0,
		bus1,
		type,
		model,
		x,
		r,
		g,
		b,
		s_nom,
		s_nom_extendable,
		s_nom_min,
		s_nom_max,
		s_max_pu,
		tap_ratio,
		tap_side,
		tap_position,
		phase_shift,
		build_year,
		lifetime,
		v_ang_min,
		v_ang_max,
		capital_cost,
		num_parallel,
		geom,
		topo
    FROM grid.egon_etrago_transformer
	WHERE scn_name='nep2037_2025';

INSERT INTO grid.egon_etrago_transformer_timeseries
    SELECT
	'nep2037_2025_lowflex' as scn_name,
	trafo_id,
	temp_id,
	s_max_pu
    FROM grid.egon_etrago_transformer_timeseries
	WHERE scn_name='nep2037_2025';
