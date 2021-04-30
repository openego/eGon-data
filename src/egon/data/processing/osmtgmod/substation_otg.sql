ALTER TABLE grid.egon_hvmv_substation
	ADD COLUMN otg_id bigint;

-- fill table with bus_i from osmtgmod
UPDATE grid.egon_hvmv_substation
	SET 	otg_id = osmtgmod_results.bus_data.bus_i
	FROM 	osmtgmod_results.bus_data
	WHERE 	osmtgmod_results.bus_data.base_kv <= 110 AND (SELECT TRIM(leading 'n' FROM TRIM(leading 'w' FROM grid.egon_hvmv_substation.osm_id))::BIGINT)=osmtgmod_results.bus_data.osm_substation_id; 

DELETE FROM grid.egon_hvmv_substation WHERE otg_id IS NULL;

UPDATE grid.egon_hvmv_substation
	SET 	bus_id = otg_id;

ALTER TABLE grid.egon_hvmv_substation
	DROP COLUMN otg_id;


-- do the same with model_draft.ego_grid_ehv_substation

-- update model_draft.ego_grid_ehv_substation table with new column of respective osmtgmod bus_i
ALTER TABLE grid.egon_ehv_substation
	ADD COLUMN otg_id bigint;

-- fill table with bus_i from osmtgmod
UPDATE grid.egon_ehv_substation
	SET otg_id = osmtgmod_results.bus_data.bus_i
	FROM osmtgmod_results.bus_data
	WHERE osmtgmod_results.bus_data.base_kv > 110 AND(SELECT TRIM(leading 'n' FROM TRIM(leading 'w' FROM TRIM(leading 'r' FROM grid.egon_ehv_substation.osm_id)))::BIGINT)=osmtgmod_results.bus_data.osm_substation_id; 

DELETE FROM grid.egon_ehv_substation WHERE otg_id IS NULL;

UPDATE grid.egon_ehv_substation
	SET 	bus_id = otg_id;

ALTER TABLE grid.egon_ehv_substation
	DROP COLUMN otg_id;




