/*
`osmTGmod <https://github.com/openego/osmTGmod>`_ provides a model of the German EHV and HV grid based on OpenStreetMap.
This script extracts `bus <grid.otg_ehvhv_bus_data>`_ and `branch data <grid.otg_ehvhv_branch_data>`_ provided by osmTGmod
and inserts the grid model into the corresponding powerflow tables.
Additionally some (electrical) properties for transformers are adjusted or added. 

__copyright__ 	= "Flensburg University of Applied Sciences, Centre for Sustainable Energy Systems"
__license__ 	= "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__ 	= "https://github.com/openego/data_processing/blob/master/LICENSE"
__author__ 	= "ulfmueller, IlkaCu, mariusves" 
*/
-- osmTGmod2pyPSA

-- CLEAN UP OF TABLES
TRUNCATE grid.egon_pf_hv_bus CASCADE;
TRUNCATE grid.egon_pf_hv_line CASCADE;
TRUNCATE grid.egon_pf_hv_transformer CASCADE;

-- BUS DATA
INSERT INTO grid.egon_pf_hv_bus (bus_id, v_nom, geom)
SELECT 
  bus_i AS bus_id,
  base_kv AS v_nom,
  geom
  FROM osmtgmod_results.bus_data
  WHERE result_id = 1;


-- BRANCH DATA
INSERT INTO grid.egon_pf_hv_line (line_id, bus0, bus1, x, r, b, s_nom, cables, frequency, geom, topo)
SELECT 
  branch_id AS line_id,
  f_bus AS bus0,
  t_bus AS bus1,
  br_x AS x,
  br_r AS r,
  br_b as b,
  rate_a as s_nom,
  cables,
  geom,
  topo
  FROM osmtgmod_results.branch_data
  WHERE result_id = 1 and (link_type = 'line' or link_type = 'cable');


-- TRANSFORMER DATA
INSERT INTO grid.egon_pf_hv_transformer (trafo_id, bus0, bus1, x, s_nom, tap_ratio, phase_shift, geom, topo)
SELECT 
  branch_id AS trafo_id,
  f_bus AS bus0,
  t_bus AS bus1,
  br_x/100 AS x,
  rate_a as s_nom,
  tap AS tap_ratio,
  shift AS phase_shift,
  geom,
  topo
  FROM osmtgmod_results.branch_data
  WHERE result_id = 1 and link_type = 'transformer';


-- per unit to absolute values

UPDATE grid.egon_pf_hv_line a
	SET 
		r = r * (((SELECT v_nom 
				FROM grid.egon_pf_hv_bus 
				WHERE bus_id=bus1)*1000)^2 / (100 * 10^6)),
		x = x * (((SELECT v_nom 
				FROM grid.egon_pf_hv_bus
				WHERE bus_id=bus1)*1000)^2 / (100 * 10^6)),
		b = b * (((SELECT v_nom 
				FROM grid.egon_pf_hv_bus
				WHERE bus_id=bus1)*1000)^2 / (100 * 10^6));

-- calculate line length (in km) from geoms

UPDATE grid.egon_pf_hv_line a
	SET 
		length = result.length
		FROM 
		(SELECT b.line_id, st_length(b.geom,false)/1000 as length 
		from grid.egon_pf_hv_line b)
		as result
WHERE a.line_id = result.line_id;

-- delete buses without connection to AC grid and generation or load assigned
-- TODO: get rid of hard coded scn_name

/*
DELETE FROM grid.egon_pf_hv_bus WHERE scn_name='Status Quo' 
AND bus_id NOT IN 
	(SELECT bus0 FROM grid.egon_pf_hv_line WHERE scn_name='Status Quo')
AND bus_id NOT IN 
	(SELECT bus1 FROM grid.egon_pf_hv_line WHERE scn_name='Status Quo')
AND bus_id NOT IN 
	(SELECT bus0 FROM grid.egon_pf_hv_transformer WHERE scn_name='Status Quo')
AND bus_id NOT IN 
	(SELECT bus1 FROM grid.egon_pf_hv_transformer WHERE scn_name='Status Quo'); 
*/

/*
-- order bus0 and bus1 IDs for easier grouping of parallel lines

UPDATE grid.egon_pf_hv_line b
SET 
bus0 = a.bus0,
bus1 = a.bus1
FROM
(SELECT 
	line_id,				
	CASE 
	WHEN bus0 < bus1 
	THEN bus0 
	ELSE bus1 
	END as bus0,
	CASE 
	WHEN bus0 < bus1 
	THEN bus1 
	ELSE bus0 
	END as bus1
FROM  grid.egon_pf_hv_line
WHERE scn_name = 'Status Quo'
ORDER BY line_id) as a
WHERE b.line_id = a.line_id AND
scn_name = 'Status Quo';

-- same for transformers:

UPDATE grid.egon_pf_hv_transformer b
SET 
bus0 = a.bus0,
bus1 = a.bus1
FROM
(SELECT 
	trafo_id,				
	CASE 
	WHEN bus0 < bus1 
	THEN bus0 
	ELSE bus1 
	END as bus0,
	CASE 
	WHEN bus0 < bus1 
	THEN bus1 
	ELSE bus0 
	END as bus1
FROM  grid.egon_pf_hv_transformer
WHERE scn_name = 'Status Quo'
ORDER BY trafo_id) as a
WHERE b.trafo_id = a.trafo_id AND
scn_name = 'Status Quo';

-- duplicate 'status quo' model with parallel lines merged to a single line

INSERT INTO grid.egon_pf_hv_line (
scn_name, line_id, bus0, bus1, x, r, b, s_nom, length, cables, frequency, geom, topo)
SELECT 
	'Status Quo grouped' as scn_name, min(line_id), bus0, bus1, sum(x^(-1))^(-1) as x, sum(r^(-1))^(-1) as r, sum(b) as b, 
	sum(s_nom) as s_nom, avg(length) as length, sum(cables) as cables, 50 as frequency,min(geom) as geom, min(topo) as topo
FROM grid.egon_pf_hv_line
WHERE scn_name = 'Status Quo'
GROUP BY bus0,bus1;

DELETE FROM  grid.egon_pf_hv_line WHERE scn_name = 'Status Quo';
UPDATE grid.egon_pf_hv_line SET scn_name = 'Status Quo' WHERE scn_name = 'Status Quo grouped';
*/
