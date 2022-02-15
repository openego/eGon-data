#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan 21 10:49:43 2022

@author: student
"""

import egon.data.config
from egon.data import db
from egon.data.datasets.electricity_demand.temporal import insert_cts_load
from egon.data.datasets import Dataset
import pandas as pd



sum_installed_gen_cap_DE= db.select_dataframe(
    f"""
SELECT scn_name, a.carrier, ROUND(SUM(p_nom::numeric), 2) as capacity_mw, ROUND(c.capacity::numeric, 2) as target_capacity
FROM grid.egon_etrago_generator a
JOIN supply.egon_scenario_capacities c 
ON (c.carrier = a.carrier)
WHERE bus IN (
SELECT bus_id FROM grid.egon_etrago_bus
WHERE scn_name = 'eGon2035'
AND country = 'DE')
AND c.scenario_name='eGon2035'
GROUP BY (scn_name, a.carrier, c.capacity);
"""
,
    )
sum_installed_gen_cap_DE.head()

sum_installed_gen_biomass_cap_DE= db.select_dataframe(
    f"""
SELECT scn_name, ROUND(SUM(p_nom::numeric), 2) as capacity_mw_biogass
FROM grid.egon_etrago_generator
WHERE bus IN (
SELECT bus_id FROM grid.egon_etrago_bus
WHERE scn_name = 'eGon2035'
AND country = 'DE')
AND carrier IN ('central_biomass_CHP_heat', 'biomass', 'industrial_biomass_CHP', 'central_biomass_CHP')
GROUP BY (scn_name);
"""
,
    )



sum_installed_gen_cap_DE["Error"] = ((
    
 (sum_installed_gen_cap_DE["capacity_mw"] - sum_installed_gen_cap_DE["target_capacity"])/sum_installed_gen_cap_DE["target_capacity"])*100
)

sum_installed_gen_biomass_cap_DE["Error"] = ((
    
 (sum_installed_gen_biomass_cap_DE["capacity_mw_biogass"] - sum_installed_gen_cap_DE["target_capacity"])/sum_installed_gen_cap_DE["target_capacity"])*100
)


a1 = sum_installed_gen_biomass_cap_DE['Error'].values[0]
a=round(a1,2)

b1 = sum_installed_gen_cap_DE['Error'].values[1]
b=round(b1,2)

c1 = sum_installed_gen_cap_DE['Error'].values[2]
c=round(c1,2)

d1 = sum_installed_gen_cap_DE['Error'].values[3]
d=round(d1,2)

f1 = sum_installed_gen_cap_DE['Error'].values[4]
f=round(f1,2)

print(f"The target values for Biomass differ by {a}  %")

print(f"The target values for Solar differ by {b}  %")

print(f"The target values for Solar Rooftop differ by {c}  %")

print(f"The target values for Wind Offshore differ by {d}  %")

print(f"The target values for Wind Onshore differ by {f}  %")

sum_installed_storage_cap_DE= db.select_dataframe(
    f"""
SELECT scn_name, a.carrier, ROUND(SUM(p_nom::numeric), 2) as capacity_mw, ROUND(c.capacity::numeric, 2) as target_capacity
FROM grid.egon_etrago_storage a
JOIN supply.egon_scenario_capacities c 
ON (c.carrier = a.carrier)
WHERE bus IN (
SELECT bus_id FROM grid.egon_etrago_bus
WHERE scn_name = 'eGon2035'
AND country = 'DE')
AND c.scenario_name='eGon2035'
GROUP BY (scn_name, a.carrier, c.capacity);

"""
,
    )

sum_installed_storage_cap_DE["Error"] = ((
    
 (sum_installed_storage_cap_DE["capacity_mw"] - sum_installed_storage_cap_DE["target_capacity"])/sum_installed_storage_cap_DE["target_capacity"])*100
)

g1 = sum_installed_storage_cap_DE['Error'].values[0]
g=round(g1,2)

print(f"The target values for Pumped Hydro differ by {g}  %")



sum_loads_DE_grid= db.select_dataframe(
    f"""
SELECT a.scn_name, a.carrier,  ROUND((SUM((SELECT SUM(p) FROM UNNEST(b.p_set) p))/1000000)::numeric, 2) as load_twh
FROM grid.egon_etrago_load a
JOIN grid.egon_etrago_load_timeseries b
ON (a.load_id = b.load_id)
JOIN grid.egon_etrago_bus c
ON (a.bus=c.bus_id)
AND b.scn_name = 'eGon2035'
AND a.scn_name = 'eGon2035'
AND c.scn_name= 'eGon2035'
AND c.country='DE'
GROUP BY (a.scn_name, a.carrier);


"""
,
    )

sum_loads_DE_AC_grid = sum_loads_DE_grid['load_twh'].values[1]
sum_loads_DE_heat_grid = sum_loads_DE_grid['load_twh'].values[2]+sum_loads_DE_grid['load_twh'].values[4]


sum_loads_DE_demand_regio_cts_ind= db.select_dataframe(
    f"""
SELECT scenario, ROUND(SUM(demand::numeric/1000000), 2) as demand_mw_regio_cts_ind
FROM demand.egon_demandregio_cts_ind
WHERE scenario= 'eGon2035'
AND year IN ('2035')
GROUP BY (scenario);


"""
,
    )
demand_regio_cts_ind = sum_loads_DE_demand_regio_cts_ind['demand_mw_regio_cts_ind'].values[0]


sum_loads_DE_demand_regio_hh= db.select_dataframe(
    f"""
SELECT scenario, ROUND(SUM(demand::numeric/1000000), 2) as demand_mw_regio_hh
FROM demand.egon_demandregio_hh
WHERE scenario= 'eGon2035'
AND year IN ('2035')
GROUP BY (scenario);


"""
,
    )

demand_regio_hh= sum_loads_DE_demand_regio_hh['demand_mw_regio_hh'].values[0]


sum_loads_DE_AC_demand=demand_regio_cts_ind+demand_regio_hh


sum_loads_DE_demand_peta_heat = db.select_dataframe(
    f"""
SELECT scenario, ROUND(SUM(demand::numeric/1000000), 2) as demand_mw_peta_heat
FROM demand.egon_peta_heat
WHERE scenario= 'eGon2035'
GROUP BY (scenario);


"""
,
    )

sum_peta_heat_DE_demand = sum_loads_DE_demand_peta_heat['demand_mw_peta_heat'].values[0]


Error_AC=(round(
    
 (sum_loads_DE_AC_grid  - sum_loads_DE_AC_demand)/sum_loads_DE_AC_demand,2)*100
)

print(f"The target values for Sum loads AC DE differ by {Error_AC}  %")

demand_peta_heat = sum_loads_DE_demand_peta_heat['demand_mw_peta_heat'].values[0]


Error_heat=(round(
    
 (sum_loads_DE_heat_grid - sum_peta_heat_DE_demand)/sum_peta_heat_DE_demand,2)*100
)

print(f"The target values for Sum loads heat DE differ by {Error_heat}  %")
