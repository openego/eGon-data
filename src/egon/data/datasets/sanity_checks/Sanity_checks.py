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

sum_installed_gen_cap_DE["Error"] = ((
    
 (sum_installed_gen_cap_DE["capacity_mw"] - sum_installed_gen_cap_DE["target_capacity"])/sum_installed_gen_cap_DE["target_capacity"])*100
)

a1 = sum_installed_gen_cap_DE['Error'].values[0]
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



sum_loads_DE= db.select_dataframe(
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



