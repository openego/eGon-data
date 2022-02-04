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



sum_installed_storage_cap_DE= db.select_dataframe(
    f"""
SELECT scn_name, carrier, ROUND(SUM(p_nom::numeric), 2) as capacity_mw
FROM grid.egon_etrago_storage
WHERE bus IN (
SELECT bus_id FROM grid.egon_etrago_bus
WHERE scn_name = 'eGon2035'
AND country = 'DE')
GROUP BY (scn_name, carrier);

"""
,
    )

sum_installed_storage_cap_DE["Error"] = (
    
 (sum_installed_storage_cap_DE["capacity_mw"])*100
)

