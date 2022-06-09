#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May 24 14:42:05 2022
Plotdatascn.py defines functions to plot to provide a better context of the different parameters part of
scenarios eGon2035 and eGon100RE .
@author: Alonso

"""

# import logging
# import os
from matplotlib import pyplot as plt
import matplotlib.patches as mpatches
import matplotlib
#import pandas as pd
#import numpy as np
#from math import sqrt, log10
#from pyproj import Proj, transform
# import tilemapbase
#import geopandas as gpd
#from egon.data import db
#from egon.data.config import settings
#from egon.data.datasets import Dataset
#import egon.data.config
import pandas as pd
from egon.data import db
from egon.data.datasets import Dataset
import egon.data.config
import geopandas as gpd


#logger = logging.getLogger(__name__)

#if 'READTHEDOCS' not in os.environ:
    #from geoalchemy2.shape import to_shape

__copyright__ = ("Flensburg University of Applied Sciences, "
                 "Europa-Universität Flensburg, "
                 "Centre for Sustainable Energy Systems, "
                 "DLR-Institute for Networked Energy Systems")
__license__ = ""
__author__ = ""

con = db.engine()
  # get MV grid districts
sql = "SELECT bus_id, geom FROM grid.egon_mv_grid_district"
distr = gpd.GeoDataFrame.from_postgis(sql, con)
distr = distr.rename({'bus_id': 'bus'},axis=1)
distr = distr.set_index("bus")
#distr = distr.rename(index={'bus_id': 'bus'})

 # Carriers and p_nom
 
sqlCarrier = "SELECT carrier, p_nom, bus FROM grid.egon_etrago_generator"
sqlCarrier = "SELECT * FROM grid.egon_etrago_generator"
Carriers = pd.read_sql(sqlCarrier,con)
Carriers = Carriers.loc[Carriers['scn_name'] == 'eGon2035']
Carriers = Carriers.set_index("bus")

#Solar rooftop 
SolarRooftop = Carriers.loc[Carriers['carrier'] == 'solar_rooftop']
SolarRooftopGEO = pd.merge(SolarRooftop, distr, on ='bus')



gdf = gpd.GeoDataFrame(SolarRooftopGEO , geometry='geom')



fig, ax = plt.subplots(figsize=(10,10))

ax.set_axis_off();

gdf.plot(column='p_nom', ax=ax, legend=True)






#Carriers.rename(columns={"bus": "bus_id"})


#result = distr.merge(Carriers.rename(columns={'bus': "bus_id"}),
                 #  how='inner',
                 #  on='bus_id', 
                 #  copy=False)
        
# Join data from sql and sqlCarrier 


#sqlFinal = pd.merge(Carriers, distr, how="inner", on="bus")
 
