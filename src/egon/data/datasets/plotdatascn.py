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
import matplotlib as mpl
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






def plot_generation(
              carrier,scenario
            ):
   con = db.engine()
   SQLBus = "SELECT bus_id, country FROM grid.egon_etrago_bus WHERE country='DE'"
   busDE = pd.read_sql(SQLBus,con)
   busDE = busDE.rename({'bus_id': 'bus'},axis=1)
   #busDE = busDE.loc[busDE['country'] == 'DE']
   sql = "SELECT bus_id, geom FROM grid.egon_mv_grid_district"
   distr = gpd.GeoDataFrame.from_postgis(sql, con)
   distr = distr.rename({'bus_id': 'bus'},axis=1)
   distr = distr.set_index("bus")
   distr = pd.merge(busDE, distr, on='bus')

   sqlCarrier = "SELECT carrier, p_nom, bus FROM grid.egon_etrago_generator"
   sqlCarrier = "SELECT * FROM grid.egon_etrago_generator"
   Carriers = pd.read_sql(sqlCarrier,con)
   Carriers = Carriers.loc[Carriers['scn_name'] == scenario]
   Carriers = Carriers.set_index("bus")


   CarrierGen = Carriers.loc[Carriers['carrier'] == carrier]

   Merge = pd.merge(CarrierGen, distr, on ='bus', how="outer")

    
   Merge.loc[Merge ['carrier'] != carrier, "p_nom" ] = 0
   Merge.loc[Merge ['country'] != "DE", "p_nom" ] = 0

   gdf = gpd.GeoDataFrame(Merge , geometry='geom')
   print(Merge)
   pnom=gdf['p_nom']
   max_pnom=pnom.quantile(0.99)
   print(max_pnom)
   fig, ax = plt.subplots(figsize=(10,10))
   ax.set_axis_off();
   plt.title(f" {carrier} installed capacity in MW , {scenario}")
   cmap = mpl.cm.coolwarm
   norm = mpl.colors.Normalize(vmin=0, vmax=max_pnom)
   gdf.plot(column='p_nom', ax=ax, legend=True,  legend_kwds={'label': "p_nom(MW)",

                       'orientation': "vertical"}, cmap=cmap, norm=norm)
               
       
   return 0
   plot_generation(carrier, scenario)   

