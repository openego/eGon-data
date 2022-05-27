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
# from matplotlib import pyplot as plt
# import matplotlib.patches as mpatches
#import matplotlib
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
distr = distr.set_index("bus_id")



           