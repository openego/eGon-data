from pathlib import Path
import random

from geoalchemy2 import Geometry
from shapely.geometry import Point
from sqlalchemy import ARRAY, REAL, Column, Integer, String, Table, inspect
from sqlalchemy.ext.declarative import declarative_base
import geopandas as gpd
import numpy as np
import pandas as pd

from egon.data import db
from egon.data.datasets import Dataset
from egon.data.datasets.hh_demand_profiles import (
    HouseholdElectricityProfilesInCensusCells,
)
import egon.data.config

engine = db.engine()
data_config = egon.data.config.datasets()

Base = declarative_base()

# Get random seed from config
RANDOM_SEED = egon.data.config.settings()["egon-data"]["--random-seed"]
