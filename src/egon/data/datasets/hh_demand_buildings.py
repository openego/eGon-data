from pathlib import Path
import importlib
import os
import random

from shapely.geometry import Point, Polygon
from sqlalchemy import Column, Integer, String, Table, inspect
from sqlalchemy.ext.declarative import declarative_base
import geopandas as gpd
import importlib_resources as resources
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from egon.data import db
from egon.data.datasets import Dataset
from egon.data.datasets.hh_demand_profiles import (
    HouseholdElectricityProfilesInCensusCells,
)
import egon.data.config

Base = declarative_base()

# Get random seed from config
RANDOM_SEED = egon.data.config.settings()["egon-data"]["--random-seed"]
