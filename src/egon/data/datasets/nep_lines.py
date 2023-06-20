#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd 
import geopandas as gpd
from sqlalchemy import create_engine

engine = create_engine(
    f"postgresql+psycopg2://postgres:"
    f"postgres@localhost:"
    f"5432/egon",
    echo=False,
)

substation_df = pd.read_sql(
    """
    SELECT * FROM 
    grid.egon_ehv_substation
    
    """
    , engine)

substation_df = gpd.read_postgis(
    """
    SELECT * FROM 
    grid.egon_ehv_substation
    
    """
    , engine, geom_col="point")
    
# change



lines_df = pd.read_excel(
    
    )
