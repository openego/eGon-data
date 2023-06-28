#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from turtle import home
import pandas as pd 
import geopandas as gpd
import psycopg2
from sqlalchemy import create_engine



engine = create_engine(
    f"postgresql+psycopg2://postgres:"
    f"postgres@localhost:"
    f"5432/etrago",
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
    


lines_df = pd.read_csv('/home/student/Documents/Powerd/NEP_Lines_compressed.csv')

# matching the row data
for index, row in lines_df.iterrows():
    Startpunkt = row['Startpoint']
    Endpunkt = row ['Endpoint']
    
    #matching_rows_start = substation_df[substation_df['subst_name']==Startpunkt]
    matching_rows_start = substation_df[substation_df['subst_name'].str.contains(str(Startpunkt), regex=False)]
    if not matching_rows_start.empty:
        lines_df.at[index, 'bus_id_1'] = matching_rows_start.iloc[0]['bus_id']
    if len(matching_rows_start) > 0:    
        lines_df.at[index, 'Point_1']=matching_rows_start.iloc[0]['point']

    #matching_rows_end = substation_df[substation_df['subst_name'].isin([Endpunkt])]
    matching_rows_end = substation_df[substation_df['subst_name'].str.contains(str(Endpunkt), regex=False)]
    if not matching_rows_end.empty:
        lines_df.at[index, 'bus_id_2'] = matching_rows_end.iloc[0]['bus_id']
    if len(matching_rows_start) > 0:
        lines_df.at[index, 'Point_2']=matching_rows_start.iloc[0]['point']
    
# Save the updated file
lines_df.to_csv('/home/student/Documents/Powerd/NEP_Lines_compressed.csv', index=False)


print("operation succesfully")


