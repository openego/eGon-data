# -*- coding: utf-8 -*-
"""
Created on Thu Jun 29 09:16:10 2023

@author: Sayed Mohammad
"""

import pandas as pd
import geopandas as gpd
import psycopg2
from sqlalchemy import create_engine
from turtle import home
from shapely.geometry import Point
from geopy.distance import geodesic
import difflib

# reading data from pgAdmin4
engine = create_engine(
    f"postgresql+psycopg2://postgres:"
    f"postgres@localhost:"
    f"5432/etrago",
    echo=False,
)

substation_df = pd.read_sql(
    """
    SELECT * FROM grid.egon_ehv_substation
    UNION
    SELECT * FROM grid.egon_hvmv_substation;
    
    """
    , engine)



substation_df = gpd.read_postgis(
    """
    SELECT * FROM grid.egon_ehv_substation
    UNION
    SELECT * FROM grid.egon_hvmv_substation;
    
    """
    , engine, geom_col="point")

# reading data from local file
lines_df = pd.read_csv("/home/student/Documents/Powerd/NEP_Lines_compressed.csv")

best_match_start=None
best_match_end=None

        # matching the row data
for index, row in lines_df.iterrows():
    Startpunkt = str(row['Startpoint'])
    Endpunkt = str(row['Endpoint'])

    for r in [1.0, 0.9, 0.8, 0.7, 0.6, 0.5]:
        
        if best_match_start is None:
            matching_rows_start = substation_df[substation_df['subst_name'].apply(lambda x: any(difflib.SequenceMatcher(None, word, Startpunkt).ratio() >= r for word in x.split()))]
            #matching_rows_start = substation_df[substation_df['subst_name'].str.contains(str(Startpunkt), regex=False)]
            if not matching_rows_start.empty:
                # Extracting required data for the first point
                best_match_start=1
                lines_df.at[index, 'bus_id_1'] = matching_rows_start.iloc[0]['bus_id']
                point_1 = matching_rows_start.iloc[0]['point']
                formatted_point_1 = f"{point_1.x}, {point_1.y}"
                lines_df.at[index, 'Point_1'] = formatted_point_1
                lines_df.at[index, 'subst_name1'] = matching_rows_start.iloc[0]['subst_name']
            else:
                matching_rows_start = substation_df[substation_df['subst_name'].apply(lambda x: any(difflib.SequenceMatcher(None, word, Startpunkt).ratio() >= r for word in x.split()))]
            
        if best_match_end is None:
            matching_rows_end = substation_df[substation_df['subst_name'].apply(lambda x: any(difflib.SequenceMatcher(None, word, Endpunkt).ratio() >= r   for word in x.split()))]

            #matching_rows_end = substation_df[substation_df['subst_name'].str.contains(str(Endpunkt), regex=False)]
            if not matching_rows_end.empty:
                best_match_end=1
                # Extracting required data for the second point
                lines_df.at[index, 'bus_id_2'] = matching_rows_end.iloc[0]['bus_id']
                point_2 = matching_rows_end.iloc[0]['point']
                formatted_point_2 = f"{point_2.x}, {point_2.y}"
                lines_df.at[index, 'Point_2'] = formatted_point_2
                lines_df.at[index, 'subst_name2'] = matching_rows_end.iloc[0]['subst_name']

                # Calculate distance if both points are available
                if pd.notna(formatted_point_1) and pd.notna(formatted_point_2):
                    lon1, lat1 = map(float, formatted_point_1.split(','))
                    lon2, lat2 = map(float, formatted_point_2.split(','))
                    distance = geodesic((lat1, lon1), (lat2, lon2)).kilometers
                    lines_df.at[index, 'Distance'] = distance
    best_match_end=None
    best_match_start=None

# Save the updated file
lines_df.to_csv('/home/student/Documents/Powerd/NEP_Lines_compressed_updated.csv', index=False)

print("Operation successful")