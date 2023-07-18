import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine
from geopy.distance import geodesic
import difflib


# Create connection with pgAdmin4 - Offline
engine = create_engine(
    f"postgresql+psycopg2://postgres:"
    f"postgres@localhost:"
    f"5432/etrago",
    echo=False,
)

# Read the Source file
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



# Read the Destination file from CSV
lines_df = pd.read_csv("/home/student/testPowerdData/powerd-data/src/egon/data/datasets/egon_etrago_line/egon_etrago_line_new.csv")

existing_lines_df = pd.read_sql(
    """
    SELECT line_id FROM grid.egon_etrago_line   
    """
    , engine)


# # Read the Destination file from pgAdmin4
# lines_df = pd.read_sql(
#     """
#     SELECT * FROM grid.egon_etrago_line_new
#     UNION
#     SELECT * FROM grid.egon_etrago_line_new;
    
#     """
#     , engine)

# lines_df = gpd.read_postgis(
#     """
#     SELECT * FROM grid.egon_etrago_line_new
#     UNION
#     SELECT * FROM grid.egon_etrago_line_new;
    
#     """
#     , engine)


# Match Similarity of Source & Destination files 
best_match_start=None
best_match_end=None
unique_line_id = 29300

for index, row in lines_df.iterrows():
    # Add Unique line id
    unique_line_id += 1
    lines_df.at[index, 'line_id'] = unique_line_id
    
    
    Startpunkt = str(row['Startpoint'])
    Endpunkt = str(row['Endpoint'])
    
    for r in [1.0, 0.9, 0.8, 0.7, 0.6, 0.5]:
        if best_match_start is None:
            matching_rows_start = substation_df[substation_df['subst_name'].apply(
                lambda x: any(difflib.SequenceMatcher(None, word, Startpunkt).ratio() >= r for word in x.split()))]
            if not matching_rows_start.empty:
                best_match_start = 1
                lines_df.at[index, 'bus0'] = matching_rows_start.iloc[0]['bus_id']
                
                # Find coordinate for first point
                point_1 = matching_rows_start.iloc[0]['point']
                formatted_point_1 = f"{point_1.x} {point_1.y}"
                lines_df.at[index, 'Coordinate0'] = formatted_point_1

                # Calculate the matching percentage
                matching_percentage_start = difflib.SequenceMatcher(None, Startpunkt, matching_rows_start.iloc[0]['subst_name']).ratio() * 100
                lines_df.at[index, 'matching1%'] = round(matching_percentage_start,2) 
                
        if best_match_end is None:
            matching_rows_end = substation_df[substation_df['subst_name'].apply(
                lambda x: any(difflib.SequenceMatcher(None, word, Endpunkt).ratio() >= r for word in x.split()))]

            if not matching_rows_end.empty:
                best_match_end = 1
                lines_df.at[index, 'bus1'] = matching_rows_end.iloc[0]['bus_id']
                
                # Find coordinate for Second point
                point_2 = matching_rows_end.iloc[0]['point']
                formatted_point_2 = f"{point_2.x} {point_2.y}"
                lines_df.at[index, 'Coordinate1'] = formatted_point_2

                # Calculate the matching percentage
                matching_percentage_end = difflib.SequenceMatcher(None, Endpunkt, matching_rows_end.iloc[0]['subst_name']).ratio() * 100
                lines_df.at[index, 'matching2%'] = round(matching_percentage_end,2)

                if pd.notna(formatted_point_1) and pd.notna(formatted_point_2):
                    lon1, lat1 = map(float, formatted_point_1.split(' '))
                    lon2, lat2 = map(float, formatted_point_2.split(' '))
                    distance = geodesic((lat1, lon1), (lat2, lon2)).kilometers
                    lines_df.at[index, 'length'] = distance
                    
    best_match_end = None
    best_match_start = None

buildyear_df = pd.read_csv("/home/student/Documents/Powerd/NEP_tables_finalVersion.csv")
buildyear_df['anvisierte Inbetriebnahme'] = buildyear_df["anvisierte Inbetriebnahme"].fillna('').str.split(r'[,/-]').apply(lambda x: x[1].strip() if len(x) > 1 else x[0].strip())

for index1, row1 in lines_df.iterrows():
    scn_name1 = str(row1['scn_name'])
    for index2, row2 in buildyear_df.iterrows():
        scn_name2 = str(row2['scn_name'])
        if scn_name1 == scn_name2:
            lines_df.loc[index1, 'build_year'] = row2['anvisierte Inbetriebnahme']


# Save the updated file
lines_df.to_csv('/home/student/testPowerdData/powerd-data/src/egon/data/datasets/egon_etrago_line/egon_etrago_line_new.csv', index=False)



print("Operation successful")