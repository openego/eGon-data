"""
The central module containing all code dealing with the H2 grid in eGon100RE

The H2 grid, present only in eGon100RE, is composed of two parts:
  * a fixed part with the same topology than the CH4 grid and with
    carrier 'H2_retrofit' corresponding to the retrofiting of a share of
    the CH4 grid into an hydrogen grid,
  * an extendable part with carrier 'H2_gridextension', linking each
    H2_salcavern bus to the closest H2 bus: this part as no
    capacity (p_nom = 0) but it could be extended.
As the CH4 grid, the H2 pipelines are modelled by PyPSA links.

"""
from geoalchemy2.types import Geometry
from shapely.geometry import LineString, MultiLineString, Point
import geopandas as gpd
import pandas as pd
import os
from urllib.request import urlretrieve
from pathlib import Path
from fuzzywuzzy import process
from shapely import wkb
import math
import re

from egon.data import config, db
from egon.data.datasets.scenario_parameters import get_sector_parameters
from egon.data.datasets.scenario_parameters.parameters import annualize_capital_costs


def insert_h2_pipelines(scn_name):
    "Insert H2_grid based on Input Data from FNB-Gas"
    
    download_h2_grid_data()
    H2_grid_Neubau, H2_grid_Umstellung, H2_grid_Erweiterung = read_h2_excel_sheets()
    h2_bus_location = pd.read_csv(Path(".")/"h2_grid_nodes.csv")
    con=db.engine()
    
    h2_buses_df = pd.read_sql(
    f"""
    SELECT bus_id, x, y FROM grid.egon_etrago_bus
    WHERE carrier in ('H2')
    AND scn_name = '{scn_name}'
    
    """
    , con)
    
    # Delete old entries
    db.execute_sql(
        f"""
            DELETE FROM grid.egon_etrago_link 
            WHERE "carrier" = 'H2_grid'
            AND scn_name = '{scn_name}'
        """
    )
    
    
    target = config.datasets()["etrago_hydrogen"]["targets"]["hydrogen_links"]
    
    for df in [H2_grid_Neubau, H2_grid_Umstellung, H2_grid_Erweiterung]:
        
        if df is H2_grid_Neubau:        
            df.rename(columns={'Planerische \nInbetriebnahme': 'Planerische Inbetriebnahme'}, inplace=True)
            df.loc[df['Endpunkt\n(Ort)'] == 'AQD Anlandung', 'Endpunkt\n(Ort)'] = 'Schillig' 
            df.loc[df['Endpunkt\n(Ort)'] == 'Hallendorf', 'Endpunkt\n(Ort)'] = 'Salzgitter'
        
        if df is H2_grid_Erweiterung:
            df.rename(columns={'Umstellungsdatum/ Planerische Inbetriebnahme': 'Planerische Inbetriebnahme', 
                               'Nenndurchmesser (DN)': 'Nenndurchmesser \n(DN)',
                               'Investitionskosten\n(Mio. Euro),\nKostenschätzung': 'Investitionskosten*\n(Mio. Euro)'}, 
                      inplace=True)
            df = df[df['Berücksichtigung im Kernnetz \n[ja/nein/zurückgezogen]'].str.strip().str.lower() == 'ja']
            df.loc[df['Endpunkt\n(Ort)'] == 'Osdorfer Straße', 'Endpunkt\n(Ort)'] = 'Berlin- Lichterfelde'
            
        h2_bus_location['Ort'] = h2_bus_location['Ort'].astype(str).str.strip()
        df['Anfangspunkt\n(Ort)'] = df['Anfangspunkt\n(Ort)'].astype(str).str.strip()
        df['Endpunkt\n(Ort)'] = df['Endpunkt\n(Ort)'].astype(str).str.strip()

        df = df[['Anfangspunkt\n(Ort)', 'Endpunkt\n(Ort)', 'Nenndurchmesser \n(DN)', 'Druckstufe (DP)\n[mind. 30 barg]', 
                              'Investitionskosten*\n(Mio. Euro)', 'Planerische Inbetriebnahme', 'Länge \n(km)']]
        
        # matching start- and endpoint of each pipeline with georeferenced data
        df['Anfangspunkt_matched'] = fuzzy_match(df, h2_bus_location, 'Anfangspunkt\n(Ort)')
        df['Endpunkt_matched'] = fuzzy_match(df, h2_bus_location, 'Endpunkt\n(Ort)')
        
        # manuell adjustments based on Detailmaßnahmenkarte der FNB-Gas [https://fnb-gas.de/wasserstoffnetz-wasserstoff-kernnetz/]
        df= fix_h2_grid_infrastructure(df)
        
        df_merged = pd.merge(df, h2_bus_location[['Ort', 'geom', 'x', 'y']], 
                             how='left', left_on='Anfangspunkt_matched', right_on='Ort').rename(
                             columns={'geom': 'geom_start', 'x': 'x_start', 'y': 'y_start'})
        df_merged = pd.merge(df_merged, h2_bus_location[['Ort', 'geom', 'x', 'y']], 
                             how='left', left_on='Endpunkt_matched', right_on='Ort').rename(
                             columns={'geom': 'geom_end', 'x': 'x_end', 'y': 'y_end'})
                 
        H2_grid_df = df_merged.dropna(subset=['geom_start', 'geom_end'])   
        H2_grid_df = H2_grid_df[H2_grid_df['geom_start'] != H2_grid_df['geom_end']]
        H2_grid_df = pd.merge(H2_grid_df, h2_buses_df, how='left', left_on=['x_start', 'y_start'], right_on=['x','y']).rename(
            columns={'bus_id': 'bus0'})
        H2_grid_df = pd.merge(H2_grid_df, h2_buses_df, how='left', left_on=['x_end', 'y_end'], right_on=['x','y']).rename(
            columns={'bus_id': 'bus1'})
        H2_grid_df[['bus0', 'bus1']] = H2_grid_df[['bus0', 'bus1']].astype('Int64')

        
        H2_grid_df['geom_start'] = H2_grid_df['geom_start'].apply(lambda x: wkb.loads(bytes.fromhex(x)))
        H2_grid_df['geom_end'] = H2_grid_df['geom_end'].apply(lambda x: wkb.loads(bytes.fromhex(x)))
        H2_grid_df['topo'] = H2_grid_df.apply(
            lambda row: LineString([row['geom_start'], row['geom_end']]), axis=1)
        H2_grid_df['geom'] = H2_grid_df.apply(
            lambda row: MultiLineString([LineString([row['geom_start'], row['geom_end']])]), axis=1)
        H2_grid_gdf = gpd.GeoDataFrame(H2_grid_df, geometry='geom', crs=4326)
        
        scn_params = get_sector_parameters("gas", scn_name)        
        next_link_id = db.next_etrago_id('link')
        
        H2_grid_gdf['link_id'] = range(next_link_id, next_link_id + len(H2_grid_gdf))
        H2_grid_gdf['scn_name'] = scn_name
        H2_grid_gdf['carrier'] = 'H2_grid'
        H2_grid_gdf['Planerische Inbetriebnahme'] = H2_grid_gdf['Planerische Inbetriebnahme'].astype(str).apply(
                lambda x: int(re.findall(r'\d{4}', x)[-1]) if re.findall(r'\d{4}', x) else 
                            int(re.findall(r'\d{2}\.\d{2}\.(\d{4})', x)[-1]) if re.findall(r'\d{2}\.\d{2}\.(\d{4})', x) else None)
        H2_grid_gdf['build_year'] = H2_grid_gdf['Planerische Inbetriebnahme'].astype('Int64')
        H2_grid_gdf['p_nom'] = H2_grid_gdf.apply(lambda row:calculate_H2_capacity(row['Druckstufe (DP)\n[mind. 30 barg]'], row['Nenndurchmesser \n(DN)']), axis=1 )
        H2_grid_gdf['p_nom_min'] = H2_grid_gdf['p_nom']
        H2_grid_gdf['p_nom_max'] = float("Inf")
        H2_grid_gdf['p_nom_extendable'] = False 
        H2_grid_gdf['lifetime'] = scn_params["lifetime"]["H2_pipeline"]
        H2_grid_gdf['capital_cost'] = H2_grid_gdf.apply(lambda row: annualize_capital_costs(
                    (float(row["Investitionskosten*\n(Mio. Euro)"]) * 10**6 / row['p_nom']) 
                    if pd.notna(row["Investitionskosten*\n(Mio. Euro)"]) 
                    and str(row["Investitionskosten*\n(Mio. Euro)"]).replace(",", "").replace(".", "").isdigit() 
                    and float(row["Investitionskosten*\n(Mio. Euro)"]) != 0  
                    else scn_params["overnight_cost"]["H2_pipeline"] * row['Länge \n(km)'], 
                    row['lifetime'], 0.05), axis=1)
        H2_grid_gdf['p_min_pu'] = -1
        
        selected_columns = [
            'scn_name', 'link_id', 'bus0', 'bus1', 'build_year', 'p_nom', 'p_nom_min', 
            'p_nom_extendable', 'capital_cost', 'geom', 'topo', 'carrier', 'p_nom_max', 'p_min_pu',
        ]
        
        H2_grid_final=H2_grid_gdf[selected_columns]
        
        # Insert data to db
        H2_grid_final.to_postgis(
             target["table"],
             con,
             schema= target["schema"],
             if_exists="append",
             dtype={"geom": Geometry()},
        )


def replace_pipeline(df, start, end, intermediate):
    """
    Method for adjusting pipelines manually by splittiing pipeline with an intermediate point.
    
    Parameters
    ----------
    df : pandas.core.frame.DataFrame
        dataframe to be adjusted
    start: str
        startpoint of pipeline
    end: str
        endpoint of pipeline
    intermediate: str
        new intermediate point for splitting given pipeline
        
    Returns
    ---------
    df : <class 'pandas.core.frame.DataFrame'>
        adjusted dataframe
    
            
    """   
    # Find rows where the start and end points match
    mask = ((df['Anfangspunkt_matched'] == start) & (df['Endpunkt_matched'] == end)) | \
           ((df['Anfangspunkt_matched'] == end) & (df['Endpunkt_matched'] == start))
    
    # Separate the rows to replace
    if mask.any():
        df_replacement = df[~mask].copy()
        row_replaced = df[mask].iloc[0]
        
        # Add new rows for the split pipelines
        new_rows = pd.DataFrame({
            'Anfangspunkt_matched': [start, intermediate],
            'Endpunkt_matched': [intermediate, end],
            'Nenndurchmesser \n(DN)': [row_replaced['Nenndurchmesser \n(DN)'], row_replaced['Nenndurchmesser \n(DN)']],  
            'Druckstufe (DP)\n[mind. 30 barg]': [row_replaced['Druckstufe (DP)\n[mind. 30 barg]'], row_replaced['Druckstufe (DP)\n[mind. 30 barg]']], 
            'Investitionskosten*\n(Mio. Euro)': [row_replaced['Investitionskosten*\n(Mio. Euro)'], row_replaced['Investitionskosten*\n(Mio. Euro)']],  
            'Planerische Inbetriebnahme': [row_replaced['Planerische Inbetriebnahme'],row_replaced['Planerische Inbetriebnahme']],  
            'Länge \n(km)': [row_replaced['Länge \n(km)'], row_replaced['Länge \n(km)']]
            })
        
        df_replacement = pd.concat([df_replacement, new_rows], ignore_index=True)
        return df_replacement
    else:
        return df



def fuzzy_match(df1, df2, column_to_match, threshold=80):
    '''
    Method for matching input data of H2_grid with georeferenced data (even if the strings are not exact the same)
    
    Parameters
    ----------
    df1 : pandas.core.frame.DataFrame
        Input dataframe
    df2 : pandas.core.frame.DataFrame
        georeferenced dataframe with h2_buses
    column_to_match: str
        matching column
    treshhold: float
        matching percentage for succesfull comparison
    
    Returns
    ---------
    matched : list
        list with all matched location names        
   
    '''
    options = df2['Ort'].unique()
    matched = []

    # Compare every locationname in df1 with locationnames in df2
    for value in df1[column_to_match]:
        match, score = process.extractOne(value, options)
        if score >= threshold:
            matched.append(match)
        else:
            matched.append(None)

    return matched


def calculate_H2_capacity(pressure, diameter):
    '''
    Method for calculagting capacity of pipelines based on data input from FNB Gas
    
    Parameters
    ----------
    pressure : float
        input for pressure of pipeline
    diameter: float
        input for diameter of pipeline
    column_to_match: str
        matching column
    treshhold: float
        matching percentage for succesfull comparison
    
    Returns
    ---------
    energy_flow: float
        transmission capacity of pipeline
                  
    '''
      
    pressure = str(pressure).replace(",", ".")
    diameter =str(diameter)
    
    def convert_to_float(value):
        try:
            return float(value)
        except ValueError:
            return 400  #average value from data-source cause capacities of some lines are not fixed yet
    
    # in case of given range for pipeline-capacity calculate average value
    if '-' in diameter:
        diameters = diameter.split('-')
        diameter = (convert_to_float(diameters[0]) + convert_to_float(diameters[1])) / 2
    elif '/' in diameter:
        diameters = diameter.split('/')
        diameter = (convert_to_float(diameters[0]) + convert_to_float(diameters[1])) / 2
    else:
        try:
            diameter = float(diameter)
        except ValueError:
            diameter = 400 #average value from data-source
    
    if '-' in pressure:
        pressures = pressure.split('-')
        pressure = (float(pressures[0]) + float(pressures[1])) / 2
    elif '/' in pressure:
        pressures = pressure.split('/')
        pressure = (float(pressures[0]) + float(pressures[1])) / 2
    else:
        try:
            pressure = float(diameter)
        except ValueError:
            pressure = 70 #averaqge value from data-source
            
            
    velocity = 40   #source: L.Koops (2023): GAS PIPELINE VERSUS LIQUID HYDROGEN TRANSPORT – PERSPECTIVES FOR TECHNOLOGIES, ENERGY DEMAND ANDv TRANSPORT CAPACITY, AND IMPLICATIONS FOR AVIATION
    temperature = 10+273.15 #source: L.Koops (2023): GAS PIPELINE VERSUS LIQUID HYDROGEN TRANSPORT – PERSPECTIVES FOR TECHNOLOGIES, ENERGY DEMAND ANDv TRANSPORT CAPACITY, AND IMPLICATIONS FOR AVIATION
    density = pressure*10**5/(4.1243*10**3*temperature) #gasconstant H2 = 4.1243 [kJ/kgK]
    mass_flow = density*math.pi*((diameter/10**3)/2)**2*velocity
    energy_flow = mass_flow * 119.988         #low_heating_value H2 = 119.988 [MJ/kg]

    return energy_flow


def download_h2_grid_data():
    """
    Download Input data for H2_grid from FNB-Gas (https://fnb-gas.de/wasserstoffnetz-wasserstoff-kernnetz/)

    The following data for H2 are downloaded into the folder
    ./datasets/h2_data:
      * Links (file Anlage_3_Wasserstoffkernnetz_Neubau.xlsx,
                    Anlage_4_Wasserstoffkernnetz_Umstellung.xlsx, 
                    Anlage_2_Wasserstoffkernetz_weitere_Leitungen.xlsx)

    Returns
    -------
    None

    """
    path = Path("datasets/h2_data") 
    os.makedirs(path, exist_ok=True)
    
    download_config = config.datasets()["etrago_hydrogen"]["sources"]["H2_grid"]
    target_file_Um = path / download_config["converted_ch4_pipes"]["path"]
    target_file_Neu = path / download_config["new_constructed_pipes"]["path"]
    target_file_Erw = path / download_config["pipes_of_further_h2_grid_operators"]["path"]
    
    for target_file in [target_file_Neu, target_file_Um, target_file_Erw]:
        if target_file is target_file_Um:
            url = download_config["converted_ch4_pipes"]["url"]
        elif target_file is target_file_Neu:
            url = download_config["new_constructed_pipes"]['url']
        else:
            url = download_config["pipes_of_further_h2_grid_operators"]['url']
        
        if not os.path.isfile(target_file):
            urlretrieve(url, target_file)
            

def read_h2_excel_sheets():
    """
   Read downloaded excel files with location names for future h2-pipelines

   Returns
   -------
   df_Neu : <class 'pandas.core.frame.DataFrame'>
   df_Um : <class 'pandas.core.frame.DataFrame'>
   df_Erw : <class 'pandas.core.frame.DataFrame'>
        

   """
    
    path = Path(".") / "datasets" / "h2_data"
    download_config = config.datasets()["etrago_hydrogen"]["sources"]["H2_grid"]
    excel_file_Um = pd.ExcelFile(f'{path}/{download_config["converted_ch4_pipes"]["path"]}')
    excel_file_Neu = pd.ExcelFile(f'{path}/{download_config["new_constructed_pipes"]["path"]}')
    excel_file_Erw = pd.ExcelFile(f'{path}/{download_config["pipes_of_further_h2_grid_operators"]["path"]}')

    df_Um= pd.read_excel(excel_file_Um, header=3)
    df_Neu = pd.read_excel(excel_file_Neu, header=3)
    df_Erw = pd.read_excel(excel_file_Erw, header=2)
    
    return df_Neu, df_Um, df_Erw

def fix_h2_grid_infrastructure(df):
    """
    Manuell adjustments for more accurate grid topology based on Detailmaßnahmenkarte der 
    FNB-Gas [https://fnb-gas.de/wasserstoffnetz-wasserstoff-kernnetz/]

    Returns
    -------
    df : <class 'pandas.core.frame.DataFrame'>       
  
    """
   
    df = replace_pipeline(df, 'Lubmin', 'Uckermark', 'Wrangelsburg')
    df = replace_pipeline(df, 'Wrangelsburg', 'Uckermark', 'Schönermark')
    df = replace_pipeline(df, 'Hemmingstedt', 'Ascheberg (Holstein)', 'Remmels Nord')
    df = replace_pipeline(df, 'Heidenau', 'Elbe-Süd', 'Weißenfelde')
    df = replace_pipeline(df, 'Weißenfelde', 'Elbe-Süd', 'Stade')
    df = replace_pipeline(df, 'Stade AOS', 'KW Schilling', 'Abzweig Stade')
    df = replace_pipeline(df, 'Rosengarten (Sottorf)', 'Moorburg', 'Leversen')
    df = replace_pipeline(df, 'Leversen', 'Moorburg', 'Hamburg Süd')
    df = replace_pipeline(df, 'Achim', 'Folmhusen', 'Wardenburg')
    df = replace_pipeline(df, 'Achim', 'Wardenburg', 'Sandkrug')
    df = replace_pipeline(df, 'Dykhausen', 'Bunde', 'Emden')
    df = replace_pipeline(df, 'Emden', 'Nüttermoor', 'Jemgum')
    df = replace_pipeline(df, 'Rostock', 'Glasewitz', 'Fliegerhorst Laage')
    df = replace_pipeline(df, 'Wilhelmshaven', 'Dykhausen', 'Sande')
    df = replace_pipeline(df, 'Wilhelmshaven Süd', 'Wilhelmshaven Nord', 'Wilhelmshaven')
    df = replace_pipeline(df, 'Sande', 'Jemgum', 'Westerstede')  
    df = replace_pipeline(df, 'Kalle', 'Ochtrup', 'Frensdorfer Bruchgraben')  
    df = replace_pipeline(df, 'Frensdorfer Bruchgraben', 'Ochtrup', 'Bad Bentheim')
    df = replace_pipeline(df, 'Bunde', 'Wettringen', 'Emsbüren')
    df = replace_pipeline(df, 'Emsbüren', 'Dorsten', 'Ochtrup')
    df = replace_pipeline(df, 'Ochtrup', 'Dorsten', 'Heek')
    df = replace_pipeline(df, 'Lemförde', 'Drohne', 'Reiningen')
    df = replace_pipeline(df, 'Edesbüttel', 'Bobbau', 'Uhrsleben')
    df = replace_pipeline(df, 'Sixdorf', 'Wiederitzsch', 'Cörmigk')
    df = replace_pipeline(df, 'Schkeuditz', 'Plaußig', 'Wiederitzsch')
    df = replace_pipeline(df, 'Wiederitzsch', 'Plaußig', 'Mockau Nord')
    df = replace_pipeline(df, 'Bobbau', 'Rückersdorf', 'Nempitz')
    df = replace_pipeline(df, 'Räpitz', 'Böhlen', 'Kleindalzig')
    df = replace_pipeline(df, 'Buchholz', 'Friedersdorf', 'Werben')
    df = replace_pipeline(df, 'Radeland', 'Uckermark', 'Friedersdorf')
    df = replace_pipeline(df, 'Friedersdorf', 'Uckermark', 'Herzfelde')
    df = replace_pipeline(df, 'Blumberg', 'Berlin-Mitte', 'Berlin-Marzahn')
    df = replace_pipeline(df, 'Radeland', 'Zethau', 'Coswig')
    df = replace_pipeline(df, 'Leuna', 'Böhlen', 'Räpitz')
    df = replace_pipeline(df, 'Dürrengleina', 'Stadtroda', 'Zöllnitz')
    df = replace_pipeline(df, 'Mailing', 'Kötz', 'Wertingen')
    df = replace_pipeline(df, 'Lampertheim', 'Rüsselsheim', 'Gernsheim-Nord')
    df = replace_pipeline(df, 'Birlinghoven', 'Rüsselsheim', 'Wiesbaden')
    df = replace_pipeline(df, 'Medelsheim', 'Mittelbrunn', 'Seyweiler')
    df = replace_pipeline(df, 'Seyweiler', 'Dillingen', 'Fürstenhausen')
    df = replace_pipeline(df, 'Reckrod', 'Wolfsbehringen', 'Eisenach')
    df = replace_pipeline(df, 'Elten', 'St. Hubert', 'Hüthum')
    df = replace_pipeline(df, 'St. Hubert', 'Hüthum', 'Uedener Bruch')
    df = replace_pipeline(df, 'Wallach', 'Möllen', 'Spellen')
    df = replace_pipeline(df, 'St. Hubert', 'Glehn', 'Krefeld')
    df = replace_pipeline(df, 'Neumühl', 'Werne', 'Bottrop')
    df = replace_pipeline(df, 'Bottrop', 'Werne', 'Recklinghausen')
    df = replace_pipeline(df, 'Werne', 'Eisenach', 'Arnsberg-Bruchhausen')
    df = replace_pipeline(df, 'Dorsten', 'Gescher', 'Gescher Süd')
    df = replace_pipeline(df, 'Dorsten', 'Hamborn', 'Averbruch')
    df = replace_pipeline(df, 'Neumühl', 'Bruckhausen', 'Hamborn')
    df = replace_pipeline(df, 'Werne', 'Paffrath', 'Westhofen')
    df = replace_pipeline(df, 'Glehn', 'Voigtslach', 'Dormagen')
    df = replace_pipeline(df, 'Voigtslach', 'Paffrath','Leverkusen')
    df = replace_pipeline(df, 'Glehn', 'Ludwigshafen','Wesseling')
    df = replace_pipeline(df, 'Rothenstadt', 'Rimpar','Reutles')
    
    return df