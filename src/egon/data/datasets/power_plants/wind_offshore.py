
import pandas as pd
import geopandas as gpd
import numpy as np
from egon.data import db
from shapely.geometry import Point

def insert():
    """
    Include the off shore wind parks in egon-data
    """
    # Connect to the data base
    offshore_path = 'data_bundle_egon_data/nep2035_version2021/NEP2035_V2021_scnC2035.xlsx'
    offshore = pd.read_excel(offshore_path, sheet_name= 'WInd_Offshore_NEP',
                             usecols= ['Netzverknuepfungspunkt',
                                       'Spannungsebene in kV',
                                       'C 2035'])
    offshore.dropna(subset= ['Netzverknuepfungspunkt'], inplace= True)
    
    # Import manually generated list of wind offshore farms with their connexion
    # points (OSM_id)
    id_bus = {"Büttel": "w136034396",
              "Heide/West":	"w30622610",
              "Suchraum Gemeinden Ibbenbüren/Mettingen/Westerkappeln":	"w114319248",
              "Suchraum Zensenbusch": "w76185022",
              "Rommerskirchen":	"w24839976",
              "Oberzier": "w26593929",
              "Garrel/Ost":	"w23837631",
              "Diele":	"w177829920",
              "Dörpen/West": "w142487746",
              "Emden/Borßum": "w34835258",
              "Emden/Ost": "w34835258",
              "Hagermarsch": "w79316833",
              "Hanekenfähr": "w61918154",
              "Inhausen": "w29420322",
              "Unterweser":	"w32076853",
              "Wehrendorf":	"w33411203",
              "Wilhelmshaven 2": "w157421333",
              "Rastede": "w23837631",
              "Bentwisch": "w32063539",
              "Lubmin":	"w460134233",
              "Suchraum Gemeinde Papendorf": "w32063539",
              "Suchraum Gemeinden Brünzow/Kemnitz":	"w460134233"}
    
    # Match wind offshore table with the corresponding OSM_id
    offshore['osm_id'] = offshore['Netzverknuepfungspunkt'].map(id_bus)
    
    # Connect to the data-base
    con = db.engine()
    
    # Import table with extra high voltage substations
    sql = 'SELECT bus_id, point, voltage, osm_id, subst_name, osm_www FROM grid.egon_ehv_substation'
    substations_ehv = gpd.GeoDataFrame.from_postgis(sql, con, crs="EPSG:4326", geom_col= 'point')
    
    # Import table with high and medium voltage substations
    sql = 'SELECT bus_id, point, voltage, osm_id, subst_name, osm_www FROM grid.egon_hvmv_substation'
    substations_mvhv = gpd.GeoDataFrame.from_postgis(sql, con, crs="EPSG:4326", geom_col= 'point')
    
    # Create columns for bus_id, geometry and osm_web in the offshore df
    offshore['bus_id'] = np.nan
    offshore['geom'] = Point(0,0)
    offshore['osm_web'] = "" # Just for testing purposes
    
    # Match bus_id, geometry and osm_web
    for index, wind_park in offshore.iterrows():
        if len(substations_ehv[substations_ehv['osm_id'] == wind_park['osm_id']].index) > 0:
            a = substations_ehv[substations_ehv['osm_id'] == wind_park['osm_id']].index[0]
            offshore.at[index, 'bus_id'] = substations_ehv.at[a, 'bus_id']
            offshore.at[index, 'geom'] = substations_ehv.at[a, 'point']
            offshore.at[index, 'osm_web'] = substations_ehv.at[a, 'osm_www']
        elif len(substations_mvhv[substations_mvhv['osm_id'] == wind_park['osm_id']].index) > 0:
            a = substations_mvhv[substations_mvhv['osm_id'] == wind_park['osm_id']].index[0]
            offshore.at[index, 'bus_id'] = substations_mvhv.at[a, 'bus_id']
            offshore.at[index, 'geom'] = substations_mvhv.at[a, 'point']
            offshore.at[index, 'osm_web'] = substations_mvhv.at[a, 'osm_www']
        else:
            print(f'Wind offshore farm not found: {wind_park["osm_id"]}')
    
    # Drop offshore wind farms without found connexion point
    offshore.dropna(subset= ['bus_id'], inplace= True)
    
    # Assign voltage levels to wind offshore parks
    offshore['voltage_level'] = np.nan
    offshore.loc[offshore[offshore['Spannungsebene in kV'] == 110].index, 'voltage_level'] = 3
    offshore.loc[offshore[offshore['Spannungsebene in kV'] > 110].index, 'voltage_level'] = 1
    
    # Assign static values
    offshore['carrier'] = 'wind_offshore'
    offshore['chp'] = False
    offshore['el_capacity'] = offshore['C 2035']
    offshore['scenario'] = 'eGon2035'
    offshore['th_capacity'] = 0
    
    #Delete unnecessary columns
    offshore.drop(['Netzverknuepfungspunkt','Spannungsebene in kV',
                   'C 2035', 'osm_id', 'osm_web'], axis = 1, inplace= True)
    
    # convert column "bus_id" to integer
    offshore['bus_id'] = offshore['bus_id'].apply(int) 
    offshore['voltage_level'] = offshore['voltage_level'].apply(int)
    
    # Look for the maximum id in the table egon_power_plants
    sql = "SELECT MAX(id) FROM supply.egon_power_plants"
    max_id = pd.read_sql(sql, con)
    max_id = max_id["max"].iat[0]
    if max_id == None:
        ini_id = 1
    else:
        ini_id = int(max_id + 1)
    
    offshore = gpd.GeoDataFrame(offshore, geometry= 'geom', crs= 4326)
    
    # write_table in egon-data database:
    # Reset index
    offshore.index = pd.RangeIndex(
        start=ini_id, stop=ini_id + len(offshore), name="id"
    )
    
    # Insert into database
    offshore.reset_index().to_postgis(
        "egon_power_plants", schema="supply", con=db.engine(), if_exists="append"
    )
    
    return 0

