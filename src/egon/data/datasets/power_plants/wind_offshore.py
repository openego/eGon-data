from pathlib import Path

from shapely.geometry import Point
import geopandas as gpd
import numpy as np
import pandas as pd

from egon.data import db
import egon.data.config

def map_to_city():
    # map from connection point to city
    to_city = {
        'SEL900790170542': 'Dörpen/West',
        'SEL903815302577': 'Dörpen/West',
        'SEL906065107428': 'Büttel',
        'SEL919433257267': 'Büttel',
        'SEL919746983181': 'Lubmin',
        'SEL919786394244': 'Emden/Ost',
        'SEL922199100944': 'Büttel',
        'SEL925169438559': 'Dörpen/West',
        'SEL928599601562': 'Büttel',
        'SEL929196129428': 'Cloppenburg',
        'SEL932958083050': 'Emden/Ost',
        'SEL933375144706': 'inhausen',
        'SEL941255164959': 'Emden/Borßum',
        'SEL943058624104': 'Bentwisch',
        'SEL943629000788': 'Emden/Ost',
        'SEL947312274135': 'Dörpen/West',
        'SEL947778987563': 'Dörpen/West',
        'SEL953673961584': 'Cloppenburg',
        'SEL960138676303': 'Büttel',
        'SEL964485571465': 'Cloppenburg',
        'SEL964827249373': 'Emden/Ost',
        'SEL969972182469': 'Büttel',
        'SEL988334563685': 'Dörpen/West',
        'SEL990492206410': 'Bentwisch',
        'SEL997728577467': 'Lubmin',
        'SEL998582120202': 'Emden/Ost',
        }
    return to_city

def map_id_bus(scenario="eGon2035"):
    # Import manually generated list of wind offshore farms with their
    # connection points (OSM_id)
    if scenario=="eGon2035":
        id_bus = {
            "Büttel": "136034396",
            "Heide/West": "603661085",
            "Suchraum Gemeinden Ibbenbüren/Mettingen/Westerkappeln": "114319248",
            "Suchraum Zensenbusch": "76185022",
            "Rommerskirchen": "24839976",
            "Oberzier": "26593929",
            "Garrel/Ost": "23837631",
            "Diele": "177829920",
            "Dörpen/West": "142487746",
            "Emden/Borßum": "34835258",
            "Emden/Ost": "34835258",
            "Hagermarsch": "79316833",
            "Hanekenfähr": "61918154",
            "Inhausen": "29420322",
            "Unterweser": "32076853",
            "Wehrendorf": "33411203",
            "Wilhelmshaven 2": "23744346",
            "Rastede": "23837631",
            "Bentwisch": "32063539",
            "Lubmin": "460134233",
            "Suchraum Gemeinde Papendorf": "32063539",
            "Suchraum Gemeinden Brünzow/Kemnitz": "460134233",
            "inhausen": "29420322",
            "Cloppenburg": "50643382"
        }
    elif scenario=="status2019":
        id_bus = {
        'UW Inhausen':'29420322',
        'UW Bentwisch':'32063539',
        'UW Emden / Borssum':'34835258',
        'UW Emden Ost':'34835258',
        'UW Cloppenburg':'50643382',
        'UW Hagermarsch':'79316833',
        'UW Büttel':'136034396',
        'UW Dörpen West':'142487746',
        'UW Diele':'177829920',
        'UW Lubmin':'460134233',
        }
    else:
        id_bus = {}
        
    return id_bus

def map_w_id():
    w_id = {
        "Büttel": "16331",
        "Heide/West": "16516",
        "Suchraum Gemeinden Ibbenbüren/Mettingen/Westerkappeln": "16326",
        "Suchraum Zensenbusch": "16139",
        "Rommerskirchen": "16139",
        "Oberzier": "16139",
        "Garrel/Ost": "16139",
        "Diele": "16138",
        "Dörpen/West": "15952",
        "Emden/Borßum": "15762",
        "Emden/Ost": "16140",
        "Hagermarsch": "15951",
        "Hanekenfähr": "16139",
        "Inhausen": "15769",
        "Unterweser": "16517",
        "Wehrendorf": "16139",
        "Wilhelmshaven 2": "16517",
        "Rastede": "16139",
        "Bentwisch": "16734",
        "Lubmin": "16548",
        "Suchraum Gemeinde Papendorf": "16352",
        "Suchraum Gemeinden Brünzow/Kemnitz": "16548",
        "inhausen": "15769",
        "Cloppenburg": "16334"
    }
    return w_id

def map_ONEP_areas():
    onep = {
        "NOR-0-1": Point(6.5,53.6),
        "NOR-0-2": Point(8.07,53.76),
        "NOR-1": Point(6.21,54.06),
        "NOR-2-1": Point(6.54,53.99),
        "NOR-2-2": Point(6.54,53.99),
        "NOR-2-3": Point(6.54,53.99),
        "NOR-3-1": Point(6.95,54.02),
        "NOR-4-1": Point(7.70,54.44),
        "NOR-4-2": Point(7.70,54.44),
        "NOR-5-1": Point(7.21,55.14),
        "NOR-6-1": Point(5.92,54.30),
        "NOR-6-2": Point(5.92,54.30),
        "NOR-7": Point(6.22,54.32),
        "NOR-8-1": Point(6.35,54.48),
        "OST-1-1": Point(14.09,54.82),
        "OST-1-2": Point(14.09,54.82),
        "OST-1-3": Point(14.09,54.82),
        "OST-2": Point(13.86,54.83),
        "OST-3-1": Point(13.16,54.98),
        "OST-3-2": Point(13.16,54.98),
        }
    return onep


def insert():
    """
    Include the offshore wind parks in egon-data.
    locations and installed capacities based on: NEP2035_V2021_scnC2035

    Parameters
    ----------
    *No parameters required
    """
    # Read file with all required input/output tables' names
    cfg = egon.data.config.datasets()["power_plants"]

    # load NEP2035_V2021_scnC2035 file
    offshore_path = (
        Path(".")
        / "data_bundle_egon_data"
        / "nep2035_version2021"
        / cfg["sources"]["nep_2035"]
    )

    offshore = pd.read_excel(
        offshore_path,
        sheet_name="WInd_Offshore_NEP",
        usecols=[
            "Netzverknuepfungspunkt",
            "Spannungsebene in kV",
            "C 2035",
            "B 2040 ",
        ],
    )
    offshore.dropna(subset=["Netzverknuepfungspunkt"], inplace=True)
    
    id_bus = map_id_bus()
    w_id = map_w_id()

    # Match wind offshore table with the corresponding OSM_id
    offshore["osm_id"] = offshore["Netzverknuepfungspunkt"].map(id_bus)

    # Connect to the data-base
    con = db.engine()

    # Import table with all the busses of the grid
    sql = f"""
        SELECT bus_i as bus_id, geom as point, CAST(osm_substation_id AS text)
        as osm_id FROM {cfg["sources"]["buses_data"]}
        """

    busses = gpd.GeoDataFrame.from_postgis(
        sql, con, crs="EPSG:4326", geom_col="point"
    )

    # Drop NANs in column osm_id
    busses.dropna(subset=["osm_id"], inplace=True)

    # Create columns for bus_id and geometry in the offshore df
    offshore["bus_id"] = np.nan
    offshore["geom"] = Point(0, 0)

    # Match bus_id and geometry
    for index, wind_park in offshore.iterrows():
        if len(busses[busses["osm_id"] == wind_park["osm_id"]].index) > 0:
            bus_ind = busses[busses["osm_id"] == wind_park["osm_id"]].index[0]
            offshore.at[index, "bus_id"] = busses.at[bus_ind, "bus_id"]
            offshore.at[index, "geom"] = busses.at[bus_ind, "point"]
        else:
            print(f'Wind offshore farm not found: {wind_park["osm_id"]}')

    offshore["weather_cell_id"] = offshore["Netzverknuepfungspunkt"].map(w_id)
    offshore["weather_cell_id"] = offshore["weather_cell_id"].apply(int)
    # Drop offshore wind farms without found connexion point
    offshore.dropna(subset=["bus_id"], inplace=True)

    # Assign voltage levels to wind offshore parks
    offshore["voltage_level"] = np.nan
    offshore.loc[
        offshore[offshore["Spannungsebene in kV"] == 110].index,
        "voltage_level",
    ] = 3
    offshore.loc[
        offshore[offshore["Spannungsebene in kV"] > 110].index, "voltage_level"
    ] = 1

    # Delete unnecessary columns
    offshore.drop(
        [
            "Netzverknuepfungspunkt",
            "Spannungsebene in kV",
            "osm_id",
        ],
        axis=1,
        inplace=True,
    )

    # Assign static values
    offshore["carrier"] = "wind_offshore"

    # Create wind farms for the different scenarios
    offshore_2035 = offshore.drop(columns=["B 2040 "]).copy()
    offshore_2035["scenario"] = "eGon2035"
    offshore_2035.rename(columns={"C 2035": "el_capacity"}, inplace=True)

    offshore_100RE = offshore.drop(columns=["C 2035"]).copy()
    offshore_100RE["scenario"] = "eGon100RE"
    offshore_100RE.rename(columns={"B 2040 ": "el_capacity"}, inplace=True)

    # Import capacity targets for wind_offshore per scenario
    sql = f"""
        SELECT * FROM {cfg["sources"]["capacities"]}
        WHERE scenario_name = 'eGon100RE' AND
        carrier = 'wind_offshore'
        """
    capacities = pd.read_sql(sql, con)
    cap_100RE = capacities.capacity.sum()

    # Scale capacities to match  target
    scale_factor = cap_100RE / offshore_100RE.el_capacity.sum()
    offshore_100RE["el_capacity"] = (
        offshore_100RE["el_capacity"] * scale_factor
    )

    # Join power plants from the different scenarios
    offshore = pd.concat([offshore_2035, offshore_100RE], axis=0)

    # convert column "bus_id" and "voltage_level" to integer
    offshore["bus_id"] = offshore["bus_id"].apply(int)
    offshore["voltage_level"] = offshore["voltage_level"].apply(int)

    # Delete, in case of existing, previous wind offshore parks

    db.execute_sql(
        f"""
    DELETE FROM {cfg['target']['schema']}.{cfg['target']['table']}
    WHERE carrier IN ('wind_offshore')
    """
    )

    # Look for the maximum id in the table egon_power_plants
    sql = (
        "SELECT MAX(id) FROM "
        + cfg["target"]["schema"]
        + "."
        + cfg["target"]["table"]
    )
    max_id = pd.read_sql(sql, con)
    max_id = max_id["max"].iat[0]
    if max_id is None:
        ini_id = 1
    else:
        ini_id = int(max_id + 1)

    offshore = gpd.GeoDataFrame(offshore, geometry="geom", crs=4326)

    # write_table in egon-data database:
    # Reset index
    offshore.index = pd.RangeIndex(
        start=ini_id, stop=ini_id + len(offshore), name="id"
    )

    # Insert into database
    offshore.reset_index().to_postgis(
        cfg["target"]["table"],
        schema=cfg["target"]["schema"],
        con=db.engine(),
        if_exists="append",
    )

    return "Off shore wind farms successfully created"
