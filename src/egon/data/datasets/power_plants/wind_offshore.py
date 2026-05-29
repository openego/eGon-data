from pathlib import Path
import logging

from shapely.geometry import Point
import geopandas as gpd
import pandas as pd

from egon.data import db
from egon.data.datasets import load_sources_and_targets
import egon.data.config


def map_id_bus(scenario, sources):
    # Import manually generated list of wind offshore farms with their
    # connection points (OSM_id)

    osm_year = sources.files["osm_config"]

    if scenario in ["eGon2035", "eGon100RE", "eGon2037", "eGon2045"]:
        id_bus = {
            "Büttel": "136034396",
            "Diele": "177829920",
            "Dörpen West": "142487746",
            "Hagermarsch": "79316833",
            "Hanekenfähr (Amprion)": "61918154",
            "Inhausen": "29420322",
            "Unterweser (TenneT)": "32076853",
            "Wehrendorf (Amprion)": "33411203",
            "Bentwisch (50Hertz)": "32063539",
            "Lubmin (50Hertz)": "460134233",
            "Gemeinde Papendorf": "32063539",
            "Rommerskirchen (Amprion)": "24839976",
            "Oberzier": "26593929",
            "Suchraum Gemeinden Brünzow/Kemnitz": "460134233",
            "Suchraum Gemeinden Ibbenbüren/Mettingen/Westerkappeln": "114319248",
        }
        if "200101" in osm_year:
            id_bus2 = {
                "Heide West": "289836713",
                "Emden Ost": "177829920",
                "Garrel / Ost (TenneT)": "23837631",
                "Emden-Borssum": "34835258",
                "Suchraum Zensenbusch": "76185022",
                "Cloppenburg": "50643382",
                "Wilhelmshaven 2": "23837631",
                "Rastede": "23837631",
            }
        elif ("250101" in osm_year) | ("240101" in osm_year):
            id_bus2 = {
                "Heide West": "603661085",
                "Emden Ost": "1280178909",
                "Garrel / Ost (TenneT)": "24493551",
                "Emden-Borssum": "34835258",
                "Westerkappeln": "954305865",
                "Niederrhein (Amprion)": "24462426",
                "Kusenhorst": "29077623",
                "Sechtem (Amprion)": "22766160",
                "Hardebek": "1107961833",
                "Suchraum BBS (50Hertz)": "1089911133",
                "Suchraum Nüttermoor (TenneT)": "1280178911",
                "Suchraum Gnewitz (50Hertz)": "940919943",
                "Suchraum Kemnitz (50Hertz)": "460134233",
                "Suchraum Ried (Amprion)": "1223405794",
                "Kriftel (Amprion)": "38661452",
                "Suchraum Pöschendorf": "258275257",
                "Cloppenburg": "24493551",
                "Wilhelmshaven 2": "637595524",
                "Suchraum Zensenbusch": "24479003",
                "Rastede": "1128250707",

                # NEP-only NVPs (no native wind park in FEP). Each is given a
                # representative offshore wind park in assign_ONEP_areas() so its
                # NEP capacity is included in the model.
                "Wilhelmshaven / Landkreis Friesland": "1134105414",
                "Suchraum Brunsbüttel (Gemeinden Brunsbüttel / Büttel / St. Margarethen / Brokdorf)": "30622610",
                "Suchraum Rastede (Ovelgönne / Rastede / Wiefelstede / Westerstede)": "1128250704",
                "Blockland / Neu": "44717036",
                "Samtgemeinde Sottrum": "955268864",
                "Lippe": "957746797",
                "Suchraum Zensenbusch": "24479003",
            }
        else:
            raise Exception("""The OSM year used is not yet compatible with
                            this function""")
        id_bus = {**id_bus, **id_bus2}

    elif "status" in scenario:
        year = int(scenario[-4:])

        id_bus = {
            "UW Inhausen": "29420322",
            "UW Bentwisch": "32063539",
            "UW Emden / Borssum": "34835258",
            "UW Emden Ost": "34835258",
            "UW Cloppenburg": "50643382",
            "UW Hagermarsch": "79316833",
            "UW Büttel": "136034396",
            "UW Dörpen West": "142487746",
            "UW Diele": "177829920",
            "UW Lubmin": "460134233",
        }

        if year >= 2023:
            # No update needed as no new stations used for offshore wind
            # between 2019 and 2023
            pass

        # TODO: If necessary add new stations when generating status quo > 2023

    else:
        id_bus = {}

    return id_bus


def assign_ONEP_areas():
    return {
        # Connection points present in BOTH NEP and FEP sheets
        "Büttel": "NOR-4-1 (HelWin1)",
        "Diele": "NOR-6-1 (BorWin1)",
        "Dörpen West": "NOR-2-2 (DolWin1)",
        "Hagermarsch": "NOR-2-1 (Alpha Ventus)",
        "Hanekenfähr (Amprion)": "NOR-6-3 (BorWin4)",
        "Inhausen": "NOR-0-2 (Nordergründe)",
        "Unterweser (TenneT)": "NOR-12-1",
        "Wehrendorf (Amprion)": "NOR-9-1 (BalWin1)",
        "Bentwisch (50Hertz)": "OST-3-1 (Baltic1)",
        "Lubmin (50Hertz)": "OST-1-1 (Ostwind 1)",
        "Gemeinde Papendorf": "OST-7-1 (nördlich Warnemünde)",
        "Rommerskirchen (Amprion)": "NOR-14-2",
        "Heide West": "NOR-10-2",
        "Emden Ost": "NOR-3-3 (DolWin6)",
        "Garrel / Ost (TenneT)": "NOR-7-1 (BorWin5)",
        "Emden-Borssum": "NOR-0-1 (Riffgat)",
        "Westerkappeln": "NOR-10-1 (BalWin2)",
        "Niederrhein (Amprion)": "NOR-5-2",
        "Kusenhorst": "NOR-6-4",
        "Sechtem (Amprion)": "NOR-5-3",
        "Hardebek": "NOR-16-1",
        "Suchraum BBS (50Hertz)": "NOR-16-2",
        "Suchraum Nüttermoor (TenneT)": "NOR-17-2",
        "Suchraum Ried (Amprion)": "NOR-17-1",
        "Kriftel (Amprion)": "NOR-16-4",
        "Suchraum Pöschendorf": "NOR-12-3",
        "Suchraum Gnewitz (50Hertz)": "OST-1-4",
        "Suchraum Kemnitz (50Hertz)": "OST-2-4 (Ostwind4)",

        # NEP-only NVPs assigned a representative offshore wind park
        # (so their NEP capacity is included; geom borrowed from a nearby FEP park)
        "Wilhelmshaven / Landkreis Friesland": "NOR-9-2",       # Sengwarden area
        "Suchraum Brunsbüttel (Gemeinden Brunsbüttel / Büttel / St. Margarethen / Brokdorf)": "NOR-11-1",  # Hochwöhrden area
        "Suchraum Rastede (Ovelgönne / Rastede / Wiefelstede / Westerstede)": "NOR-13-1",  # Großenmeer area
        "Blockland / Neu": "NOR-14-1",                          # Großenmeer (TenneT) area
        "Samtgemeinde Sottrum": "NOR-9-4 (BalWin5)",            # Werderland area
        "Lippe": "NOR-12-2 (LanWin2)",                          # Hochwörden area
        "Suchraum Zensenbusch": "NOR-19-1",                     # Esens area
    }


def map_ONEP_areas():
    return {
     
        "NOR-0-1 (Riffgat)": Point(6.5, 53.6),
        "NOR-0-2 (Nordergründe)": Point(8.07, 53.76),
        "NOR-1-1 (DolWin5)": Point(6.21, 54.06),
        "NOR-2-1 (Alpha Ventus)": Point(6.54, 53.99),
        "NOR-2-2 (DolWin1)": Point(6.54, 53.99),
        "NOR-2-3 (DolWin3)": Point(6.54, 53.99),
        "NOR-3-1 (DolWin2)": Point(6.95, 54.02),
        "NOR-3-2 (DolWin4)": Point(6.95, 54.02),
        "NOR-3-3 (DolWin6)": Point(6.95, 54.02),
        "NOR-4-1 (HelWin1)": Point(7.70, 54.44),
        "NOR-4-2 (HelWin2)": Point(7.70, 54.44),
        "NOR-5-1 (SylWin1)": Point(7.21, 55.14),
        "NOR-6-1 (BorWin1)": Point(5.92, 54.30),
        "NOR-6-2 (BorWin2)": Point(5.92, 54.30),
        "NOR-6-3 (BorWin4)": Point(5.92, 54.30),
        "NOR-7-1 (BorWin5)": Point(6.22, 54.32),
        "NOR-7-2 (BorWin6)": Point(6.22, 54.32),
        "NOR-8-1 (BorWin3)": Point(6.35, 54.48),
        "NOR-9-1 (BalWin1)": Point(5.79475, 54.43928),
        "NOR-10-2": Point(6, 54.75),
        "OST-1-1 (Ostwind 1)": Point(14.09, 54.82),
        "OST-1-2 (Ostwind 1)": Point(14.09, 54.82),
        "OST-1-3 (Ostwind 1)": Point(14.09, 54.82),
        "OST-1-4": Point(14.09, 54.82),
        "OST-2-1 (Ostwind 2)": Point(14.09, 54.82),
        "OST-2-2 (Ostwind 2)": Point(14.09, 54.82),
        "OST-2-3 (Ostwind 2)": Point(14.09, 54.82),
        "OST-3-1 (Baltic1)": Point(13.16, 54.98),
        "OST-3-2 (Baltic2)": Point(13.16, 54.98),
        "OST-7-1 (nördlich Warnemünde)": Point(12.25, 54.5),

        
        "NOR-5-2": Point(7.01693, 55.0888),
        "NOR-5-3": Point(6.86276, 55.23015),
        "NOR-6-4": Point(6.02912, 54.42698),
        "NOR-9-2": Point(5.6928, 54.49997),
        "NOR-9-3 (BalWin4)": Point(5.59334, 54.53894),
        "NOR-9-4 (BalWin5)": Point(5.90591, 54.53444),
        "NOR-10-1 (BalWin2)": Point(6.0569, 54.61459),
        "NOR-11-1": Point(6.5, 54.75),
        "NOR-11-2": Point(6.28843, 54.90406),
        "NOR-12-1": Point(6.17076, 54.74855),
        "NOR-12-2 (LanWin2)": Point(6.08071, 54.84685),
        "NOR-12-3": Point(5.9539, 54.87547),
        "NOR-12-4": Point(6.14039, 55.06388),
        "NOR-13-2": Point(6.38346, 55.18865),
        "NOR-14-1": Point(5.11342, 54.72641),
        "NOR-14-2": Point(5.14326, 54.89121),
        "NOR-16-1": Point(5.77932, 55.39819),
        "NOR-16-2": Point(5.59797, 55.24033),
        "NOR-16-3": Point(5.32235, 55.1404),
        "NOR-16-4": Point(5.04044, 55.19885),
        "NOR-17-1": Point(5.0382, 55.08595),
        "NOR-17-2": Point(4.52533, 55.32292),
        "NOR-19-1": Point(3.73521, 55.72058),
        "NOR-19-2": Point(3.5456, 55.79089),
        "OST-2-4 (Ostwind4)": Point(14.01305, 54.8837),
        "NOR-13-1": Point(6.25970, 55.04013),
    }


def insert():
    """
    Include the offshore wind parks in egon-data.

    Parameters
    ----------
    *No parameters required
    """
    sources, targets = load_sources_and_targets("PowerPlants")

    scenarios = egon.data.config.settings()["egon-data"]["--scenarios"]

    for scenario in scenarios:

        db.execute_sql(f"""
            DELETE FROM {targets.tables['power_plants']}
            WHERE carrier = 'wind_offshore'
            AND scenario = '{scenario}'
            """)

        # load file
        if scenario in ["eGon2035", "eGon2037", "eGon2045"]:
            filename = "NEP2035_2037_2045_V2025_2023_scnC2037.xlsx"

            # Map scenario to its capacity column
            capacity_col = {
                "eGon2035": "C 2035",
                "eGon2037": "C 2037",
                "eGon2045": "C 2045",
            }[scenario]

            offshore_path = (
                Path(".")
                / "data_bundle_egon_data"
                / "nep2035_version2021"
                / filename
            )
            offshore = pd.read_excel(
                offshore_path,
                sheet_name="WInd_Offshore_NEP",
                usecols=[
                    "Netzverknuepfungspunkt",
                    "Spannungsebene in kV",
                    capacity_col,
                ],
            )
            offshore.dropna(subset=["Netzverknuepfungspunkt"], inplace=True)
            offshore.rename(columns={capacity_col: "el_capacity"}, inplace=True)
            offshore = offshore[offshore["el_capacity"] > 0]

        elif scenario == "eGon100RE":
            offshore_path = (
                Path(".")
                / "data_bundle_egon_data"
                / "nep2035_version2021"
                / sources.files["nep_2035"]
            )

            offshore = pd.read_excel(
                offshore_path,
                sheet_name="WInd_Offshore_NEP",
                usecols=[
                    "Netzverknuepfungspunkt",
                    "Spannungsebene in kV",
                    "B 2040 ",
                ],
            )
            offshore.dropna(subset=["Netzverknuepfungspunkt"], inplace=True)
            offshore.rename(columns={"B 2040 ": "el_capacity"}, inplace=True)
            offshore = offshore[offshore["el_capacity"] > 0]

        elif "status" in scenario:
            year = int(scenario[-4:])

            offshore_path = (
                Path(".")
                / "data_bundle_egon_data"
                / "wind_offshore_status2019"
                / sources.files["wind_offshore_status2019"]
            )
            offshore = pd.read_excel(
                offshore_path,
                sheet_name="wind_offshore",
                usecols=[
                    "Name ONEP/NEP",
                    "NVP",
                    "Spannung [kV]",
                    "Inbetriebnahme",
                    "Kapazität Gesamtsystem [MW]",
                ],
            )
            offshore.dropna(subset=["Name ONEP/NEP"], inplace=True)
            offshore.rename(
                columns={
                    "NVP": "Netzverknuepfungspunkt",
                    "Spannung [kV]": "Spannungsebene in kV",
                    "Kapazität Gesamtsystem [MW]": "el_capacity",
                },
                inplace=True,
            )
            offshore = offshore[offshore["Inbetriebnahme"] <= year]

        else:
            raise ValueError(f"{scenario=} is not valid.")

        id_bus = map_id_bus(scenario, sources)

        # Match wind offshore table with the corresponding OSM_id
        offshore["osm_id"] = offshore["Netzverknuepfungspunkt"].map(id_bus)

        buses = db.select_geodataframe(
            f"""
                SELECT bus_i as bus_id, base_kv, geom as point, CAST(osm_substation_id AS text)
                as osm_id FROM {sources.tables['buses_data']}
                """,
            epsg=4326,
            geom_col="point",
        )

        # Drop NANs in column osm_id
        buses.dropna(subset=["osm_id"], inplace=True)

        # Create columns for bus_id and geometry in the offshore df
        offshore["bus_id"] = pd.NA
        offshore["geom"] = Point(0, 0)

        # Match bus_id
        for index, wind_park in offshore.iterrows():
            if not buses[
                (buses["osm_id"] == wind_park["osm_id"])
                & (buses["base_kv"] == wind_park["Spannungsebene in kV"])
            ].empty:
                bus_ind = buses[buses["osm_id"] == wind_park["osm_id"]].index[
                    0
                ]
                offshore.at[index, "bus_id"] = buses.at[bus_ind, "bus_id"]
            else:
                print(f'Wind offshore farm not found: {wind_park["osm_id"]}')

        offshore.dropna(subset=["bus_id"], inplace=True)

        # Overwrite geom for status2019 parks
        if scenario in ["eGon2035", "eGon100RE", "eGon2037", "eGon2045"]:
            offshore["Name ONEP/NEP"] = offshore["Netzverknuepfungspunkt"].map(
                assign_ONEP_areas()
            )

        offshore["geom"] = offshore["Name ONEP/NEP"].map(map_ONEP_areas())
        offshore["weather_cell_id"] = pd.NA

        offshore.drop(["Name ONEP/NEP"], axis=1, inplace=True)

        if "status" in scenario:
            offshore.drop(["Inbetriebnahme"], axis=1, inplace=True)

        # Scale capacities for eGon100RE
        if scenario == "eGon100RE":
            # Import capacity targets for wind_offshore per scenario
            cap_100RE = db.select_dataframe(f"""
                    SELECT SUM(capacity)
                    FROM {sources.tables['capacities']}
                    WHERE scenario_name = 'eGon100RE' AND
                    carrier = 'wind_offshore'
                    """).iloc[0, 0]

            # Scale capacities to match  target
            scale_factor = cap_100RE / offshore.el_capacity.sum()
            offshore["el_capacity"] *= scale_factor

        # Assign voltage levels to wind offshore parks
        offshore["voltage_level"] = 0
        offshore.loc[
            offshore[offshore["Spannungsebene in kV"] == 110].index,
            "voltage_level",
        ] = 3
        offshore.loc[
            offshore[offshore["Spannungsebene in kV"] > 110].index,
            "voltage_level",
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

        # Set static columns
        offshore["carrier"] = "wind_offshore"
        offshore["scenario"] = scenario

        offshore = gpd.GeoDataFrame(offshore, geometry="geom", crs=4326)

        # Look for the maximum id in the table egon_power_plants
        next_id = db.select_dataframe(
            f"SELECT MAX(id) FROM {targets.tables['power_plants']}"
        ).iloc[0, 0]

        if next_id:
            next_id += 1
        else:
            next_id = 1

        # Reset index
        offshore.index = pd.RangeIndex(
            start=next_id, stop=next_id + len(offshore), name="id"
        )

        # Insert into database
        offshore.reset_index().to_postgis(
            targets.get_table_name("power_plants"),
            schema=targets.get_table_schema("power_plants"),
            con=db.engine(),
            if_exists="append",
        )

        logging.info(f"""
              {len(offshore)} wind_offshore generators with a total installed capacity of
              {offshore['el_capacity'].sum()}MW were inserted into the db
              """)