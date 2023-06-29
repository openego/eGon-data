from pathlib import Path
import logging

from shapely.geometry import Point
import geopandas as gpd
import pandas as pd

from egon.data import db
import egon.data.config


def map_id_bus(scenario):
    # Import manually generated list of wind offshore farms with their
    # connection points (OSM_id)
    if scenario == "eGon2035":
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
            "Cloppenburg": "50643382",
        }
    elif scenario == "status2019":
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
        "Cloppenburg": "16334",
    }
    return w_id


def map_ONEP_areas():
    onep = {
        "NOR-0-1": Point(6.5, 53.6),
        "NOR-0-2": Point(8.07, 53.76),
        "NOR-1": Point(6.21, 54.06),
        "NOR-2-1": Point(6.54, 53.99),
        "NOR-2-2": Point(6.54, 53.99),
        "NOR-2-3": Point(6.54, 53.99),
        "NOR-3-1": Point(6.95, 54.02),
        "NOR-4-1": Point(7.70, 54.44),
        "NOR-4-2": Point(7.70, 54.44),
        "NOR-5-1": Point(7.21, 55.14),
        "NOR-6-1": Point(5.92, 54.30),
        "NOR-6-2": Point(5.92, 54.30),
        "NOR-7": Point(6.22, 54.32),
        "NOR-8-1": Point(6.35, 54.48),
        "OST-1-1": Point(14.09, 54.82),
        "OST-1-2": Point(14.09, 54.82),
        "OST-1-3": Point(14.09, 54.82),
        "OST-2": Point(13.86, 54.83),
        "OST-3-1": Point(13.16, 54.98),
        "OST-3-2": Point(13.16, 54.98),
    }
    return onep


def insert():
    """
    Include the offshore wind parks in egon-data.

    Parameters
    ----------
    *No parameters required
    """
    # Read file with all required input/output tables' names
    cfg = egon.data.config.datasets()["power_plants"]

    scenarios = egon.data.config.settings()["egon-data"]["--scenarios"]

    for scenario in scenarios:
        # load file
        if scenario == "eGon2035":
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
                ],
            )
            offshore.dropna(subset=["Netzverknuepfungspunkt"], inplace=True)

        elif scenario == "eGon100RE":
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
                    "B 2040 ",
                ],
            )
            offshore.dropna(subset=["Netzverknuepfungspunkt"], inplace=True)

        elif scenario == "status2019":
            offshore_path = (
                Path(".")
                / "data_bundle_powerd_data"
                / "wind_offshore_status2019"
                / cfg["sources"]["wind_offshore_status2019"]
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
            offshore = offshore[offshore["Inbetriebnahme"] <= 2019]

        id_bus = map_id_bus(scenario)
        w_id = map_w_id()

        # Match wind offshore table with the corresponding OSM_id
        offshore["osm_id"] = offshore["Netzverknuepfungspunkt"].map(id_bus)

        busses = db.select_geodataframe(
            f"""
                SELECT bus_i as bus_id, base_kv, geom as point, CAST(osm_substation_id AS text)
                as osm_id FROM {cfg["sources"]["buses_data"]}
                """,
            epsg=4326,
            geom_col="point",
        )

        # Drop NANs in column osm_id
        busses.dropna(subset=["osm_id"], inplace=True)

        # Create columns for bus_id and geometry in the offshore df
        offshore["bus_id"] = 0
        offshore["geom"] = Point(0, 0)

        # Match bus_id
        for index, wind_park in offshore.iterrows():
            if not busses[
                (busses["osm_id"] == wind_park["osm_id"])
                & (busses["base_kv"] == wind_park["Spannungsebene in kV"])
            ].empty:
                bus_ind = busses[
                    busses["osm_id"] == wind_park["osm_id"]
                ].index[0]
                offshore.at[index, "bus_id"] = busses.at[bus_ind, "bus_id"]
            else:
                print(f'Wind offshore farm not found: {wind_park["osm_id"]}')

        offshore.dropna(subset=["bus_id"], inplace=True)

        # Overwrite geom for status2019 parks
        if scenario == "status2019":
            offshore["geom"] = offshore["Name ONEP/NEP"].map(map_ONEP_areas())
            offshore["weather_cell_id"] = -1
            offshore.drop(
                ["Name ONEP/NEP", "Inbetriebnahme"], axis=1, inplace=True
            )
        else:
            offshore["weather_cell_id"] = offshore[
                "Netzverknuepfungspunkt"
            ].map(w_id)
            offshore["weather_cell_id"] = offshore["weather_cell_id"].apply(
                int
            )

        # Scale capacities for eGon100RE
        if scenario == "eGon100RE":
            # Import capacity targets for wind_offshore per scenario
            cap_100RE = db.select_dataframe(
                f"""
                    SELECT SUM(capacity)
                    FROM {cfg["sources"]["capacities"]}
                    WHERE scenario_name = 'eGon100RE' AND
                    carrier = 'wind_offshore'
                    """
            )

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
        next_id = (
            db.select_dataframe(
                "SELECT MAX(id) FROM "
                + cfg["target"]["schema"]
                + "."
                + cfg["target"]["table"]
            ).iloc[0, 0]
            + 1
        )

        # Reset index
        offshore.index = pd.RangeIndex(
            start=next_id, stop=next_id + len(offshore), name="id"
        )

        # Insert into database
        offshore.reset_index().to_postgis(
            cfg["target"]["table"],
            schema=cfg["target"]["schema"],
            con=db.engine(),
            if_exists="append",
        )

        logging.info(
            f"""
              {len(offshore)} wind_offshore generators with a total installed capacity of
              {offshore['Kapazität Gesamtsystem [MW]'].sum()}MW were inserted into the db
              """
        )
