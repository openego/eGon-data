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
        "Büttel": "NOR-4-1",
        "Heide/West": "NOR-10-2",
        "Suchraum Gemeinden Ibbenbüren/Mettingen/Westerkappeln": "NOR-9-2",
        "Suchraum Zensenbusch": "NOR-7-1",
        "Rommerskirchen": "NOR-7-1",
        "Oberzier": "NOR-7-1",
        "Garrel/Ost": "NOR-7-1",
        "Diele": "NOR-6-1",
        "Dörpen/West": "NOR-2-2",
        "Emden/Borßum": "NOR-0-1",
        "Emden/Ost": "NOR-3-3",
        "Hagermarsch": "NOR-2-1",
        "Hanekenfähr": "NOR-6-3",
        "Inhausen": "NOR-0-2",
        "Unterweser": "NOR-9-1",
        "Wehrendorf": "NOR-7-1",
        "Wilhelmshaven 2": "NOR-11-1",
        "Rastede": "NOR-7-1",
        "Bentwisch": "OST-3-1",
        "Lubmin": "OST-1-1",
        "Suchraum Gemeinde Papendorf": "OST-7-1",
        "Suchraum Gemeinden Brünzow/Kemnitz": "OST-1-4",
        "inhausen": "NOR-0-2",
        "Cloppenburg": "NOR-4-1",
    }


def map_ONEP_areas():
    return {
        "NOR-0-1": Point(6.5, 53.6),
        "NOR-0-2": Point(8.07, 53.76),
        "NOR-1": Point(6.21, 54.06),
        "NOR-1-1": Point(6.21, 54.06),
        "NOR-2-1": Point(6.54, 53.99),
        "NOR-2-2": Point(6.54, 53.99),
        "NOR-2-3": Point(6.54, 53.99),
        "NOR-3-1": Point(6.95, 54.02),
        "NOR-3-3": Point(6.95, 54.02),
        "NOR-4-1": Point(7.70, 54.44),
        "NOR-4-2": Point(7.70, 54.44),
        "NOR-5-1": Point(7.21, 55.14),
        "NOR-6-1": Point(5.92, 54.30),
        "NOR-6-2": Point(5.92, 54.30),
        "NOR-6-3": Point(5.92, 54.30),
        "NOR-7": Point(6.22, 54.32),
        "NOR-7-1": Point(6.22, 54.32),
        "NOR-8-1": Point(6.35, 54.48),
        "NOR-9-1": Point(5.75, 54.5),
        "NOR-9-2": Point(5.75, 54.5),
        "NOR-10-2": Point(6, 54.75),
        "NOR-11-1": Point(6.5, 54.75),
        "OST-1-1": Point(14.09, 54.82),
        "OST-1-2": Point(14.09, 54.82),
        "OST-1-3": Point(14.09, 54.82),
        "OST-1-4": Point(14.09, 54.82),
        "OST-2": Point(13.86, 54.83),
        "OST-3-1": Point(13.16, 54.98),
        "OST-3-2": Point(13.16, 54.98),
        "OST-7-1": Point(12.25, 54.5),
    }


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
        # Delete previous generators
        db.execute_sql(
            f"""
            DELETE FROM {cfg['target']['schema']}.{cfg['target']['table']}
            WHERE carrier = 'wind_offshore'
            AND scenario = '{scenario}'
            """
        )

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
            offshore.rename(columns={"C 2035": "el_capacity"}, inplace=True)

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

        elif "status" in scenario:
            year = int(scenario[-4:])

            offshore_path = (
                Path(".")
                / "data_bundle_egon_data"
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
            offshore = offshore[offshore["Inbetriebnahme"] <= year]

        else:
            raise ValueError(f"{scenario=} is not valid.")

        id_bus = map_id_bus(scenario)

        # Match wind offshore table with the corresponding OSM_id
        offshore["osm_id"] = offshore["Netzverknuepfungspunkt"].map(id_bus)

        buses = db.select_geodataframe(
            f"""
                SELECT bus_i as bus_id, base_kv, geom as point, CAST(osm_substation_id AS text)
                as osm_id FROM {cfg["sources"]["buses_data"]}
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
        if scenario in ["eGon2035", "eGon100RE"]:
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
              {offshore['el_capacity'].sum()}MW were inserted into the db
              """
        )
