from matplotlib import pyplot as plt
from shapely.geometry import MultiPoint, Point
import geopandas as gpd
import numpy as np
import pandas as pd

from egon.data import db
from egon.data.datasets.mastr import WORKING_DIR_MASTR_NEW
import egon.data.config


def insert():
    """Main function. Import power objectives generate results calling the
    functions "generate_wind_farms" and  "wind_power_states".

    Parameters
    ----------
    *No parameters required

    """

    con = db.engine()

    # federal_std has the shapes of the German states
    sql = "SELECT  gen, gf, nuts, geometry FROM boundaries.vg250_lan"
    federal_std = gpd.GeoDataFrame.from_postgis(
        sql, con, geom_col="geometry", crs=4326
    )

    # target_power_df has the expected capacity of each federal state
    sql = (
        "SELECT  carrier, capacity, nuts, scenario_name FROM "
        "supply.egon_scenario_capacities"
    )
    target_power_df = pd.read_sql(sql, con)

    # mv_districts has geographic info of medium voltage districts in Germany
    sql = "SELECT geom FROM grid.egon_mv_grid_district"
    mv_districts = gpd.GeoDataFrame.from_postgis(sql, con)

    # Delete all the water bodies from the federal states shapes
    federal_std = federal_std[federal_std["gf"] == 4]
    federal_std.drop(columns=["gf"], inplace=True)
    # Filter the potential expected from wind_onshore
    target_power_df = target_power_df[
        target_power_df["carrier"] == "wind_onshore"
    ]
    target_power_df.set_index("nuts", inplace=True)
    target_power_df["geom"] = Point(0, 0)

    # Join the geometries which belong to the same states
    for std in target_power_df.index:
        df = federal_std[federal_std["nuts"] == std]
        if df.size > 0:
            target_power_df.at[std, "name"] = df["gen"].iat[0]
        else:
            target_power_df.at[std, "name"] = np.nan
        target_power_df.at[std, "geom"] = df.unary_union
    target_power_df = gpd.GeoDataFrame(
        target_power_df, geometry="geom", crs=4326
    )
    target_power_df = target_power_df[target_power_df["capacity"] > 0]
    target_power_df = target_power_df.to_crs(3035)

    # Create the shape for full Germany
    target_power_df.at["DE", "geom"] = target_power_df["geom"].unary_union
    target_power_df.at["DE", "name"] = "Germany"
    # Generate WFs for Germany based on potential areas and existing WFs
    wf_areas, wf_areas_ni = generate_wind_farms()

    # Change the columns "geometry" of this GeoDataFrames
    wf_areas.set_geometry("centroid", inplace=True)
    wf_areas_ni.set_geometry("centroid", inplace=True)

    # Create centroids of mv_districts to apply the clip function
    mv_districts["centroid"] = mv_districts.centroid
    mv_districts.set_geometry("centroid", inplace=True)

    summary_t = pd.DataFrame()
    farms = pd.DataFrame()

    # Fit wind farms scenarions for each one of the states
    for scenario in target_power_df.index:
        state_wf = gpd.clip(wf_areas, target_power_df.at[scenario, "geom"])
        state_wf_ni = gpd.clip(
            wf_areas_ni, target_power_df.at[scenario, "geom"]
        )
        state_mv_districts = gpd.clip(
            mv_districts, target_power_df.at[scenario, "geom"]
        )
        target_power = target_power_df.at[scenario, "capacity"]
        scenario_year = target_power_df.at[scenario, "scenario_name"]
        source = target_power_df.at[scenario, "carrier"]
        fed_state = target_power_df.at[scenario, "name"]
        wind_farms_state, summary_state = wind_power_states(
            state_wf,
            state_wf_ni,
            state_mv_districts,
            target_power,
            scenario_year,
            source,
            fed_state,
        )
        summary_t = pd.concat([summary_t, summary_state])
        farms = pd.concat([farms, wind_farms_state])

    generate_map()

    return (summary_t, farms)


def generate_wind_farms():
    """Generate wind farms based on existing wind farms.

    Parameters
    ----------
    *No parameters required

    """
    # get config
    cfg = egon.data.config.datasets()["power_plants"]

    # Due to typos in some inputs, some areas of existing wind farms
    # should be discarded using perimeter and area filters
    def filter_current_wf(wf_geometry):
        if wf_geometry.geom_type == "Point":
            return True
        if wf_geometry.geom_type == "Polygon":
            area = wf_geometry.area
            length = wf_geometry.length
            # Filter based on the biggest (# of WT) wind farm
            return (area < 40000000) & (length < 40000)
        if wf_geometry.geom_type == "LineString":
            length = wf_geometry.length
            return length < 1008  # 8 * rotor diameter (8*126m)

    # The function 'wind_farm' returns the connection point of a wind turbine
    def wind_farm(x):
        try:
            return map_ap_wea_farm[x]
        except:
            return np.nan

    # The function 'voltage' returns the voltage level a wind turbine operates
    def voltage(x):
        try:
            return map_ap_wea_voltage[x]
        except:
            return np.nan

    # Connect to the data base
    con = db.engine()
    sql = "SELECT geom FROM supply.egon_re_potential_area_wind"
    # wf_areas has all the potential areas geometries for wind farms
    wf_areas = gpd.GeoDataFrame.from_postgis(sql, con)
    # bus has the connection points of the wind farms
    bus = pd.read_csv(
        WORKING_DIR_MASTR_NEW / cfg["sources"]["mastr_location"],
        index_col="MaStRNummer",
    )
    # Drop all the rows without connection point
    bus.dropna(subset=["NetzanschlusspunktMastrNummer"], inplace=True)
    # wea has info of each wind turbine in Germany.
    wea = pd.read_csv(WORKING_DIR_MASTR_NEW / cfg["sources"]["mastr_wind"])

    # Delete all the rows without information about geographical location
    wea = wea[(pd.notna(wea["Laengengrad"])) & (pd.notna(wea["Breitengrad"]))]
    # Delete all the offshore wind turbines
    wea = wea.loc[wea["Lage"] == "Windkraft an Land"]
    # the variable map_ap_wea_farm have the connection point of all the
    # available wt in the dataframe bus.
    map_ap_wea_farm = {}
    map_ap_wea_voltage = {}

    wea["connection point"] = wea["LokationMastrNummer"].map(
        bus["NetzanschlusspunktMastrNummer"]
    )
    wea["voltage"] = wea["LokationMastrNummer"].map(
        bus["NetzanschlusspunktMastrNummer"]
    )

    # Create the columns 'geometry' which will have location of each WT in a
    # point type
    wea = gpd.GeoDataFrame(
        wea,
        geometry=gpd.points_from_xy(
            wea["Laengengrad"], wea["Breitengrad"], crs=4326
        ),
    )

    # wf_size storages the number of WT connected to each connection point
    wf_size = wea["connection point"].value_counts()
    # Delete all the connection points with less than 3 WT
    wf_size = wf_size[wf_size >= 3]
    # Filter all the WT which are not part of a wind farm of at least 3 WT
    wea = wea[wea["connection point"].isin(wf_size.index)]
    # current_wfs has all the geometries that represent the existing wind
    # farms
    current_wfs = gpd.GeoDataFrame(
        index=wf_size.index, crs=4326, columns=["geometry", "voltage"]
    )
    for conn_point, wt_location in wea.groupby("connection point"):
        current_wfs.at[conn_point, "geometry"] = MultiPoint(
            wt_location["geometry"].values
        ).convex_hull
        current_wfs.at[conn_point, "voltage"] = wt_location["voltage"].iat[0]

    current_wfs["geometry2"] = current_wfs["geometry"].to_crs(3035)
    current_wfs["area"] = current_wfs["geometry2"].apply(lambda x: x.area)
    current_wfs["length"] = current_wfs["geometry2"].apply(lambda x: x.length)
    # The 'filter_wts' is used to discard atypical values for the current wind
    # farms
    current_wfs["filter2"] = current_wfs["geometry2"].apply(
        lambda x: filter_current_wf(x)
    )

    # Apply the filter based on area and perimeter
    current_wfs = current_wfs[current_wfs["filter2"]]
    current_wfs = current_wfs.drop(columns=["geometry2", "filter2"])

    wf_areas["area [km²]"] = wf_areas.area / 1000000

    # Exclude areas smaller than X m². X was calculated as the area of
    # 3 WT in the corners of an equilateral triangle with l = 4*rotor_diameter
    min_area = 4 * (0.126**2) * np.sqrt(3)
    wf_areas = wf_areas[wf_areas["area [km²]"] > min_area]

    # Find the centroid of all the suitable potential areas
    wf_areas["centroid"] = wf_areas.centroid

    # find the potential areas that intersects the convex hulls of current
    # wind farms and assign voltage levels
    wf_areas = wf_areas.to_crs(4326)
    for i in wf_areas.index:
        intersection = current_wfs.intersects(wf_areas.at[i, "geom"])
        if intersection.any() == False:
            wf_areas.at[i, "voltage"] = "No Intersection"
        else:
            wf_areas.at[i, "voltage"] = current_wfs[intersection].voltage[0]

    # wf_areas_ni has the potential areas which don't intersect any current
    # wind farm
    wf_areas_ni = wf_areas[wf_areas["voltage"] == "No Intersection"]
    wf_areas = wf_areas[wf_areas["voltage"] != "No Intersection"]
    return wf_areas, wf_areas_ni


def wind_power_states(
    state_wf,
    state_wf_ni,
    state_mv_districts,
    target_power,
    scenario_year,
    source,
    fed_state,
):
    """Import OSM data from a Geofabrik `.pbf` file into a PostgreSQL
    database.

    Parameters
    ----------
    state_wf: geodataframe, mandatory
        gdf containing all the wf in the state created based on existing wf.
    state_wf_ni: geodataframe, mandatory
        potential areas in the the state wich don't intersect any existing wf
    state_mv_districts: geodataframe, mandatory
        gdf containing all the MV/HV substations in the state
    target_power: int, mandatory
        Objective power for a state given in MW
    scenario_year: str, mandatory
        name of the scenario
    source: str, mandatory
        Type of energy genetor. Always "Wind_onshore" for this script.
    fed_state: str, mandatory
        Name of the state where the wind farms will be allocated

    """

    def match_district_se(x):
        for sub in hvmv_substation.index:
            if x["geom"].contains(hvmv_substation.at[sub, "point"]):
                return hvmv_substation.at[sub, "point"]

    con = db.engine()
    sql = "SELECT point, voltage FROM grid.egon_hvmv_substation"
    # hvmv_substation has the information about HV transmission lines in
    # Germany
    hvmv_substation = gpd.GeoDataFrame.from_postgis(sql, con, geom_col="point")

    # Set wind potential depending on geographical location
    power_north = 21.05  # MW/km²
    power_south = 16.81  # MW/km²
    # Set a maximum installed capacity to limit the power of big potential
    # areas
    max_power_hv = 120  # in MW
    max_power_mv = 20  # in MW
    # Max distance between WF (connected to MV) and nearest HV substation
    # that allows its connection to HV.
    max_dist_hv = 20000  # in meters

    summary = pd.DataFrame(
        columns=["state", "target", "from existin WF", "MV districts"]
    )

    north = [
        "Schleswig-Holstein",
        "Mecklenburg-Vorpommern",
        "Niedersachsen",
        "Bremen",
        "Hamburg",
    ]

    if fed_state in north:
        state_wf["inst capacity [MW]"] = power_north * state_wf["area [km²]"]
    else:
        state_wf["inst capacity [MW]"] = power_south * state_wf["area [km²]"]

    # Divide selected areas based on voltage of connection points
    wf_mv = state_wf[
        (state_wf["voltage"] != "Hochspannung")
        & (state_wf["voltage"] != "Hoechstspannung")
        & (state_wf["voltage"] != "UmspannungZurHochspannung")
    ]

    wf_hv = state_wf[
        (state_wf["voltage"] == "Hochspannung")
        | (state_wf["voltage"] == "Hoechstspannung")
        | (state_wf["voltage"] == "UmspannungZurHochspannung")
    ]

    # Wind farms connected to MV network will be connected to HV network if
    # the distance to the closest HV substation is =< max_dist_hv, and the
    # installed capacity is bigger than max_power_mv
    hvmv_substation = hvmv_substation.to_crs(3035)
    hvmv_substation["voltage"] = hvmv_substation["voltage"].apply(
        lambda x: int(x.split(";")[0])
    )
    hv_substations = hvmv_substation[hvmv_substation["voltage"] >= 110000]
    hv_substations = hv_substations.unary_union  # join all the hv_substations
    wf_mv["dist_to_HV"] = (
        state_wf["geom"].to_crs(3035).distance(hv_substations)
    )
    wf_mv_to_hv = wf_mv[
        (wf_mv["dist_to_HV"] <= max_dist_hv)
        & (wf_mv["inst capacity [MW]"] >= max_power_mv)
    ]
    wf_mv_to_hv = wf_mv_to_hv.drop(columns=["dist_to_HV"])
    wf_mv_to_hv["voltage"] = "Hochspannung"

    wf_hv = pd.concat([wf_hv, wf_mv_to_hv])
    wf_mv = wf_mv[
        (wf_mv["dist_to_HV"] > max_dist_hv)
        | (wf_mv["inst capacity [MW]"] < max_power_mv)
    ]
    wf_mv = wf_mv.drop(columns=["dist_to_HV"])

    wf_hv["inst capacity [MW]"] = wf_hv["inst capacity [MW]"].apply(
        lambda x: x if x < max_power_hv else max_power_hv
    )

    wf_mv["inst capacity [MW]"] = wf_mv["inst capacity [MW]"].apply(
        lambda x: x if x < max_power_mv else max_power_mv
    )

    wind_farms = pd.concat([wf_hv, wf_mv])

    # Adjust the total installed capacity to the scenario
    total_wind_power = (
        wf_hv["inst capacity [MW]"].sum() + wf_mv["inst capacity [MW]"].sum()
    )
    if total_wind_power > target_power:
        scale_factor = target_power / total_wind_power
        wf_mv["inst capacity [MW]"] = (
            wf_mv["inst capacity [MW]"] * scale_factor
        )
        wf_hv["inst capacity [MW]"] = (
            wf_hv["inst capacity [MW]"] * scale_factor
        )
        wind_farms = pd.concat([wf_hv, wf_mv])
        summary = pd.concat(
            [
                summary,
                pd.DataFrame(
                    index=[summary.index.max() + 1],
                    data={
                        "state": fed_state,
                        "target": target_power,
                        "from existin WF": wind_farms[
                            "inst capacity [MW]"
                        ].sum(),
                        "MV districts": 0,
                    },
                ),
            ],
            ignore_index=True,
        )
    else:
        extra_wf = state_mv_districts.copy()
        extra_wf = extra_wf.drop(columns=["centroid"])
        # the column centroid has the coordinates of the substation
        # corresponding to each mv_grid_district
        extra_wf["centroid"] = extra_wf.apply(match_district_se, axis=1)
        extra_wf = extra_wf.set_geometry("centroid")
        extra_wf["area [km²]"] = 0.0
        for district in extra_wf.index:
            try:
                pot_area_district = gpd.clip(
                    state_wf_ni, extra_wf.at[district, "geom"]
                )
                extra_wf.at[district, "area [km²]"] = pot_area_district[
                    "area [km²]"
                ].sum()
            except:
                print(district)
        extra_wf = extra_wf[extra_wf["area [km²]"] != 0]
        total_new_area = extra_wf["area [km²]"].sum()
        scale_factor = (target_power - total_wind_power) / total_new_area
        extra_wf["inst capacity [MW]"] = extra_wf["area [km²]"] * scale_factor
        extra_wf["voltage"] = "Hochspannung"
        summary = pd.concat(
            [
                summary,
                pd.DataFrame(
                    index=[summary.index.max() + 1],
                    data={
                        "state": fed_state,
                        "target": target_power,
                        "from existin WF": wind_farms[
                            "inst capacity [MW]"
                        ].sum(),
                        "MV districts": extra_wf["inst capacity [MW]"].sum(),
                    },
                ),
            ],
            ignore_index=True,
        )
        wind_farms = pd.concat([wind_farms, extra_wf], ignore_index=True)

    # Use Definition of thresholds for voltage level assignment
    wind_farms["voltage_level"] = 0
    for i in wind_farms.index:
        try:
            if wind_farms.at[i, "inst capacity [MW]"] < 5.5:
                wind_farms.at[i, "voltage_level"] = 5
                continue
            if wind_farms.at[i, "inst capacity [MW]"] < 20:
                wind_farms.at[i, "voltage_level"] = 4
                continue
            if wind_farms.at[i, "inst capacity [MW]"] >= 20:
                wind_farms.at[i, "voltage_level"] = 3
                continue
        except:
            print(i)

    # Look for the maximum id in the table egon_power_plants
    sql = "SELECT MAX(id) FROM supply.egon_power_plants"
    max_id = pd.read_sql(sql, con)
    max_id = max_id["max"].iat[0]
    if max_id is None:
        wind_farm_id = 1
    else:
        wind_farm_id = int(max_id + 1)

    # write_table in egon-data database:

    # Copy relevant columns from wind_farms
    insert_wind_farms = wind_farms[
        ["inst capacity [MW]", "voltage_level", "centroid"]
    ]

    # Set static column values
    insert_wind_farms["carrier"] = source
    insert_wind_farms["scenario"] = scenario_year

    # Change name and crs of geometry column
    insert_wind_farms = (
        insert_wind_farms.rename(
            {"centroid": "geom", "inst capacity [MW]": "el_capacity"}, axis=1
        )
        .set_geometry("geom")
        .to_crs(4326)
    )

    # Reset index
    insert_wind_farms.index = pd.RangeIndex(
        start=wind_farm_id,
        stop=wind_farm_id + len(insert_wind_farms),
        name="id",
    )

    # Insert into database
    insert_wind_farms.reset_index().to_postgis(
        "egon_power_plants",
        schema="supply",
        con=db.engine(),
        if_exists="append",
    )
    return wind_farms, summary


def generate_map():
    """Generates a map with the position of all the wind farms

    Parameters
    ----------
    *No parameters required

    """
    con = db.engine()

    # Import wind farms from egon-data
    sql = (
        "SELECT  carrier, el_capacity, geom, scenario FROM "
        "supply.egon_power_plants WHERE carrier = 'wind_onshore'"
    )
    wind_farms_t = gpd.GeoDataFrame.from_postgis(
        sql, con, geom_col="geom", crs=4326
    )
    wind_farms_t = wind_farms_t.to_crs(3035)

    for scenario in wind_farms_t.scenario.unique():
        wind_farms = wind_farms_t[wind_farms_t["scenario"] == scenario]
        # mv_districts has geographic info of medium voltage districts in
        # Germany
        sql = "SELECT geom FROM grid.egon_mv_grid_district"
        mv_districts = gpd.GeoDataFrame.from_postgis(sql, con)
        mv_districts = mv_districts.to_crs(3035)

        mv_districts["power"] = 0.0
        for std in mv_districts.index:
            try:
                mv_districts.at[std, "power"] = gpd.clip(
                    wind_farms, mv_districts.at[std, "geom"]
                ).el_capacity.sum()
            except:
                print(std)

        fig, ax = plt.subplots(1, 1)
        mv_districts.geom.plot(linewidth=0.2, ax=ax, color="black")
        mv_districts.plot(
            ax=ax,
            column="power",
            cmap="magma_r",
            legend=True,
            legend_kwds={
                "label": "Installed capacity in MW",
                "orientation": "vertical",
            },
        )
        plt.savefig(f"wind_farms_{scenario}.png", dpi=300)
    return 0
