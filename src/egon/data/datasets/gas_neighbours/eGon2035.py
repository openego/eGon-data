"""Central module containing code dealing with gas neighbours for eGon2035
"""

from pathlib import Path
from urllib.request import urlretrieve
import ast
import zipfile

from geoalchemy2.types import Geometry
from shapely.geometry import LineString, MultiLineString
import geopandas as gpd
import numpy as np
import pandas as pd
import pypsa

from egon.data import config, db
from egon.data.datasets import Dataset
from egon.data.datasets.electrical_neighbours import (
    get_foreign_bus_id,
    get_map_buses,
)
from egon.data.datasets.gas_neighbours.gas_abroad import (
    get_foreign_gas_bus_id,
    insert_ch4_stores,
    insert_gas_grid_capacities,
    insert_generators,
)
from egon.data.datasets.scenario_parameters import get_sector_parameters

countries = [
    "AT",
    "BE",
    "CH",
    "CZ",
    "DK",
    "FR",
    "GB",
    "LU",
    "NL",
    "NO",
    "PL",
    "RU",
    "SE",
    "UK",
]


def read_LNG_capacities():
    lng_file = "datasets/gas_data/data/IGGIELGN_LNGs.csv"
    IGGIELGN_LNGs = gpd.read_file(lng_file)

    map_countries_scigrid = {
        "BE": "BE00",
        "EE": "EE00",
        "EL": "GR00",
        "ES": "ES00",
        "FI": "FI00",
        "FR": "FR00",
        "GB": "UK00",
        "IT": "ITCN",
        "LT": "LT00",
        "LV": "LV00",
        "MT": "MT00",
        "NL": "NL00",
        "PL": "PL00",
        "PT": "PT00",
        "SE": "SE01",
    }

    conversion_factor = 437.5  # MCM/day to MWh/h
    c2 = 24 / 1000  # MWh/h to GWh/d
    p_nom = []

    for index, row in IGGIELGN_LNGs.iterrows():
        param = ast.literal_eval(row["param"])
        p_nom.append(
            param["max_cap_store2pipe_M_m3_per_d"] * conversion_factor * c2
        )

    IGGIELGN_LNGs["LNG max_cap_store2pipe_M_m3_per_d (in GWh/d)"] = p_nom

    IGGIELGN_LNGs.drop(
        [
            "uncertainty",
            "method",
            "param",
            "comment",
            "tags",
            "source_id",
            "lat",
            "long",
            "geometry",
            "id",
            "name",
            "node_id",
        ],
        axis=1,
        inplace=True,
    )

    IGGIELGN_LNGs["Country"] = IGGIELGN_LNGs["country_code"].map(
        map_countries_scigrid
    )
    IGGIELGN_LNGs = (
        IGGIELGN_LNGs.groupby(["Country"])[
            "LNG max_cap_store2pipe_M_m3_per_d (in GWh/d)"
        ]
        .sum()
        .sort_index()
    )

    return IGGIELGN_LNGs


def calc_capacities():
    """Calculates gas production capacities from TYNDP data

    Returns
    -------
    grouped_capacities : pandas.DataFrame
        Gas production capacities per foreign node

    """

    sources = config.datasets()["gas_neighbours"]["sources"]

    # insert installed capacities
    file = zipfile.ZipFile(f"tyndp/{sources['tyndp_capacities']}")
    df = pd.read_excel(
        file.open("TYNDP-2020-Scenario-Datafile.xlsx").read(),
        sheet_name="Gas Data",
    )

    df = (
        df.query(
            'Scenario == "Distributed Energy" & '
            '(Case == "Peak" | Case == "Average") &'  # Case: 2 Week/Average/DF/Peak
            'Category == "Production"'
        )
        .drop(
            columns=[
                "Generator_ID",
                "Climate Year",
                "Simulation_ID",
                "Node 1",
                "Path",
                "Direct/Indirect",
                "Sector",
                "Note",
                "Category",
                "Scenario",
            ]
        )
        .set_index("Node/Line")
        .sort_index()
    )

    lng = read_LNG_capacities()
    df_2030 = calc_capacity_per_year(df, lng, 2030)
    df_2040 = calc_capacity_per_year(df, lng, 2040)

    # Conversion GWh/d to MWh/h
    conversion_factor = 1000 / 24

    df_2035 = pd.concat([df_2040, df_2030], axis=1)
    df_2035 = df_2035.drop(
        columns=[
            "Value_conv_2040",
            "Value_conv_2030",
            "Value_bio_2040",
            "Value_bio_2030",
        ]
    )
    df_2035["cap_2035"] = (df_2035["CH4_2030"] + df_2035["CH4_2040"]) / 2
    df_2035["e_nom_max"] = (
        ((df_2035["e_nom_max_2030"] + df_2035["e_nom_max_2040"]) / 2)
        * conversion_factor
        * 8760
    )
    df_2035["ratioConv_2035"] = (
        df_2035["ratioConv_2030"] + df_2035["ratioConv_2040"]
    ) / 2
    grouped_capacities = df_2035.drop(
        columns=[
            "ratioConv_2030",
            "ratioConv_2040",
            "CH4_2040",
            "CH4_2030",
            "e_nom_max_2030",
            "e_nom_max_2040",
        ]
    ).reset_index()

    grouped_capacities["cap_2035"] = (
        grouped_capacities["cap_2035"] * conversion_factor
    )

    # Add generator in Russia of infinite capacity
    grouped_capacities = grouped_capacities.append(
        {
            "cap_2035": 1e9,
            "e_nom_max": np.inf,
            "ratioConv_2035": 1,
            "index": "RU",
        },
        ignore_index=True,
    )

    # choose capacities for considered countries
    grouped_capacities = grouped_capacities[
        grouped_capacities["index"].str[:2].isin(countries)
    ]
    return grouped_capacities


def calc_capacity_per_year(df, lng, year):
    """Calculates gas production capacities from TYNDP data for a specified year

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame containing all TYNDP data.

    lng : geopandas.GeoDataFrame
        Georeferenced LNG terminal capacities.

    year : int
        Year to calculate gas production capacity for.

    Returns
    -------
    pandas.DataFrame
        Gas production capacities per foreign node and energy carrier
    """
    df_conv_peak = (
        df[
            (df["Parameter"] == "Conventional")
            & (df["Year"] == year)
            & (df["Case"] == "Peak")
        ]
        .rename(columns={"Value": f"Value_conv_{year}_peak"})
        .drop(columns=["Parameter", "Year", "Case"])
    )
    df_conv_average = (
        df[
            (df["Parameter"] == "Conventional")
            & (df["Year"] == year)
            & (df["Case"] == "Average")
        ]
        .rename(columns={"Value": f"Value_conv_{year}_average"})
        .drop(columns=["Parameter", "Year", "Case"])
    )
    df_bioch4 = (
        df[
            (df["Parameter"] == "Biomethane")
            & (df["Year"] == year)
            & (
                df["Case"] == "Peak"
            )  # Peak and Average have the same valus for biogas production in 2030 and 2040
        ]
        .rename(columns={"Value": f"Value_bio_{year}"})
        .drop(columns=["Parameter", "Year", "Case"])
    )

    # Some values are duplicated (DE00 in 2030)
    df_conv_peak = df_conv_peak[~df_conv_peak.index.duplicated(keep="first")]
    df_conv_average = df_conv_average[
        ~df_conv_average.index.duplicated(keep="first")
    ]

    df_year = pd.concat(
        [df_conv_peak, df_conv_average, df_bioch4, lng], axis=1
    ).fillna(0)
    df_year = df_year[
        ~(
            (df_year[f"Value_conv_{year}_peak"] == 0)
            & (df_year[f"Value_bio_{year}"] == 0)
            & (df_year["LNG max_cap_store2pipe_M_m3_per_d (in GWh/d)"] == 0)
        )
    ]
    df_year[f"Value_conv_{year}"] = (
        df_year[f"Value_conv_{year}_peak"]
        + df_year["LNG max_cap_store2pipe_M_m3_per_d (in GWh/d)"]
    )
    df_year[f"CH4_{year}"] = (
        df_year[f"Value_conv_{year}"] + df_year[f"Value_bio_{year}"]
    )
    df_year[f"ratioConv_{year}"] = (
        df_year[f"Value_conv_{year}_peak"] / df_year[f"CH4_{year}"]
    )
    df_year[f"e_nom_max_{year}"] = (
        df_year[f"Value_conv_{year}_average"] + df_year[f"Value_bio_{year}"]
    )
    df_year = df_year.drop(
        columns=[
            "LNG max_cap_store2pipe_M_m3_per_d (in GWh/d)",
            f"Value_conv_{year}_average",
            f"Value_conv_{year}_peak",
        ]
    )

    return df_year


def calc_global_ch4_demand(Norway_global_demand_1y):
    """Calculates global CH4 demands abroad for eGon2035 scenario

    The data comes from TYNDP 2020 according to NEP 2021 from the
    scenario 'Distributed Energy', linear interpolate between 2030
    and 2040.

    Returns
    -------
    pandas.DataFrame
        Global (yearly) CH4 final demand per foreign node

    """

    sources = config.datasets()["gas_neighbours"]["sources"]

    file = zipfile.ZipFile(f"tyndp/{sources['tyndp_capacities']}")
    df = pd.read_excel(
        file.open("TYNDP-2020-Scenario-Datafile.xlsx").read(),
        sheet_name="Gas Data",
    )

    df = (
        df.query(
            'Scenario == "Distributed Energy" & '
            'Case == "Average" &'
            'Category == "Demand"'
        )
        .drop(
            columns=[
                "Generator_ID",
                "Climate Year",
                "Simulation_ID",
                "Node 1",
                "Path",
                "Direct/Indirect",
                "Sector",
                "Note",
                "Category",
                "Case",
                "Scenario",
            ]
        )
        .set_index("Node/Line")
    )

    df_2030 = (
        df[(df["Parameter"] == "Final demand") & (df["Year"] == 2030)]
        .rename(columns={"Value": "Value_2030"})
        .drop(columns=["Parameter", "Year"])
    )

    df_2040 = (
        df[(df["Parameter"] == "Final demand") & (df["Year"] == 2040)]
        .rename(columns={"Value": "Value_2040"})
        .drop(columns=["Parameter", "Year"])
    )

    # Conversion GWh/d to MWh/h
    conversion_factor = 1000 / 24

    df_2035 = pd.concat([df_2040, df_2030], axis=1)
    df_2035["GlobD_2035"] = (
        (df_2035["Value_2030"] + df_2035["Value_2040"]) / 2
    ) * conversion_factor
    df_2035.loc["NOS0"] = [
        0,
        0,
        Norway_global_demand_1y / 8760,
    ]  # Manually add Norway demand
    grouped_demands = df_2035.drop(
        columns=["Value_2030", "Value_2040"]
    ).reset_index()

    # choose demands for considered countries
    return grouped_demands[
        grouped_demands["Node/Line"].str[:2].isin(countries)
    ]


def import_ch4_demandTS():
    """Calculate global CH4 demand in Norway and CH4 demand profile

    Import from the PyPSA-eur-sec run the timeseries of residential
    rural heat per neighbor country. This timeserie is used to
    calculate:
      * the global (yearly) heat demand of Norway
        (that will be supplied by CH4)
      * the normalized CH4 hourly resolved demand profile

    Parameters
    ----------
    None.

    Returns
    -------
    Norway_global_demand: Float
        Yearly heat demand of Norway in MWh
    neighbor_loads_t: pandas.DataFrame
        Normalized CH4 hourly resolved demand profiles per neighbor country

    """

    cwd = Path(".")
    target_file = (
        cwd
        / "data_bundle_egon_data"
        / "pypsa_eur_sec"
        / "2022-07-26-egondata-integration"
        / "postnetworks"
        / "elec_s_37_lv2.0__Co2L0-1H-T-H-B-I-dist1_2050.nc"
    )

    network = pypsa.Network(str(target_file))

    # Set country tag for all buses
    network.buses.country = network.buses.index.str[:2]
    neighbors = network.buses[network.buses.country != "DE"]
    neighbors = neighbors[
        (neighbors["country"].isin(countries))
        & (neighbors["carrier"] == "residential rural heat")
    ].drop_duplicates(subset="country")

    neighbor_loads = network.loads[network.loads.bus.isin(neighbors.index)]
    neighbor_loads_t_index = neighbor_loads.index[
        neighbor_loads.index.isin(network.loads_t.p_set.columns)
    ]
    neighbor_loads_t = network.loads_t["p_set"][neighbor_loads_t_index]
    Norway_global_demand = neighbor_loads_t[
        "NO3 0 residential rural heat"
    ].sum()

    for i in neighbor_loads_t.columns:
        neighbor_loads_t[i] = neighbor_loads_t[i] / neighbor_loads_t[i].sum()

    return Norway_global_demand, neighbor_loads_t


def insert_ch4_demand(global_demand, normalized_ch4_demandTS):
    """Insert CH4 demands abroad in the database for eGon2035

    Parameters
    ----------
    global_demand : pandas.DataFrame
        Global CH4 demand per foreign node in 1 year
    gas_demandTS : pandas.DataFrame
        Normalized time serie of the demand per foreign country

    Returns
    -------
    None.

    """
    sources = config.datasets()["gas_neighbours"]["sources"]
    targets = config.datasets()["gas_neighbours"]["targets"]
    map_buses = get_map_buses()

    scn_name = "eGon2035"
    carrier = "CH4"

    # Delete existing data
    db.execute_sql(
        f"""
        DELETE FROM 
        {targets['load_timeseries']['schema']}.{targets['load_timeseries']['table']}
        WHERE "load_id" IN (
            SELECT load_id FROM 
            {targets['loads']['schema']}.{targets['loads']['table']}
            WHERE bus IN (
                SELECT bus_id FROM
                {sources['buses']['schema']}.{sources['buses']['table']}
                WHERE country != 'DE'
                AND scn_name = '{scn_name}')
            AND scn_name = '{scn_name}'
            AND carrier = '{carrier}'
        );
        """
    )

    db.execute_sql(
        f"""
        DELETE FROM
        {targets['loads']['schema']}.{targets['loads']['table']}
        WHERE bus IN (
            SELECT bus_id FROM
            {sources['buses']['schema']}.{sources['buses']['table']}
            WHERE country != 'DE'
            AND scn_name = '{scn_name}')
        AND scn_name = '{scn_name}'
        AND carrier = '{carrier}'
        """
    )

    # Set bus_id
    global_demand.loc[
        global_demand[global_demand["Node/Line"].isin(map_buses.keys())].index,
        "Node/Line",
    ] = global_demand.loc[
        global_demand[global_demand["Node/Line"].isin(map_buses.keys())].index,
        "Node/Line",
    ].map(
        map_buses
    )
    global_demand.loc[:, "bus"] = (
        get_foreign_gas_bus_id().loc[global_demand.loc[:, "Node/Line"]].values
    )

    # Add missing columns
    c = {"scn_name": scn_name, "carrier": carrier}
    global_demand = global_demand.assign(**c)

    new_id = db.next_etrago_id("load")
    global_demand["load_id"] = range(new_id, new_id + len(global_demand))

    ch4_demand_TS = global_demand.copy()
    # Remove useless columns
    global_demand = global_demand.drop(columns=["Node/Line", "GlobD_2035"])

    # Insert data to db
    global_demand.to_sql(
        targets["loads"]["table"],
        db.engine(),
        schema=targets["loads"]["schema"],
        index=False,
        if_exists="append",
    )

    # Insert time series
    ch4_demand_TS["Node/Line"] = ch4_demand_TS["Node/Line"].replace(
        ["UK00"], "GB"
    )

    p_set = []
    for index, row in ch4_demand_TS.iterrows():
        normalized_TS_df = normalized_ch4_demandTS.loc[
            :,
            normalized_ch4_demandTS.columns.str.contains(row["Node/Line"][:2]),
        ]
        p_set.append(
            (
                normalized_TS_df[normalized_TS_df.columns[0]]
                * row["GlobD_2035"]
            ).tolist()
        )

    ch4_demand_TS["p_set"] = p_set
    ch4_demand_TS["temp_id"] = 1
    ch4_demand_TS = ch4_demand_TS.drop(
        columns=["Node/Line", "GlobD_2035", "bus", "carrier"]
    )

    # Insert data to DB
    ch4_demand_TS.to_sql(
        targets["load_timeseries"]["table"],
        db.engine(),
        schema=targets["load_timeseries"]["schema"],
        index=False,
        if_exists="append",
    )


def calc_ch4_storage_capacities():
    target_file = (
        Path(".") / "datasets" / "gas_data" / "data" / "IGGIELGN_Storages.csv"
    )

    ch4_storage_capacities = pd.read_csv(
        target_file,
        delimiter=";",
        decimal=".",
        usecols=["country_code", "param"],
    )

    ch4_storage_capacities = ch4_storage_capacities[
        ch4_storage_capacities["country_code"].isin(countries)
    ]

    map_countries_scigrid = {
        "AT": "AT00",
        "BE": "BE00",
        "CZ": "CZ00",
        "DK": "DKE1",
        "EE": "EE00",
        "EL": "GR00",
        "ES": "ES00",
        "FI": "FI00",
        "FR": "FR00",
        "GB": "UK00",
        "IT": "ITCN",
        "LT": "LT00",
        "LV": "LV00",
        "MT": "MT00",
        "NL": "NL00",
        "PL": "PL00",
        "PT": "PT00",
        "SE": "SE01",
    }

    # Define new columns
    max_workingGas_M_m3 = []
    end_year = []

    for index, row in ch4_storage_capacities.iterrows():
        param = ast.literal_eval(row["param"])
        end_year.append(param["end_year"])
        max_workingGas_M_m3.append(param["max_workingGas_M_m3"])

    end_year = [float("inf") if x == None else x for x in end_year]
    ch4_storage_capacities = ch4_storage_capacities.assign(end_year=end_year)
    ch4_storage_capacities = ch4_storage_capacities[
        ch4_storage_capacities["end_year"] >= 2035
    ]

    # Calculate e_nom
    conv_factor = (
        10830  # M_m3 to MWh - gross calorific value = 39 MJ/m3 (eurogas.org)
    )
    ch4_storage_capacities["e_nom"] = [
        conv_factor * i for i in max_workingGas_M_m3
    ]

    ch4_storage_capacities.drop(
        ["param", "end_year"],
        axis=1,
        inplace=True,
    )

    ch4_storage_capacities["Country"] = ch4_storage_capacities[
        "country_code"
    ].map(map_countries_scigrid)
    ch4_storage_capacities = ch4_storage_capacities.groupby(
        ["country_code"]
    ).agg(
        {
            "e_nom": "sum",
            "Country": "first",
        },
    )

    ch4_storage_capacities = ch4_storage_capacities.drop(["RU"])
    ch4_storage_capacities.loc[:, "bus"] = (
        get_foreign_gas_bus_id()
        .loc[ch4_storage_capacities.loc[:, "Country"]]
        .values
    )

    ch4_storage_capacities.drop(
        ["Country"],
        axis=1,
        inplace=True,
    )
    return ch4_storage_capacities


def calc_global_power_to_h2_demand():
    """Calculates H2 demand abroad for eGon2035 scenario

    Calculates global power demand abroad linked to H2 production.
    The data comes from TYNDP 2020 according to NEP 2021 from the
    scenario 'Distributed Energy', linear interpolate between 2030
    and 2040.

    Returns
    -------
    pandas.DataFrame
        Global power-to-h2 demand per foreign node

    """
    sources = config.datasets()["gas_neighbours"]["sources"]

    file = zipfile.ZipFile(f"tyndp/{sources['tyndp_capacities']}")
    df = pd.read_excel(
        file.open("TYNDP-2020-Scenario-Datafile.xlsx").read(),
        sheet_name="Gas Data",
    )

    df = (
        df.query(
            'Scenario == "Distributed Energy" & '
            'Case == "Average" &'
            'Parameter == "P2H2"'
        )
        .drop(
            columns=[
                "Generator_ID",
                "Climate Year",
                "Simulation_ID",
                "Node 1",
                "Path",
                "Direct/Indirect",
                "Sector",
                "Note",
                "Category",
                "Case",
                "Scenario",
                "Parameter",
            ]
        )
        .set_index("Node/Line")
    )

    df_2030 = (
        df[df["Year"] == 2030]
        .rename(columns={"Value": "Value_2030"})
        .drop(columns=["Year"])
    )
    df_2040 = (
        df[df["Year"] == 2040]
        .rename(columns={"Value": "Value_2040"})
        .drop(columns=["Year"])
    )

    # Conversion GWh/d to MWh/h
    conversion_factor = 1000 / 24

    df_2035 = pd.concat([df_2040, df_2030], axis=1)
    df_2035["GlobD_2035"] = (
        (df_2035["Value_2030"] + df_2035["Value_2040"]) / 2
    ) * conversion_factor

    global_power_to_h2_demand = df_2035.drop(
        columns=["Value_2030", "Value_2040"]
    )

    # choose demands for considered countries
    global_power_to_h2_demand = global_power_to_h2_demand[
        (global_power_to_h2_demand.index.str[:2].isin(countries))
        & (global_power_to_h2_demand["GlobD_2035"] != 0)
    ]

    # Split in two the demands for DK and UK
    global_power_to_h2_demand.loc["DKW1"] = (
        global_power_to_h2_demand.loc["DKE1"] / 2
    )
    global_power_to_h2_demand.loc["DKE1"] = (
        global_power_to_h2_demand.loc["DKE1"] / 2
    )
    global_power_to_h2_demand.loc["UKNI"] = (
        global_power_to_h2_demand.loc["UK00"] / 2
    )
    global_power_to_h2_demand.loc["UK00"] = (
        global_power_to_h2_demand.loc["UK00"] / 2
    )
    global_power_to_h2_demand = global_power_to_h2_demand.reset_index()

    return global_power_to_h2_demand


def insert_power_to_h2_demand(global_power_to_h2_demand):
    """Insert H2 demands into database for eGon2035

    Detailled description
    This function insert data in the database and has no return.

    Parameters
    ----------
    global_power_to_h2_demand : pandas.DataFrame
        Global H2 demand per foreign node in 1 year

    """
    sources = config.datasets()["gas_neighbours"]["sources"]
    targets = config.datasets()["gas_neighbours"]["targets"]
    map_buses = get_map_buses()

    scn_name = "eGon2035"
    carrier = "H2_for_industry"

    db.execute_sql(
        f"""
        DELETE FROM
        {targets['loads']['schema']}.{targets['loads']['table']}
        WHERE bus IN (
            SELECT bus_id FROM
            {sources['buses']['schema']}.{sources['buses']['table']}
            WHERE country != 'DE'
            AND scn_name = '{scn_name}')
        AND scn_name = '{scn_name}'
        AND carrier = '{carrier}'
        """
    )

    # Set bus_id
    global_power_to_h2_demand.loc[
        global_power_to_h2_demand[
            global_power_to_h2_demand["Node/Line"].isin(map_buses.keys())
        ].index,
        "Node/Line",
    ] = global_power_to_h2_demand.loc[
        global_power_to_h2_demand[
            global_power_to_h2_demand["Node/Line"].isin(map_buses.keys())
        ].index,
        "Node/Line",
    ].map(
        map_buses
    )
    global_power_to_h2_demand.loc[:, "bus"] = (
        get_foreign_bus_id()
        .loc[global_power_to_h2_demand.loc[:, "Node/Line"]]
        .values
    )

    # Add missing columns
    c = {"scn_name": scn_name, "carrier": carrier}
    global_power_to_h2_demand = global_power_to_h2_demand.assign(**c)

    new_id = db.next_etrago_id("load")
    global_power_to_h2_demand["load_id"] = range(
        new_id, new_id + len(global_power_to_h2_demand)
    )

    global_power_to_h2_demand = global_power_to_h2_demand.rename(
        columns={"GlobD_2035": "p_set"}
    )

    power_to_h2_demand_TS = global_power_to_h2_demand.copy()
    # Remove useless columns
    global_power_to_h2_demand = global_power_to_h2_demand.drop(
        columns=["Node/Line"]
    )

    # Insert data to db
    global_power_to_h2_demand.to_sql(
        targets["loads"]["table"],
        db.engine(),
        schema=targets["loads"]["schema"],
        index=False,
        if_exists="append",
    )


def calculate_ch4_grid_capacities():
    """Calculates CH4 grid capacities for foreign countries based on TYNDP-data

    Parameters
    ----------
    None.

    Returns
    -------
    Neighbouring_pipe_capacities_list : pandas.DataFrame

    """
    sources = config.datasets()["gas_neighbours"]["sources"]

    # Download file
    basename = "ENTSOG_TYNDP_2020_Annex_C2_Capacities_per_country.xlsx"
    url = "https://www.entsog.eu/sites/default/files/2021-07/" + basename
    target_file = Path(".") / "datasets" / "gas_data" / basename

    urlretrieve(url, target_file)
    map_pipelines = {
        "NORDSTREAM": "RU00",
        "NORDSTREAM 2": "RU00",
        "OPAL": "DE",
        "YAMAL (BY)": "RU00",
        "Denmark": "DKE1",
        "Belgium": "BE00",
        "Netherlands": "NL00",
        "Norway": "NOM1",
        "Switzerland": "CH00",
        "Poland": "PL00",
        "United Kingdom": "UK00",
        "Germany": "DE",
        "Austria": "AT00",
        "France": "FR00",
        "Czechia": "CZ00",
        "Russia": "RU00",
        "Luxemburg": "LUB1",
    }

    grid_countries = [
        "NORDSTREAM",
        "NORDSTREAM 2",
        "OPAL",
        "YAMAL (BY)",
        "Denmark",
        "Belgium",
        "Netherlands",
        "Norway",
        "Switzerland",
        "Poland",
        "United Kingdom",
        "Germany",
        "Austria",
        "France",
        "Czechia",
        "Russia",
        "Luxemburg",
    ]

    # Read-in data from csv-file
    pipe_capacities_list = pd.read_excel(
        target_file,
        sheet_name="Transmission Peak Capacity",
        skiprows=range(4),
    )
    pipe_capacities_list = pipe_capacities_list[
        ["To Country", "Unnamed: 3", "From Country", 2035]
    ].rename(
        columns={
            "Unnamed: 3": "Scenario",
            "To Country": "To_Country",
            "From Country": "From_Country",
        }
    )
    pipe_capacities_list["To_Country"] = pd.Series(
        pipe_capacities_list["To_Country"]
    ).fillna(method="ffill")
    pipe_capacities_list["From_Country"] = pd.Series(
        pipe_capacities_list["From_Country"]
    ).fillna(method="ffill")
    pipe_capacities_list = pipe_capacities_list[
        pipe_capacities_list["Scenario"] == "Advanced"
    ].drop(columns={"Scenario"})
    pipe_capacities_list = pipe_capacities_list[
        (
            (pipe_capacities_list["To_Country"].isin(grid_countries))
            & (pipe_capacities_list["From_Country"].isin(grid_countries))
        )
        & (pipe_capacities_list[2035] != 0)
    ]
    pipe_capacities_list["To_Country"] = pipe_capacities_list[
        "To_Country"
    ].map(map_pipelines)
    pipe_capacities_list["From_Country"] = pipe_capacities_list[
        "From_Country"
    ].map(map_pipelines)
    pipe_capacities_list["countrycombination"] = pipe_capacities_list[
        ["To_Country", "From_Country"]
    ].apply(
        lambda x: tuple(sorted([str(x.To_Country), str(x.From_Country)])),
        axis=1,
    )

    pipeline_strategies = {
        "To_Country": "first",
        "From_Country": "first",
        2035: sum,
    }

    pipe_capacities_list = pipe_capacities_list.groupby(
        ["countrycombination"]
    ).agg(pipeline_strategies)

    # Add manually DK-SE and AT-CH pipes (Scigrid gas data)
    pipe_capacities_list.loc["(DKE1, SE02)"] = ["DKE1", "SE02", 651]
    pipe_capacities_list.loc["(AT00, CH00)"] = ["AT00", "CH00", 651]

    # Conversion GWh/d to MWh/h
    pipe_capacities_list["p_nom"] = pipe_capacities_list[2035] * (1000 / 24)

    # Border crossing CH4 pipelines between foreign countries

    Neighbouring_pipe_capacities_list = pipe_capacities_list[
        (pipe_capacities_list["To_Country"] != "DE")
        & (pipe_capacities_list["From_Country"] != "DE")
    ].reset_index()

    Neighbouring_pipe_capacities_list.loc[:, "bus0"] = (
        get_foreign_gas_bus_id()
        .loc[Neighbouring_pipe_capacities_list.loc[:, "To_Country"]]
        .values
    )
    Neighbouring_pipe_capacities_list.loc[:, "bus1"] = (
        get_foreign_gas_bus_id()
        .loc[Neighbouring_pipe_capacities_list.loc[:, "From_Country"]]
        .values
    )

    # Adjust columns
    Neighbouring_pipe_capacities_list = Neighbouring_pipe_capacities_list.drop(
        columns=[
            "To_Country",
            "From_Country",
            "countrycombination",
            2035,
        ]
    )

    new_id = db.next_etrago_id("link")
    Neighbouring_pipe_capacities_list["link_id"] = range(
        new_id, new_id + len(Neighbouring_pipe_capacities_list)
    )

    # Border crossing CH4 pipelines between DE and neighbouring countries
    DE_pipe_capacities_list = pipe_capacities_list[
        (pipe_capacities_list["To_Country"] == "DE")
        | (pipe_capacities_list["From_Country"] == "DE")
    ].reset_index()

    dict_cross_pipes_DE = {
        ("AT00", "DE"): "AT",
        ("BE00", "DE"): "BE",
        ("CH00", "DE"): "CH",
        ("CZ00", "DE"): "CZ",
        ("DE", "DKE1"): "DK",
        ("DE", "FR00"): "FR",
        ("DE", "LUB1"): "LU",
        ("DE", "NL00"): "NL",
        ("DE", "NOM1"): "NO",
        ("DE", "PL00"): "PL",
        ("DE", "RU00"): "RU",
    }

    DE_pipe_capacities_list["country_code"] = DE_pipe_capacities_list[
        "countrycombination"
    ].map(dict_cross_pipes_DE)
    DE_pipe_capacities_list = DE_pipe_capacities_list.set_index("country_code")

    for country_code in [e for e in countries if e not in ("GB", "SE", "UK")]:

        # Select cross-bording links
        cap_DE = db.select_dataframe(
            f"""SELECT link_id, bus0, bus1
                FROM {sources['links']['schema']}.{sources['links']['table']}
                    WHERE scn_name = 'eGon2035' 
                    AND carrier = 'CH4'
                    AND (("bus0" IN (
                        SELECT bus_id FROM {sources['buses']['schema']}.{sources['buses']['table']}
                            WHERE country = 'DE'
                            AND carrier = 'CH4'
                            AND scn_name = 'eGon2035')
                        AND "bus1" IN (SELECT bus_id FROM {sources['buses']['schema']}.{sources['buses']['table']}
                            WHERE country = '{country_code}'
                            AND carrier = 'CH4'
                            AND scn_name = 'eGon2035')
                    )
                    OR ("bus0" IN (
                        SELECT bus_id FROM {sources['buses']['schema']}.{sources['buses']['table']}
                            WHERE country = '{country_code}'
                            AND carrier = 'CH4'
                            AND scn_name = 'eGon2035')
                        AND "bus1" IN (SELECT bus_id FROM {sources['buses']['schema']}.{sources['buses']['table']}
                            WHERE country = 'DE'
                            AND carrier = 'CH4'
                            AND scn_name = 'eGon2035'))
                    )
            ;"""
        )

        cap_DE["p_nom"] = DE_pipe_capacities_list.at[
            country_code, "p_nom"
        ] / len(cap_DE.index)
        Neighbouring_pipe_capacities_list = (
            Neighbouring_pipe_capacities_list.append(cap_DE)
        )

    # Add topo, geom and length
    bus_geom = db.select_geodataframe(
        """SELECT bus_id, geom
        FROM grid.egon_etrago_bus
        WHERE scn_name = 'eGon2035'
        AND carrier = 'CH4'
        """,
        epsg=4326,
    ).set_index("bus_id")

    coordinates_bus0 = []
    coordinates_bus1 = []

    for index, row in Neighbouring_pipe_capacities_list.iterrows():
        coordinates_bus0.append(bus_geom["geom"].loc[int(row["bus0"])])
        coordinates_bus1.append(bus_geom["geom"].loc[int(row["bus1"])])

    Neighbouring_pipe_capacities_list["coordinates_bus0"] = coordinates_bus0
    Neighbouring_pipe_capacities_list["coordinates_bus1"] = coordinates_bus1

    Neighbouring_pipe_capacities_list[
        "topo"
    ] = Neighbouring_pipe_capacities_list.apply(
        lambda row: LineString(
            [row["coordinates_bus0"], row["coordinates_bus1"]]
        ),
        axis=1,
    )
    Neighbouring_pipe_capacities_list[
        "geom"
    ] = Neighbouring_pipe_capacities_list.apply(
        lambda row: MultiLineString([row["topo"]]), axis=1
    )
    Neighbouring_pipe_capacities_list[
        "length"
    ] = Neighbouring_pipe_capacities_list.apply(
        lambda row: row["topo"].length, axis=1
    )

    # Remove useless columns
    Neighbouring_pipe_capacities_list = Neighbouring_pipe_capacities_list.drop(
        columns=[
            "coordinates_bus0",
            "coordinates_bus1",
        ]
    )

    # Add missing columns
    c = {"scn_name": "eGon2035", "carrier": "CH4", "p_min_pu": -1.0}
    Neighbouring_pipe_capacities_list = (
        Neighbouring_pipe_capacities_list.assign(**c)
    )

    Neighbouring_pipe_capacities_list = (
        Neighbouring_pipe_capacities_list.set_geometry("geom", crs=4326)
    )

    return Neighbouring_pipe_capacities_list


def tyndp_gas_generation():
    """Insert data from TYNDP 2020 accordning to NEP 2021
    Scenario 'Distributed Energy', linear interpolate between 2030 and 2040

    Returns
    -------
    None.
    """
    capacities = calc_capacities()
    insert_generators(capacities, "eGon2035")

    ch4_storage_capacities = calc_ch4_storage_capacities()
    insert_ch4_stores(ch4_storage_capacities, "eGon2035")


def tyndp_gas_demand():
    """Insert gas demands abroad for eGon2035

    Insert CH4 and H2 demands abroad for eGon2035 by executing the
    following steps:
      * CH4
          * Calculation of the global CH4 demand in Norway and of the
            CH4 demand profile by executing the function
            :py:func:`import_ch4_demandTS`
          * Calculation of the global CH4 demands by executing the
            function :py:func:`calc_global_ch4_demand`
          * Insertion the CH4 loads and their associated time series
            in the database by executing the function
            :py:func:`insert_ch4_demand`
      * H2
          * Calculation of the global power demand abroad linked
            to H2 production by executing the function
            :py:func:`calc_global_power_to_h2_demand`
          * Insertion of these loads in the database by executing the
            function :py:func:`insert_power_to_h2_demand`
    This function insert data in the database and has no return.

    """
    Norway_global_demand_1y, normalized_ch4_demandTS = import_ch4_demandTS()
    global_ch4_demand = calc_global_ch4_demand(Norway_global_demand_1y)
    insert_ch4_demand(global_ch4_demand, normalized_ch4_demandTS)

    global_power_to_h2_demand = calc_global_power_to_h2_demand()
    insert_power_to_h2_demand(global_power_to_h2_demand)


def grid():
    """Insert data from TYNDP 2020 accordning to NEP 2021
    Scenario 'Distributed Energy', linear interpolate between 2030 and 2040

    Returns
    -------
    None.
    """
    Neighbouring_pipe_capacities_list = calculate_ch4_grid_capacities()
    insert_gas_grid_capacities(
        Neighbouring_pipe_capacities_list, scn_name="eGon2035"
    )
