"""The central module containing all code dealing with gas neighbours
"""

import ast
import zipfile

from pathlib import Path
from shapely.geometry import LineString
from sqlalchemy.orm import sessionmaker
import geopandas as gpd
import pandas as pd
import pypsa

from egon.data import config, db
from egon.data.datasets import Dataset
from egon.data.datasets.electrical_neighbours import (
    get_map_buses,
    get_foreign_bus_id,
)
from egon.data.datasets.scenario_parameters import get_sector_parameters
import egon.data.datasets.etrago_setup as etrago

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


class GasNeighbours(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="GasNeighbours",
            version="0.0.0",
            dependencies=dependencies,
            tasks=({tyndp_gas_generation, tyndp_gas_demand}),  # grid
        )


def get_foreign_gas_bus_id():
    """Calculate the etrago bus id from gas nodes of TYNDP based on the geometry

    Returns
    -------
    pandas.Series
        List of mapped node_ids from TYNDP and etragos bus_id

    """

    sources = config.datasets()["gas_neighbours"]["sources"]

    bus_id = db.select_geodataframe(
        """SELECT bus_id, ST_Buffer(geom, 1) as geom, country
        FROM grid.egon_etrago_bus
        WHERE scn_name = 'eGon2035'
        AND carrier = 'CH4'
        AND country != 'DE'
        """,
        epsg=3035,
    )

    # insert installed capacities
    file = zipfile.ZipFile(f"tyndp/{sources['tyndp_capacities']}")

    # Select buses in neighbouring countries as geodataframe
    buses = pd.read_excel(
        file.open("TYNDP-2020-Scenario-Datafile.xlsx").read(),
        sheet_name="Nodes - Dict",
    ).query("longitude==longitude")
    buses = gpd.GeoDataFrame(
        buses,
        crs=4326,
        geometry=gpd.points_from_xy(buses.longitude, buses.latitude),
    ).to_crs(3035)

    buses["bus_id"] = 0

    # Select bus_id from etrago with shortest distance to TYNDP node
    for i, row in buses.iterrows():
        distance = bus_id.set_index("bus_id").geom.distance(row.geometry)
        buses.loc[i, "bus_id"] = distance[
            distance == distance.min()
        ].index.values[0]

    return buses.set_index("node_id").bus_id


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
    pandas.DataFrame
        Gas production capacities per foreign node and energy carrier

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

    df_conv_2030_peak = (
        df[
            (df["Parameter"] == "Conventional")
            & (df["Year"] == 2030)
            & (df["Case"] == "Peak")
        ]
        .rename(columns={"Value": "Value_conv_2030_peak"})
        .drop(columns=["Parameter", "Year", "Case"])
    )
    df_conv_2030_average = (
        df[
            (df["Parameter"] == "Conventional")
            & (df["Year"] == 2030)
            & (df["Case"] == "Average")
        ]
        .rename(columns={"Value": "Value_conv_2030_average"})
        .drop(columns=["Parameter", "Year", "Case"])
    )
    df_bioch4_2030 = (
        df[
            (df["Parameter"] == "Biomethane")
            & (df["Year"] == 2030)
            & (
                df["Case"] == "Peak"
            )  # Peak and Average have the same valus for biogas production in 2030 and 2040
        ]
        .rename(columns={"Value": "Value_bio_2030"})
        .drop(columns=["Parameter", "Year", "Case"])
    )

    df_conv_2030_peak = df_conv_2030_peak[
        ~df_conv_2030_peak.index.duplicated(keep="first")
    ]  # DE00 is duplicated
    df_conv_2030_average = df_conv_2030_average[
        ~df_conv_2030_average.index.duplicated(keep="first")
    ]  # DE00 is duplicated

    lng = read_LNG_capacities()
    df_2030 = pd.concat(
        [df_conv_2030_peak, df_conv_2030_average, df_bioch4_2030, lng], axis=1
    ).fillna(0)
    df_2030 = df_2030[
        ~(
            (df_2030["Value_conv_2030_peak"] == 0)
            & (df_2030["Value_bio_2030"] == 0)
            & (df_2030["LNG max_cap_store2pipe_M_m3_per_d (in GWh/d)"] == 0)
        )
    ]
    df_2030["Value_conv_2030"] = (
        df_2030["Value_conv_2030_peak"]
        + df_2030["LNG max_cap_store2pipe_M_m3_per_d (in GWh/d)"]
    )
    df_2030["CH4_2030"] = (
        df_2030["Value_conv_2030"] + df_2030["Value_bio_2030"]
    )
    df_2030["ratioConv_2030"] = (
        df_2030["Value_conv_2030_peak"] / df_2030["CH4_2030"]
    )
    df_2030["e_nom_max_2030"] = (
        df_2030["Value_conv_2030_average"] + df_2030["Value_bio_2030"]
    )
    df_2030 = df_2030.drop(
        columns=[
            "LNG max_cap_store2pipe_M_m3_per_d (in GWh/d)",
            "Value_conv_2030_peak",
            "Value_conv_2030_average",
        ]
    )
    df_conv_2040_peak = (
        df[
            (df["Parameter"] == "Conventional")
            & (df["Year"] == 2040)
            & (df["Case"] == "Peak")
        ]
        .rename(columns={"Value": "Value_conv_2040_peak"})
        .drop(columns=["Parameter", "Year", "Case"])
    )
    df_conv_2040_average = (
        df[
            (df["Parameter"] == "Conventional")
            & (df["Year"] == 2040)
            & (df["Case"] == "Average")
        ]
        .rename(columns={"Value": "Value_conv_2040_average"})
        .drop(columns=["Parameter", "Year", "Case"])
    )
    df_bioch4_2040 = (
        df[
            (df["Parameter"] == "Biomethane")
            & (df["Year"] == 2040)
            & (
                df["Case"] == "Peak"
            )  # Peak and Average have the same valus for biogas production in 2030 and 2040
        ]
        .rename(columns={"Value": "Value_bio_2040"})
        .drop(columns=["Parameter", "Year", "Case"])
    )
    df_2040 = pd.concat(
        [df_conv_2040_peak, df_conv_2040_average, df_bioch4_2040, lng], axis=1
    ).fillna(0)
    df_2040 = df_2040[
        ~(
            (df_2040["Value_conv_2040_peak"] == 0)
            & (df_2040["Value_bio_2040"] == 0)
            & (df_2040["LNG max_cap_store2pipe_M_m3_per_d (in GWh/d)"] == 0)
        )
    ]
    df_2040["Value_conv_2040"] = (
        df_2040["Value_conv_2040_peak"]
        + df_2040["LNG max_cap_store2pipe_M_m3_per_d (in GWh/d)"]
    )
    df_2040["CH4_2040"] = (
        df_2040["Value_conv_2040"] + df_2040["Value_bio_2040"]
    )
    df_2040["ratioConv_2040"] = (
        df_2040["Value_conv_2040_peak"] / df_2040["CH4_2040"]
    )
    df_2040["e_nom_max_2040"] = (
        df_2040["Value_conv_2040_average"] + df_2040["Value_bio_2040"]
    )
    df_2040 = df_2040.drop(
        columns=[
            "LNG max_cap_store2pipe_M_m3_per_d (in GWh/d)",
            "Value_conv_2040_average",
            "Value_conv_2040_peak",
        ]
    )

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

    # Add generator in Russia
    grouped_capacities = grouped_capacities.append(
        {
            "cap_2035": 100000000000,
            "e_nom_max": 8.76e14,
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


def insert_generators(gen):
    """Insert gas generators for foreign countries based on TYNDP-data

    Parameters
    ----------
    gen : pandas.DataFrame
        Gas production capacities per foreign node and energy carrier

    Returns
    -------
    None.

    """
    sources = config.datasets()["gas_neighbours"]["sources"]
    targets = config.datasets()["gas_neighbours"]["targets"]
    map_buses = get_map_buses()
    scn_params = get_sector_parameters("gas", "eGon2035")

    # Delete existing data
    db.execute_sql(
        f"""
        DELETE FROM
        {targets['generators']['schema']}.{targets['generators']['table']}
        WHERE bus IN (
            SELECT bus_id FROM
            {sources['buses']['schema']}.{sources['buses']['table']}
            WHERE country != 'DE'
            AND scn_name = 'eGon2035')
        AND scn_name = 'eGon2035'
        AND carrier = 'CH4'
        """
    )

    # Set bus_id
    gen.loc[gen[gen["index"].isin(map_buses.keys())].index, "index"] = gen.loc[
        gen[gen["index"].isin(map_buses.keys())].index, "index"
    ].map(map_buses)
    gen.loc[:, "bus"] = (
        get_foreign_gas_bus_id().loc[gen.loc[:, "index"]].values
    )

    # Add missing columns
    c = {"scn_name": "eGon2035", "carrier": "CH4"}
    gen = gen.assign(**c)

    new_id = db.next_etrago_id("generator")
    gen["generator_id"] = range(new_id, new_id + len(gen))
    gen["p_nom"] = gen["cap_2035"]
    gen["marginal_cost"] = (
        gen["ratioConv_2035"] * scn_params["marginal_cost"]["CH4"]
        + (1 - gen["ratioConv_2035"]) * scn_params["marginal_cost"]["biogas"]
    )

    # Remove useless columns
    gen = gen.drop(columns=["index", "ratioConv_2035", "cap_2035"])

    print(gen)
    # Insert data to db
    gen.to_sql(
        targets["generators"]["table"],
        db.engine(),
        schema=targets["generators"]["schema"],
        index=False,
        if_exists="append",
    )


# def grid():
#     """Insert gas grid compoenents for neighbouring countries

#     Returns
#     -------
#     None.

#     """


def calc_global_ch4_demand(Norway_global_demand_1y):
    """Calculates global gas demands from TYNDP data

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
            'Case == "Average" &'  # Case: 2 Week/Average/DF/Peak
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
    """Import from the PyPSA-eur-sec run the timeseries of
    residential rural heat per neighbor country.
    This timeserie is used to calculate:
    - the global (yearly) heat demand of Norway (that will be supplied by CH4)
    - the normalized CH4 hourly resolved demand profile

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
        / "2021-egondata-integration"
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


def import_power_to_h2_demandTS():
    """Import from the PyPSA-eur-sec run the timeseries of
    industry demand heat per neighbor country and normalize it
    in order to model the power-to-H2 hourly resolved demand profile.

    Parameters
    ----------
    None.

    Returns
    -------
    neighbor_loads_t: pandas.DataFrame
        Normalized CH4 hourly resolved demand profiles per neighbor country

    """

    cwd = Path(".")
    target_file = (
        cwd
        / "data_bundle_egon_data"
        / "pypsa_eur_sec"
        / "2021-egondata-integration"
        / "postnetworks"
        / "elec_s_37_lv2.0__Co2L0-1H-T-H-B-I-dist1_2050.nc"
    )

    network = pypsa.Network(str(target_file))

    # Set country tag for all buses
    network.buses.country = network.buses.index.str[:2]
    neighbors = network.buses[network.buses.country != "DE"]
    neighbors = neighbors[
        (neighbors["country"].isin(countries))
        & (
            neighbors["carrier"] == "residential rural heat"
        )  # no available industry profile for now, using another timeserie
    ]  # .drop_duplicates(subset="country")

    neighbor_loads = network.loads[network.loads.bus.isin(neighbors.index)]
    neighbor_loads_t_index = neighbor_loads.index[
        neighbor_loads.index.isin(network.loads_t.p_set.columns)
    ]
    neighbor_loads_t = network.loads_t["p_set"][neighbor_loads_t_index]

    for i in neighbor_loads_t.columns:
        neighbor_loads_t[i] = neighbor_loads_t[i] / neighbor_loads_t[i].sum()

    return neighbor_loads_t


def insert_ch4_demand(global_demand, normalized_ch4_demandTS):
    """Insert gas final demands for foreign countries

    Parameters
    ----------
    global_demand : pandas.DataFrame
        Global gas demand per foreign node in 1 year
    gas_demandTS : pandas.DataFrame
        Normalized time serie of the demand per foreign country

    Returns
    -------
    None.

    """
    sources = config.datasets()["gas_neighbours"]["sources"]
    targets = config.datasets()["gas_neighbours"]["targets"]
    map_buses = get_map_buses()

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
                AND scn_name = 'eGon2035')
            AND scn_name = 'eGon2035'
            AND carrier = 'CH4'            
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
            AND scn_name = 'eGon2035')
        AND scn_name = 'eGon2035'
        AND carrier = 'CH4'
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
    c = {"scn_name": "eGon2035", "carrier": "CH4"}
    global_demand = global_demand.assign(**c)

    new_id = db.next_etrago_id("load")
    global_demand["load_id"] = range(new_id, new_id + len(global_demand))

    ch4_demand_TS = global_demand.copy()
    # Remove useless columns
    global_demand = global_demand.drop(columns=["Node/Line", "GlobD_2035"])

    print(global_demand)
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

    print(ch4_demand_TS)
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

    return ch4_storage_capacities


def insert_storage(ch4_storage_capacities):
    sources = config.datasets()["gas_neighbours"]["sources"]
    targets = config.datasets()["gas_neighbours"]["targets"]

    # Clean table
    db.execute_sql(
        f"""
        DELETE FROM {targets['stores']['schema']}.{targets['stores']['table']}  
        WHERE "carrier" = 'CH4'
        AND scn_name = 'eGon2035'
        AND bus IN (
            SELECT bus_id FROM {sources['buses']['schema']}.{sources['buses']['table']}
            WHERE scn_name = 'eGon2035' 
            AND country != 'DE'
            );
        """
    )
    # Add missing columns
    c = {"scn_name": "eGon2035", "carrier": "CH4"}
    ch4_storage_capacities = ch4_storage_capacities.assign(**c)

    new_id = db.next_etrago_id("store")
    ch4_storage_capacities["store_id"] = range(
        new_id, new_id + len(ch4_storage_capacities)
    )

    ch4_storage_capacities.drop(
        ["Country"],
        axis=1,
        inplace=True,
    )

    ch4_storage_capacities = ch4_storage_capacities.reset_index(drop=True)
    print(ch4_storage_capacities)
    # Insert data to db
    ch4_storage_capacities.to_sql(
        targets["stores"]["table"],
        db.engine(),
        schema=targets["stores"]["schema"],
        index=False,
        if_exists="append",
    )


def calc_global_power_to_h2_demand():
    """Calculates global power demand linked to h2 production from TYNDP data

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
            'Case == "Average" &'  # Case: 2 Week/Average/DF/Peak
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


def insert_power_to_h2_demand(
    global_power_to_h2_demand, normalized_power_to_h2_demandTS
):
    sources = config.datasets()["gas_neighbours"]["sources"]
    targets = config.datasets()["gas_neighbours"]["targets"]
    map_buses = get_map_buses()

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
                AND scn_name = 'eGon2035')
            AND scn_name = 'eGon2035'
            AND carrier = 'H2 for industry'            
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
            AND scn_name = 'eGon2035')
        AND scn_name = 'eGon2035'
        AND carrier = 'H2 for industry'
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
    c = {"scn_name": "eGon2035", "carrier": "H2 for industry"}
    global_power_to_h2_demand = global_power_to_h2_demand.assign(**c)

    new_id = db.next_etrago_id("load")
    global_power_to_h2_demand["load_id"] = range(
        new_id, new_id + len(global_power_to_h2_demand)
    )

    power_to_h2_demand_TS = global_power_to_h2_demand.copy()
    # Remove useless columns
    global_power_to_h2_demand = global_power_to_h2_demand.drop(
        columns=["Node/Line", "GlobD_2035"]
    )

    print(global_power_to_h2_demand)
    # Insert data to db
    global_power_to_h2_demand.to_sql(
        targets["loads"]["table"],
        db.engine(),
        schema=targets["loads"]["schema"],
        index=False,
        if_exists="append",
    )

    # Insert time series
    normalized_power_to_h2_demandTS = normalized_power_to_h2_demandTS.drop(
        columns=[
            "NO3 0 residential rural heat",
            "CH0 0 residential rural heat",
            "LU0 0 residential rural heat",
        ]
    )
    power_to_h2_demand_TS["Node/Line"] = power_to_h2_demand_TS[
        "Node/Line"
    ].replace(["UK00"], "GB4")
    power_to_h2_demand_TS["Node/Line"] = power_to_h2_demand_TS[
        "Node/Line"
    ].replace(["UKNI"], "GB5")
    power_to_h2_demand_TS["Node/Line"] = power_to_h2_demand_TS[
        "Node/Line"
    ].replace(["DKW1"], "DK3")
    power_to_h2_demand_TS["Node/Line"] = power_to_h2_demand_TS[
        "Node/Line"
    ].replace(["DKE1"], "DK0")
    power_to_h2_demand_TS["Node/Line"] = power_to_h2_demand_TS[
        "Node/Line"
    ].replace(["SE02"], "SE3")

    p_set = []
    for index, row in power_to_h2_demand_TS.iterrows():
        normalized_TS_df = normalized_power_to_h2_demandTS.loc[
            :,
            normalized_power_to_h2_demandTS.columns.str.contains(
                row["Node/Line"][:3]
            ),
        ]
        p_set.append(
            (
                normalized_TS_df[normalized_TS_df.columns[0]]
                * row["GlobD_2035"]
            ).tolist()
        )

    power_to_h2_demand_TS["p_set"] = p_set
    power_to_h2_demand_TS["temp_id"] = 1
    power_to_h2_demand_TS = power_to_h2_demand_TS.drop(
        columns=["Node/Line", "GlobD_2035", "bus", "carrier"]
    )

    print(power_to_h2_demand_TS)
    power_to_h2_demand_TS.to_sql(
        targets["load_timeseries"]["table"],
        db.engine(),
        schema=targets["load_timeseries"]["schema"],
        index=False,
        if_exists="append",
    )


def tyndp_gas_generation():
    """Insert data from TYNDP 2020 accordning to NEP 2021
    Scenario 'Distributed Energy', linear interpolate between 2030 and 2040

    Returns
    -------
    None.
    """
    capacities = calc_capacities()
    insert_generators(capacities)

    ch4_storage_capacities = calc_ch4_storage_capacities()
    insert_storage(ch4_storage_capacities)


def tyndp_gas_demand():
    """Insert data from TYNDP 2020 accordning to NEP 2021
    Scenario 'Distributed Energy', linear interpolate between 2030 and 2040

    Returns
    -------
    None.
    """
    Norway_global_demand_1y, normalized_ch4_demandTS = import_ch4_demandTS()
    global_ch4_demand = calc_global_ch4_demand(Norway_global_demand_1y)
    insert_ch4_demand(global_ch4_demand, normalized_ch4_demandTS)

    normalized_power_to_h2_demandTS = import_power_to_h2_demandTS()
    global_power_to_h2_demand = calc_global_power_to_h2_demand()
    insert_power_to_h2_demand(
        global_power_to_h2_demand, normalized_power_to_h2_demandTS
    )
