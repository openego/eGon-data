"""
Central module containing code dealing with gas neighbours for the scenarios

The data used to model the gas sector in the neighbouring countries are
mainly from the TYNDP 2024 of ENTSOG and ENTSO-E:

* gas Annex C1 (LNG and cross-border capacities) and Annex E (supply),
* the demand scenarios and the electricity results, scenario
  'Distributed Energy' (CH4 final demand, electrolysers, gas turbines),
* the IGGIELGN SciGRID_gas dataset for CH4 stores.

For more information on these data, refer to the documentation of the gas
sector.

"""

from functools import lru_cache
from pathlib import Path
import ast
import io
import zipfile

from shapely.geometry import LineString, MultiLineString
import geopandas as gpd
import pandas as pd

from egon.data import db
from egon.data.datasets import load_sources_and_targets
from egon.data.datasets.electrical_neighbours import (
    TYNDP_NODE_COORDINATES,
    ElectricalNeighbours,
    _bracket_tyndp_years,
    _extract_output_block,
    _select_tyndp_capacity_member,
    get_foreign_bus_id,
    get_map_buses,
    map_carriers_tyndp,
)
from egon.data.datasets.gas_neighbours.gas_abroad import (
    insert_gas_grid_capacities,
)
from egon.data.datasets.pypsaeur import prepared_network
from egon.data.datasets.scenario_parameters import (
    get_scenario_year,
    get_sector_parameters,
)
from egon.data.datasets.scenario_parameters.parameters import INCLUDE_RU

TYNDP_PROXY_YEAR = 2035


def tyndp_year(scn_name):
    return TYNDP_PROXY_YEAR


# Matching TYNDP 2024 gas Annex C1/E's "LEVEL" and "Scenario" columns to
# the scenario parameters used in this module
TYNDP2024_GAS_LEVEL = "ADVANCED"
TYNDP2024_GAS_SCENARIO = "NT+ Reference"
TYNDP2024_GAS_INFRA_LEVEL_CH4 = "ADVANCED"
TYNDP2024_GAS_INFRA_LEVEL_H2 = "ADVANCED"
TYNDP2024_GAS_INITIAL_STORAGE_LEVEL = 0.3

TYNDP2024_ANNEX_C1_COUNTRY_TO_NODE = {
    "Austria": "AT00",
    "Belgium": "BE00",
    "Switzerland": "CH00",
    "Czechia": "CZ00",
    "Denmark": "DKE1",
    "France": "FR00",
    "Germany": "DE",
    "Luxemburg": "LUB1",
    "Netherlands": "NL00",
    "Norway": "NOM1",
    "Poland": "PL00",
    "United Kingdom": "UK00",
    "Russia": "RU00",
}

TYNDP2024_ANNEX_E_ISO2_TO_NODE = {
    "AT": "AT00",
    "BE": "BE00",
    "CH": "CH00",
    "CZ": "CZ00",
    "DK": "DKE1",
    "FR": "FR00",
    "DE": "DE",
    "LU": "LUB1",
    "NL": "NL00",
    "NO": "NOM1",
    "PL": "PL00",
    "UK": "UK00",
    "GB": "UK00",
    "SE": "SE02",
}


def _bracket_tyndp2024_gas_years(year, anchors):
    if len(anchors) == 2:
        return anchors
    lo, mid, hi = anchors
    return (lo, mid) if year <= mid else (mid, hi)


def _interpolate_tyndp2024_gas(values_by_year, scn_name):
    """Linearly interpolate/extrapolate TYNDP 2024 gas Annex data

    Parameters
    ----------
    values_by_year : dict
        Mapping of anchor year (a subset of 2030, 2040, 2050) to a
        value or pandas.Series of values for that year
    scn_name : str
        Scenario whose target year (from get_scenario_year) is
        interpolated/extrapolated for

    Returns
    -------
    Value or pandas.Series interpolated/extrapolated to the
    scenario's target year

    """
    year = get_scenario_year(scn_name)
    anchors = tuple(sorted(values_by_year))
    lo, hi = _bracket_tyndp2024_gas_years(year, anchors)
    weight = (year - lo) / (hi - lo)
    return (
        values_by_year[lo] + (values_by_year[hi] - values_by_year[lo]) * weight
    )


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
    "SE",
    "UK",
]

if INCLUDE_RU:
    countries.append("RU")


def get_foreign_gas_bus_id(scn_name, carrier="CH4"):
    """
    Calculate the etrago bus id based on the geometry

    Map node_ids from TYNDP and etragos bus_id

    Parameters
    ----------
    scn_name:
        Name of the scenario
    carrier : str
        Name of the carrier

    Returns
    -------
    pandas.Series
        List of mapped node_ids from TYNDP and etragos bus_id

    """

    bus_id = db.select_geodataframe(
        f"""
        SELECT bus_id, ST_Buffer(geom, 1) as geom, country
        FROM grid.egon_etrago_bus
        WHERE scn_name = '{scn_name}'
        AND carrier = '{carrier}'
        AND country != 'DE'
        """,
        epsg=3035,
    )

    # Select buses in neighbouring countries as geodataframe
    buses = pd.DataFrame(
        [
            {"node_id": node_id, "latitude": lat, "longitude": lon}
            for node_id, (lat, lon) in TYNDP_NODE_COORDINATES.items()
        ]
    )
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


def read_LNG_capacities(scn_name):
    """
    Read LNG import capacities from TYNDP 2024 Annex C1

    Returns
    -------
    pandas.Series
        LNG terminal capacities per foreign country node (in GWh/d)

    """
    # Downloaded in download_tyndp2024_gas_data (GasNeighbours)
    sources, _ = load_sources_and_targets("GasNeighbours")
    target_file = Path(sources.files["tyndp2024_annex_c1"])
    lng = pd.read_excel(target_file, sheet_name="Annex C1_LNG", header=0)
    lng.columns = ["Country", "LEVEL", 2030, 2040, 2050, "unit"]
    lng = lng[lng["LEVEL"] == TYNDP2024_GAS_LEVEL]
    lng = lng.assign(
        node=lng["Country"].map(TYNDP2024_ANNEX_C1_COUNTRY_TO_NODE)
    )
    lng = lng.dropna(subset=["node"]).set_index("node")

    capacities = _interpolate_tyndp2024_gas(
        {2030: lng[2030], 2040: lng[2040], 2050: lng[2050]}, scn_name
    )
    capacities.name = "LNG_capacity_GWh_d"
    return capacities


def read_tyndp2024_gas_supply():
    """
    Read the CH4 supply per country of TYNDP 2024 Annex E ("Supply, GWh")

    Returns
    -------
    pandas.DataFrame
        Columns: node, Categorytype, ModelYear, DailySupply (one row
        per simulated month)

    """
    # Downloaded in download_tyndp2024_gas_data (GasNeighbours)
    sources, _ = load_sources_and_targets("GasNeighbours")
    target_file = Path(sources.files["tyndp2024_annex_e"])
    supply = pd.read_excel(target_file, sheet_name="Supply, GWh", header=0)
    supply = supply[
        (supply["Scenario"] == TYNDP2024_GAS_SCENARIO)
        & (supply["InfraLevelCH4"] == TYNDP2024_GAS_INFRA_LEVEL_CH4)
        & (supply["InfraLevelH2"] == TYNDP2024_GAS_INFRA_LEVEL_H2)
        & (
            supply["InitialStorageLevel"]
            == TYNDP2024_GAS_INITIAL_STORAGE_LEVEL
        )
    ].copy()

    is_national_production = supply["Name"].str.startswith("NPc")
    supply.loc[is_national_production, "node"] = (
        supply.loc[is_national_production, "Name"]
        .str[3:5]
        .map(TYNDP2024_ANNEX_E_ISO2_TO_NODE)
    )
    supply.loc[supply["Name"] == "NO_supply", "node"] = "NOM1"
    supply.loc[supply["Name"] == "RU_supply", "node"] = "RU00"

    return supply.dropna(subset=["node"])[
        ["node", "Categorytype", "ModelYear", "DailySupply"]
    ]


def calc_capacities(scn_name):
    """
    Calculates gas production capacities of neighbouring countries

    For each neighbouring country, the capacity (peak) and the production
    over the year (mean) of TYNDP 2024 Annex E are interpolated to the year
    of the scenario, plus the LNG import capacities of Annex C1. In
    reGon2045 only biomethane is kept.

    Parameters
    ----------
    scn_name : str
        Name of the scenario

    Returns
    -------
    grouped_capacities: pandas.DataFrame
        Gas production capacities per foreign node

    """
    # Conversion GWh/d to MWh/h
    conversion_factor = 1000 / 24

    supply = read_tyndp2024_gas_supply()

    agg = (
        supply.groupby(["node", "Categorytype", "ModelYear"])["DailySupply"]
        .agg(peak="max", avg="mean")
        .reset_index()
    )

    def pivot(categorytype):
        sub = agg[agg["Categorytype"] == categorytype].set_index(
            ["node", "ModelYear"]
        )
        return (
            sub["peak"].unstack("ModelYear"),
            sub["avg"].unstack("ModelYear"),
        )

    conv_peak, conv_avg = pivot("CH4-National Production-Conventional")
    bio_peak, bio_avg = pivot("CH4-National Production-Biomethane")
    p2g_peak, p2g_avg = pivot("CH4-National Production-P2G")

    index = conv_peak.index.union(bio_peak.index).union(p2g_peak.index)
    conv_peak, p2g_peak, bio_peak = (
        frame.reindex(index, fill_value=0)
        for frame in (conv_peak, p2g_peak, bio_peak)
    )
    conv_avg, p2g_avg, bio_avg = (
        frame.reindex(index, fill_value=0)
        for frame in (conv_avg, p2g_avg, bio_avg)
    )
    # P2G (synthetic/e-methane) folded into the non-biogenic share.
    conv_peak = conv_peak + p2g_peak
    conv_avg = conv_avg + p2g_avg

    conv_peak_y = _interpolate_tyndp2024_gas(dict(conv_peak.items()), scn_name)
    conv_avg_y = _interpolate_tyndp2024_gas(dict(conv_avg.items()), scn_name)
    bio_peak_y = _interpolate_tyndp2024_gas(dict(bio_peak.items()), scn_name)
    bio_avg_y = _interpolate_tyndp2024_gas(dict(bio_avg.items()), scn_name)

    lng = read_LNG_capacities(scn_name).reindex(index, fill_value=0)

    total_avg = conv_avg_y + bio_avg_y + lng

    grouped_capacities = pd.DataFrame(
        {
            "cap_2035": (conv_peak_y + bio_peak_y + lng) * conversion_factor,
            "cap_bio_2035": bio_peak_y * conversion_factor,
            "e_nom_max": total_avg * conversion_factor * 8760,
            "share_LNG_2035": (lng / total_avg).fillna(0),
            "share_conv_pipe_2035": (conv_avg_y / total_avg).fillna(0),
            "share_bio_2035": (bio_avg_y / total_avg).fillna(0),
        }
    )
    grouped_capacities.index.name = "index"
    grouped_capacities = grouped_capacities.reset_index()

    # Only biomethane for reGon2045, drop the LNG and conventional-pipe shares
    if scn_name == "reGon2045":
        grouped_capacities["cap_2035"] = grouped_capacities["cap_bio_2035"]
        grouped_capacities["e_nom_max"] = grouped_capacities["cap_2035"] * 8760
        grouped_capacities["share_LNG_2035"] = 0.0
        grouped_capacities["share_conv_pipe_2035"] = 0.0
        grouped_capacities["share_bio_2035"] = 1.0

    grouped_capacities = grouped_capacities.drop(columns=["cap_bio_2035"])

    # Add extra-EU pipe-supply generators for Norway (and Russia if
    # INCLUDE_RU) from Annex E's "CH4 Pipe Supply" category.
    extra_eu_nodes = ["NOM1"]
    if INCLUDE_RU:
        extra_eu_nodes.append("RU00")

    pipe_supply = supply[
        (supply["Categorytype"] == "CH4 Pipe Supply")
        & (supply["node"].isin(extra_eu_nodes))
    ]
    pipe_avg = (
        pipe_supply.groupby(["node", "ModelYear"])["DailySupply"]
        .mean()
        .unstack("ModelYear")
    )
    pipe_cap_y = _interpolate_tyndp2024_gas(dict(pipe_avg.items()), scn_name)

    df_conv_2035 = pd.DataFrame({"cap_2035": pipe_cap_y * conversion_factor})
    df_conv_2035["e_nom_max"] = df_conv_2035["cap_2035"] * 8760
    df_conv_2035["share_LNG_2035"] = 0
    df_conv_2035["share_conv_pipe_2035"] = 1
    df_conv_2035["share_bio_2035"] = 0
    df_conv_2035.index.name = "index"
    df_conv_2035 = df_conv_2035.reset_index()

    if scn_name != "reGon2045":
        grouped_capacities = pd.concat([grouped_capacities, df_conv_2035])

    # drop countries with no capacity at all (e.g. Switzerland has
    # neither domestic CH4 production nor LNG import capacity)
    grouped_capacities = grouped_capacities[
        grouped_capacities["cap_2035"] != 0
    ]

    # choose capacities for considered countries
    grouped_capacities = grouped_capacities[
        grouped_capacities["index"].str[:2].isin(countries)
    ]
    return grouped_capacities


def insert_generators(gen, scn_name):
    """
    Insert gas generators for foreign countries into the database.
    The marginal cost of the methane is calculated as the sum of the
    imported LNG cost, the conventional natural gas cost and the
    biomethane cost, weighted by their share in the total import/
    production capacity.
    LNG gas is considered to be 30% more expensive than the natural gas
    transported by pipelines (source: iwd, 2022).

    Parameters
    ----------
    gen : pandas.DataFrame
        Gas production capacities per foreign node and energy carrier
    scn_name : str
        Name of the scenario

    Returns
    -------
    None
    """
    sources, targets = load_sources_and_targets("GasNeighbours")

    map_buses = get_map_buses()
    scn_params = get_sector_parameters("gas", scn_name)

    year = str(tyndp_year(scn_name))

    # Delete existing data
    db.execute_sql(
        f"""
        DELETE FROM
        {targets.tables['generators']}
        WHERE bus IN (
            SELECT bus_id FROM
            {sources.tables['buses']}
            WHERE country != 'DE'
            AND scn_name = '{scn_name}')
        AND scn_name = '{scn_name}'
        AND carrier = 'CH4';
        """
    )

    # Set bus_id
    gen.loc[gen[gen["index"].isin(map_buses.keys())].index, "index"] = gen.loc[
        gen[gen["index"].isin(map_buses.keys())].index, "index"
    ].map(map_buses)
    gen.loc[:, "bus"] = (
        get_foreign_gas_bus_id(scn_name).loc[gen.loc[:, "index"]].values
    )

    # Add missing columns
    c = {"scn_name": scn_name, "carrier": "CH4"}
    gen = gen.assign(**c)

    gen["generator_id"] = db.next_etrago_id("generator", len(gen))
    gen["p_nom"] = gen[f"cap_{year}"]
    gen["marginal_cost"] = (
        gen[f"share_LNG_{year}"] * scn_params["marginal_cost"]["CH4"] * 1.3
        + gen[f"share_conv_pipe_{year}"] * scn_params["marginal_cost"]["CH4"]
        + gen[f"share_bio_{year}"] * scn_params["marginal_cost"]["biogas"]
    )

    # Remove useless columns
    gen = gen.drop(
        columns=[
            "index",
            f"share_LNG_{year}",
            f"share_conv_pipe_{year}",
            f"share_bio_{year}",
            f"cap_{year}",
        ]
    )

    # Insert data to db
    gen.to_sql(
        targets.get_table_name("generators").split(".")[-1],
        db.engine(),
        schema=targets.get_table_schema("generators"),
        index=False,
        if_exists="append",
    )


# Final demand of natural gas in 2019 [MWh] of the neighbours outside the
# EU27 (Eurostat, BFE), see calc_global_ch4_demand
NON_EU_CH4_FINAL_DEMAND_2019 = {
    # Eurostat, nrg_bal_c, natural gas (G3000), final consumption - energy
    # use (FC_E) plus non-energy use (FC_NE), 2019, GWh (data of 2026-08-27)
    "NO": (4_802.128 + 6_241.085) * 1e3,
    "UK": (457_191.764 + 4_196.5) * 1e3,
    # BFE, Schweizerische Gesamtenergiestatistik 2019, final consumption by
    # consumer group (p. 26): gas 115,200 TJ
    "CH": 115_200 / 3.6 * 1e3,
}


@lru_cache(maxsize=1)
def read_tyndp2024_final_demand():
    """
    Read the final energy demand of the TYNDP 2024 demand scenarios

    Returns
    -------
    pandas.DataFrame
        One row per country, sector, subsector and carrier, with the
        columns REF_2019, DE_2040, DE_2050, GA_2040 and GA_2050

    """
    sources, _ = load_sources_and_targets("GasNeighbours")
    with zipfile.ZipFile(sources.files["tyndp2024_demand"]) as archive:
        member = next(
            name
            for name in archive.namelist()
            if name.endswith(".xlsb") and "__MACOSX" not in name
        )
        raw = pd.read_excel(
            io.BytesIO(archive.read(member)),
            sheet_name="3_DEMAND_OUTPUT",
            header=None,
            engine="pyxlsb",
        )
    raw.columns = [
        f"{scenario}_{int(year)}" if isinstance(scenario, str) else year
        for scenario, year in zip(raw.iloc[0], raw.iloc[1])
    ]
    return raw.iloc[2:].reset_index(drop=True)


def calc_global_ch4_demand(scn_name):
    """
    Calculates the CH4 final demand of the neighbouring countries

    The data comes from the TYNDP 2024 demand scenarios, scenario
    'Distributed Energy', interpolated to the year of the scenario;
    Norway, Switzerland and the United Kingdom are scaled from 2019.

    Parameters
    ----------
    scn_name : str
        Name of the scenario

    Returns
    -------
    pandas.DataFrame
        Yearly CH4 final demand (column "demand", MWh) per foreign node
        ("Node/Line")

    """
    demand = read_tyndp2024_final_demand()
    methane = demand[
        (demand["ENERGY_CARRIER"] == "Methane")
        & (demand["SUBSECTOR"] == "Total")
    ]
    per_country = (
        methane.groupby("COUNTRY")[["REF_2019", "DE_2040", "DE_2050"]]
        .sum()
        .astype(float)
        * 1e6  # TWh -> MWh
    )
    demand = _interpolate_tyndp2024_gas(
        {
            2019: per_country["REF_2019"],
            2040: per_country["DE_2040"],
            2050: per_country["DE_2050"],
        },
        scn_name,
    )

    # Non-EU countries: development of the EU27
    eu_trend = demand["EU"] / per_country.at["EU", "REF_2019"]
    for country, demand_2019 in NON_EU_CH4_FINAL_DEMAND_2019.items():
        demand[country] = demand_2019 * eu_trend

    demand = demand[demand.index.isin(countries)]
    global_demand = pd.DataFrame(
        {
            "Node/Line": demand.index.map(TYNDP2024_ANNEX_E_ISO2_TO_NODE),
            "demand": demand.values,
        }
    ).dropna(subset=["Node/Line"])

    return global_demand.groupby("Node/Line", as_index=False)["demand"].sum()


def import_ch4_demandTS():
    """
    Calculate the CH4 demand profiles abroad

    Import from the PyPSA-eur-sec run the time series of residential rural
    heat per neighbouring country and normalize it. It is used as the
    profile of the CH4 final demand of the country (the demand over the
    year comes from :py:func:`calc_global_ch4_demand`).

    Returns
    -------
    neighbor_loads_t: pandas.DataFrame
        Normalized CH4 hourly resolved demand profiles per neighbor country

    """

    network = prepared_network()

    # Set country tag for all buses
    network.buses.country = network.buses.index.str[:2]
    neighbors = network.buses[network.buses.country != "DE"]
    neighbors = neighbors[
        (neighbors["country"].isin(countries))
        & (neighbors["carrier"].str.contains("rural heat"))
    ].drop_duplicates(subset="country")

    neighbor_loads = network.loads[network.loads.bus.isin(neighbors.index)]
    neighbor_loads_t_index = neighbor_loads.index[
        neighbor_loads.index.isin(network.loads_t.p_set.columns)
    ]
    neighbor_loads_t = network.loads_t["p_set"][neighbor_loads_t_index].copy()

    for i in neighbor_loads_t.columns:
        neighbor_loads_t[i] = neighbor_loads_t[i] / neighbor_loads_t[i].sum()

    return neighbor_loads_t


def insert_ch4_demand(global_demand, normalized_ch4_demandTS, scn_name):
    """Insert CH4 demands abroad into the database

    Parameters
    ----------
    global_demand : pandas.DataFrame
        Global CH4 demand per foreign node in 1 year
    gas_demandTS : pandas.DataFrame
        Normalized time series of the demand per foreign country
    scn_name : str
        Name of the scenario

    Returns
    -------
    None
    """
    sources, targets = load_sources_and_targets("GasNeighbours")

    map_buses = get_map_buses()

    carrier = "CH4"

    # Delete existing data
    db.execute_sql(
        f"""
        DELETE FROM
        {targets.tables['load_timeseries']}
        WHERE "load_id" IN (
            SELECT load_id FROM
            {targets.tables['loads']}
            WHERE bus IN (
                SELECT bus_id FROM
                {sources.tables['buses']}
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
        {targets.tables['loads']}
        WHERE bus IN (
            SELECT bus_id FROM
            {sources.tables['buses']}
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
        get_foreign_gas_bus_id(scn_name)
        .loc[global_demand.loc[:, "Node/Line"]]
        .values
    )

    # Add missing columns
    c = {"scn_name": scn_name, "carrier": carrier}
    global_demand = global_demand.assign(**c)

    global_demand["load_id"] = db.next_etrago_id("load", len(global_demand))

    ch4_demand_TS = global_demand.copy()
    # Remove useless columns
    global_demand = global_demand.drop(columns=["Node/Line", "demand"])

    # Insert data to db
    global_demand.to_sql(
        targets.get_table_name("loads").split(".")[-1],
        db.engine(),
        schema=targets.get_table_schema("loads"),
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
                normalized_TS_df[normalized_TS_df.columns[0]] * row["demand"]
            ).tolist()
        )

    ch4_demand_TS["p_set"] = p_set
    ch4_demand_TS["temp_id"] = 1
    ch4_demand_TS = ch4_demand_TS.drop(
        columns=["Node/Line", "demand", "bus", "carrier"]
    )

    # Insert data to DB
    ch4_demand_TS.to_sql(
        targets.get_table_name("load_timeseries").split(".")[-1],
        db.engine(),
        schema=targets.get_table_schema("load_timeseries"),
        index=False,
        if_exists="append",
    )


def calc_ch4_storage_capacities(scn_name):
    """
    Calculates gas storage capacities of neighbouring countries

    This function reads from the SciGRID_gas dataset the existing CH4
    cavern stores, adjusts and returns them.
    Caverns reference: SciGRID_gas dataset (datasets/gas_data/data/IGGIELGN_Storages.csv
    downloaded in :func:`download_SciGRID_gas_data <egon.data.datasets.gas_grid.download_SciGRID_gas_data>`).
    For more information on these data refer, to the
    `SciGRID_gas IGGIELGN documentation <https://zenodo.org/record/4767098>`_.

    Returns
    -------
        ch4_storage_capacities: pandas.DataFrame
        Methane gas storage capacities per country in MWh

    """
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

    end_year = [float("inf") if x is None else x for x in end_year]
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

    # Russia is only present here if INCLUDE_RU (see scenario_parameters)
    # is True - the raw data is already filtered to `countries` above.
    if INCLUDE_RU:
        ch4_storage_capacities = ch4_storage_capacities.drop(["RU"])
    ch4_storage_capacities.loc[:, "bus"] = (
        get_foreign_gas_bus_id(scn_name)
        .loc[ch4_storage_capacities.loc[:, "Country"]]
        .values
    )

    return ch4_storage_capacities


def insert_storage(ch4_storage_capacities, scn_name):
    """
    Inserts CH4 stores for foreign countries into the database

    This function inserts the CH4 stores for foreign countries
    with the following steps:
      * Receive as argument the CH4 store capacities per foreign node
      * Clean the database
      * Add missing columns (scn_name, carrier and store_id)
      * Insert the table into the database

    Parameters
    ----------
    ch4_storage_capacities : pandas.DataFrame
        Methane gas storage capacities per country in MWh
    scn_name : str
        Name of the scenario

    Returns
    -------
    None
    """
    sources, targets = load_sources_and_targets("GasNeighbours")

    # Clean table
    db.execute_sql(
        f"""
        DELETE FROM {targets.tables['stores']}
        WHERE "carrier" = 'CH4'
        AND scn_name = '{scn_name}'
        AND bus IN (
            SELECT bus_id
            FROM {sources.tables['buses']}
            WHERE scn_name = '{scn_name}'
            AND country != 'DE'
            );
        """
    )
    # Add missing columns
    c = {"scn_name": scn_name, "carrier": "CH4"}
    ch4_storage_capacities = ch4_storage_capacities.assign(**c)

    ch4_storage_capacities["store_id"] = db.next_etrago_id(
        "store", len(ch4_storage_capacities)
    )
    ch4_storage_capacities.drop(
        ["Country"],
        axis=1,
        inplace=True,
    )

    ch4_storage_capacities = ch4_storage_capacities.reset_index(drop=True)
    # Insert data to db
    ch4_storage_capacities.to_sql(
        targets.get_table_name("stores").split(".")[-1],
        db.engine(),
        schema=targets.get_table_schema("stores"),
        index=False,
        if_exists="append",
    )


@lru_cache(maxsize=None)
def read_tyndp2024_yearly_outputs(year):
    """
    Read the sheet "Yearly Outputs" of the TYNDP 2024 electricity results

    Parameters
    ----------
    year : int
        2035, 2040 or 2050

    Returns
    -------
    pandas.DataFrame
        The sheet, read without header

    """
    with zipfile.ZipFile(
        ElectricalNeighbours.sources.files[f"tyndp_capacities_{year}"]
    ) as archive:
        member = _select_tyndp_capacity_member(archive)
        return pd.read_excel(
            io.BytesIO(archive.read(member)),
            sheet_name="Yearly Outputs",
            header=None,
            engine="pyxlsb",
        )


def tyndp2024_output(output_type, generator_ids, scn_name):
    """
    Value of the TYNDP 2024 electricity results per node for a scenario

    Parameters
    ----------
    output_type : str
        Block of the sheet, e.g. "Installed Capacities [MW]"
    generator_ids : list
        Technologies to sum up
    scn_name : str
        Name of the scenario

    Returns
    -------
    pandas.Series
        Value per TYNDP node, interpolated to the year of the scenario

    """
    year = get_scenario_year(scn_name)
    lo, hi = _bracket_tyndp_years(year)
    values = {}
    for anchor in (lo, hi):
        block = _extract_output_block(
            read_tyndp2024_yearly_outputs(anchor), output_type
        )
        block = block[block["Generator_ID"].isin(generator_ids)]
        values[anchor] = (
            pd.to_numeric(block["Value"], errors="coerce")
            .fillna(0)
            .groupby(block["Node/Line"])
            .sum()
        )
    index = values[lo].index.union(values[hi].index)
    value_lo = values[lo].reindex(index, fill_value=0)
    value_hi = values[hi].reindex(index, fill_value=0)
    weight = (year - lo) / (hi - lo)
    return (value_lo + (value_hi - value_lo) * weight).clip(lower=0)


def calc_global_power_to_h2_demand(scn_name):
    """
    Calculate H2 demand abroad

    Calculates global power demand abroad linked to H2 production.
    The data comes from the TYNDP 2024 electricity results, scenario
    'Distributed Energy', interpolated to the year of the scenario.

    Parameters
    ----------
    scn_name : str
        Name of the scenario

    Returns
    -------
    pandas.DataFrame
        Constant electricity demand ("p_set", MW) per foreign node

    """
    demand = tyndp2024_output(
        "Annual generation [GWh]", ["Electrolyser (load)"], scn_name
    )
    demand = demand[(demand.index.str[:2].isin(countries)) & (demand > 0)]

    return pd.DataFrame(
        {
            "Node/Line": demand.index,
            # GWh per year -> MW
            "p_set": demand.values * 1e3 / 8760,
        }
    )


def insert_power_to_h2_demand(global_power_to_h2_demand, scn_name):
    """
    Insert H2 demands into the database

    These loads are considered as constant and are attributed to AC
    buses.

    Parameters
    ----------
    global_power_to_h2_demand : pandas.DataFrame
        Global hourly power-to-h2 demand per foreign node
    scn_name : str
        Name of the scenario

    Returns
    -------
    None

    """
    sources, targets = load_sources_and_targets("GasNeighbours")

    map_buses = get_map_buses()

    carrier = "H2_for_industry"

    db.execute_sql(
        f"""
        DELETE FROM
        {targets.tables['loads']}
        WHERE bus IN (
            SELECT bus_id FROM
            {sources.tables['buses']}
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
        get_foreign_bus_id(scenario=scn_name)
        .loc[global_power_to_h2_demand.loc[:, "Node/Line"]]
        .values
    )

    # One load per bus (several TYNDP nodes can share a bus)
    global_power_to_h2_demand = global_power_to_h2_demand.groupby(
        "bus", as_index=False
    )["p_set"].sum()

    # Add missing columns
    c = {"scn_name": scn_name, "carrier": carrier}
    global_power_to_h2_demand = global_power_to_h2_demand.assign(**c)

    global_power_to_h2_demand["load_id"] = db.next_etrago_id(
        "load", len(global_power_to_h2_demand)
    )

    # Insert data to db
    global_power_to_h2_demand.to_sql(
        targets.get_table_name("loads").split(".")[-1],
        db.engine(),
        schema=targets.get_table_schema("loads"),
        index=False,
        if_exists="append",
    )


def calculate_ch4_grid_capacities(scn_name):
    """
    Calculates CH4 grid capacities for foreign countries based on TYNDP-data

    For the crossbordering gas pipeline with Germany, each global
    capacity (neighbouring country specific, TYNDP 2024 Annex C1) is
    uniformly distributed between all the links connecting Germany to this
    specific neighbouring country.

    Parameters
    ----------
    scn_name : str
        Name of the scenario

    Returns
    -------
    Neighbouring_pipe_capacities_list : pandas.DataFrame
        Table containing the CH4 grid capacity for each foreign
        country

    """
    sources, _ = load_sources_and_targets("GasNeighbours")

    # Column name only, the capacity is interpolated to the scenario year
    year = tyndp_year(scn_name)

    target_file = Path(sources.files["tyndp2024_annex_c1"])
    pipe_capacities_list = pd.read_excel(
        target_file, sheet_name="Annex C1_Cross-border", header=0
    )
    pipe_capacities_list.columns = [
        "From_Country",
        "To_Country",
        "LEVEL",
        2030,
        2040,
        2050,
        "unit",
    ]
    pipe_capacities_list["From_Country"] = pipe_capacities_list[
        "From_Country"
    ].ffill()
    pipe_capacities_list = pipe_capacities_list[
        pipe_capacities_list["LEVEL"] == TYNDP2024_GAS_LEVEL
    ]

    grid_countries = [
        country
        for country in TYNDP2024_ANNEX_C1_COUNTRY_TO_NODE
        if country != "Russia" or INCLUDE_RU
    ]

    pipe_capacities_list = pipe_capacities_list[
        pipe_capacities_list["To_Country"].isin(grid_countries)
        & pipe_capacities_list["From_Country"].isin(grid_countries)
    ]
    pipe_capacities_list[year] = _interpolate_tyndp2024_gas(
        {
            2030: pipe_capacities_list[2030],
            2040: pipe_capacities_list[2040],
            2050: pipe_capacities_list[2050],
        },
        scn_name,
    )
    pipe_capacities_list = pipe_capacities_list[
        pipe_capacities_list[year] != 0
    ]
    pipe_capacities_list["To_Country"] = pipe_capacities_list[
        "To_Country"
    ].map(TYNDP2024_ANNEX_C1_COUNTRY_TO_NODE)
    pipe_capacities_list["From_Country"] = pipe_capacities_list[
        "From_Country"
    ].map(TYNDP2024_ANNEX_C1_COUNTRY_TO_NODE)
    pipe_capacities_list["countrycombination"] = pipe_capacities_list[
        ["To_Country", "From_Country"]
    ].apply(
        lambda x: tuple(sorted([str(x.To_Country), str(x.From_Country)])),
        axis=1,
    )

    pipeline_strategies = {
        "To_Country": "first",
        "From_Country": "first",
        year: sum,
    }

    pipe_capacities_list = pipe_capacities_list.groupby(
        ["countrycombination"]
    ).agg(pipeline_strategies)

    # Add manually DK-SE and AT-CH pipes (not in Annex C1; SciGRID gas
    # data, same as before).
    pipe_capacities_list.loc["(DKE1, SE02)"] = ["DKE1", "SE02", 651]
    pipe_capacities_list.loc["(AT00, CH00)"] = ["AT00", "CH00", 651]

    # Conversion GWh/d to MWh/h
    pipe_capacities_list["p_nom"] = pipe_capacities_list[year] * (1000 / 24)

    # Border crossing CH4 pipelines between foreign countries
    Neighbouring_pipe_capacities_list = pipe_capacities_list[
        (pipe_capacities_list["To_Country"] != "DE")
        & (pipe_capacities_list["From_Country"] != "DE")
    ].reset_index()

    Neighbouring_pipe_capacities_list.loc[:, "bus0"] = (
        get_foreign_gas_bus_id(scn_name)
        .loc[Neighbouring_pipe_capacities_list.loc[:, "To_Country"]]
        .values
    )
    Neighbouring_pipe_capacities_list.loc[:, "bus1"] = (
        get_foreign_gas_bus_id(scn_name)
        .loc[Neighbouring_pipe_capacities_list.loc[:, "From_Country"]]
        .values
    )

    # Adjust columns
    Neighbouring_pipe_capacities_list = Neighbouring_pipe_capacities_list.drop(
        columns=[
            "To_Country",
            "From_Country",
            "countrycombination",
            year,
        ]
    )

    Neighbouring_pipe_capacities_list["link_id"] = db.next_etrago_id(
        "link", len(Neighbouring_pipe_capacities_list)
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

    schema_bus = sources.get_table_schema("buses")
    table_bus = sources.get_table_name("buses").split(".")[-1]
    for country_code in [e for e in countries if e not in ("GB", "SE", "UK")]:
        # Select cross-bording links
        cap_DE = db.select_dataframe(
            f"""SELECT link_id, bus0, bus1
                FROM {sources.tables['links']}
                    WHERE scn_name = '{scn_name}'
                    AND carrier = 'CH4'
                    AND (("bus0" IN (
                        SELECT bus_id FROM {schema_bus}.{table_bus}
                            WHERE country = 'DE'
                            AND carrier = 'CH4'
                            AND scn_name = '{scn_name}')
                        AND "bus1" IN (SELECT bus_id FROM {schema_bus}.{table_bus}
                            WHERE country = '{country_code}'
                            AND carrier = 'CH4'
                            AND scn_name = '{scn_name}')
                    )
                    OR ("bus0" IN (
                        SELECT bus_id FROM {schema_bus}.{table_bus}
                            WHERE country = '{country_code}'
                            AND carrier = 'CH4'
                            AND scn_name = '{scn_name}')
                        AND "bus1" IN (SELECT bus_id FROM {schema_bus}.{table_bus}
                            WHERE country = 'DE'
                            AND carrier = 'CH4'
                            AND scn_name = '{scn_name}'))
                    )
            ;"""
        )

        # No German CH4 link to this country, e.g. in the test mode with
        # a federal state as boundary
        if cap_DE.empty:
            print(
                f"{scn_name}: no CH4 link between DE and {country_code}, "
                "its cross-border capacity is not inserted."
            )
            continue

        cap_DE["p_nom"] = DE_pipe_capacities_list.at[
            country_code, "p_nom"
        ] / len(cap_DE.index)
        Neighbouring_pipe_capacities_list = pd.concat(
            [Neighbouring_pipe_capacities_list, cap_DE]
        )

    # Add topo, geom and length
    bus_geom = db.select_geodataframe(
        f"""SELECT bus_id, geom
        FROM grid.egon_etrago_bus
        WHERE scn_name = '{scn_name}'
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
    # Length in km, as for the other links and lines (was in degrees)
    Neighbouring_pipe_capacities_list["length"] = (
        gpd.GeoSeries(
            Neighbouring_pipe_capacities_list["topo"].to_list(), crs=4326
        )
        .to_crs(3035)
        .length
        / 1000
    ).to_numpy()

    # Remove useless columns
    Neighbouring_pipe_capacities_list = Neighbouring_pipe_capacities_list.drop(
        columns=[
            "coordinates_bus0",
            "coordinates_bus1",
        ]
    )

    # Add missing columns
    c = {"scn_name": scn_name, "carrier": "CH4", "p_min_pu": -1.0}
    Neighbouring_pipe_capacities_list = (
        Neighbouring_pipe_capacities_list.assign(**c)
    )

    Neighbouring_pipe_capacities_list = (
        Neighbouring_pipe_capacities_list.set_geometry("geom", crs=4326)
    )

    return Neighbouring_pipe_capacities_list


def tyndp_gas_generation(scn_name):
    """Insert data from TYNDP 2020 according to NEP 2021
    Scenario 'Distributed Energy'; linear interpolate between 2030 and 2040
    TO DO: revise methods

    Returns
    -------
    None
    """
    capacities = calc_capacities(scn_name)
    insert_generators(capacities, scn_name)

    ch4_storage_capacities = calc_ch4_storage_capacities(scn_name)
    insert_storage(ch4_storage_capacities, scn_name)


def tyndp_gas_demand(scn_name):
    """
    Insert gas demands abroad

    Insert CH4 and H2 demands abroad for the scenarios by
    executing the following steps:
      * CH4
          * Calculation of the CH4 demand profiles by executing the
            function :py:func:`import_ch4_demandTS`
          * Calculation of the global CH4 demands by executing the
            function :py:func:`calc_global_ch4_demand`
          * Insertion of the CH4 loads and their associated time
            series in the database by executing the function
            :py:func:`insert_ch4_demand`
      * H2
          * Calculation of the global power demand abroad linked
            to H2 production by executing the function
            :py:func:`calc_global_power_to_h2_demand`
          * Insertion of these loads in the database by executing the
            function :py:func:`insert_power_to_h2_demand`

    Returns
    -------
    None

    """
    normalized_ch4_demandTS = import_ch4_demandTS()
    global_ch4_demand = calc_global_ch4_demand(scn_name)
    insert_ch4_demand(global_ch4_demand, normalized_ch4_demandTS, scn_name)

    global_power_to_h2_demand = calc_global_power_to_h2_demand(scn_name)
    insert_power_to_h2_demand(global_power_to_h2_demand, scn_name)


def grid(scn_name):
    """
    Insert CH4 grid capacities for crossbordering pipelines

    This function inserts CH4 grid capacities into the database for
    crossbordering pipelines in the scenarios by executing the
    following steps:
      * Calculating the crossbordering CH4 pipeline capacities with the
        function :py:func:`calculate_ch4_grid_capacities`,
      * Inserting them into the database by executing the function
        :py:func:`insert_gas_grid_capacities <egon.data.datasets.gas_neighbours.gas_abroad.insert_gas_grid_capacities>`.

    Paramaneters
    ------------
    scn_name : str
        Name of the scenario

    Returns
    -------
    None
    """
    Neighbouring_pipe_capacities_list = calculate_ch4_grid_capacities(scn_name)
    insert_gas_grid_capacities(Neighbouring_pipe_capacities_list, scn_name)


def calculate_ocgt_capacities(scn_name):
    """
    Calculate gas turbine capacities abroad

    Calculate gas turbine capacities abroad based on the TYNDP 2024
    electricity results, scenario "Distributed Energy", interpolated to the
    year of the scenario.

    Parameters
    ----------
    scn_name : str
        Name of the scenario

    Returns
    -------
    df_ocgt: pandas.DataFrame
        Gas turbine capacities per foreign node

    """
    gas = [
        generator
        for generator, carrier in map_carriers_tyndp().items()
        if carrier == "gas"
    ]
    capacity = tyndp2024_output("Installed Capacities [MW]", gas, scn_name)
    capacity = capacity[
        capacity.index.str[:2].isin(countries) & (capacity > 0)
    ]

    map_buses = get_map_buses()
    nodes = [map_buses.get(node, node) for node in capacity.index]
    df_ocgt = pd.DataFrame(
        {
            "p_nom": capacity.values,
            "bus0": get_foreign_gas_bus_id(scn_name)[nodes].values,
            "bus1": get_foreign_bus_id(scenario=scn_name)[nodes].values,
        }
    )

    return df_ocgt.groupby(by=["bus0", "bus1"], as_index=False)["p_nom"].sum()


def insert_ocgt_abroad(scn_name):
    """Insert gas turbine capacities abroad in the database

    Parameters
    ----------
    scn_name : str
        Name of the scenario
    df_ocgt: pandas.DataFrame
        Gas turbine capacities per foreign node

    Returns
    -------
    None
    """

    carrier = "OCGT"

    # Connect to local database
    engine = db.engine()

    df_ocgt = calculate_ocgt_capacities(scn_name)

    df_ocgt["p_nom_extendable"] = False
    df_ocgt["carrier"] = carrier
    df_ocgt["scn_name"] = scn_name

    buses = tuple(
        db.select_dataframe(
            f"""SELECT bus_id FROM grid.egon_etrago_bus
            WHERE scn_name = '{scn_name}' AND country != 'DE';
        """
        )["bus_id"]
    )

    # Delete old entries
    db.execute_sql(
        f"""
        DELETE FROM grid.egon_etrago_link WHERE "carrier" = '{carrier}'
        AND scn_name = '{scn_name}'
        AND bus0 IN {buses} AND bus1 IN {buses};
        """
    )

    # read carrier information from scnario parameter data
    scn_params = get_sector_parameters("gas", scn_name)
    df_ocgt["efficiency"] = scn_params["efficiency"][carrier]
    # VOM per MWh of fuel = VOM per MWh_el x efficiency (as PyPSA-Eur)
    df_ocgt["marginal_cost"] = (
        scn_params["marginal_cost"][carrier]
        * scn_params["efficiency"][carrier]
    )

    # Adjust p_nom
    df_ocgt["p_nom"] = df_ocgt["p_nom"] / scn_params["efficiency"][carrier]

    # Select next id value
    df_ocgt["link_id"] = db.next_etrago_id("link", len(df_ocgt))

    # Insert data to db
    df_ocgt.to_sql(
        "egon_etrago_link",
        engine,
        schema="grid",
        index=False,
        if_exists="append",
    )
