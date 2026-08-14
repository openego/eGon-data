# -*- coding: utf-8 -*-
"""Allocate future data center capacities and integrate data center buses,
loads, connection lines and waste-heat links into the database."""

import geopandas as gpd
import numpy as np
import pandas as pd
import scipy.stats as stats

from geoalchemy2 import Geometry
from scipy.spatial.distance import cdist
from shapely.geometry import LineString, MultiLineString

from egon.data import config, db
from egon.data.datasets import Dataset, DatasetSources, DatasetTargets

TARGET_CAPACITY_MW = (
    19460  # 15680 (Szenario A), 19460 (Szenario B), 23240 (Szenario C)
)
MU = 3.297
SIGMA = 1.325
MAX_RZ_SIZE = 1000.0
GENERATOR_RUNS = 1000
RANDOM_SEED = 43

# radii
RADIUS_STROM = 5000
RADIUS_WAERME = 5000
RADIUS_IXP = 50000

# Weights
W_STROM = 1 / 3
W_WAERME = 1 / 3
W_IXP = 1 / 3

# Monte-Carlo
MW_PER_HA = 8.6
ALPHA = 3.0
MC_RUNS = 100

# Electrical parameters for AC data center connection lines
# Values taken from scenario_parameters/parameters.py
S_NOM_DATA_CENTER_CONNECTION_110KV = 260
R_PER_KM_110KV = 0.109
L_PER_KM_110KV = 1.2e-3

S_NOM_DATA_CENTER_CONNECTION_380KV = 1790
R_PER_KM_380KV = 0.028
L_PER_KM_380KV = 0.8e-3

# Data center waste heat
REUSABLE_HEAT_FACTOR = 0.20


def dist_score(dist, radius):
    """Calculate distance score used in allocation method."""
    return np.where(dist < radius, (radius - dist) / radius, 0)


def identify_voltage_level(df):
    """Identify voltage level based on peak load."""
    df["voltage_level"] = np.nan

    df.loc[df["peak_load"] <= 0.1, "voltage_level"] = 7
    df.loc[df["peak_load"] > 0.1, "voltage_level"] = 6
    df.loc[df["peak_load"] > 0.2, "voltage_level"] = 5
    df.loc[df["peak_load"] > 5.5, "voltage_level"] = 4
    df.loc[df["peak_load"] > 20, "voltage_level"] = 3
    df.loc[df["peak_load"] > 120, "voltage_level"] = 1

    return df


def generate_data_center_sizes():
    """Generate scenario-dependent data center sizes"""
    # Generate a representative distribution of individual data center capacities
    # whose total capacity matches the scenario target.

    np.random.seed(RANDOM_SEED)
    scale_param = np.exp(MU)

    massive_pool = []

    for _ in range(GENERATOR_RUNS):
        current_sum = 0.0

        while current_sum < TARGET_CAPACITY_MW:
            while True:
                rz_size = stats.lognorm.rvs(s=SIGMA, scale=scale_param)

                if rz_size <= MAX_RZ_SIZE:
                    break

            if current_sum + rz_size > TARGET_CAPACITY_MW:
                rz_size = TARGET_CAPACITY_MW - current_sum

            massive_pool.append(rz_size)
            current_sum += rz_size

    massive_pool = np.array(massive_pool)
    massive_pool.sort()

    ideal_scenario = massive_pool[GENERATOR_RUNS // 2 :: GENERATOR_RUNS].copy()
    ideal_scenario *= TARGET_CAPACITY_MW / np.sum(ideal_scenario)

    return pd.DataFrame(
        {
            "RZ_ID": np.arange(1, len(ideal_scenario) + 1),
            "Leistung_MW": ideal_scenario,
        }
    )


def load_commercial_areas():
    sources = DataCenters.sources

    gdf = db.select_geodataframe(
        f"""
        SELECT geom
        FROM {sources.tables["commercial_areas"]}
        WHERE sector_name IN ('industrial', 'retail')
        """,
        geom_col="geom",
        epsg=3035,
    ).to_crs(epsg=25832)

    return gdf.rename_geometry("geometry")


def load_substations():
    sources = DataCenters.sources

    gdf = db.select_geodataframe(
        f"""
        SELECT point
        FROM {sources.tables["substations"]}
        """,
        geom_col="point",
        epsg=4326,
    ).to_crs(epsg=25832)

    return gdf.rename_geometry("geometry")


def load_district_heating_areas():
    sources = DataCenters.sources

    gdf = db.select_geodataframe(
        f"""
        SELECT geom_polygon, residential_and_service_demand
        FROM {sources.tables["district_heating_areas"]}
        """,
        geom_col="geom_polygon",
        epsg=3035,
    ).to_crs(epsg=25832)

    return gdf.rename_geometry("geometry")


def load_internet_nodes():
    """Load original internet node input."""
    sources = DataCenters.sources

    return gpd.read_file(
        sources.files["internet_nodes"],
    ).to_crs(epsg=25832)


def load_regional_factors():
    """Load original regional factor input."""
    sources = DataCenters.sources

    return gpd.read_file(
        sources.files["regional_factors"],
    ).to_crs(epsg=25832)


def create_data_center_allocation():
    """Run data center allocation workflow and return rz_punkte."""
    # Allocate generated data center capacities to suitable commercial areas
    # based on electricity, district-heating, and internet-location criteria.
    
    rz_df = generate_data_center_sizes()
    gewerbe_raw = load_commercial_areas()
    strom_raw = load_substations()
    waerme_raw = load_district_heating_areas()
    ixp_raw = load_internet_nodes()
    regio_raw = load_regional_factors()

    def convert_percent(val):
        if pd.isna(val) or val == "-":
            return None
        try:
            s = str(val).replace("%", "").replace(",", ".").strip()
            return float(s) / 100
        except:
            return None

    regio_ref = regio_raw.copy()
    regio_ref["Faktor"] = regio_ref["Faktor"].apply(convert_percent)
    regio_ref = regio_ref.dropna(subset=["Faktor"])

    strom_final = (
        gpd.sjoin_nearest(
            strom_raw.copy(),
            regio_ref[["geometry", "Faktor"]],
            how="left",
            distance_col="dist_to_regio",
        )
        .drop(columns=["index_right"])
        .drop_duplicates(subset=["geometry"])
    )

    gewerbe_final = gewerbe_raw[["geometry"]].copy()
    gewerbe_final["area_ha"] = gewerbe_final.geometry.area / 10000

    for sub, cols, dist_col in [
        (strom_final, ["geometry", "Faktor"], "dist_strom"),
        (
            waerme_raw,
            ["geometry", "residential_and_service_demand"],
            "dist_waerme",
        ),
        (ixp_raw, ["geometry", "networks_int"], "dist_ixp"),
    ]:
        gewerbe_final = (
            gpd.sjoin_nearest(
                gewerbe_final,
                sub[cols],
                how="left",
                distance_col=dist_col,
            )
            .drop(columns=["index_right"])
            .drop_duplicates(subset=["geometry"])
        )

    gewerbe_scored = gewerbe_final.copy()

    gewerbe_scored["score_dist_strom"] = dist_score(
        gewerbe_scored["dist_strom"], RADIUS_STROM
    )
    gewerbe_scored["score_regio_strom"] = 1.2 - gewerbe_scored["Faktor"]

    gewerbe_scored["score_dist_waerme"] = dist_score(
        gewerbe_scored["dist_waerme"], RADIUS_WAERME
    )
    max_waerme = waerme_raw["residential_and_service_demand"].quantile(0.95)
    gewerbe_scored["score_cap_waerme"] = np.clip(
        gewerbe_scored["residential_and_service_demand"] / max_waerme,
        0,
        1,
    )

    gewerbe_scored["score_dist_ixp"] = dist_score(
        gewerbe_scored["dist_ixp"], RADIUS_IXP
    )
    max_ixp = ixp_raw["networks_int"].quantile(0.95)
    gewerbe_scored["score_cap_ixp"] = np.clip(
        gewerbe_scored["networks_int"] / max_ixp,
        0,
        1,
    )

    gewerbe_scored["cat_score_strom"] = (
        0.5 * gewerbe_scored["score_dist_strom"]
        + 0.5 * gewerbe_scored["score_regio_strom"]
    ) * (gewerbe_scored["score_dist_strom"] > 0)

    gewerbe_scored["cat_score_waerme"] = (
        0.5 * gewerbe_scored["score_dist_waerme"]
        + 0.5 * gewerbe_scored["score_cap_waerme"]
    ) * (gewerbe_scored["score_dist_waerme"] > 0)

    gewerbe_scored["cat_score_ixp"] = (
        0.5 * gewerbe_scored["score_dist_ixp"]
        + 0.5 * gewerbe_scored["score_cap_ixp"]
    ) * (gewerbe_scored["score_dist_ixp"] > 0)

    gewerbe_scored["total_score"] = (
        W_STROM * gewerbe_scored["cat_score_strom"]
        + W_WAERME * gewerbe_scored["cat_score_waerme"]
        + W_IXP * gewerbe_scored["cat_score_ixp"]
    )

    rz_sizes_mw = (
        rz_df["Leistung_MW"].astype(float).sort_values(ascending=False).values
    )

    num_areas = len(gewerbe_scored)
    base_areas = gewerbe_scored["area_ha"].values.copy()
    base_scores = np.clip(gewerbe_scored["total_score"].values.copy(), 0, None)

    history_mw = np.zeros((MC_RUNS, num_areas))
    history_count = np.zeros((MC_RUNS, num_areas))

    np.random.seed(RANDOM_SEED)

    for run in range(MC_RUNS):
        current_capacity = base_areas * MW_PER_HA

        for rz_mw in rz_sizes_mw:
            eligible_mask = current_capacity >= rz_mw

            if not eligible_mask.any():
                continue

            weights = base_scores[eligible_mask] ** ALPHA
            weights_sum = weights.sum()

            if weights_sum > 0:
                weights = weights / weights_sum
            else:
                weights = np.ones(weights.shape) / len(weights)

            chosen_idx = np.random.choice(
                np.where(eligible_mask)[0],
                p=weights,
            )

            current_capacity[chosen_idx] -= rz_mw
            history_mw[run, chosen_idx] += rz_mw
            history_count[run, chosen_idx] += 1

    distances = cdist(history_mw, history_mw, metric="cityblock")
    medoid_idx = np.argmin(distances.sum(axis=1))

    gewerbe_scored["allocated_mw"] = history_mw[medoid_idx]
    gewerbe_scored["rz_count"] = history_count[medoid_idx]

    rz_punkte = gewerbe_scored[gewerbe_scored["allocated_mw"] > 0].copy()
    rz_punkte["geometry"] = rz_punkte["geometry"].centroid
    # Classify each allocated data center by peak load and assign the
    # corresponding eTraGo connection voltage. Data centers up to 120 MW are
    #  represented at 110 kV; data centers above 120 MW are represented at 380 kV.
    rz_punkte["peak_load"] = rz_punkte["allocated_mw"]
    rz_punkte = identify_voltage_level(rz_punkte)
    rz_punkte = rz_punkte.drop(columns=["peak_load"])
    rz_punkte["v_nom"] = np.where(rz_punkte["voltage_level"] == 1, 380, 110)

    return rz_punkte


####################
# Electrical integration part 
def get_existing_ac_buses(scenario):
    """Get existing 110 kV and 380 kV AC buses from eTraGo."""
    sources = DataCenters.sources

    gdf = db.select_geodataframe(
        f"""
        SELECT bus_id, v_nom, carrier, x, y, geom
        FROM {sources.tables["buses"]}
        WHERE scn_name = '{scenario}'
        AND carrier = 'AC'
        AND v_nom IN (110, 380)
        AND country = 'DE'
        """,
        geom_col="geom",
        epsg=4326,
    )

    return gdf.rename_geometry("geometry")


def get_existing_central_heat_buses(scenario):
    """Get existing central heat buses from eTraGo."""
    sources = DataCenters.sources

    gdf = db.select_geodataframe(
        f"""
        SELECT bus_id, carrier, x, y, geom
        FROM {sources.tables["buses"]}
        WHERE scn_name = '{scenario}'
        AND carrier = 'central_heat'
        """,
        geom_col="geom",
        epsg=4326,
    )

    return gdf.rename_geometry("geometry")


def assign_nearest_bus(data_centers, existing_buses):
    """Assign nearest existing AC bus with matching nominal voltage."""
    data_centers_projected = data_centers.to_crs(epsg=3035)
    existing_buses_projected = existing_buses.to_crs(epsg=3035)

    assigned_data_centers = []

    for v_nom in [110, 380]:
        assigned_data_centers.append(
            gpd.sjoin_nearest(
                data_centers_projected[
                    data_centers_projected["v_nom"] == v_nom
                ],
                existing_buses_projected[
                    existing_buses_projected["v_nom"] == v_nom
                ][["bus_id", "geometry"]].rename(
                    columns={"bus_id": "nearest_bus_id"}
                ),
                how="left",
                distance_col="connection_length_km",
            )
        )

    data_centers_projected = pd.concat(assigned_data_centers)
    data_centers_projected["connection_length_km"] = (
        data_centers_projected["connection_length_km"] / 1000
    )
    data_centers_projected["nearest_bus_id"] = data_centers_projected[
        "nearest_bus_id"
    ].astype(int)
    data_centers_projected["nearest_bus_geom"] = (
        existing_buses_projected.set_index("bus_id")
        .geometry[data_centers_projected["nearest_bus_id"]]
        .values
    )

    data_centers_projected = data_centers_projected.drop(
        columns=["index_right"]
    )

    return data_centers_projected.to_crs(epsg=4326)


def assign_nearest_heat_bus(data_centers, central_heat_buses):
    """Assign nearest existing central heat bus to each data center."""
    data_centers_projected = data_centers.to_crs(epsg=3035)
    central_heat_buses_projected = central_heat_buses.to_crs(epsg=3035)

    data_centers_projected = gpd.sjoin_nearest(
        data_centers_projected,
        central_heat_buses_projected[["bus_id", "geometry"]].rename(
            columns={"bus_id": "central_heat_bus_id"}
        ),
        how="left",
        distance_col="distance_to_heat_bus_km",
    )

    data_centers_projected["distance_to_heat_bus_km"] = (
        data_centers_projected["distance_to_heat_bus_km"] / 1000
    )
    data_centers_projected["central_heat_bus_id"] = data_centers_projected[
        "central_heat_bus_id"
    ].astype(int)
    
    # Get geometry of the assigned heat bus for the waste-heat link.
    central_heat_bus_geom = (
        central_heat_buses.set_index("bus_id")
        .geometry[data_centers_projected["central_heat_bus_id"]]
        .values
    )

    data_centers_projected = data_centers_projected.drop(
        columns=["index_right"]
    )

    data_centers_projected = data_centers_projected.to_crs(epsg=4326)

    data_centers_projected["central_heat_bus_geom"] = central_heat_bus_geom

    return data_centers_projected

def create_data_center_buses(data_centers, scenario):
    """Create new AC buses for data centers."""
    data_center_buses = gpd.GeoDataFrame(
        {
            "scn_name": scenario,
            "bus_id": db.next_etrago_id("bus", len(data_centers)),
            "v_nom": data_centers["v_nom"].values,
            "type": "data_center",
            "carrier": "AC",
            "geom": data_centers.geometry.values,
            "country": "DE",
        },
        geometry="geom",
        crs="EPSG:4326",
    )

    data_center_buses["x"] = data_center_buses.geom.x
    data_center_buses["y"] = data_center_buses.geom.y

    data_centers["data_center_bus_id"] = data_center_buses["bus_id"].values

    return data_center_buses, data_centers


def create_data_center_lines(data_centers, scenario):
    """Create AC connection lines from data center buses to existing AC buses."""
    data_centers_projected = data_centers.to_crs(epsg=3035)

    lines = []

    for _, row in data_centers_projected.iterrows():
        topo = LineString([row.geometry, row.nearest_bus_geom])
        length_km = topo.length / 1000

        lines.append(
            {
                "scn_name": scenario,
                "bus0": row.data_center_bus_id,
                "bus1": row.nearest_bus_id,
                "type": "data_center_connection",
                "carrier": "AC",
                "v_nom": row.v_nom,
                "length": length_km,
                # Reactance x is calculated from the inductance L given in
                # scenario_parameters/parameters.py:
                # x = 2 * pi * f * L * length, with f = 50 Hz and L in H/km.
                "x": 2
                * np.pi
                * 50
                * (L_PER_KM_380KV if row.v_nom == 380 else L_PER_KM_110KV)
                * length_km,
                # Resistance r is calculated from R given in
                # scenario_parameters/parameters.py:
                # r = R_per_km * length.
                "r": (R_PER_KM_380KV if row.v_nom == 380 else R_PER_KM_110KV)
                * length_km,
                # b is not set here because scenario_parameters does not
                # provide capacitance/susceptance values. The eTraGo line
                # table defines b with server_default="0.".
                # s_nom defines the nominal apparent power capacity of the
                # connection line. We use the standard/median capacities from
                # scenario_parameters.py and existing eTraGo lines:
                # 110 kV: median = 260 MVA, max = 1040 MVA
                # 380 kV: median = 1790 MVA, max ≈ 7820 MVA
                # The maximum values are not used because they likely represent
                # special high-capacity or parallel-line cases, while the median
                # values are the normal line capacities and are already sufficient
                # for the modeled data center loads.
                "s_nom": (
                    S_NOM_DATA_CENTER_CONNECTION_380KV
                    if row.v_nom == 380
                    else S_NOM_DATA_CENTER_CONNECTION_110KV
                ),
                "s_nom_min": (
                    S_NOM_DATA_CENTER_CONNECTION_380KV
                    if row.v_nom == 380
                    else S_NOM_DATA_CENTER_CONNECTION_110KV
                ),
                "s_nom_extendable": False,
                "num_parallel": 1,
                "topo": topo,
            }
        )

    data_center_lines = gpd.GeoDataFrame(lines, geometry="topo", crs="EPSG:3035")
    data_center_lines = data_center_lines.to_crs(epsg=4326)
    data_center_lines["line_id"] = db.next_etrago_id("line", len(data_center_lines))

    return data_center_lines


def create_data_center_loads(data_centers, scenario):
    """Create electricity loads for the new data center buses."""
    return pd.DataFrame(
        {
            "scn_name": scenario,
            "load_id": db.next_etrago_id("load", len(data_centers)),
            "bus": data_centers["data_center_bus_id"].values,
            "type": "data_center",
            "carrier": "AC",
            "p_set": data_centers["allocated_mw"].values,
            "q_set": None,
            "sign": -1,
        }
    )

def create_data_center_heat_links(data_centers, scenario):
    """Create waste-heat links from data center AC buses to central heat buses."""

    links = []

    for _, row in data_centers.iterrows():
        topo = LineString(
            [
                (row.geometry.x, row.geometry.y),
                (row.central_heat_bus_geom.x, row.central_heat_bus_geom.y),
            ]
        )

        links.append(
            {
                "scn_name": scenario,
                "link_id": db.next_etrago_id("link"),
                "bus0": row.data_center_bus_id,
                "bus1": row.central_heat_bus_id,
                "carrier": "data_center_waste_heat",
                "efficiency": 1,
                # Assume 20% of the data center electrical capacity is reusable waste heat.
                # The resulting heat-link capacity is fixed and not optimized by eTraGo.
                "p_nom": row.allocated_mw * REUSABLE_HEAT_FACTOR,
                "p_nom_extendable": False,
                "geom": MultiLineString([topo]),
                "topo": topo,
            }
        )

    return gpd.GeoDataFrame(links, geometry="geom", crs="EPSG:4326")


def delete_existing_data_centers(scenario):
    """Delete previously inserted data center components before rerun."""
    targets = DataCenters.targets

    db.execute_sql(f"""
        DELETE FROM {targets.tables["links"]}
        WHERE scn_name = '{scenario}'
        AND carrier = 'data_center_waste_heat';
        
        DELETE FROM {targets.tables["loads"]}
        WHERE scn_name = '{scenario}'
        AND type = 'data_center';

        DELETE FROM {targets.tables["lines"]}
        WHERE scn_name = '{scenario}'
        AND type = 'data_center_connection';

        DELETE FROM {targets.tables["buses"]}
        WHERE scn_name = '{scenario}'
        AND type = 'data_center';
        """)


def insert_data_centers(scenario):
    """Insert data center buses, lines, loads and heat links into the database."""
    targets = DataCenters.targets
    delete_existing_data_centers(scenario)
    data_centers = create_data_center_allocation()
    existing_buses = get_existing_ac_buses(scenario)
    central_heat_buses = get_existing_central_heat_buses(scenario)
    data_centers = assign_nearest_bus(data_centers, existing_buses)

    data_center_buses, data_centers = create_data_center_buses(data_centers, scenario)
    data_centers = assign_nearest_heat_bus(data_centers, central_heat_buses)
    data_center_lines = create_data_center_lines(data_centers, scenario)
    data_center_loads = create_data_center_loads(data_centers, scenario)
    data_center_heat_links = create_data_center_heat_links(
    data_centers, scenario)

    data_center_buses.to_postgis(
        targets.get_table_name("buses"),
        schema=targets.get_table_schema("buses"),
        if_exists="append",
        con=db.engine(),
        index=False,
        dtype={"geom": Geometry()},
    )

    data_center_lines.to_postgis(
        targets.get_table_name("lines"),
        schema=targets.get_table_schema("lines"),
        if_exists="append",
        con=db.engine(),
        index=False,
        dtype={"topo": Geometry()},
    )
    data_center_loads.to_sql(
        targets.get_table_name("loads"),
        schema=targets.get_table_schema("loads"),
        if_exists="append",
        con=db.engine(),
        index=False,
    )
    
    data_center_heat_links.to_postgis(
        targets.get_table_name("links"),
        schema=targets.get_table_schema("links"),
        if_exists="append",
        con=db.engine(),
        index=False,
    )


def insert_data_centers_for_scenarios():
    """Insert data centers for configured scenarios using Scenario B assumption."""
    global TARGET_CAPACITY_MW
    

    if (
        config.settings()["egon-data"]["--dataset-boundary"]
        == "Schleswig-Holstein"
    ):
        TARGET_CAPACITY_MW = TARGET_CAPACITY_MW / 16

    for scenario in config.settings()["egon-data"]["--scenarios"]:
        if scenario == "eGon2035":
            insert_data_centers(scenario)


class DataCenters(Dataset):
    """Integrate future data center demand"""

    name: str = "DataCenters"
    version: str = "0.0.1"

    sources = DatasetSources(
        tables={
            "buses": "grid.egon_etrago_bus",
            "lines": "grid.egon_etrago_line",
            "loads": "grid.egon_etrago_load",
            "commercial_areas": "openstreetmap.osm_landuse",
            "district_heating_areas": "demand.egon_district_heating_areas",
            "substations": "grid.egon_hvmv_substation",
        },
        files={
            "internet_nodes": (
                "data_bundle_egon_data/data_centers/Internetknoten.gpkg"
            ),
            "regional_factors": (
                "data_bundle_egon_data/data_centers/Regionalisierungsfaktoren.gpkg"
            ),
            # Original input containing pre-defined regional Faktor values.
            # The factor is used in the electricity-location score through:
            # score_regio_strom = 1.2 - Faktor.
        },
    )

    targets = DatasetTargets(
        tables={
            "buses": "grid.egon_etrago_bus",
            "lines": "grid.egon_etrago_line",
            "loads": "grid.egon_etrago_load",
            "links": "grid.egon_etrago_link",
        },
    )

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(insert_data_centers_for_scenarios,),
        )
