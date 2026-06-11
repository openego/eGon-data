# -*- coding: utf-8 -*-
"""Integrate future data center demand into eTraGo."""

import geopandas as gpd
import numpy as np
import pandas as pd
import scipy.stats as stats

from geoalchemy2 import Geometry
from scipy.spatial import cKDTree
from scipy.spatial.distance import cdist
from shapely.geometry import LineString

from egon.data import config, db
from egon.data.datasets import Dataset, DatasetSources, DatasetTargets


TARGET_CAPACITY_MW = 19460   # 15680 (Szenario A), 19460 (Szenario B), 23240 (Szenario C)
MU = 3.297
SIGMA = 1.325
MAX_RZ_SIZE = 1000.0
GENERATOR_RUNS = 1000
RANDOM_SEED = 43

#radii
RADIUS_STROM = 5000
RADIUS_WAERME = 5000
RADIUS_IXP = 50000

# wheights 
W_STROM = 1 / 3
W_WAERME = 1 / 3
W_IXP = 1 / 3

# Monte-Carlo
MW_PER_HA = 8.6
ALPHA = 3.0
MC_RUNS = 100

#assumed parameter values for x, r,b (egon_line integration)
#X_PER_KM_110KV = 0.29163337906660414
#R_PER_KM_110KV = 0.0831925880581288
#B_PER_KM_110KV = 1.3120960297359745e-05
#S_NOM_DC_CONNECTION = 1040



def dist_score(dist, radius):
    """Calculate distance score used in allocation method."""
    return np.where(dist < radius, (radius - dist) / radius, 0)


def generate_data_center_sizes():
    """Generate scenario-dependent data center sizes"""
    
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
    sources = DataCentersEtrago.sources

    return gpd.read_file(
        sources.files["commercial_areas"],
    ).to_crs(epsg=25832)


def load_substations():
    sources = DataCentersEtrago.sources

    return gpd.read_file(
        sources.files["substations"],
    ).to_crs(epsg=25832)


def load_district_heating_areas():
    sources = DataCentersEtrago.sources

    return gpd.read_file(
        sources.files["district_heating_areas"],
    ).to_crs(epsg=25832)


def load_internet_nodes():
    """Load original internet node input."""
    sources = DataCentersEtrago.sources

    return gpd.read_file(
        sources.files["internet_nodes"],
    ).to_crs(epsg=25832)


def load_regional_factors():
    """Load original regional factor input."""
    sources = DataCentersEtrago.sources

    return gpd.read_file(
        sources.files["regional_factors"],
    ).to_crs(epsg=25832)


def create_data_center_allocation():
    """Run data center allocation workflow and return rz_punkte."""
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
        rz_df["Leistung_MW"]
        .astype(float)
        .sort_values(ascending=False)
        .values
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

    return rz_punkte


def get_existing_110kv_ac_buses(scenario):
    """Get existing 110 kV AC buses from eTraGo."""
    sources = DataCentersEtrago.sources

    return db.select_geodataframe(
        f"""
        SELECT bus_id, v_nom, carrier, x, y, geom
        FROM {sources.tables["buses"]}
        WHERE scn_name = '{scenario}'
        AND carrier = 'AC'
        AND v_nom = 110
        AND country = 'DE'
        """,
        geom_col="geom",
        epsg=4326,
    )

####################
#integration part inital draft

def assign_nearest_110kv_bus(data_centers, existing_buses):
    """Assign nearest existing 110 kV AC bus to every data center."""
    data_centers_projected = data_centers.to_crs(epsg=3035)
    existing_buses_projected = existing_buses.to_crs(epsg=3035)

    bus_coords = np.array(
        [(geom.x, geom.y) for geom in existing_buses_projected.geometry]
    )
    dc_coords = np.array(
        [(geom.x, geom.y) for geom in data_centers_projected.geometry]
    )

    tree = cKDTree(bus_coords)
    distances, nearest_idx = tree.query(dc_coords, k=1)

    nearest_buses = existing_buses_projected.iloc[nearest_idx].reset_index(
        drop=True
    )

    data_centers_projected["nearest_bus_id"] = nearest_buses["bus_id"].values
    data_centers_projected["nearest_bus_geom"] = nearest_buses.geometry.values
    data_centers_projected["connection_length_km"] = distances / 1000

    return data_centers_projected.to_crs(epsg=4326)


def create_data_center_buses(data_centers, scenario):
    """Create new 110 kV AC buses for data centers."""
    dc_buses = gpd.GeoDataFrame(
        {
            "scn_name": scenario,
            "bus_id": db.next_etrago_id("bus", len(data_centers)),
            "v_nom": 110,
            "type": "data_center",
            "carrier": "AC",
            "geom": data_centers.geometry.values,
            "country": "DE",
        },
        geometry="geom",
        crs="EPSG:4326",
    )

    dc_buses["x"] = dc_buses.geom.x
    dc_buses["y"] = dc_buses.geom.y

    data_centers["dc_bus_id"] = dc_buses["bus_id"].values

    return dc_buses, data_centers


def create_data_center_lines(data_centers, scenario):
    """Create AC connection lines from data center buses to 110 kV buses."""
    data_centers_projected = data_centers.to_crs(epsg=3035)

    lines = []

    for _, row in data_centers_projected.iterrows():
        topo = LineString([row.geometry, row.nearest_bus_geom])
        length_km = topo.length / 1000

        lines.append(
            {
                "scn_name": scenario,
                "bus0": row.dc_bus_id,
                "bus1": row.nearest_bus_id,
                "type": "data_center_connection",
                "carrier": "AC",
                "v_nom": 110,
                "length": length_km,
                "x": X_PER_KM_110KV * length_km,
                "r": R_PER_KM_110KV * length_km,
                "b": B_PER_KM_110KV * length_km,
                "s_nom": S_NOM_DC_CONNECTION,
                "s_nom_min": S_NOM_DC_CONNECTION,
                "s_nom_extendable": False,
                "num_parallel": 1,
                "topo": topo,
            }
        )

    dc_lines = gpd.GeoDataFrame(lines, geometry="topo", crs="EPSG:3035")
    dc_lines = dc_lines.to_crs(epsg=4326)
    dc_lines["line_id"] = db.next_etrago_id("line", len(dc_lines))

    return dc_lines


def create_data_center_loads(data_centers, scenario):
    """Create electricity loads for the new data center buses."""
    return pd.DataFrame(
        {
            "scn_name": scenario,
            "load_id": db.next_etrago_id("load", len(data_centers)),
            "bus": data_centers["dc_bus_id"].values,
            "type": "data_center",
            "carrier": "AC",
            "p_set": data_centers["allocated_mw"].values,
            "q_set": 0,
            "sign": -1,
        }
    )


def insert_data_centers(scenario):
    """Insert data center buses, lines and loads into eTraGo tables."""
    targets = DataCentersEtrago.targets

    data_centers = create_data_center_allocation()
    existing_buses = get_existing_110kv_ac_buses(scenario)
    data_centers = assign_nearest_110kv_bus(data_centers, existing_buses)

    dc_buses, data_centers = create_data_center_buses(data_centers, scenario)
    #dc_lines = create_data_center_lines(data_centers, scenario)
    dc_loads = create_data_center_loads(data_centers, scenario)

    dc_buses.to_postgis(
        targets.get_table_name("buses"),
        schema=targets.get_table_schema("buses"),
        if_exists="append",
        con=db.engine(),
        index=False,
        dtype={"geom": Geometry()},
    )

    #dc_lines.to_postgis(
     #   targets.get_table_name("lines"),
     #   schema=targets.get_table_schema("lines"),
     #   if_exists="append",
      #  con=db.engine(),
      #  index=False,
      #  dtype={"topo": Geometry()},
   # )

    dc_loads.to_sql(
        targets.get_table_name("loads"),
        schema=targets.get_table_schema("loads"),
        if_exists="append",
        con=db.engine(),
        index=False,
    )


def insert_data_centers_for_scenarios():
    """Insert data centers for configured scenarios using Scenario B assumption."""
    for scenario in config.settings()["egon-data"]["--scenarios"]:
        if scenario == "eGon2035":
            insert_data_centers(scenario)


class DataCentersEtrago(Dataset):
    """Integrate future data center demand into eTraGo."""

    name: str = "DataCentersEtrago"
    version: str = "0.0.1"

    sources = DatasetSources(
        tables={
            "buses": "grid.egon_etrago_bus",
            "lines": "grid.egon_etrago_line",
            "loads": "grid.egon_etrago_load",
        },
        files={
            "commercial_areas": "Gewerbeflaechen.gpkg", #check openstreetmap.osm_landuse
            "district_heating_areas": "Wärmenetze.gpkg", #check demand.egon_district_heating_areas
            "internet_nodes": "Internetknoten.gpkg",
            "regional_factors": "Regionalisierungsfaktoren.gpkg",
            "substations": "Umspannwerke.gpkg", #check grid.egon_hmv_substation
        },
    )

    targets = DatasetTargets(
        tables={
            "buses": "grid.egon_etrago_bus",
            "lines": "grid.egon_etrago_line",
            "loads": "grid.egon_etrago_load",
        },
    )

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(insert_data_centers_for_scenarios,),
        )

