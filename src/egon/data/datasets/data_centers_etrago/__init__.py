# -*- coding: utf-8 -*-
"""Allocate future data center capacities and integrate data center buses,
loads, connection lines and reusable waste heat into the database."""

import geopandas as gpd
import numpy as np
import pandas as pd
import scipy.stats as stats

from geoalchemy2 import Geometry
from scipy.spatial.distance import cdist
from shapely.geometry import LineString

from egon.data import config, db
from egon.data.datasets import Dataset, DatasetSources, DatasetTargets
from egon.data.datasets.industry.temporal import identify_voltage_level
from egon.data.datasets.scenario_parameters import (
    align_weekdays,
    get_sector_parameters,
)

# Data center target capacities for Scenarios A, B and C are derived from
# the data center electricity demand ("davon aus neuen Rechenzentren") in the
# NEP 2037/2045 draft (p. 32) using 5,000 full-load hours from the
# Szenariorahmen NEP 2037/2045 draft (p. 45).
# The NEP reports the same demand for 2037 and 2045 in every scenario, because
# the set of considered data center projects does not differ between the two
# target years ("Aufgrund der frühen Inbetriebnahmen unterscheidet sich die
# Menge der berücksichtigten Projekte zwischen 2037 und 2045 nicht") and no
# build-out beyond the registered projects is assumed.
# This results in:
# Scenario A: 78.4 TWh / 5,000 h = 15.68 GW (2037 and 2045)
# Scenario B: 97.3 TWh / 5,000 h = 19.46 GW (2037 and 2045)
# Scenario C: 116.2 TWh / 5,000 h = 23.24 GW (2037 and 2045)
# Sources:
# https://www.netzentwicklungsplan.de/sites/default/files/2025-12/NEP_2037_2045_V2025_1_Entwurf.pdf
# https://www.netzentwicklungsplan.de/sites/default/files/2024-07/Szenariorahmenentwurf_NEP2037_2025_1.pdf
# The reGon scenarios follow NEP Scenario C, cf. the scenario descriptions in
# scenario_parameters/__init__.py.
TARGET_CAPACITY_MW = {
    "reGon2037": 23240,  # Szenario C 2037
    "reGon2045": 23240,  # Szenario C 2045
}
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

# The regional factor scores the cost of a grid connection through
# score_regio_strom = REGIO_SCORE_OFFSET - Faktor. Faktor runs from 0.2 to 1.0,
# so the offset of 1.2 is a deliberate choice rather than a normalisation: it
# leaves the most expensive regions a score of 0.2 instead of zero. Half of the
# reference points sit at Faktor 0.8 and another tenth at 1.0, so normalising
# the score to 0-1 would write off most of Germany. The consequence is that
# this sub-score runs from 0.2 to 1.0 while the five others run from 0 to 1,
# which gives the electricity category a small floor the other categories do
# not have.
REGIO_SCORE_OFFSET = 1.2

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
# Assume 20% reusable waste heat based on EnEfG § 11(2):
# data centers operating from 1 July 2028 must provide at least 20%
# planned reused energy.
# Source: https://www.gesetze-im-internet.de/enefg/__11.html
REUSABLE_HEAT_FACTOR = 0.20

# Waste heat can only be reused where a district heating network is close
# enough to connect to. The same radius is used to score district-heating
# proximity during the allocation (RADIUS_WAERME) and matches the maximum
# connection distance assumed for electrolyser waste heat in
# hydrogen_etrago/power_to_h2.py (max_buffer_heat).
MAX_HEAT_CONNECTION_DISTANCE_M = RADIUS_WAERME

# UKPN data center load-profile parameters
# Profiles are filtered by annual utilisation and converted to hourly
# resolution load time series. The annual demand target follows from the
# 5,000 full-load hours given in the Szenariorahmen NEP 2037/2045 draft
# (p. 45).
FULL_LOAD_HOURS = 5000
# Calendar year the UKPN profiles were measured in.
UKPN_PROFILE_YEAR = 2025
# Bounds on the annual mean utilisation of a site.
MIN_UTILISATION = 0.30
MAX_UTILISATION = 0.90
# A site cannot draw more than its own capacity, so a utilisation ratio above
# one means the recorded capacity does not match the metered demand. Such a
# site is dropped rather than clipped, because the error affects the whole
# shape and not only the hours above the limit.
MAX_PEAK_UTILISATION = 1.0
LOAD_PROFILE_RANDOM_SEED = 43


def dist_score(dist, radius):
    """Calculate distance score used in allocation method."""
    return np.where(dist < radius, (radius - dist) / radius, 0)


def get_target_capacity(scenario):
    """Return the data center target capacity of a scenario in MW."""
    target_capacity_mw = TARGET_CAPACITY_MW[scenario]

    # The test mode only covers Schleswig-Holstein, one of the 16 federal
    # states, so the national target is scaled down accordingly.
    if (
        config.settings()["egon-data"]["--dataset-boundary"]
        == "Schleswig-Holstein"
    ):
        target_capacity_mw = target_capacity_mw / 16

    return target_capacity_mw


def generate_data_center_sizes(target_capacity_mw):
    """Generate scenario-dependent data center sizes"""
    # Generate a representative distribution of individual data center capacities
    # whose total capacity matches the scenario target.

    np.random.seed(RANDOM_SEED)
    scale_param = np.exp(MU)

    massive_pool = []

    for _ in range(GENERATOR_RUNS):
        current_sum = 0.0

        while current_sum < target_capacity_mw:
            while True:
                rz_size = stats.lognorm.rvs(s=SIGMA, scale=scale_param)

                if rz_size <= MAX_RZ_SIZE:
                    break

            if current_sum + rz_size > target_capacity_mw:
                rz_size = target_capacity_mw - current_sum

            massive_pool.append(rz_size)
            current_sum += rz_size

    massive_pool = np.array(massive_pool)
    massive_pool.sort()

    ideal_scenario = massive_pool[GENERATOR_RUNS // 2 :: GENERATOR_RUNS].copy()
    ideal_scenario *= target_capacity_mw / np.sum(ideal_scenario)

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


def load_district_heating_areas(scenario):
    sources = DataCenters.sources

    gdf = db.select_geodataframe(
        f"""
        SELECT area_id, geom_polygon, residential_and_service_demand
        FROM {sources.tables["district_heating_areas"]}
        WHERE scenario = '{scenario}'
        """,
        geom_col="geom_polygon",
        epsg=3035,
    ).to_crs(epsg=25832)

    return gdf.rename_geometry("geometry")

# Internet exchange locations are based on PeeringDB facilities in Germany.
# Source:
# https://www.peeringdb.com/advanced_search?country__in=DE&reftag=fac
def load_internet_nodes():
    """Load original internet node input."""
    sources = DataCenters.sources

    return gpd.read_file(
        sources.files["internet_nodes"],
    ).to_crs(epsg=25832)

# Original input containing pre-defined regional Faktor values.
# The factor expresses how expensive a grid connection is in that region, so
# it enters the electricity-location score inverted, see REGIO_SCORE_OFFSET.
# Regional factors are based on the Baukostenzuschuss data from
# Netztransparenz.de, published by the German transmission system
# operators 50Hertz, Amprion, TenneT and TransnetBW.
def load_regional_factors():
    """Load original regional factor input."""
    sources = DataCenters.sources

    return gpd.read_file(
        sources.files["regional_factors"],
    ).to_crs(epsg=25832)

def load_ukpn_profiles():
    """Load the UKPN data center demand profiles of UKPN_PROFILE_YEAR."""
    sources = DataCenters.sources

    profiles = pd.read_csv(
        sources.files["ukpn_profiles"]
    )

    # UTC gives a continuous time index without daylight-saving gaps or duplicates.
    profiles["utc_timestamp"] = pd.to_datetime(
        profiles["utc_timestamp"],
        utc=True,
    )

    profiles = profiles[
        profiles["utc_timestamp"].dt.year == UKPN_PROFILE_YEAR
    ].copy()

    return profiles

    
def create_data_center_allocation(scenario):
    """Run data center allocation workflow and return rz_punkte."""
    # Allocate generated data center capacities to suitable commercial areas
    # based on electricity, district-heating, and internet-location criteria.

    rz_df = generate_data_center_sizes(get_target_capacity(scenario))
    gewerbe_raw = load_commercial_areas()
    strom_raw = load_substations()
    waerme_raw = load_district_heating_areas(scenario)
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
    # Runs from 0.2 to 1.0 by design, not 0 to 1, cf. REGIO_SCORE_OFFSET.
    gewerbe_scored["score_regio_strom"] = (
        REGIO_SCORE_OFFSET - gewerbe_scored["Faktor"]
    )

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

    # A data center that does not fit into any single commercial area is
    # skipped by the loop above, which would silently put the scenario below
    # the NEP target without any other symptom.
    allocated_mw = gewerbe_scored["allocated_mw"].sum()
    target_capacity_mw = get_target_capacity(scenario)

    if not np.isclose(allocated_mw, target_capacity_mw, rtol=1e-6):
        raise ValueError(
            f"Allocated {allocated_mw:.1f} MW of data centers but the "
            f"{scenario} target is {target_capacity_mw:.1f} MW. "
            f"{int((rz_sizes_mw > (base_areas * MW_PER_HA).max()).sum())} of "
            f"{len(rz_sizes_mw)} data centers are larger than the biggest "
            "commercial area can host, so they could not be placed."
        )

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


def get_central_heat_bus_per_area(scenario):
    """Map each district heating area to its central heat bus."""
    sources = DataCenters.sources

    # heat_etrago.insert_buses() creates exactly one central heat bus per
    # district heating area, placed at the centroid of the area. Matching the
    # centroid back to the bus point therefore identifies the area's bus. The
    # tiny buffer absorbs coordinate rounding and mirrors the join used in
    # heat_etrago.insert_central_direct_heat().
    return db.select_dataframe(
        f"""
        SELECT a.area_id, b.bus_id
        FROM {sources.tables["district_heating_areas"]} AS a
        JOIN {sources.tables["buses"]} AS b
        ON ST_Intersects(
            ST_Transform(
                ST_Buffer(ST_Centroid(a.geom_polygon), 0.0000001), 4326),
            b.geom)
        WHERE a.scenario = '{scenario}'
        AND b.scn_name = '{scenario}'
        AND b.carrier = 'central_heat'
        """,
        index_col="area_id",
    )


def assign_nearest_bus(data_centers, existing_buses):
    """Assign nearest existing AC bus with matching nominal voltage."""
    data_centers_projected = data_centers.to_crs(epsg=3035)
    existing_buses_projected = existing_buses.to_crs(epsg=3035)

    assigned_data_centers = []

    for v_nom in [110, 380]:
        data_centers_at_level = data_centers_projected[
            data_centers_projected["v_nom"] == v_nom
        ]
        buses_at_level = existing_buses_projected[
            existing_buses_projected["v_nom"] == v_nom
        ]

        if data_centers_at_level.empty:
            continue

        # Without a bus at this voltage the join would return NaN bus ids and
        # fail on the conversion to int below with no usable message. This is
        # reachable in test mode, where the dataset boundary may not contain
        # any 380 kV bus.
        if buses_at_level.empty:
            raise ValueError(
                f"{len(data_centers_at_level)} data centers require a "
                f"{v_nom} kV bus but none exists in this scenario. Reduce the "
                "target capacity so that no data center exceeds the 380 kV "
                "threshold, or run on a boundary that contains such buses."
            )

        assigned_data_centers.append(
            gpd.sjoin_nearest(
                data_centers_at_level,
                buses_at_level[["bus_id", "geometry"]].rename(
                    columns={"bus_id": "nearest_bus_id"}
                ),
                how="left",
                distance_col="connection_length_km",
            )
        )

    data_centers_projected = pd.concat(assigned_data_centers)

    # sjoin_nearest repeats a data center once per tie when several buses are
    # equidistant. Keep a single bus per data center, otherwise the duplicate
    # would get its own bus, line and load further down.
    data_centers_projected = data_centers_projected[
        ~data_centers_projected.index.duplicated()
    ]

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

    data_centers_projected = data_centers_projected.to_crs(epsg=4326)

    # to_crs only transforms the active geometry column, so the assigned bus
    # geometry is converted explicitly and the whole frame stays in one CRS.
    data_centers_projected["nearest_bus_geom"] = (
        gpd.GeoSeries(
            data_centers_projected["nearest_bus_geom"], crs="EPSG:3035"
        )
        .to_crs(epsg=4326)
        .values
    )

    return data_centers_projected


def assign_district_heating_area(data_centers, district_heating_areas):
    """Assign the nearest district heating area within the connection radius."""
    # The distance is measured against the area polygons rather than the
    # central heat buses: those buses sit at the centroid of their area, so a
    # data center right next to a large network can be far from its bus.
    data_centers_projected = data_centers.to_crs(epsg=3035)
    areas_projected = district_heating_areas.to_crs(epsg=3035)

    assigned = (
        gpd.sjoin_nearest(
            data_centers_projected,
            areas_projected[["area_id", "geometry"]],
            how="left",
            distance_col="distance_to_heat_area_m",
        )
        .drop(columns=["index_right"])
        # sjoin_nearest repeats a data center once per tie when several areas
        # are equidistant. Keep a single area per data center.
        .drop_duplicates(subset=["load_id"])
    )

    assigned = assigned[
        assigned["distance_to_heat_area_m"]
        <= MAX_HEAT_CONNECTION_DISTANCE_M
    ].copy()

    assigned["area_id"] = assigned["area_id"].astype(int)

    return assigned


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

    # to_crs only transforms the active geometry column, so the assigned bus
    # geometry is projected explicitly. Both ends have to be metric for the
    # line length below.
    nearest_bus_geom = gpd.GeoSeries(
        data_centers["nearest_bus_geom"].values, crs=data_centers.crs
    ).to_crs(epsg=3035)

    lines = []

    for (_, row), bus_geom in zip(
        data_centers_projected.iterrows(), nearest_bus_geom
    ):
        topo = LineString([row.geometry, bus_geom])
        length_km = topo.length / 1000

        lines.append(
            {
                "scn_name": scenario,
                "bus0": row.data_center_bus_id,
                "bus1": row.nearest_bus_id,
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

    data_center_lines = gpd.GeoDataFrame(
        lines, geometry="topo", crs="EPSG:3035"
    )
    data_center_lines = data_center_lines.to_crs(epsg=4326)
    data_center_lines["line_id"] = db.next_etrago_id(
        "line", len(data_center_lines)
    )

    return data_center_lines


def create_data_center_loads(data_centers, scenario):
    """Create electricity loads for the new data center buses."""
    data_center_loads = pd.DataFrame(
        {
            "scn_name": scenario,
            "load_id": db.next_etrago_id("load", len(data_centers)),
            "bus": data_centers["data_center_bus_id"].values,
            "type": "data_center",
            "carrier": "AC",
            # p_set is deliberately left unset. The hourly demand goes to
            # egon_etrago_load_timeseries instead, as in
            # electricity_demand_etrago.py, which drops the column once a time
            # series exists. A static p_set here would describe a data center
            # drawing its full capacity around the clock, which is 8,760
            # instead of FULL_LOAD_HOURS full-load hours.
            "q_set": None,
            "sign": -1,
        }
    )

    data_centers["load_id"] = data_center_loads["load_id"].values

    return data_center_loads, data_centers


def create_waste_heat_generators(data_centers, heat_bus_per_area, scenario):
    """Create waste heat generators at the assigned central heat buses.

    Waste heat is a byproduct of electricity the data center already consumes,
    so it is modelled as a generator feeding the district heating bus rather
    than as a link from the data center's AC bus. A link would withdraw grid
    electricity a second time, on top of the data center load, and deliver heat
    even while the data center is idle. This mirrors how non-dispatchable heat
    sources are represented in heat_etrago.insert_central_direct_heat().
    """
    return pd.DataFrame(
        {
            "scn_name": scenario,
            "generator_id": db.next_etrago_id(
                "generator", len(data_centers)
            ),
            "bus": heat_bus_per_area.bus_id[data_centers["area_id"]].values,
            "carrier": "data_center_waste_heat",
            # Together with p_max_pu below this yields exactly
            # REUSABLE_HEAT_FACTOR times the data center load in every hour,
            # and therefore that share of its annual demand, which is what
            # EnEfG § 11(2) requires.
            "p_nom": (
                data_centers["allocated_mw"].values * REUSABLE_HEAT_FACTOR
            ),
            "p_nom_extendable": False,
            # The heat is a byproduct of demand that is paid for anyway, so
            # reusing it carries no fuel or opportunity cost.
            "marginal_cost": 0,
        }
    )


def create_waste_heat_timeseries(generators, data_centers, scenario):
    """Bound waste heat availability by the data center's own hourly load."""
    # p_max_pu is the data center load normalised by its allocated capacity.
    # cap_and_redistribute_profiles() keeps the load at or below that capacity,
    # so the ratio never exceeds 1. Because it is an upper bound rather than a
    # fixed injection, unused waste heat is simply spilled instead of forcing
    # heat into a district heating network that does not need it.
    p_max_pu = [
        (np.array(profile) / capacity).tolist()
        for profile, capacity in zip(
            data_centers["profile"], data_centers["allocated_mw"]
        )
    ]

    return pd.DataFrame(
        {
            "scn_name": scenario,
            "generator_id": generators["generator_id"].values,
            "temp_id": 1,
            "p_max_pu": p_max_pu,
        }
    )


def delete_existing_data_centers(scenario):
    """Delete previously inserted data center components before rerun."""
    targets = DataCenters.targets

    db.execute_sql(f"""
        DELETE FROM {targets.tables["load_timeseries"]}
        WHERE scn_name = '{scenario}'
        AND load_id IN (
            SELECT load_id
            FROM {targets.tables["loads"]}
            WHERE scn_name = '{scenario}'
            AND type = 'data_center'
        );

        DELETE FROM {targets.tables["generator_timeseries"]}
        WHERE scn_name = '{scenario}'
        AND generator_id IN (
            SELECT generator_id
            FROM {targets.tables["generators"]}
            WHERE scn_name = '{scenario}'
            AND carrier = 'data_center_waste_heat'
        );

        DELETE FROM {targets.tables["generators"]}
        WHERE scn_name = '{scenario}'
        AND carrier = 'data_center_waste_heat';

        -- Waste heat used to be modelled as a link from the data center AC bus
        -- to the central heat bus. It is a generator at the heat bus now, see
        -- create_waste_heat_generators(), but the delete is kept so databases
        -- written by earlier versions of this dataset are cleaned up.
        DELETE FROM {targets.tables["links"]}
        WHERE scn_name = '{scenario}'
        AND carrier = 'data_center_waste_heat';

        DELETE FROM {targets.tables["loads"]}
        WHERE scn_name = '{scenario}'
        AND type = 'data_center';

        DELETE FROM {targets.tables["lines"]}
        WHERE scn_name = '{scenario}'
        AND bus0 IN (
            SELECT bus_id
            FROM {targets.tables["buses"]}
            WHERE scn_name = '{scenario}'
            AND type = 'data_center'
        );

        DELETE FROM {targets.tables["buses"]}
        WHERE scn_name = '{scenario}'
        AND type = 'data_center';
        """)


def insert_data_centers(scenario):
    """Insert all data center components of one scenario into the database.

    Buses, lines, loads, load time series and waste heat are built in one go so
    that the allocated capacity of each data center stays in memory. Reading it
    back from the database would mean storing it in the load table's p_set,
    which describes demand rather than capacity, cf. create_data_center_loads().
    """
    targets = DataCenters.targets
    delete_existing_data_centers(scenario)

    data_centers = create_data_center_allocation(scenario)
    existing_buses = get_existing_ac_buses(scenario)
    data_centers = assign_nearest_bus(data_centers, existing_buses)

    data_center_buses, data_centers = create_data_center_buses(
        data_centers, scenario
    )
    data_center_lines = create_data_center_lines(data_centers, scenario)
    data_center_loads, data_centers = create_data_center_loads(
        data_centers, scenario
    )

    # The hourly load drives both the load time series and the waste heat
    # available in each hour, so it is derived once and kept on the frame.
    load_profiles = build_data_center_load_profiles(data_centers, scenario)
    data_centers["profile"] = data_centers["load_id"].map(load_profiles)

    load_timeseries = create_data_center_load_timeseries(
        load_profiles, scenario
    )

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
    load_timeseries.to_sql(
        targets.get_table_name("load_timeseries"),
        schema=targets.get_table_schema("load_timeseries"),
        if_exists="append",
        con=db.engine(),
        index=False,
    )

    insert_data_center_waste_heat(data_centers, scenario)

####################
# Load time-series integration part

def get_valid_ukpn_sites(profiles):
    """Select plausible UKPN data center profiles with near-complete annual data."""

    # Sites are pooled across UKPN voltage levels rather than split into
    # separate pools per level. UKPN's "High Voltage" and "Extra-High Voltage"
    # classes describe the British 33/66 kV and 132 kV connection levels, which
    # do not correspond to the German 110 kV and 380 kV levels modelled here.
    # Splitting on them would also leave only two usable extra-high voltage
    # profiles, so the data centers above the 380 kV threshold, which carry
    # more than half of the national capacity, would all share two load shapes
    # and their demand would be almost perfectly correlated.
    site_stats = (
        profiles
        .groupby("anonymised_data_centre_name")["hh_utilisation_ratio"]
        .agg(["mean", "count", "max"])
    )

    # Keep sites with plausible annual utilisation, no half-hour above their
    # own capacity, and almost complete 2025 data.
    valid_sites = site_stats[
        (site_stats["mean"] >= MIN_UTILISATION)
        & (site_stats["mean"] <= MAX_UTILISATION)
        & (site_stats["max"] <= MAX_PEAK_UTILISATION)
        & (site_stats["count"] >= 17500)
    ]

    return valid_sites.index.tolist()

def assign_ukpn_profiles(
    data_centers,
    profiles,
    valid_sites,
    weather_year,
):
    """Assign and prepare one UKPN load profile for each modeled data center."""

    rng = np.random.default_rng(LOAD_PROFILE_RANDOM_SEED)
    raw_profiles = {}

    # Stable ordering keeps the random profile assignment reproducible.
    data_centers = data_centers.sort_values("load_id")

    for _, row in data_centers.iterrows():

        selected_site = rng.choice(valid_sites)

        profile = (
            profiles[
                profiles["anonymised_data_centre_name"] == selected_site
            ]
            .set_index("utc_timestamp")["hh_utilisation_ratio"]
            .sort_index()
        )

        full_index = pd.date_range(
            start=f"{UKPN_PROFILE_YEAR}-01-01 00:00:00+00:00",
            end=f"{UKPN_PROFILE_YEAR}-12-31 23:30:00+00:00",
            freq="30min",
        )

        # Fill the few missing half-hours before converting to hourly resolution.
        profile = profile.reindex(full_index).ffill()

        # Convert half-hourly UKPN utilisation to hourly eTraGo resolution.
        profile = profile.resample("1h").mean()

        # Every scenario timeseries shares the hours of the weather year, so
        # the measured profile is shifted by whole days onto that calendar.
        # Without it the weekends of the data centers would fall on other
        # model days than those of the timeseries built for the weather year.
        profile = align_weekdays(
            profile,
            source_year=UKPN_PROFILE_YEAR,
            target_year=weather_year,
        )

        raw_profiles[row.load_id] = (
            profile.to_numpy()
            * row.allocated_mw
        )

    return raw_profiles

def scale_data_center_profiles(
    raw_profiles,
    target_capacity_mw,
):
    """Scale all data center profiles to the annual demand target."""

    raw_total_mwh = sum(
        profile.sum()
        for profile in raw_profiles.values()
    )

    # Annual demand target follows from installed capacity and 5,000 full-load hours.
    target_total_mwh = (
        target_capacity_mw
        * FULL_LOAD_HOURS
    )

    scaling_factor = (
        target_total_mwh
        / raw_total_mwh
    )

    scaled_profiles = {
        load_id: profile * scaling_factor
        for load_id, profile in raw_profiles.items()
    }

    return scaled_profiles

def cap_and_redistribute_profiles(scaled_profiles, data_centers):
    """Cap hourly loads at allocated capacity and redistribute excess energy."""

    capacities = data_centers.set_index("load_id")["allocated_mw"]

    for load_id, profile in scaled_profiles.items():
        capacity = capacities.loc[load_id]

        capped_profile = np.minimum(profile, capacity)
        excess_energy = profile.sum() - capped_profile.sum()

        if excess_energy > 0:
            headroom = capacity - capped_profile
            capped_profile += excess_energy * headroom / headroom.sum()

        scaled_profiles[load_id] = capped_profile

    return scaled_profiles

def create_data_center_load_timeseries(
    scaled_profiles,
    scenario,
):
    """Create load time-series rows for data centers."""

    timeseries = []

    for load_id, profile in scaled_profiles.items():
        timeseries.append(
            {
                "scn_name": scenario,
                "load_id": load_id,
                "temp_id": 1,
                "p_set": profile.tolist(),
                "q_set": None,
            }
        )

    return pd.DataFrame(timeseries)

def build_data_center_load_profiles(data_centers, scenario):
    """Build hourly load profiles in MW for the modeled data centers."""

    profiles = load_ukpn_profiles()
    valid_sites = get_valid_ukpn_sites(profiles)

    raw_profiles = assign_ukpn_profiles(
        data_centers,
        profiles,
        valid_sites,
        get_sector_parameters("global", scenario)["weather_year"],
    )

    # Use the actual created data center capacity as the annual scaling basis.
    scaled_profiles = scale_data_center_profiles(
        raw_profiles,
        data_centers["allocated_mw"].sum(),
    )

    return cap_and_redistribute_profiles(
        scaled_profiles,
        data_centers,
    )


####################
# Waste heat integration part

def insert_data_center_waste_heat(data_centers, scenario):
    """Create and insert reusable waste heat for the modeled data centers."""
    targets = DataCenters.targets

    data_centers_at_heat = assign_district_heating_area(
        data_centers, load_district_heating_areas(scenario)
    )

    if data_centers_at_heat.empty:
        print(
            f"No data center within {MAX_HEAT_CONNECTION_DISTANCE_M} m of a "
            f"district heating area in scenario {scenario}."
        )
        return

    generators = create_waste_heat_generators(
        data_centers_at_heat,
        get_central_heat_bus_per_area(scenario),
        scenario,
    )
    timeseries = create_waste_heat_timeseries(
        generators, data_centers_at_heat, scenario
    )

    generators.to_sql(
        targets.get_table_name("generators"),
        schema=targets.get_table_schema("generators"),
        if_exists="append",
        con=db.engine(),
        index=False,
    )

    timeseries.to_sql(
        targets.get_table_name("generator_timeseries"),
        schema=targets.get_table_schema("generator_timeseries"),
        if_exists="append",
        con=db.engine(),
        index=False,
    )


def insert_data_centers_for_scenarios():
    """Insert data center components for configured scenarios."""
    for scenario in config.settings()["egon-data"]["--scenarios"]:
        if scenario in TARGET_CAPACITY_MW:
            insert_data_centers(scenario)


class DataCenters(Dataset):
    """Integrate future data center demand"""

    name: str = "DataCenters"
    version: str = "0.0.5"

    sources = DatasetSources(
        tables={
            "buses": "grid.egon_etrago_bus",
            "commercial_areas": "openstreetmap.osm_landuse",
            "district_heating_areas": "demand.egon_district_heating_areas",
            "substations": "grid.egon_hvmv_substation",
        },
        files={
            # Internet exchange locations, from PeeringDB facilities in Germany.
            # https://www.peeringdb.com/advanced_search?country__in=DE&reftag=fac
            "internet_nodes": (
                "data_bundle_egon_data/data_centers/Internetknoten.gpkg"
            ),
            # Regional factors, from the Netztransparenz.de Baukostenzuschuss
            # data published by 50Hertz, Amprion, TenneT and TransnetBW.
            "regional_factors": (
                "data_bundle_egon_data/data_centers/Regionalisierungsfaktoren.gpkg"
            ),
            # Individual half-hourly UKPN data center demand profiles, used to
            # derive hourly load time series for the modeled data centers.
            "ukpn_profiles": (
                "data_bundle_egon_data/data_centers/ukpn-data-centre-demand-profiles.csv"
            ),
        },
    )

    targets = DatasetTargets(
        tables={
            "buses": "grid.egon_etrago_bus",
            "lines": "grid.egon_etrago_line",
            "loads": "grid.egon_etrago_load",
            "links": "grid.egon_etrago_link",
            "load_timeseries": "grid.egon_etrago_load_timeseries",
            "generators": "grid.egon_etrago_generator",
            "generator_timeseries": "grid.egon_etrago_generator_timeseries",
        },
    )

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(insert_data_centers_for_scenarios,),
        )
