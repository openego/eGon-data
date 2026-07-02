"""Rail transport electricity demand (reGon) -> eGon eTraGo loads.

Additional Data sources: the data bundle carries ONLY data eGon cannot derive
(GTFS-based energy weights + curated converter locations + load profiles);
everything else is done here on eGon's own OSM tables.

Bundle inputs (``data_bundle_egon_data/rail_transport_demand/``):
  - converter_load_points.csv  16.7-Hz converters: site, lat/lon, base energy
    (2024), profile, grid_level. (OSM under-tags converters, so curated.)
  - dc_city_energy.csv          per city+system: base DC energy + centroid.
  - load_profiles.csv           normalized hourly shapes (traction/sbahn/tram).

Done here:
  - DC rectifier Unterwerke extracted + classified from OSM tables; each
    city's energy distributed over its nearby rectifiers (else centroid).
  - Bus assignment by coupling level: HöS/HS -> EHV substation voronoi bus,
    MS -> MV grid district bus.
  - Per-scenario scaling from the rail-transport demand totals stored in the
    scenario_parameters (mobility sector): status2024 is used 1:1, the futures
    scale by total(scn) / total(status2024).
  - Load profiles re-indexed to the eGon weather year 2011 (weekday order
    differs from 2025!), then scaled to absolute MW:
        p_set[h] = energy_mwh_a * factor(scn) * profile_2011[h]
  - Written to grid.egon_etrago_load + grid.egon_etrago_load_timeseries.
"""

from pathlib import Path
import re

import geopandas as gpd
import pandas as pd

from egon.data import db
from egon.data.datasets import Dataset, DatasetSources, DatasetTargets
from egon.data.datasets.scenario_parameters import get_sector_parameters

BUNDLE = Path(".") / "data_bundle_egon_data" / "rail_transport_demand"

#: profile column in load_profiles.csv -> eTraGo load carrier
CARRIERS = {
    "traction_16_7hz": "rail_traction",
    "tram_ubahn_dc": "rail_transit_dc",
    "sbahn_dc": "rail_sbahn_dc",
}

#: reGon scenarios this dataset writes loads for. ``status2024`` uses the
#: absolute (50-Hz) time series 1:1; the futures scale by the ratio of the
#: rail-transport demand totals stored in the scenario_parameters (mobility
#: sector). NEP figures live there, not here (see scenario_parameters).
BASE_SCENARIO = "status2024"
SCENARIOS = ("status2024", "reGon2037", "reGon2045")

#: max distance [m] from a city centroid to attach its DC rectifiers
DC_CITY_RADIUS_M = 25_000
#: DC output voltage levels that mark a rectifier substation
_DC_VOLT = {"600", "660", "750", "800", "1200", "1500", "2400"}
_RE_16_7 = re.compile(r"16[.,]?6?7")

#: OSM substations (nodes + ways-as-polygons) with the tags we classify on
_OSM_SUBSTATIONS_SQL = """
    SELECT ST_Transform(geom, 3035) AS geom, power, substation, voltage,
           frequency
    FROM (
        SELECT p.geom,
               hstore(p.tags)->'power' AS power,
               hstore(p.tags)->'substation' AS substation,
               hstore(p.tags)->'voltage' AS voltage,
               hstore(p.tags)->'frequency' AS frequency
        FROM openstreetmap.osm_point p
        WHERE hstore(p.tags)->'power'
              IN ('substation', 'sub_station', 'station')
        UNION ALL
        SELECT ST_Centroid(poly.geom) AS geom,
               hstore(w.tags)->'power' AS power,
               hstore(w.tags)->'substation' AS substation,
               hstore(w.tags)->'voltage' AS voltage,
               hstore(w.tags)->'frequency' AS frequency
        FROM openstreetmap.osm_ways w
        JOIN openstreetmap.osm_polygon poly ON w.id = poly.osm_id
        WHERE hstore(w.tags)->'power'
              IN ('substation', 'sub_station', 'station')
    ) sub
"""


class RailTransitDemand(Dataset):
    name: str = "RailTransitDemand"
    version: str = "0.0.3"

    sources = DatasetSources(
        tables={
            "osm_point": "openstreetmap.osm_point",
            "osm_ways": "openstreetmap.osm_ways",
            "osm_polygon": "openstreetmap.osm_polygon",
            "mv_grid_districts": "grid.egon_mv_grid_district",
            "ehv_voronoi": "grid.egon_ehv_substation_voronoi",
            "etrago_bus": "grid.egon_etrago_bus",
        },
        files={"bundle": str(BUNDLE)},
    )
    targets = DatasetTargets(
        tables={
            "etrago_load": "grid.egon_etrago_load",
            "etrago_load_timeseries": "grid.egon_etrago_load_timeseries",
        },
    )

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(insert_rail_demand,),
        )


def _is_dc_rectifier(row) -> bool:
    """Substation that feeds a DC traction net (tram/U-Bahn/S-Bahn)."""
    volt = set(re.findall(r"\d+", row["voltage"] or ""))
    freq = re.split(r"[;,]", row["frequency"] or "")
    if "50" in freq and "0" in freq:  # 50 Hz -> DC rectifier
        return True
    if volt & _DC_VOLT:  # DC output voltage
        return True
    if (row["frequency"] or "").strip() == "0":
        return True
    return False


def _osm_dc_rectifiers() -> gpd.GeoDataFrame:
    """Extract + classify DC rectifier Unterwerke from eGon's OSM tables."""
    subs = db.select_geodataframe(
        _OSM_SUBSTATIONS_SQL, geom_col="geom", epsg=3035
    )
    rect = subs[subs.apply(_is_dc_rectifier, axis=1)].reset_index(drop=True)
    return rect


def _bundle_points() -> gpd.GeoDataFrame:
    """Load points from the bundle: converters (curated) + DC per city."""
    conv = pd.read_csv(BUNDLE / "converter_load_points.csv")
    conv = gpd.GeoDataFrame(
        conv, geometry=gpd.points_from_xy(conv.lon, conv.lat), crs=4326
    ).to_crs(3035)
    conv["bus_level"] = "ehv"
    conv["carrier"] = conv["profile"].map(CARRIERS)
    conv = conv[
        ["energy_mwh_a", "profile", "carrier", "bus_level", "geometry"]
    ]

    cities = pd.read_csv(BUNDLE / "dc_city_energy.csv")
    rect = _osm_dc_rectifiers()
    rows = []
    for _, c in cities.iterrows():
        centroid = (
            gpd.GeoSeries(gpd.points_from_xy([c.lon], [c.lat]), crs=4326)
            .to_crs(3035)
            .iloc[0]
        )
        near = rect[rect.distance(centroid) <= DC_CITY_RADIUS_M]
        carrier = CARRIERS[c["profile"]]
        if len(near):  # split city energy equally over its rectifiers
            e = c["energy_mwh_a"] / len(near)
            for g in near.geometry:
                rows.append((e, c["profile"], carrier, "mv", g))
        else:  # no rectifier mapped -> load at city centroid
            rows.append(
                (c["energy_mwh_a"], c["profile"], carrier, "mv", centroid)
            )
    dc = gpd.GeoDataFrame(
        rows,
        columns=[
            "energy_mwh_a",
            "profile",
            "carrier",
            "bus_level",
            "geometry",
        ],
        crs=3035,
    )
    return gpd.GeoDataFrame(pd.concat([conv, dc], ignore_index=True), crs=3035)


def _assign_bus(pts: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """HöS/HS -> EHV voronoi bus, MS -> MV grid district bus (nearest fb)."""
    src = RailTransitDemand.sources.tables
    # Each gdf carries only bus_id + its own active geometry. sjoin joins on
    # the active geometry of each frame, so the differing column names
    # (pts: 'geometry', districts: 'geom') do not matter.
    poly = {
        "mv": db.select_geodataframe(
            f"SELECT bus_id, geom FROM {src['mv_grid_districts']}", epsg=3035
        ),
        "ehv": db.select_geodataframe(
            f"SELECT bus_id, geom FROM {src['ehv_voronoi']}", epsg=3035
        ),
    }
    out = []
    for level, p in poly.items():
        sub = pts[pts["bus_level"] == level]
        if sub.empty:
            continue
        j = gpd.sjoin(sub, p, how="left", predicate="within").drop(
            columns="index_right"
        )
        miss = j["bus_id"].isna()
        if miss.any():
            nn = gpd.sjoin_nearest(j[miss].drop(columns="bus_id"), p).drop(
                columns="index_right"
            )
            j.loc[nn.index, "bus_id"] = nn["bus_id"].values
        out.append(j)
    res = gpd.GeoDataFrame(pd.concat(out))
    res["bus_id"] = res["bus_id"].astype(int)
    return res


def _profiles_2011() -> pd.DataFrame:
    """Re-index the (2025) normalized profiles onto weather year 2011.

    The weekday sequence differs between 2025 and 2011, so we match each 2011
    hour to a 2025 hour with the same (ISO week, weekday, hour), falling back
    to the (weekday, hour) mean; each column is renormalized to sum 1.
    """
    p = pd.read_csv(BUNDLE / "load_profiles.csv")
    dt = pd.to_datetime(p["datetime"])
    iso = dt.dt.isocalendar()
    p = p.assign(
        woy=iso.week.values, dow=dt.dt.weekday.values, hod=dt.dt.hour.values
    )
    cols = list(CARRIERS)
    by_key = p.groupby(["woy", "dow", "hod"])[cols].mean()
    by_dh = p.groupby(["dow", "hod"])[cols].mean()

    idx = pd.date_range("2011-01-01", "2011-12-31 23:00", freq="h")
    iso11 = idx.isocalendar()
    keys = list(zip(iso11.week.values, idx.weekday, idx.hour))
    out = by_key.reindex(keys).reset_index(drop=True)
    fb = by_dh.reindex(list(zip(idx.weekday, idx.hour))).reset_index(drop=True)
    out = out.fillna(fb)
    return out[cols] / out[cols].sum()


def insert_rail_demand():
    """Write rail-transport loads + 2011 timeseries to eTraGo, per scenario."""
    tgt = RailTransitDemand.targets
    pts = _assign_bus(_bundle_points())
    prof = _profiles_2011()
    carriers = tuple(sorted(set(CARRIERS.values())))
    base_mwh = get_sector_parameters("mobility", BASE_SCENARIO)[
        "rail_transport_demand"
    ]["annual_demand"]

    for scn in SCENARIOS:
        scn_mwh = get_sector_parameters("mobility", scn)[
            "rail_transport_demand"
        ]["annual_demand"]
        factor = scn_mwh / base_mwh
        # p_set [MW]: profile sums to 1 over 8760 h, so energy_mwh_a [MWh] *
        # profile[h] is the MWh in hour h = average MW (dt = 1 h).
        p_set = pts.apply(
            lambda r: (
                r["energy_mwh_a"] * factor * prof[r["profile"]].to_numpy()
            )
            .round(4)
            .tolist(),
            axis=1,
        )
        db.execute_sql(
            f"""
            DELETE FROM {tgt.tables['etrago_load_timeseries']}
            WHERE scn_name = '{scn}' AND load_id IN (
                SELECT load_id FROM {tgt.tables['etrago_load']}
                WHERE scn_name = '{scn}' AND carrier IN {carriers});
            DELETE FROM {tgt.tables['etrago_load']}
            WHERE scn_name = '{scn}' AND carrier IN {carriers};
        """
        )
        ids = db.next_etrago_id("load", len(pts))
        load = pd.DataFrame(
            {
                "scn_name": scn,
                "load_id": ids,
                "bus": pts["bus_id"].values,
                "type": "rail_transport",
                "carrier": pts["carrier"].values,
                "sign": -1.0,
            }
        ).set_index(["scn_name", "load_id"])
        ts = pd.DataFrame(
            {
                "scn_name": scn,
                "load_id": ids,
                "temp_id": 1,
                "p_set": p_set.values,
            }
        ).set_index(["scn_name", "load_id", "temp_id"])

        load.to_sql(
            tgt.get_table_name("etrago_load"),
            schema=tgt.get_table_schema("etrago_load"),
            con=db.engine(),
            if_exists="append",
        )
        ts.to_sql(
            tgt.get_table_name("etrago_load_timeseries"),
            schema=tgt.get_table_schema("etrago_load_timeseries"),
            con=db.engine(),
            if_exists="append",
        )
        twh = pts["energy_mwh_a"].sum() * factor / 1e6
        print(
            f"{scn}: {len(load)} rail loads, {twh:.2f} TWh "
            f"(factor {factor:.3f})."
        )
