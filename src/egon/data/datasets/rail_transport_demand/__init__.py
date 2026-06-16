"""Rail transport electricity demand (reGon) → eGon eTraGo loads.

Ingests the prebuilt rail-transport-demand GeoPackage from the data bundle
(``data_bundle_egon_data/rail_transport_demand/``) and writes time-varying
loads into ``grid.egon_etrago_load`` + ``grid.egon_etrago_load_timeseries``.

The GeoPackage + ``load_profiles.csv`` come from a separate preprocessing
repo (OSM converter/rectifier extraction + GTFS frequency + §23c profiles);
this dataset only does the eGon-side ingestion and bus assignment.

Load layers -> eGon mapping:
  - converter_load: 16.7-Hz traction (Umrichterwerke), HöS/HS,
    carrier rail_traction.
  - dc_load_points: tram/U-Bahn + S-Bahn (Gleichrichter-Uw), MS,
    carriers rail_transit_dc / rail_sbahn_dc.

Each load point carries ``energy_mwh_a`` (annual) and ``profile`` (a column
in ``load_profiles.csv``, normalized 8760-h shape, sum=1). Load curve:
  p_set[h] = energy_mwh_a * profile[h]   # MWh/h = MW; sum -> energy_mwh_a

Bus assignment by coupling level (cf. power_plants.assign_bus_id):
  - HöS/HS converters -> nearest EHV substation Voronoi bus,
  - MS DC feed points -> containing MV grid district bus.

NOTE (TODO): the same annual energy is written to every configured
scenario; scenario-specific scaling (eGon2035/100RE growth) is not applied.
"""

from pathlib import Path

import geopandas as gpd
import pandas as pd

from egon.data import config, db
from egon.data.datasets import Dataset, DatasetSources, DatasetTargets

#: profile column in load_profiles.csv  ->  eTraGo load carrier
CARRIERS = {
    "traction_16_7hz": "rail_traction",
    "tram_ubahn_dc": "rail_transit_dc",
    "sbahn_dc": "rail_sbahn_dc",
}

BUNDLE = Path(".") / "data_bundle_egon_data" / "rail_transport_demand"


class RailTransitDemand(Dataset):
    name: str = "RailTransitDemand"
    version: str = "0.0.1"

    sources = DatasetSources(
        tables={
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


def _load_points(crs: int = 3035) -> gpd.GeoDataFrame:
    """Read the converter + DC load points from the bundle GeoPackage."""
    gpkg = sorted(BUNDLE.glob("rail_demand_dataset_*.gpkg"))[-1]
    frames = []
    for layer, bus_level in [
        ("converter_load", "ehv"),
        ("dc_load_points", "mv"),
    ]:
        g = gpd.read_file(gpkg, layer=layer).to_crs(crs)
        g = g[["energy_mwh_a", "profile", "geometry"]].copy()
        g["bus_level"] = bus_level
        frames.append(g)
    pts = gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), crs=crs)
    pts["carrier"] = pts["profile"].map(CARRIERS)
    return pts


def _assign_bus(pts: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Assign a bus: HöS/HS -> EHV Voronoi, MS -> MV district (nearest)."""
    src = RailTransitDemand.sources.tables
    polys = {
        "mv": db.select_geodataframe(
            f"SELECT bus_id, geom FROM {src['mv_grid_districts']}",
            epsg=pts.crs.to_epsg(),
        ),
        "ehv": db.select_geodataframe(
            f"SELECT bus_id, geom FROM {src['ehv_voronoi']}",
            epsg=pts.crs.to_epsg(),
        ),
    }
    out = []
    for level, poly in polys.items():
        sub = pts[pts["bus_level"] == level]
        if sub.empty:
            continue
        j = gpd.sjoin(
            sub, poly[["bus_id", "geometry"]], how="left", predicate="within"
        ).drop(columns="index_right")
        miss = j["bus_id"].isna()
        if miss.any():  # points outside any cell -> nearest bus
            nn = gpd.sjoin_nearest(
                j[miss].drop(columns="bus_id"), poly[["bus_id", "geometry"]]
            ).drop(columns="index_right")
            j.loc[nn.index, "bus_id"] = nn["bus_id"].values
        out.append(j)
    res = gpd.GeoDataFrame(pd.concat(out))
    res["bus_id"] = res["bus_id"].astype(int)
    return res


def insert_rail_demand():
    """Write the rail-transport loads + timeseries to eTraGo, per scenario."""
    tgt = RailTransitDemand.targets
    pts = _assign_bus(_load_points())

    profiles = pd.read_csv(BUNDLE / "load_profiles.csv")
    pts["p_set"] = pts.apply(
        lambda r: (r["energy_mwh_a"] * profiles[r["profile"]].to_numpy())
        .round(4)
        .tolist(),
        axis=1,
    )

    carriers = tuple(sorted(set(CARRIERS.values())))
    for scn in config.settings()["egon-data"]["--scenarios"]:
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
                "p_set": pts["p_set"].values,
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
        print(
            f"{scn}: {len(load)} rail loads "
            f"({pts['energy_mwh_a'].sum() / 1e6:.2f} TWh) written."
        )
