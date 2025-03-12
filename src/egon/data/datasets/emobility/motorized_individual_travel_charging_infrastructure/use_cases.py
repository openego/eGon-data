"""
Functions related to the four different use cases
"""
from __future__ import annotations

from loguru import logger
import geopandas as gpd
import numpy as np
import pandas as pd

from egon.data import config

DATASET_CFG = config.datasets()["charging_infrastructure"]


def hpc(hpc_points: gpd.GeoDataFrame, uc_dict: dict) -> gpd.GeoDataFrame:
    """
    Calculate placements and energy distribution for use case hpc.

    :param hpc_points: gpd.GeoDataFrame
        GeoDataFrame of possible hpc locations
    :param uc_dict: dict
        contains basic run info like region boundary and save directory
    """
    uc_id = "hpc"
    logger.debug(f"Use case: {uc_id}")

    num_hpc = 10**6
    energy_sum = 1

    # filter hpc points by region
    in_region_bool = hpc_points["geometry"].within(uc_dict["region"].iat[0])
    in_region = hpc_points.loc[in_region_bool]

    if "has_hpc" in in_region.columns:
        in_region = in_region.loc[in_region["has_hpc"]]

    cols = [
        "geometry",
        "hpc_count",
        "potential",
        "new_hpc_index",
        "new_hpc_tag",
    ]
    in_region = in_region[cols]

    # select all hpc points tagged 0 (all registered points)
    real_mask = in_region["new_hpc_tag"] == 0
    real_in_region = in_region.loc[real_mask]
    num_hpc_real = real_in_region["hpc_count"].sum()

    if num_hpc_real < num_hpc:
        sim_in_region = in_region.loc[~real_mask]
        sim_in_region = sim_in_region.loc[in_region["new_hpc_index"] > 0]
        sim_in_region_sorted = sim_in_region.sort_values(
            "potential", ascending=False
        )
        additional_hpc = int(
            min(num_hpc - num_hpc_real, len(sim_in_region.index))
        )
        selected_hpc = sim_in_region_sorted.iloc[:additional_hpc]
        real_in_region = pd.concat([real_in_region, selected_hpc])
    if not len(real_in_region.index):
        logger.warning(
            f"No potential charging points found in region {uc_dict['key']}!"
        )
    else:
        real_in_region["potential"] = (
            real_in_region["potential"] * real_in_region["hpc_count"]
        )
        total_potential = real_in_region["potential"].sum()
        real_in_region = real_in_region.assign(
            share=real_in_region["potential"] / total_potential
        ).round(6)
        real_in_region["exists"] = real_in_region["new_hpc_tag"] == 0

        # outputs
        logger.debug(
            f"{round(energy_sum, 1)} kWh got fastcharged in region {uc_dict['key']}."
        )

    return gpd.GeoDataFrame(real_in_region)


def public(
    public_points: gpd.GeoDataFrame,
    public_data: gpd.GeoDataFrame,
    uc_dict: dict,
) -> gpd.GeoDataFrame:
    """
    Calculate placements and energy distribution for use case hpc.

    :param public_points: gpd.GeoDataFrame
        existing public charging points
    :param public_data: gpd.GeoDataFrame
        clustered POI
    :param uc_dict: dict
        contains basic run info like region boundary and save directory
    """

    uc_id = "public"
    logger.debug(f"Use case: {uc_id}")

    num_public = 10**6
    energy_sum = 1

    # filter hpc points by region
    in_region_bool = public_points["geometry"].within(uc_dict["region"].iat[0])
    in_region = public_points.loc[in_region_bool]

    poi_in_region_bool = public_data["geometry"].within(
        uc_dict["region"].iat[0]
    )
    poi_in_region = public_data.loc[poi_in_region_bool]

    num_public_real = in_region["count"].sum()

    # match with clusters anyway (for weights)
    region_points, region_poi = match_existing_points(in_region, poi_in_region)
    region_points["exists"] = True

    if num_public_real < num_public:
        additional_public = num_public - num_public_real
        # distribute additional public points via POI
        add_points = distribute_by_poi(region_poi, additional_public)
        region_points = pd.concat([region_points, add_points])

    region_points["energy"] = (
        region_points["potential"]
        / region_points["potential"].sum()
        * energy_sum
    )

    # outputs
    logger.debug(
        f"{round(energy_sum, 1)} kWh got charged in region {uc_dict['key']}."
    )

    return gpd.GeoDataFrame(region_points, crs=public_points.crs)


def distribute_by_poi(region_poi: gpd.GeoDataFrame, num_points: int | float):
    # sort clusters without existing points by weight, then choose highest
    region_poi = region_poi.copy()
    region_poi.sort_values("potential", inplace=True, ascending=False)
    num_points = int(min(num_points, len(region_poi.index)))
    # choose point in cluster that is closest to big street
    return region_poi.iloc[:num_points]


def match_existing_points(
    region_points: gpd.GeoDataFrame, region_poi: gpd.GeoDataFrame
):
    region_poi = region_poi.assign(exists=False)
    poi_buffer = region_poi.buffer(region_poi["radius"].astype(int))
    region_points = region_points.assign(potential=0)
    for i in region_points.index:
        lis_point = region_points.at[i, "geometry"]
        cluster = poi_buffer.contains(lis_point)
        clusters = region_poi.loc[cluster]
        num_clusters = len(clusters.index)

        if num_clusters == 0:
            # decent average as fallback
            region_points.at[i, "potential"] = 5
        elif num_clusters == 1:
            region_points.at[i, "potential"] = clusters["potential"].values[0]
            region_poi.loc[cluster, "exists"] = True

        elif num_clusters > 1:
            # choose cluster with closest Point
            dist = clusters.distance(lis_point)
            idx = dist.idxmin()
            region_poi.at[idx, "exists"] = True
            region_points.at[i, "potential"] = clusters.at[idx, "potential"]

    # delete all clusters with exists = True
    region_poi = region_poi.loc[~region_poi["exists"]]

    return region_points, region_poi


def home(
    home_data: gpd.GeoDataFrame,
    uc_dict: dict,
) -> gpd.GeoDataFrame:
    """
    Calculate placements and energy distribution for use case hpc.

    :param home_data: gpd.GeoDataFrame
        info about house types
    :param uc_dict: dict
        contains basic run info like region boundary and save directory
    """
    uc_id = "home"
    logger.debug(f"Use case: {uc_id}")

    num_home = 1000000
    energy_sum = 1

    # filter houses by region
    in_region_bool = home_data["geometry"].within(uc_dict["region"].iat[0])

    in_region = home_data.loc[in_region_bool]
    if in_region.empty:
        return in_region

    in_region = in_region.assign(
        num=in_region["num"].fillna(value=0),
        num_mfh=in_region["num_mfh"].fillna(value=0),
    )

    potential = apportion_home(in_region, num_home, uc_dict)

    in_region["charge_spots"] = potential
    in_region = in_region.loc[in_region["charge_spots"] > 0]
    in_region["energy"] = energy_sum * in_region["charge_spots"] / num_home
    in_region = in_region.sort_values(by="energy", ascending=False)

    logger.debug(
        f"{round(energy_sum, 1)} kWh got charged in region {uc_dict['key']}."
    )

    return gpd.GeoDataFrame(in_region, crs=home_data.crs)


def apportion_home(home_df: pd.DataFrame, num_spots: int, config: dict):
    # use parameters to set number of possible charge spots per row
    home_df["num_available"] = home_df[["num", "num_mfh"]].apply(
        home_charge_spots, axis=1, raw=True, args=(config,)
    )
    # if too many spots need to be placed, every house gets a spot
    if num_spots >= home_df["num_available"].sum():
        logger.debug(
            f"All private home spots have been filled. Leftover: "
            f"{num_spots - home_df['num_available'].sum()}"
        )
        return home_df.loc[:, "num_available"]
    # distribute charge points based on houses per square
    samples = home_df.sample(
        num_spots, weights="num_available", random_state=1, replace=True
    )
    result = pd.Series([0] * len(home_df.index), index=home_df.index)
    for i in samples.index:
        result.at[i] += 1
    return result


def home_charge_spots(house_array: pd.Series | np.array, config: dict):
    # take number of houses, random seed, average spots per house and share of houses
    # with possible spots
    sfh = (
        house_array[0]
        * config["sfh_avg_spots"]
        * max(config["random_seed"].normal(config["sfh_available"], 0.1), 0)
    )
    mfh = (
        house_array[1]
        * config["mfh_avg_spots"]
        * max(config["random_seed"].normal(config["mfh_available"], 0.1), 0)
    )
    return round(sfh + mfh)


def work(
    landuse: gpd.GeoDataFrame,
    weights_dict: dict,
    uc_dict: dict,
) -> gpd.GeoDataFrame:
    """
    Calculate placements and energy distribution for use case hpc.

    :param landuse: gpd.GeoDataFrame
        work areas by land use
    :param weights_dict: dict
        weights for different land use types
    :param uc_dict: dict
        contains basic run info like region boundary and save directory
    """
    uc_id = "work"
    logger.debug(f"Use case: {uc_id}")

    energy_sum = 1

    in_region_bool = landuse.within(uc_dict["region"].iat[0])
    in_region = landuse[in_region_bool]

    # calculating the area of polygons
    in_region = in_region.assign(area=in_region["geometry"].area / 10**6)

    groups = in_region.groupby("landuse")
    group_labels = ["retail", "commercial", "industrial"]

    srid = DATASET_CFG["original_data"]["sources"]["tracbev"]["srid"]

    result = gpd.GeoDataFrame(
        columns=["geometry", "landuse", "potential"], crs=f"EPSG:{srid}"
    )

    for g in group_labels:
        if g in groups.groups:
            group = groups.get_group(g)
            group = group.assign(
                potential=group["geometry"].area * weights_dict[g]
            )
            group.to_crs(srid)
            result = gpd.GeoDataFrame(
                pd.concat([result, group]), crs=f"EPSG:{srid}"
            )

    result["energy"] = (
        result["potential"] * energy_sum / result["potential"].sum()
    )
    # outputs
    logger.debug(
        f"{round(energy_sum, 1)} kWh got charged in region {uc_dict['key']}."
    )

    return gpd.GeoDataFrame(result, crs=landuse.crs)
