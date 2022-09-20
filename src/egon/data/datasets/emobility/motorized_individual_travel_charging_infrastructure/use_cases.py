from loguru import logger
import geopandas as gpd
import pandas as pd


def hpc(hpc_points: gpd.GeoDataFrame, uc_dict):
    """
    Calculate placements and energy distribution for use case hpc.

    :param hpc_points: gpd.GeoDataFrame
        GeoDataFrame of possible hpc locations
    :param uc_dict: dict
        contains basic run info like region boundary and save directory
    :param timestep: int
        time step of the simbev input series, default: 15 (minutes)
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


def public(
    public_points: gpd.GeoDataFrame,
    public_data: gpd.GeoDataFrame,
    uc_dict,
    timestep=15,
):
    """
    Calculate placements and energy distribution for use case hpc.

    :param public_points: gpd.GeoDataFrame
        existing public charging points
    :param public_data: gpd.GeoDataFrame
        clustered POI
    :param uc_dict: dict
        contains basic run info like region boundary and save directory
    :param timestep: int
        time step of the simbev input series, default: 15 (minutes)
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
    logger.debug(f"{round(energy_sum, 1)} kWh got charged in region")


def distribute_by_poi(region_poi: gpd.GeoDataFrame, num_points):
    # sort clusters without existing points by weight, then choose highest
    region_poi = region_poi.copy()
    region_poi.sort_values("potential", inplace=True, ascending=False)
    num_points = int(min(num_points, len(region_poi.index)))
    selected_hpc = region_poi.iloc[:num_points]
    # choose point in cluster that is closest to big street
    return selected_hpc


def match_existing_points(
    region_points: gpd.GeoDataFrame, region_poi: gpd.GeoDataFrame
):

    region_poi["exists"] = False
    poi_buffer = region_poi.buffer(region_poi["radius"].astype(int))
    region_points["potential"] = 0
    for i in region_points.index:
        lis_point = region_points.at[i, "geometry"]
        cluster = poi_buffer.contains(lis_point)
        clusters = region_poi.loc[cluster]
        num_clusters = len(clusters.index)

        if num_clusters == 0:
            # decent average as fallback
            region_points.at[i, "potential"] = 5
        elif num_clusters == 1:
            # region_poi.loc[cluster, "exists"] = True
            region_points.at[i, "potential"] = clusters["potential"]
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


# def home(
#     home_data: gpd.GeoDataFrame,
#     uc_dict,
#     home_charge_prob,
#     car_num,
#     timestep=15,
# ):
#     """
#     Calculate placements and energy distribution for use case hpc.
#
#     :param home_data: gpd.GeoDataFrame
#         info about house types
#     :param uc_dict: dict
#         contains basic run info like region boundary and save directory
#     :param home_charge_prob: float
#         probability of privately available home charging
#     :param car_num: pd.Series
#         total cars per car type in scenario
#     :param timestep: int
#         time step of the simbev input series, default: 15 (minutes)
#     """
#     uc_id = "home"
#     print("Use case: " + uc_id)
#     if uc_dict["mode"] == "potential":
#         num_home = 1000000
#         energy_sum = 1
#     else:
#         ts_dict = uc_dict["timeseries"]
#         load = ts_dict[uc_dict["key"]].loc[:, "sum home"]
#         load_sum = load.sum()
#         energy_sum = load_sum * timestep / 60
#         if len(car_num.index) == 1:
#             car_sum = sum(car_num.at["single_region"].values())
#         else:
#             car_sum = sum(car_num.at[uc_dict["key"]].values())
#         num_home = math.ceil(car_sum * home_charge_prob)
#
#     if num_home > 0:
#         # filter houses by region
#         in_region_bool = home_data["geometry"].within(uc_dict["region"].loc[0])
#         in_region = home_data.loc[in_region_bool].copy()
#         in_region[["num", "num_mfh"]] = in_region[["num", "num_mfh"]].fillna(
#             value=0
#         )
#         potential = uc_helpers.apportion_home(in_region, num_home, uc_dict)
#         in_region["charge_spots"] = potential
#         in_region = in_region.loc[in_region["charge_spots"] > 0]
#         in_region["energy"] = energy_sum * in_region["charge_spots"] / num_home
#         in_region = in_region.sort_values(by="energy", ascending=False)
#         # in_region = in_region.iloc[:num_home]
#         # in_region = in_region.assign(energy=energy_sum/num_home)
#         # outputs
#         print(round(energy_sum, 1), "kWh got charged in region")
#         cols = ["geometry", "charge_spots", "energy"]
#         utility.save(in_region, uc_id, cols, uc_dict)
#
#
# def work(landuse, weights_dict, uc_dict, timestep=15):
#     """
#     Calculate placements and energy distribution for use case hpc.
#
#     :param landuse: gpd.GeoDataFrame
#         work areas by land use
#     :param weights_dict: dict
#         weights for different land use types
#     :param uc_dict: dict
#         contains basic run info like region boundary and save directory
#     :param timestep: int
#         time step of the simbev input series, default: 15 (minutes)
#     """
#     uc_id = "work"
#     print("Use case: " + uc_id)
#     if uc_dict["mode"] == "potential":
#         energy_sum = 1
#     else:
#         ts_dict = uc_dict["timeseries"]
#         load = ts_dict[uc_dict["key"]].loc[:, "sum work"]
#         load_sum = load.sum()
#         energy_sum = load_sum * timestep / 60
#
#     in_region_bool = landuse.within(uc_dict["region"].loc[0])
#     in_region = landuse[in_region_bool].copy()
#     # calculating the area of polygons
#     in_region["area"] = in_region["geometry"].area / 10**6
#     groups = in_region.groupby("landuse")
#     group_labels = ["retail", "commercial", "industrial"]
#     result = gpd.GeoDataFrame(
#         columns=["geometry", "landuse", "potential"], crs="EPSG:3035"
#     )
#     for g in group_labels:
#         if g in groups.groups:
#             group = groups.get_group(g)
#             group = group.assign(
#                 potential=group["geometry"].area * weights_dict[g]
#             )
#             group.to_crs(3035)
#             result = gpd.GeoDataFrame(
#                 pd.concat([result, group]), crs="EPSG:3035"
#             )
#
#     result["energy"] = (
#         result["potential"] * energy_sum / result["potential"].sum()
#     )
#     # outputs
#     print(round(energy_sum, 1), "kWh got charged in region")
#     cols = ["geometry", "landuse", "potential", "energy"]
#     utility.save(result, uc_id, cols, uc_dict)
