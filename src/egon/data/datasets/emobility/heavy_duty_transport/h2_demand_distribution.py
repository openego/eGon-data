from geovoronoi import points_to_coords, voronoi_regions_from_coords
from loguru import logger
from shapely import wkt
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.polygon import Polygon
from shapely.ops import cascaded_union
import geopandas as gpd

from egon.data.datasets.emobility.heavy_duty_transport import DATASET_CFG
from egon.data.datasets.emobility.heavy_duty_transport.data_io import get_data


def run_egon_truck(
    scenario: str = "eGon2035",
):
    germany_gdf, bast_gdf, grid_districts_gdf = get_data()

    bast_gdf_within = bast_gdf.dropna().loc[
        bast_gdf.within(germany_gdf.geometry.iat[0])
    ]

    voronoi_gdf = voronoi(bast_gdf_within, germany_gdf)

    grid_districts_gdf = geo_intersect(voronoi_gdf, grid_districts_gdf)

    total_hydrogen_consumption = calculate_total_hydrogen_consumption(
        scenario=scenario
    )

    grid_districts = grid_districts_gdf.assign(
        normalized_truck_traffic=(
            grid_districts_gdf.truck_traffic
            / grid_districts_gdf.truck_traffic.sum()
        )
    )

    grid_districts = grid_districts.assign(
        hydrogen_consumption=(
            grid_districts.normalized_truck_traffic
            * total_hydrogen_consumption
        )
    )


def calculate_total_hydrogen_consumption(scenario: str = "eGon2035"):
    """Calculate the total hydrogen demand for trucking in Germany"""
    constants = DATASET_CFG["constants"]
    hgv_mileage = DATASET_CFG["    hgv_mileage"]

    leakage = constants["leakage"]
    leakage_rate = constants["leakage_rate"]
    hydrogen_consumption = constants["hydrogen_consumption"]  # kg/100km
    fcev_share = constants["fcev_share"]

    hgv_mileage = hgv_mileage[scenario]  # km

    hydrogen_consumption_per_km = hydrogen_consumption / 100  # kg/km

    # calculate total hydrogen consumption kg/a
    if leakage:
        return (
            hgv_mileage
            * hydrogen_consumption_per_km
            * fcev_share
            / (1 - leakage_rate)
        )
    else:
        return hgv_mileage * hydrogen_consumption_per_km * fcev_share


def geo_intersect(
    voronoi_gdf: gpd.GeoDataFrame,
    grid_districts: gpd.GeoDataFrame,
    mode: str = "intersection",
):
    """Calculate Intersections between two GeoDataFrames and distribute truck traffic"""
    logger.info(
        "Calculating Intersections between Voronoi Field and Grid Districts "
        "and distributing truck traffic accordingly to the area share."
    )
    voronoi_gdf = voronoi_gdf.assign(voronoi_id=voronoi_gdf.index.tolist())

    # Find Intersections between both GeoDataFrames
    intersection_gdf = gpd.overlay(
        voronoi_gdf, grid_districts[["subst_id", "geometry"]], how=mode
    )

    # Calc Area of Intersections
    intersection_gdf = intersection_gdf.assign(
        surface_area=intersection_gdf.geometry.area / 10**6
    )  # km²

    # Initialize results column
    grid_districts = grid_districts.assign(truck_traffic=0)

    grid_districts.index = grid_districts.subst_id.tolist()

    for voronoi_id in intersection_gdf.voronoi_id.unique():
        voronoi_id_intersection_gdf = intersection_gdf.loc[
            intersection_gdf.voronoi_id == voronoi_id
        ]

        total_area = voronoi_id_intersection_gdf.surface_area.sum()

        truck_traffic = voronoi_id_intersection_gdf.truck_traffic.iat[0]

        for idx, row in voronoi_id_intersection_gdf.iterrows():
            traffic_share = truck_traffic * row["surface_area"] / total_area

            grid_districts.at[
                row["subst_id"], "truck_traffic"
            ] += traffic_share

    logger.info("Done.")

    return grid_districts


def voronoi(
    points: gpd.GeoDataFrame,
    boundary: gpd.GeoDataFrame,
):
    """Building a Voronoi Field from points and a boundary"""
    logger.info("Building Voronoi Field.")

    sources = DATASET_CFG["original_data"]["sources"]
    relevant_columns = sources["BAST"]["relevant_columns"]
    truck_col = relevant_columns[0]
    srid = DATASET_CFG["tables"]["srid"]

    # convert the boundary geometry into a union of the polygon
    # convert the Geopandas GeoSeries of Point objects to NumPy array of coordinates.
    boundary_shape = cascaded_union(boundary.geometry)
    coords = points_to_coords(points.geometry)

    # calculate Voronoi regions
    poly_shapes, pts, unassigned_pts = voronoi_regions_from_coords(
        coords, boundary_shape, return_unassigned_points=True
    )

    multipoly_shapes = {}

    for key, shape in poly_shapes.items():
        if isinstance(shape, Polygon):
            shape = wkt.loads(str(shape))
            shape = MultiPolygon([shape])

        multipoly_shapes[key] = [shape]

    poly_gdf = gpd.GeoDataFrame.from_dict(
        multipoly_shapes, orient="index", columns=["geometry"]
    )

    # match points to old index
    # FIXME: This seems overcomplicated
    poly_gdf.index = [v[0] for v in pts.values()]

    poly_gdf = poly_gdf.sort_index()

    unmatched = [points.index[idx] for idx in unassigned_pts]

    points_matched = points.drop(unmatched)

    poly_gdf.index = points_matched.index

    # match truck traffic to new polys
    poly_gdf = poly_gdf.assign(
        truck_traffic=points.loc[poly_gdf.index][truck_col]
    )

    poly_gdf = poly_gdf.to_crs(epsg=srid)

    logger.info("Done.")

    return poly_gdf
