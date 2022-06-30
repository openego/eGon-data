from shapely.geometry import Point
import geopandas as gpd
import numpy as np
import pandas as pd


def random_point_in_square(geom, tol):
    """
    Generate a random point within a square

    Parameters
    ----------
    geom: gpd.Series
        Geometries of square
    tol: int
        tolerance to square bounds

    Returns
    -------
    points:gpd.Series
        Series of random points
    """
    # cell bounds - half edge_length to not build buildings on the cell border
    xmin = geom.bounds["minx"] + tol / 2
    xmax = geom.bounds["maxx"] - tol / 2
    ymin = geom.bounds["miny"] + tol / 2
    ymax = geom.bounds["maxy"] - tol / 2

    # generate random coordinates within bounds - half edge_length
    x = (xmax - xmin) * np.random.rand(geom.shape[0]) + xmin
    y = (ymax - ymin) * np.random.rand(geom.shape[0]) + ymin

    points = pd.Series([Point(cords) for cords in zip(x, y)])
    points = gpd.GeoSeries(points, crs="epsg:3035")

    return points
