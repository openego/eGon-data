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


# distribute amenities evenly
def specific_int_until_sum(s_sum, i_int):
    """
    Generate list `i_int` summing to `s_sum`. Last value will be <= `i_int`
    """
    list_i = [] if [s_sum % i_int] == [0] else [s_sum % i_int]
    list_i += s_sum // i_int * [i_int]
    return list_i


def random_ints_until_sum(s_sum, m_max):
    """
    Generate non-negative random integers < `m_max` summing to `s_sum`.
    """
    list_r = []
    while s_sum > 0:
        r = np.random.randint(1, m_max + 1)
        r = r if r <= m_max and r < s_sum else s_sum
        list_r.append(r)
        s_sum -= r
    return list_r
