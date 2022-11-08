"""
Created on Tue May 24 14:42:05 2022
Plotdatascn.py defines functions to plot to provide a better context of the different parameters part of
scenarios eGon2035 and eGon100RE .
@author: Alonso

"""
import logging
import os

from matplotlib import pyplot as plt
import geopandas as gpd
import matplotlib as mpl
import pandas as pd

from egon.data import db

logger = logging.getLogger(__name__)

if "READTHEDOCS" not in os.environ:
    from geoalchemy2.shape import to_shape

__copyright__ = (
    "Flensburg University of Applied Sciences, "
    "Europa-Universität Flensburg, "
    "Centre for Sustainable Energy Systems, "
    "DLR-Institute for Networked Energy Systems"
)
__license__ = ""
__author__ = ""


def plot_installedcapacity(carrier, scenario="eGon2035"):
    """
    Plots color maps according to the capacity of different generators
    of the two existing scenarios (eGon2035 and eGon100RE)


    Parameters
    ----------
    carrier : generators
    The list of generators: biomass, central_biomass_CHP, central_biomass_CHP_heat,
    industrial_biomass_CHP, solar, solar_rooftop, wind_offshore, wind_onshore.

    scenario: eGon2035, eGon100RE

    Returns
    ----------
    """

    # This function must be called while in the folder
    # that contains the file egon-data.configuration.yaml.
    con = db.engine()
    # imports buses of Germany
    SQLBus = (
        "SELECT bus_id, country FROM grid.egon_etrago_bus WHERE country='DE'"
    )
    busDE = pd.read_sql(SQLBus, con)
    busDE = busDE.rename({"bus_id": "bus"}, axis=1)
    # Imports grid districs
    sql = "SELECT bus_id, geom FROM grid.egon_mv_grid_district"
    distr = gpd.GeoDataFrame.from_postgis(sql, con)
    distr = distr.rename({"bus_id": "bus"}, axis=1)
    distr = distr.set_index("bus")
    # merges grid districts with buses
    distr = pd.merge(busDE, distr, on="bus")
    # Imports generator
    sqlCarrier = "SELECT carrier, p_nom, bus FROM grid.egon_etrago_generator"
    sqlCarrier = "SELECT * FROM grid.egon_etrago_generator"
    Carriers = pd.read_sql(sqlCarrier, con)
    Carriers = Carriers.loc[Carriers["scn_name"] == scenario]
    Carriers = Carriers.set_index("bus")

    CarrierGen = Carriers.loc[Carriers["carrier"] == carrier]
    # merges districts with generators
    Merge = pd.merge(CarrierGen, distr, on="bus", how="outer")

    Merge.loc[Merge["carrier"] != carrier, "p_nom"] = 0
    Merge.loc[Merge["country"] != "DE", "p_nom"] = 0

    gdf = gpd.GeoDataFrame(Merge, geometry="geom")
    pnom = gdf["p_nom"]  #
    # 0.95 quantile is used to filter values that are too high and make noise in the plots.
    max_pnom = pnom.quantile(0.95)
    gdf = gdf.to_crs(epsg=3857)

    fig, ax = plt.subplots(1, 1)

    ax.set_axis_off()
    plt.title(f" {carrier} installed capacity in MW , {scenario}")
    cmap = mpl.cm.coolwarm

    norm = mpl.colors.Normalize(vmin=0, vmax=max_pnom)
    gdf.plot(
        column="p_nom",
        ax=ax,
        legend=False,
        legend_kwds={"label": "p_nom(MW)", "orientation": "vertical"},
        cmap=cmap,
        norm=norm,
        edgecolor="black",
        linewidth=0.1,
        zorder=2,
    )
    scatter = ax.collections[0]
    cbar = plt.colorbar(scatter, ax=ax, extend="max")
    cbar.set_label("p_nom(MW)", rotation=90)

    return
