# -*- coding: utf-8 -*-

# This script is part of eGon-data.

# license text - to be added.

"""
Module containing all code creating with plots of district heating areas
"""
import os
from egon.data.datasets.scenario_parameters import get_sector_parameters
import pandas as pd
from matplotlib import pyplot as plt

# heat_denisty_per_scenario = {}
# heat_denisty_per_scenario['eGon2035'] = district_heating_areas(
#     'eGon2035', plotting = True)
# heat_denisty_per_scenario['eGon100RE'] = district_heating_areas(
#     'eGon100RE', plotting = True)

# if plotting:

#     from egon.data.processing.district_heating_areas.plot import (
#         plot_heat_density_sorted)
#     plot_heat_density_sorted({scenario_name:collection}, scenario_name )
def plot_heat_density_sorted(heat_denisty_per_scenario, scenario_name=None):
    """
    Create diagrams for visualisation, sorted by HDD
    sorted census dh first, sorted new areas, left overs, DH share
    create one dataframe with all data: first the cells with existing,
    then the cells with new district heating systems and in the end the
    ones without

    Parameters
    ----------
    scenario_name : TYPE
        DESCRIPTION.
    collection : TYPE
        DESCRIPTION.

    Returns
    -------
    None.

    """

    # create directory to store files
    results_path = "district_heating_areas/"

    if not os.path.exists(results_path):
        os.mkdir(results_path)

    fig, ax = plt.subplots(1, 1)

    colors = pd.DataFrame(
        columns=["share", "curve"], index=["status2019", "status2023", "eGon2035",
                                           "nep2037_2025",
                                           "eGon100RE"]
    )

    colors["share"]["eGon2035"] = "darkblue"
    colors["curve"]["eGon2035"] = "blue"
    colors["share"]["nep2037_2025"] = "brown"
    colors["curve"]["nep2037_2025"] = "pink"
    colors["share"]["eGon100RE"] = "red"
    colors["curve"]["eGon100RE"] = "orange"
    colors["share"]["status2019"] = "darkgreen"
    colors["curve"]["status2019"] = "green"
    colors["share"]["status2023"] = "darkgrey"
    colors["curve"]["status2023"] = "grey"
    # TODO status2023 set plotting=False?

    for scenario in heat_denisty_per_scenario.keys():

        heat_parameters = get_sector_parameters("heat", scenario=scenario)

        district_heating_share = heat_parameters["DE_district_heating_share"]
        collection = heat_denisty_per_scenario[scenario]

        total_district_heat = (
            district_heating_share
            * collection.residential_and_service_demand.sum()
        )

        # add the district heating share as a line
        procent = round(district_heating_share * 100, 0)
        plt.axvline(
            x=total_district_heat / 1000000,
            ls="--",
            lw=0.5,
            label=(f"{scenario}: District Heating Share of {procent} %"),
            color=colors["share"][scenario],
        )
        # add the sorted heat demand density curve

        ax.plot(
            collection.Cumulative_Sum,
            collection.residential_and_service_demand,
            label=f"{scenario}: Heat demand densities, sorted",
            color=colors["curve"][scenario],
        )

        # annotations
        x1 = total_district_heat / 1000000 / 2
        x2 = x1 * 4
        # x2 = (total_district_heat + ((heat_demand_cells[
        #     'residential_and_service_demand'].sum() -
        #     total_district_heat) / 2)) / 1000000
        y = collection.residential_and_service_demand.max() * 0.7

        ax.text(
            x1,
            y,
            "District\nheat",
            ha="center",
            va="center",
            size=8,
            bbox=dict(
                boxstyle="round, pad=0.5",
                fc="none",
                ec=colors["share"][scenario],  # lw=2
            ),
        )
        ax.text(
            x2,
            y,
            "Individual\nheat supply",
            ha="center",
            va="center",
            size=8,
            bbox=dict(
                boxstyle="round, pad=0.5",
                fc="none",
                ec=colors["share"][scenario],  # lw=2
            ),
        )

    if scenario_name:
        ax.set(title=("Heat Sector in " + scenario_name))
    ax.set_xlabel("Cumulative Heat Demand [TWh / a]")
    ax.set_ylabel("Heat Demand Densities [MWh / (ha a)]")

    # remove empty space between axis and graph
    ax.margins(x=0, y=0)
    # axes style
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.plot(1, 0, ">k", transform=ax.get_yaxis_transform(), clip_on=False)
    ax.plot(0, 1, "^k", transform=ax.get_xaxis_transform(), clip_on=False)
    ax.legend()  # or: plt.legend()
    if scenario_name:
        plt.savefig(
            results_path + f"HeatDemandDensities_Curve_{scenario_name}.png"
        )
    else:
        plt.savefig(results_path + "HeatDemandDensities_Curves.png")
