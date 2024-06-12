"""The module containing all code dealing with geothermal potentials and costs

Main source: Ableitung eines Korridors für den Ausbau
der erneuerbaren Wärme im Gebäudebereich, Beuth Hochschule für Technik
Berlin ifeu – Institut für Energie- und Umweltforschung Heidelberg GmbH
Februar 2017

"""
from pathlib import Path

import numpy as np

import geopandas as gpd
import pandas as pd
from egon.data import config, db


def calc_geothermal_potentials():
    # Set parameters
    ## specific thermal capacity of water in kJ/kg*K (p. 95)
    c_p = 4
    ## full load hours per year in h (p. 95)
    flh = 3000
    ## mass flow per reservoir in kg/s (p. 95)
    m_flow = pd.Series(data={"NDB": 35, "ORG": 90, "SMB": 125}, name="m_flow")

    ## geothermal potentials per temperature (p. 94)
    file_path = (
        Path(".")
        / "data_bundle_egon_data"
        / "geothermal_potential"
        / "geothermal_potential_germany.shp"
    )
    potentials = gpd.read_file(file_path)
    ## temperature heating system in °C (p. 95)
    sys_temp = 60
    ## temeprature losses heat recuperator in °C (p. 95)
    loss_temp = 5

    # calc mean temperatures per region (p. 93/94):
    potentials["mean_temperature"] = potentials["min_temper"] + 15

    # exclude regions with mean_temp < 60°C (p. 93):
    potentials = potentials[potentials.mean_temperature >= 60]

    # exclude regions outside of NDB, ORG or SMB because of missing mass flow
    potentials = potentials[~potentials.reservoir.isnull()]

    ## set mass flows per region
    potentials["m_flow"] = potentials.join(m_flow, on="reservoir").m_flow

    # calculate flow in kW
    potentials["Q_flow"] = (
        potentials.m_flow
        * c_p
        * (potentials.mean_temperature - loss_temp - sys_temp)
    )

    potentials["Q"] = potentials.Q_flow * flh

    return potentials


def calc_geothermal_costs(max_costs=np.inf, min_costs=0):
    # Set parameters
    ## drilling depth per reservoir in m (p. 99)
    depth = pd.Series(
        data={"NDB": 2500, "ORG": 3400, "SMB": 2800}, name="depth"
    )
    ## drillings costs in EUR/m (p. 99)
    depth_costs = 1500
    ## ratio of investment costs to drilling costs  (p. 99)
    ratio = 1.4
    ## annulazaion factors
    p = 0.045
    T = 30
    PVA = 1 / p - 1 / (p * (1 + p) ** T)

    # calculate overnight investment costs per drilling and region
    overnight_investment = depth * depth_costs * ratio
    investment_per_year = overnight_investment / PVA

    # investment costs per well according to p.99
    costs = pd.Series(
        data={"NDB": 12.5e6, "ORG": 17e6, "SMB": 14e6}, name="costs"
    )

    potentials = calc_geothermal_potentials()

    potentials["cost_per_well"] = potentials.join(costs, on="reservoir").costs

    potentials["cost_per_well_mw"] = (
        potentials.cost_per_well / 1000 / potentials.Q_flow
    )

    potentials = potentials.to_crs(3035)

    # area weighted mean costs per well and mw
    np.average(potentials.cost_per_well_mw, weights=potentials.area)

    return potentials[
        (potentials["cost_per_well_mw"] <= max_costs)
        & (potentials["cost_per_well_mw"] > min_costs)
    ]


def calc_usable_geothermal_potential(max_costs=2, min_costs=0):
    """ Calculate geothermal potentials close to district heating demands

    Parameters
    ----------
    max_costs : float, optional
        Maximum accepted costs for geo thermal in EUR/MW_th. The default is 2.
    min_costs : float, optional
        Minimum accepted costs for geo thermal in EUR/MW_th. The default is 0.

    Returns
    -------
    float
        Geothermal potential close to district heating areas in MW

    """
    sources = config.datasets()["heat_supply"]["sources"]

    # Select 1km buffer arround large district heating areas as possible areas
    district_heating = db.select_geodataframe(
        f"""
        SELECT area_id,
        residential_and_service_demand as demand,
        ST_Difference(
            ST_Buffer(geom_polygon, 1000), geom_polygon) as geom
        FROM {sources['district_heating_areas']['schema']}.
        {sources['district_heating_areas']['table']}
        WHERE scenario = 'eGon100RE'
        AND residential_and_service_demand > 96000
        """,
        index_col="area_id",
    )

    # Select geothermal potential areas where investments costs per MW
    # are in given range
    geothermal_potential = calc_geothermal_costs(
        max_costs=max_costs, min_costs=min_costs
    )

    # Intersect geothermal potential areas with district heating areas:
    # geothermal will be build only if demand of a large district heating
    # grid is close
    overlay = gpd.overlay(district_heating.reset_index(), geothermal_potential)

    if len(overlay) > 0:

        # Calculate available area for geothermal power plants
        overlay["area_sqkm"] = overlay.area * 1e-6

        # Assmue needed area per well
        pw_km = 0.25

        # Calculate number of possible wells per intersecting area
        overlay["number_wells"] = overlay["area_sqkm"].mul(pw_km)

        # Calculate share of overlaying areas per district heating grid
        overlay["area_share"] = (
            overlay.groupby("area_id")
            .apply(lambda grp: grp.area_sqkm / grp.area_sqkm.sum())
            .values
        )

        # Possible installable capacity per intersecting area
        overlay["Q_per_area"] = overlay.Q_flow.mul(overlay.number_wells)

        # Prepare geothermal potenital per district heating area
        gt_potential_dh = pd.DataFrame(index=district_heating.index)
        gt_potential_dh["demand"] = district_heating.demand

        # Group intersecting areas by district heating area
        grouped = overlay[
            overlay.area_id.isin(
                gt_potential_dh[
                    gt_potential_dh.index.isin(overlay.area_id)
                ].index
            )
        ].groupby(overlay.area_id)

        # Calculate geo thermal capacity per district heating area
        gt_potential_dh["Q_flow"] = grouped.Q_per_area.sum() / 1000
        gt_potential_dh["installed_MW"] = gt_potential_dh["Q_flow"]

        # Demand resitriction: If technical potential exceeds demand of
        # district heating area, reduce potential according to demand
        idx_demand_restriction = (
            gt_potential_dh["Q_flow"] * 3000 > gt_potential_dh["demand"]
        )
        gt_potential_dh.loc[idx_demand_restriction, "installed_MW"] = (
            gt_potential_dh.loc[idx_demand_restriction, "demand"] / 3000
        )

        print(
            f"""Geothermal potential in Germany:
              {round(gt_potential_dh["Q_flow"].sum()/1000, 3)} GW_th"""
        )
        print(
            f"""
            Geothermal potential in Germany close to large district heating:
                {round(gt_potential_dh['installed_MW'].sum()/1000, 3)} GW_th
            """
        )

        return gt_potential_dh["installed_MW"].sum()
    else:
        return 0


def potential_germany():
    """Calculates geothermal potentials for different investment costs.

    The investment costs for geothermal district heating highly depend on
    the location because of different mass flows and drilling depths.
    This functions calcultaes the geothermal potentials close to germany
    for five different costs ranges.
    This data can be used in pypsa-eur-sec to optimise the share of
    geothermal district heating by considering different investment costs.

    Returns
    -------
    None.

    """
    geothermal_costs_and_potentials = pd.Series(index=[0.5, 1, 2, 5, 10])

    geothermal_costs_and_potentials[0.5] = calc_usable_geothermal_potential(
        max_costs=0.5, min_costs=0
    )

    geothermal_costs_and_potentials[1] = calc_usable_geothermal_potential(
        max_costs=1, min_costs=0.5
    )

    geothermal_costs_and_potentials[2] = calc_usable_geothermal_potential(
        max_costs=2, min_costs=1
    )

    geothermal_costs_and_potentials[5] = calc_usable_geothermal_potential(
        max_costs=5, min_costs=2
    )

    geothermal_costs_and_potentials[10] = calc_usable_geothermal_potential(
        max_costs=10, min_costs=5
    )

    pd.DataFrame(geothermal_costs_and_potentials).reset_index().rename(
        {"index": "cost [EUR/kW]", 0: "potential [MW]"}, axis=1
    ).to_csv("geothermal_potential_germany.csv")
