"""The module containing all parameters for the scenario table
"""
from urllib.request import urlretrieve
import zipfile

import egon.data.config
from pathlib import Path
import shutil


def global_settings(scenario):
    """Returns global paramaters for the selected scenario.

    Parameters
    ----------
    scenario : str
        Name of the scenario.

    Returns
    -------
    parameters : dict
        List of global parameters

    """

    if scenario == "eGon2035":
        parameters = {
            "weather_year": 2011,
            "population_year": 2035,
            "fuel_costs": { # Netzentwicklungsplan Strom 2035, Version 2021, 1. Entwurf, p. 39, table 6
                "oil": 73.8, # [EUR/MWh]
                "gas": 25.6, # [EUR/MWh]
                "coal": 20.2, # [EUR/MWh]
                "lignite": 4.0, # [EUR/MWh]
                "nuclear": 1.7, # [EUR/MWh]
            },
            "co2_costs": 76.5, # [EUR/t_CO2]
            "co2_emissions":{ # Netzentwicklungsplan Strom 2035, Version 2021, 1. Entwurf, p. 40, table 8
                "waste": 0.165, # [t_CO2/MW_th]
                "lignite": 0.393, # [t_CO2/MW_th]
                "gas": 0.201, # [t_CO2/MW_th]
                "nuclear": 0.0, # [t_CO2/MW_th]
                "oil": 0.288, # [t_CO2/MW_th]
                "coal": 0.335, # [t_CO2/MW_th]
                "other_non_renewable": 0.268 # [t_CO2/MW_th]
                }
           }

    elif scenario == "eGon100RE":
        parameters = {"weather_year": 2011, "population_year": 2050}

    else:
        print(f"Scenario name {scenario} is not valid.")

    return parameters


def electricity(scenario):
    """Returns paramaters of the electricity sector for the selected scenario.

    Parameters
    ----------
    scenario : str
        Name of the scenario.

    Returns
    -------
    parameters : dict
        List of parameters of electricity sector

    """

    if scenario == "eGon2035":
        parameters = {"grid_topology": "Status Quo"}

    elif scenario == "eGon100RE":
        parameters = {"grid_topology": "Status Quo"}

    else:
        print(f"Scenario name {scenario} is not valid.")

    return parameters


def gas(scenario):
    """Returns paramaters of the gas sector for the selected scenario.

    Parameters
    ----------
    scenario : str
        Name of the scenario.

    Returns
    -------
    parameters : dict
        List of parameters of gas sector

    """

    if scenario == "eGon2035":
        parameters = {}

    elif scenario == "eGon100RE":
        parameters = {}

    else:
        print(f"Scenario name {scenario} is not valid.")

    return parameters


def mobility(scenario):
    """Returns paramaters of the mobility sector for the selected scenario.

    Parameters
    ----------
    scenario : str
        Name of the scenario.

    Returns
    -------
    parameters : dict
        List of parameters of mobility sector

    """

    if scenario == "eGon2035":
        parameters = {}

    elif scenario == "eGon100RE":
        parameters = {}

    else:
        print(f"Scenario name {scenario} is not valid.")

    return parameters


def heat(scenario):
    """Returns paramaters of the heat sector for the selected scenario.

    Parameters
    ----------
    scenario : str
        Name of the scenario.

    Returns
    -------
    parameters : dict
        List of parameters of heat sector

    """

    if scenario == "eGon2035":
        parameters = {
            "DE_demand_reduction_residential": 0.854314018923104,
            "DE_demand_reduction_service": 0.498286864771128,
            "DE_district_heating_share": 0.14,
        }

    elif scenario == "eGon100RE":
        parameters = {
            "DE_demand_reduction_residential": 0.640720648501849,
            "DE_demand_reduction_service": 0.390895195300713,
            "DE_district_heating_share": 0.19,
        }

    else:
        print(f"Scenario name {scenario} is not valid.")

    return parameters
