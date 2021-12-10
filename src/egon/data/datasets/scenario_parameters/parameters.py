"""The module containing all parameters for the scenario table
"""
from urllib.request import urlretrieve
import zipfile
import pandas as pd

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
        parameters = {"weather_year": 2011, "population_year": 2035}

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


def download_pypsa_technology_data():
    """Downlad PyPSA technology data results."""
    data_path = Path(".") / "pypsa_technology_data"
    # Delete folder if it already exists
    if data_path.exists() and data_path.is_dir():
        shutil.rmtree(data_path)
    # Get parameters from config and set download URL
    sources = egon.data.config.datasets()["pypsa-technology-data"]["sources"]["zenodo"]
    url = f"""https://zenodo.org/record/{sources['deposit_id']}/files/{sources['file']}"""
    target_file = egon.data.config.datasets()["pypsa-technology-data"]["targets"]["file"]

    # Retrieve files
    urlretrieve(url, target_file)

    with zipfile.ZipFile(target_file, "r") as zip_ref:
        zip_ref.extractall(".")


def insert_pypsa_technology_data(scn_name):
    """Insert the technology data from pypsa into the scenario table.

    Parameters
    ----------
    scn_name : str
        Name of the scenario.
    """
    if scn_name == "eGon2035":
        data = '2035'

    elif scn_name == "eGon100RE":
        data = '2050'

    else:
        print(f"Scenario name {scn_name} is not valid.")

    df = pd.read_csv(egon.data.config.datasets()["pypsa-technology-data"]["targets"]["data_dir"] + 'costs_' + data + '.csv')
