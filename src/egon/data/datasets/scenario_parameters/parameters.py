"""The module containing all parameters for the scenario table
"""


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
