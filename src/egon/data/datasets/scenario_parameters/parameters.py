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
        parameters = {
            "grid_topology": "Status Quo",
            "re_marginal_cost_fixed": 0,
            "phes_efficiency_store": 0.88, # according to Acatech2015
            "phes_efficiency_dispatch": 0.89, # according to Acatech2015
            "phes_standing_loss": 0.00052, # according to Acatech2015
            "phes_max_hours": 6, # max_hours as an average for existing German PSH, as in open_eGo
            "phes_control": "PV",
            "phes_p_nom_extendable": False

                      }

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
    """Returns parameters of the mobility sector for the selected scenario.

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
        parameters = {
            "motorized_individual_travel": {
                "NEP C 2035": {
                    "ev_count": 14000000,
                    "bev_mini_share": 0.1589,
                    "bev_medium_share": 0.3533,
                    "bev_luxury_share": 0.1053,
                    "phev_mini_share": 0.0984,
                    "phev_medium_share": 0.2189,
                    "phev_luxury_share": 0.0652
                }
            }
        }

    elif scenario == "eGon100RE":
        parameters = {
            "motorized_individual_travel": {
                "Reference 2050": {
                    "ev_count": 25065000,
                    "bev_mini_share": 0.1589,
                    "bev_medium_share": 0.3533,
                    "bev_luxury_share": 0.1053,
                    "phev_mini_share": 0.0984,
                    "phev_medium_share": 0.2189,
                    "phev_luxury_share": 0.0652
                },
                "Mobility Transition 2050": {
                    "ev_count": 37745000,
                    "bev_mini_share": 0.1589,
                    "bev_medium_share": 0.3533,
                    "bev_luxury_share": 0.1053,
                    "phev_mini_share": 0.0984,
                    "phev_medium_share": 0.2189,
                    "phev_luxury_share": 0.0652
                },
                "Electrification 2050": {
                    "ev_count": 47700000,
                    "bev_mini_share": 0.1589,
                    "bev_medium_share": 0.3533,
                    "bev_luxury_share": 0.1053,
                    "phev_mini_share": 0.0984,
                    "phev_medium_share": 0.2189,
                    "phev_luxury_share": 0.0652
                },
            }
        }

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
