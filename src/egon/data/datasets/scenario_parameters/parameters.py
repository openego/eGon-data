"""The module containing all parameters for the scenario table
"""

import pandas as pd

import egon.data.config


def read_csv(year):
    source = egon.data.config.datasets()["pypsa-technology-data"]["targets"][
        "data_dir"
    ]

    return pd.read_csv(f"{source}costs_{year}.csv")


def read_costs(df, technology, parameter, value_only=True):
    result = df.loc[
        (df.technology == technology) & (df.parameter == parameter)
    ].squeeze()

    # Rescale costs to EUR/MW
    if "EUR/kW" in result.unit:
        result.value *= 1e3
        result.unit = result.unit.replace("kW", "MW")

    if value_only:
        return result.value
    else:
        return result


def annualize_capital_costs(overnight_costs, lifetime, p):
    """

    Parameters
    ----------
    overnight_costs : float
        Overnight investment costs in EUR/MW or EUR/MW/km
    lifetime : int
        Number of years in which payments will be made
    p : float
        Interest rate in p.u.

    Returns
    -------
    float
        Annualized capital costs in EUR/MW/a or EUR/MW/km/a

    """

    # Calculate present value of an annuity (PVA)
    PVA = (1 / p) - (1 / (p * (1 + p) ** lifetime))

    return overnight_costs / PVA


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
            "fuel_costs": {  # Netzentwicklungsplan Strom 2035, Version 2021, 1. Entwurf, p. 39, table 6
                "oil": 73.8,  # [EUR/MWh]
                "gas": 25.6,  # [EUR/MWh]
                "coal": 20.2,  # [EUR/MWh]
                "lignite": 4.0,  # [EUR/MWh]
                "nuclear": 1.7,  # [EUR/MWh]
                "biomass": 40,  # Dummyvalue, ToDo: Find a suitable source
            },
            "co2_costs": 76.5,  # [EUR/t_CO2]
            "co2_emissions": {  # Netzentwicklungsplan Strom 2035, Version 2021, 1. Entwurf, p. 40, table 8
                "waste": 0.165,  # [t_CO2/MW_th]
                "lignite": 0.393,  # [t_CO2/MW_th]
                "gas": 0.201,  # [t_CO2/MW_th]
                "nuclear": 0.0,  # [t_CO2/MW_th]
                "oil": 0.288,  # [t_CO2/MW_th]
                "coal": 0.335,  # [t_CO2/MW_th]
                "other_non_renewable": 0.268,  # [t_CO2/MW_th]
            },
            "interest_rate": 0.05,  # [p.u.]
        }

    elif scenario == "eGon100RE":
        parameters = {
            "weather_year": 2011,
            "population_year": 2050,
            "fuel_costs": {  # Netzentwicklungsplan Strom 2035, Version 2021, 1. Entwurf, p. 39, table 6
                "oil": 73.8,  # [EUR/MWh]
                "gas": 25.6,  # [EUR/MWh]
                "coal": 20.2,  # [EUR/MWh]
                "lignite": 4.0,  # [EUR/MWh]
                "nuclear": 1.7,  # [EUR/MWh]
            },
            "co2_costs": 76.5,  # [EUR/t_CO2]
            "co2_emissions": {  # Netzentwicklungsplan Strom 2035, Version 2021, 1. Entwurf, p. 40, table 8
                "waste": 0.165,  # [t_CO2/MW_th]
                "lignite": 0.393,  # [t_CO2/MW_th]
                "gas": 0.201,  # [t_CO2/MW_th]
                "nuclear": 0.0,  # [t_CO2/MW_th]
                "oil": 0.288,  # [t_CO2/MW_th]
                "coal": 0.335,  # [t_CO2/MW_th]
                "other_non_renewable": 0.268,  # [t_CO2/MW_th]
            },
            "interest_rate": 0.05,  # [p.u.]
        }

    elif scenario == "eGon2021":
        parameters = {
            "weather_year": 2011,
            "population_year": 2021,
        }
    elif scenario == "status2019":
        parameters = {
            "weather_year": 2019,
            "population_year": 2019,
            "fuel_costs": {  # TYNDP 2020, data for 2020 (https://2020.entsos-tyndp-scenarios.eu/fuel-commodities-and-carbon-prices/)
                "oil": 12.9*3.6,  # [EUR/MWh]
                "gas": 5.6*3.6,  # [EUR/MWh]
                "coal": 3.0*3.6,  # [EUR/MWh]
                "lignite": 1.1*3.6,  # [EUR/MWh]
                "nuclear": 0.47*3.6,  # [EUR/MWh]
                "biomass": read_costs(read_csv(2020), "biomass", "fuel"),
            },
            "co2_costs": 24.7,  # [EUR/t_CO2], source: 
                #https://de.statista.com/statistik/daten/studie/1304069/umfrage/preisentwicklung-von-co2-emissionsrechten-in-eu/
            "co2_emissions": {  # Netzentwicklungsplan Strom 2035, Version 2021, 1. Entwurf, p. 40, table 8
                "waste": 0.165,  # [t_CO2/MW_th]
                "lignite": 0.393,  # [t_CO2/MW_th]
                "gas": 0.201,  # [t_CO2/MW_th]
                "nuclear": 0.0,  # [t_CO2/MW_th]
                "oil": 0.288,  # [t_CO2/MW_th]
                "coal": 0.335,  # [t_CO2/MW_th]
                "other_non_renewable": 0.268,  # [t_CO2/MW_th]
            },
            "interest_rate": 0.05,  # [p.u.]
        }

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
        costs = read_csv(2035)

        parameters = {"grid_topology": "Status Quo"}
        # Insert effciencies in p.u.
        parameters["efficiency"] = {
            "oil": read_costs(costs, "oil", "efficiency"),
            "battery": {
                "store": read_costs(costs, "battery inverter", "efficiency")
                ** 0.5,
                "dispatch": read_costs(costs, "battery inverter", "efficiency")
                ** 0.5,
                "standing_loss": 0,
                "max_hours": 6,
                "cyclic_state_of_charge": True,
            },
            "pumped_hydro": {
                "store": read_costs(costs, "PHS", "efficiency") ** 0.5,
                "dispatch": read_costs(costs, "PHS", "efficiency") ** 0.5,
                "standing_loss": 0,
                "max_hours": 6,
                "cyclic_state_of_charge": True,
            },
        }
        # Warning: Electrical parameters are set in osmTGmod, editing these values will not change the data!
        parameters["electrical_parameters"] = {
            "ac_line_110kV": {
                "s_nom": 260,  # [MVA]
                "R": 0.109,  # [Ohm/km]
                "L": 1.2,  # [mH/km]
            },
            "ac_cable_110kV": {
                "s_nom": 280,  # [MVA]
                "R": 0.0177,  # [Ohm/km]
                "L": 0.3,  # [mH/km]
            },
            "ac_line_220kV": {
                "s_nom": 520,  # [MVA]
                "R": 0.109,  # [Ohm/km]
                "L": 1.0,  # [mH/km]
            },
            "ac_cable_220kV": {
                "s_nom": 550,  # [MVA]
                "R": 0.0176,  # [Ohm/km]
                "L": 0.3,  # [mH/km]
            },
            "ac_line_380kV": {
                "s_nom": 1790,  # [MVA]
                "R": 0.028,  # [Ohm/km]
                "L": 0.8,  # [mH/km]
            },
            "ac_cable_380kV": {
                "s_nom": 925,  # [MVA]
                "R": 0.0175,  # [Ohm/km]
                "L": 0.3,  # [mH/km]
            },
        }

        # Insert overnight investment costs
        # Source for eHV grid costs: Netzentwicklungsplan Strom 2035, Version 2021, 2. Entwurf
        # Source for HV lines and cables: Dena Verteilnetzstudie 2021, p. 146
        parameters["overnight_cost"] = {
            "ac_ehv_overhead_line": 2.5e6
            / (
                2
                * parameters["electrical_parameters"]["ac_line_380kV"]["s_nom"]
            ),  # [EUR/km/MW]
            "ac_ehv_cable": 11.5e6
            / (
                2
                * parameters["electrical_parameters"]["ac_cable_380kV"][
                    "s_nom"
                ]
            ),  # [EUR/km/MW]
            "ac_hv_overhead_line": 0.06e6
            / parameters["electrical_parameters"]["ac_line_110kV"][
                "s_nom"
            ],  # [EUR/km/MW]
            "ac_hv_cable": 0.8e6
            / parameters["electrical_parameters"]["ac_cable_110kV"][
                "s_nom"
            ],  # [EUR/km/MW]
            "dc_overhead_line": 0.5e3,  # [EUR/km/MW]
            "dc_cable": 3.25e3,  # [EUR/km/MW]
            "dc_inverter": 0.3e6,  # [EUR/MW]
            "transformer_380_110": 17.33e3,  # [EUR/MVA]
            "transformer_380_220": 13.33e3,  # [EUR/MVA]
            "transformer_220_110": 17.5e3,  # [EUR/MVA]
            "battery inverter": read_costs(
                costs, "battery inverter", "investment"
            ),
            "battery storage": read_costs(
                costs, "battery storage", "investment"
            ),
        }

        parameters["lifetime"] = {
            "ac_ehv_overhead_line": read_costs(
                costs, "HVAC overhead", "lifetime"
            ),
            "ac_ehv_cable": read_costs(costs, "HVAC overhead", "lifetime"),
            "ac_hv_overhead_line": read_costs(
                costs, "HVAC overhead", "lifetime"
            ),
            "ac_hv_cable": read_costs(costs, "HVAC overhead", "lifetime"),
            "dc_overhead_line": read_costs(costs, "HVDC overhead", "lifetime"),
            "dc_cable": read_costs(costs, "HVDC overhead", "lifetime"),
            "dc_inverter": read_costs(costs, "HVDC inverter pair", "lifetime"),
            "transformer_380_110": read_costs(
                costs, "HVAC overhead", "lifetime"
            ),
            "transformer_380_220": read_costs(
                costs, "HVAC overhead", "lifetime"
            ),
            "transformer_220_110": read_costs(
                costs, "HVAC overhead", "lifetime"
            ),
            "battery inverter": read_costs(
                costs, "battery inverter", "lifetime"
            ),
            "battery storage": read_costs(
                costs, "battery storage", "lifetime"
            ),
        }
        # Insert annualized capital costs
        # lines in EUR/km/MW/a
        # transfermer, inverter, battery in EUR/MW/a
        parameters["capital_cost"] = {}

        for comp in parameters["overnight_cost"].keys():
            parameters["capital_cost"][comp] = annualize_capital_costs(
                parameters["overnight_cost"][comp],
                parameters["lifetime"][comp],
                global_settings("eGon2035")["interest_rate"],
            )

        parameters["capital_cost"]["battery"] = (
            parameters["capital_cost"]["battery inverter"]
            + parameters["efficiency"]["battery"]["max_hours"]
            * parameters["capital_cost"]["battery storage"]
        )

        # Insert marginal_costs in EUR/MWh
        # marginal cost can include fuel, C02 and operation and maintenance costs
        parameters["marginal_cost"] = {
            "oil": global_settings(scenario)["fuel_costs"]["oil"]
            / read_costs(costs, "oil", "efficiency")
            + read_costs(costs, "oil", "VOM")
            + global_settings(scenario)["co2_costs"]
            * global_settings(scenario)["co2_emissions"]["oil"]
            / read_costs(costs, "oil", "efficiency"),
            "other_non_renewable": global_settings(scenario)["fuel_costs"][
                "gas"
            ] / read_costs(costs, "OCGT", "efficiency")
            + global_settings(scenario)["co2_costs"]
            * global_settings(scenario)["co2_emissions"][
                "other_non_renewable"
            ] / read_costs(costs, "OCGT", "efficiency"),
            "lignite": global_settings(scenario)["fuel_costs"]["lignite"]
            / read_costs(costs, "lignite", "efficiency")
            + read_costs(costs, "lignite", "VOM")
            + global_settings(scenario)["co2_costs"]
            * global_settings(scenario)["co2_emissions"]["lignite"]
            / read_costs(costs, "lignite", "efficiency"),
            "coal": global_settings(scenario)["fuel_costs"]["coal"]
            / read_costs(costs, "coal", "efficiency")
            + read_costs(costs, "coal", "VOM")
            + global_settings(scenario)["co2_costs"]
            * global_settings(scenario)["co2_emissions"]["coal"]
            / read_costs(costs, "coal", "efficiency"),
            "nuclear": global_settings(scenario)["fuel_costs"]["nuclear"]
            / read_costs(costs, "nuclear", "efficiency")
            + read_costs(costs, "nuclear", "VOM"),
            "biomass": global_settings(scenario)["fuel_costs"]["biomass"]
            / read_costs(costs, "biomass", "efficiency")
            + read_costs(costs, "biomass CHP", "VOM"),
            "wind_offshore": read_costs(costs, "offwind", "VOM"),
            "wind_onshore": read_costs(costs, "onwind", "VOM"),
            "solar": read_costs(costs, "solar", "VOM"),
        }

    elif scenario == "eGon100RE":
        costs = read_csv(2050)

        parameters = {"grid_topology": "Status Quo"}

        # Insert effciencies in p.u.
        parameters["efficiency"] = {
            "battery": {
                "store": read_costs(costs, "battery inverter", "efficiency")
                ** 0.5,
                "dispatch": read_costs(costs, "battery inverter", "efficiency")
                ** 0.5,
                "standing_loss": 0,
                "max_hours": 6,
                "cyclic_state_of_charge": True,
            },
            "pumped_hydro": {
                "store": read_costs(costs, "PHS", "efficiency") ** 0.5,
                "dispatch": read_costs(costs, "PHS", "efficiency") ** 0.5,
                "standing_loss": 0,
                "max_hours": 6,
                "cyclic_state_of_charge": True,
            },
        }
        # Warning: Electrical parameters are set in osmTGmod, editing these values will not change the data!
        parameters["electrical_parameters"] = {
            "ac_line_110kV": {
                "s_nom": 260,  # [MVA]
                "R": 0.109,  # [Ohm/km]
                "L": 1.2,  # [mH/km]
            },
            "ac_cable_110kV": {
                "s_nom": 280,  # [MVA]
                "R": 0.0177,  # [Ohm/km]
                "L": 0.3,  # [mH/km]
            },
            "ac_line_220kV": {
                "s_nom": 520,  # [MVA]
                "R": 0.109,  # [Ohm/km]
                "L": 1.0,  # [mH/km]
            },
            "ac_cable_220kV": {
                "s_nom": 550,  # [MVA]
                "R": 0.0176,  # [Ohm/km]
                "L": 0.3,  # [mH/km]
            },
            "ac_line_380kV": {
                "s_nom": 1790,  # [MVA]
                "R": 0.028,  # [Ohm/km]
                "L": 0.8,  # [mH/km]
            },
            "ac_cable_380kV": {
                "s_nom": 925,  # [MVA]
                "R": 0.0175,  # [Ohm/km]
                "L": 0.3,  # [mH/km]
            },
        }

        # Insert overnight investment costs
        # Source for transformer costs: Netzentwicklungsplan Strom 2035, Version 2021, 2. Entwurf
        # Source for HV lines and cables: Dena Verteilnetzstudie 2021, p. 146
        parameters["overnight_cost"] = {
            "ac_ehv_overhead_line": read_costs(
                costs, "HVAC overhead", "investment"
            ),  # [EUR/km/MW]
            "ac_hv_overhead_line": 0.06e6
            / parameters["electrical_parameters"]["ac_line_110kV"][
                "s_nom"
            ],  # [EUR/km/MW]
            "ac_hv_cable": 0.8e6
            / parameters["electrical_parameters"]["ac_cable_110kV"][
                "s_nom"
            ],  # [EUR/km/MW]
            "dc_overhead_line": read_costs(
                costs, "HVDC overhead", "investment"
            ),
            "dc_cable": read_costs(costs, "HVDC overhead", "investment"),
            "dc_inverter": read_costs(
                costs, "HVDC inverter pair", "investment"
            ),
            "transformer_380_110": 17.33e3,  # [EUR/MVA]
            "transformer_380_220": 13.33e3,  # [EUR/MVA]
            "transformer_220_110": 17.5e3,  # [EUR/MVA]
            "battery inverter": read_costs(
                costs, "battery inverter", "investment"
            ),
            "battery storage": read_costs(
                costs, "battery storage", "investment"
            ),
        }

        parameters["lifetime"] = {
            "ac_ehv_overhead_line": read_costs(
                costs, "HVAC overhead", "lifetime"
            ),
            "ac_ehv_cable": read_costs(costs, "HVAC overhead", "lifetime"),
            "ac_hv_overhead_line": read_costs(
                costs, "HVAC overhead", "lifetime"
            ),
            "ac_hv_cable": read_costs(costs, "HVAC overhead", "lifetime"),
            "dc_overhead_line": read_costs(costs, "HVDC overhead", "lifetime"),
            "dc_cable": read_costs(costs, "HVDC overhead", "lifetime"),
            "dc_inverter": read_costs(costs, "HVDC inverter pair", "lifetime"),
            "transformer_380_110": read_costs(
                costs, "HVAC overhead", "lifetime"
            ),
            "transformer_380_220": read_costs(
                costs, "HVAC overhead", "lifetime"
            ),
            "transformer_220_110": read_costs(
                costs, "HVAC overhead", "lifetime"
            ),
            "battery inverter": read_costs(
                costs, "battery inverter", "lifetime"
            ),
            "battery storage": read_costs(
                costs, "battery storage", "lifetime"
            ),
        }
        # Insert annualized capital costs
        # lines in EUR/km/MW/a
        # transfermer, inverter, battery in EUR/MW/a
        parameters["capital_cost"] = {}

        for comp in parameters["overnight_cost"].keys():
            parameters["capital_cost"][comp] = annualize_capital_costs(
                parameters["overnight_cost"][comp],
                parameters["lifetime"][comp],
                global_settings("eGon2035")["interest_rate"],
            )

        parameters["capital_cost"]["battery"] = (
            parameters["capital_cost"]["battery inverter"]
            + parameters["efficiency"]["battery"]["max_hours"]
            * parameters["capital_cost"]["battery storage"]
        )

        # Insert marginal_costs in EUR/MWh
        # marginal cost can include fuel, C02 and operation and maintenance costs
        parameters["marginal_cost"] = {
            "wind_offshore": read_costs(costs, "offwind", "VOM"),
            "wind_onshore": read_costs(costs, "onwind", "VOM"),
            "solar": read_costs(costs, "solar", "VOM"),
        }

    elif scenario == "eGon2021":
        parameters = {}

    elif scenario == "status2019":

        costs = read_csv(2020)

        parameters = {"grid_topology": "Status Quo"}
        # Insert effciencies in p.u.
        parameters["efficiency"] = {
            "oil": read_costs(costs, "oil", "efficiency"),
            "battery": {
                "store": read_costs(costs, "battery inverter", "efficiency")
                ** 0.5,
                "dispatch": read_costs(costs, "battery inverter", "efficiency")
                ** 0.5,
                "standing_loss": 0,
                "max_hours": 6,
                "cyclic_state_of_charge": True,
            },
            "pumped_hydro": {
                "store": read_costs(costs, "PHS", "efficiency") ** 0.5,
                "dispatch": read_costs(costs, "PHS", "efficiency") ** 0.5,
                "standing_loss": 0,
                "max_hours": 6,
                "cyclic_state_of_charge": True,
            },
        }
        # Warning: Electrical parameters are set in osmTGmod, editing these values will not change the data!
        parameters["electrical_parameters"] = {
            "ac_line_110kV": {
                "s_nom": 260,  # [MVA]
                "R": 0.109,  # [Ohm/km]
                "L": 1.2,  # [mH/km]
            },
            "ac_cable_110kV": {
                "s_nom": 280,  # [MVA]
                "R": 0.0177,  # [Ohm/km]
                "L": 0.3,  # [mH/km]
            },
            "ac_line_220kV": {
                "s_nom": 520,  # [MVA]
                "R": 0.109,  # [Ohm/km]
                "L": 1.0,  # [mH/km]
            },
            "ac_cable_220kV": {
                "s_nom": 550,  # [MVA]
                "R": 0.0176,  # [Ohm/km]
                "L": 0.3,  # [mH/km]
            },
            "ac_line_380kV": {
                "s_nom": 1790,  # [MVA]
                "R": 0.028,  # [Ohm/km]
                "L": 0.8,  # [mH/km]
            },
            "ac_cable_380kV": {
                "s_nom": 925,  # [MVA]
                "R": 0.0175,  # [Ohm/km]
                "L": 0.3,  # [mH/km]
            },
        }

        # Insert overnight investment costs
        # Source for eHV grid costs: Netzentwicklungsplan Strom 2035, Version 2021, 2. Entwurf
        # Source for HV lines and cables: Dena Verteilnetzstudie 2021, p. 146
        parameters["overnight_cost"] = {
            "ac_ehv_overhead_line": 2.5e6
            / (
                2
                * parameters["electrical_parameters"]["ac_line_380kV"]["s_nom"]
            ),  # [EUR/km/MW]
            "ac_ehv_cable": 11.5e6
            / (
                2
                * parameters["electrical_parameters"]["ac_cable_380kV"][
                    "s_nom"
                ]
            ),  # [EUR/km/MW]
            "ac_hv_overhead_line": 0.06e6
            / parameters["electrical_parameters"]["ac_line_110kV"][
                "s_nom"
            ],  # [EUR/km/MW]
            "ac_hv_cable": 0.8e6
            / parameters["electrical_parameters"]["ac_cable_110kV"][
                "s_nom"
            ],  # [EUR/km/MW]
            "dc_overhead_line": 0.5e3,  # [EUR/km/MW]
            "dc_cable": 3.25e3,  # [EUR/km/MW]
            "dc_inverter": 0.3e6,  # [EUR/MW]
            "transformer_380_110": 17.33e3,  # [EUR/MVA]
            "transformer_380_220": 13.33e3,  # [EUR/MVA]
            "transformer_220_110": 17.5e3,  # [EUR/MVA]
            "battery inverter": read_costs(
                costs, "battery inverter", "investment"
            ),
            "battery storage": read_costs(
                costs, "battery storage", "investment"
            ),
        }

        parameters["lifetime"] = {
            "ac_ehv_overhead_line": read_costs(
                costs, "HVAC overhead", "lifetime"
            ),
            "ac_ehv_cable": read_costs(costs, "HVAC overhead", "lifetime"),
            "ac_hv_overhead_line": read_costs(
                costs, "HVAC overhead", "lifetime"
            ),
            "ac_hv_cable": read_costs(costs, "HVAC overhead", "lifetime"),
            "dc_overhead_line": read_costs(costs, "HVDC overhead", "lifetime"),
            "dc_cable": read_costs(costs, "HVDC overhead", "lifetime"),
            "dc_inverter": read_costs(costs, "HVDC inverter pair", "lifetime"),
            "transformer_380_110": read_costs(
                costs, "HVAC overhead", "lifetime"
            ),
            "transformer_380_220": read_costs(
                costs, "HVAC overhead", "lifetime"
            ),
            "transformer_220_110": read_costs(
                costs, "HVAC overhead", "lifetime"
            ),
            "battery inverter": read_costs(
                costs, "battery inverter", "lifetime"
            ),
            "battery storage": read_costs(
                costs, "battery storage", "lifetime"
            ),
        }
        # Insert annualized capital costs
        # lines in EUR/km/MW/a
        # transfermer, inverter, battery in EUR/MW/a
        parameters["capital_cost"] = {}

        for comp in parameters["overnight_cost"].keys():
            parameters["capital_cost"][comp] = annualize_capital_costs(
                parameters["overnight_cost"][comp],
                parameters["lifetime"][comp],
                global_settings("status2019")["interest_rate"],
            )

        parameters["capital_cost"]["battery"] = (
            parameters["capital_cost"]["battery inverter"]
            + parameters["efficiency"]["battery"]["max_hours"]
            * parameters["capital_cost"]["battery storage"]
        )

        parameters["marginal_cost"] = {
            "oil": global_settings(scenario)["fuel_costs"]["oil"]
            / read_costs(costs, "oil", "efficiency")
            + read_costs(costs, "oil", "VOM")
            + global_settings(scenario)["co2_costs"]
            * global_settings(scenario)["co2_emissions"]["oil"]
            / read_costs(costs, "oil", "efficiency"),
            "other_non_renewable": global_settings(scenario)["fuel_costs"][
                "gas"
            ] / read_costs(costs, "OCGT", "efficiency")
            + global_settings(scenario)["co2_costs"]
            * global_settings(scenario)["co2_emissions"][
                "other_non_renewable"
            ] / read_costs(costs, "OCGT", "efficiency"),
            "lignite": global_settings(scenario)["fuel_costs"]["lignite"]
            / read_costs(costs, "lignite", "efficiency")
            + read_costs(costs, "lignite", "VOM")
            + global_settings(scenario)["co2_costs"]
            * global_settings(scenario)["co2_emissions"]["lignite"]
            / read_costs(costs, "lignite", "efficiency"),
            "coal": global_settings(scenario)["fuel_costs"]["coal"]
            / read_costs(costs, "coal", "efficiency")
            + read_costs(costs, "coal", "VOM")
            + global_settings(scenario)["co2_costs"]
            * global_settings(scenario)["co2_emissions"]["coal"]
            / read_costs(costs, "coal", "efficiency"),
            "OCGT": global_settings(scenario)["fuel_costs"]["gas"]
            / read_costs(costs, "OCGT", "efficiency")
            + read_costs(costs, "OCGT", "VOM")
            + global_settings(scenario)["co2_costs"]
            * global_settings(scenario)["co2_emissions"]["gas"]
            / read_costs(costs, "OCGT", "efficiency"),
            "nuclear": global_settings(scenario)["fuel_costs"]["nuclear"]
            / read_costs(costs, "nuclear", "efficiency")
            + read_costs(costs, "nuclear", "VOM"),
            "biomass": global_settings(scenario)["fuel_costs"]["biomass"]
            / read_costs(costs, "biomass CHP", "efficiency")
            + read_costs(costs, "biomass CHP", "VOM"),
            "wind_offshore": read_costs(costs, "offwind", "VOM"),
            "wind_onshore": read_costs(costs, "onwind", "VOM"),
            "solar": read_costs(costs, "solar", "VOM"),
        }

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
        costs = read_csv(2035)

        parameters = {
            "main_gas_carrier": "CH4",
            "H2_feedin_volumetric_fraction": 0.15,
        }
        # Insert effciencies in p.u.
        parameters["efficiency"] = {
            "power_to_H2": read_costs(costs, "electrolysis", "efficiency"),
            "H2_to_power": read_costs(costs, "fuel cell", "efficiency"),
            "CH4_to_H2": read_costs(costs, "SMR", "efficiency"),
            "H2_feedin": 1,
            "H2_to_CH4": read_costs(costs, "methanation", "efficiency"),
            "OCGT": read_costs(costs, "OCGT", "efficiency"),
        }
        # Insert overnight investment costs
        parameters["overnight_cost"] = {
            "power_to_H2": read_costs(costs, "electrolysis", "investment"),
            "H2_to_power": read_costs(costs, "fuel cell", "investment"),
            "CH4_to_H2": read_costs(costs, "SMR", "investment"),
            "H2_to_CH4": read_costs(costs, "methanation", "investment"),
            "H2_feedin": 0,
            "H2_underground": read_costs(
                costs, "hydrogen storage underground", "investment"
            ),
            "H2_overground": read_costs(
                costs, "hydrogen storage tank incl. compressor", "investment"
            ),
            "H2_pipeline": read_costs(
                costs, "H2 (g) pipeline", "investment"
            ),  # [EUR/MW/km]
        }

        # Insert lifetime
        parameters["lifetime"] = {
            "power_to_H2": read_costs(costs, "electrolysis", "lifetime"),
            "H2_to_power": read_costs(costs, "fuel cell", "lifetime"),
            "CH4_to_H2": read_costs(costs, "SMR", "lifetime"),
            "H2_to_CH4": read_costs(costs, "methanation", "lifetime"),
            "H2_underground": read_costs(
                costs, "hydrogen storage underground", "lifetime"
            ),
            "H2_overground": read_costs(
                costs, "hydrogen storage tank incl. compressor", "lifetime"
            ),
            "H2_pipeline": read_costs(costs, "H2 (g) pipeline", "lifetime"),
            "H2_feedin": read_costs(costs, "CH4 (g) pipeline", "lifetime"),
        }

        # Insert annualized capital costs
        parameters["capital_cost"] = {}

        for comp in parameters["overnight_cost"].keys():
            parameters["capital_cost"][comp] = annualize_capital_costs(
                parameters["overnight_cost"][comp],
                parameters["lifetime"][comp],
                global_settings("eGon2035")["interest_rate"],
            )

        parameters["marginal_cost"] = {
            "CH4": global_settings(scenario)["fuel_costs"]["gas"]
            + global_settings(scenario)["co2_costs"]
            * global_settings(scenario)["co2_emissions"]["gas"],
            "OCGT": read_costs(costs, "OCGT", "VOM"),
            "biogas": global_settings(scenario)["fuel_costs"]["gas"],
            "chp_gas": read_costs(costs, "central gas CHP", "VOM"),
        }

        # Insert max gas production (generator) over the year
        parameters["max_gas_generation_overtheyear"] = {
            "CH4": 36000000,  # [MWh] Netzentwicklungsplan Gas 2020–2030
            "biogas": 10000000,  # [MWh] Netzentwicklungsplan Gas 2020–2030
        }

    elif scenario == "eGon100RE":
        costs = read_csv(2050)
        interest_rate = 0.07  # [p.u.]

        parameters = {
            "main_gas_carrier": "H2",
            "retrofitted_CH4pipeline-to-H2pipeline_share": 0.23,
            # p-e-s result, this value is overwritten if p-e-s is run
        }
        # Insert effciencies in p.u.
        parameters["efficiency"] = {
            "power_to_H2": read_costs(costs, "electrolysis", "efficiency"),
            "H2_to_power": read_costs(costs, "fuel cell", "efficiency"),
            "CH4_to_H2": read_costs(costs, "SMR", "efficiency"),
            "H2_to_CH4": read_costs(costs, "methanation", "efficiency"),
            "OCGT": read_costs(costs, "OCGT", "efficiency"),
        }

        # Insert FOM in %
        parameters["FOM"] = {
            "H2_underground": read_costs(
                costs, "hydrogen storage underground", "FOM"
            ),
            "H2_overground": read_costs(
                costs, "hydrogen storage tank incl. compressor", "FOM"
            ),
            "power_to_H2": read_costs(costs, "electrolysis", "FOM"),
            "H2_to_power": read_costs(costs, "fuel cell", "FOM"),
            "CH4_to_H2": read_costs(costs, "SMR", "FOM"),
            "H2_to_CH4": read_costs(costs, "methanation", "FOM"),
            "H2_pipeline": read_costs(costs, "H2 (g) pipeline", "FOM"),
            "H2_pipeline_retrofit": read_costs(
                costs, "H2 (g) pipeline repurposed", "FOM"
            ),
        }

        # Insert overnight investment costs
        parameters["overnight_cost"] = {
            "power_to_H2": read_costs(costs, "electrolysis", "investment"),
            "H2_to_power": read_costs(costs, "fuel cell", "investment"),
            "CH4_to_H2": read_costs(costs, "SMR", "investment"),
            "H2_to_CH4": read_costs(costs, "methanation", "investment"),
            "H2_underground": read_costs(
                costs, "hydrogen storage underground", "investment"
            ),
            "H2_overground": read_costs(
                costs, "hydrogen storage tank incl. compressor", "investment"
            ),
            "H2_pipeline": read_costs(
                costs, "H2 (g) pipeline", "investment"
            ),  # [EUR/MW/km]
            "H2_pipeline_retrofit": read_costs(
                costs, "H2 (g) pipeline repurposed", "investment"
            ),  # [EUR/MW/km]
        }

        # Insert lifetime
        parameters["lifetime"] = {
            "power_to_H2": read_costs(costs, "electrolysis", "lifetime"),
            "H2_to_power": read_costs(costs, "fuel cell", "lifetime"),
            "CH4_to_H2": read_costs(costs, "SMR", "lifetime"),
            "H2_to_CH4": read_costs(costs, "methanation", "lifetime"),
            "H2_underground": read_costs(
                costs, "hydrogen storage underground", "lifetime"
            ),
            "H2_overground": read_costs(
                costs, "hydrogen storage tank incl. compressor", "lifetime"
            ),
            "H2_pipeline": read_costs(costs, "H2 (g) pipeline", "lifetime"),
            "H2_pipeline_retrofit": read_costs(
                costs, "H2 (g) pipeline repurposed", "lifetime"
            ),
        }

        # Insert costs
        parameters["capital_cost"] = {}

        for comp in parameters["overnight_cost"].keys():
            parameters["capital_cost"][comp] = annualize_capital_costs(
                parameters["overnight_cost"][comp],
                parameters["lifetime"][comp],
                interest_rate,
            ) + parameters["overnight_cost"][comp] * (
                parameters["FOM"][comp] / 100
            )

        for comp in ["H2_to_power", "H2_to_CH4"]:
            parameters["capital_cost"][comp] = (
                annualize_capital_costs(
                    parameters["overnight_cost"][comp],
                    parameters["lifetime"][comp],
                    interest_rate,
                )
                + parameters["overnight_cost"][comp]
                * (parameters["FOM"][comp] / 100)
            ) * parameters["efficiency"][comp]

        parameters["marginal_cost"] = {
            "OCGT": read_costs(costs, "OCGT", "VOM"),
            "biogas": read_costs(costs, "biogas", "fuel"),
            "chp_gas": read_costs(costs, "central gas CHP", "VOM"),
        }

    elif scenario == "eGon2021":
        parameters = {}

    elif scenario == "status2019":
        costs = read_csv(2020)
        parameters = {
            "main_gas_carrier": "CH4",
        }

        parameters["marginal_cost"] = {
            "CH4": global_settings(scenario)["fuel_costs"]["gas"]
            + global_settings(scenario)["co2_costs"]
            * global_settings(scenario)["co2_emissions"]["gas"],
            "OCGT": read_costs(costs, "OCGT", "VOM"),
            "biogas": global_settings(scenario)["fuel_costs"]["gas"],
            "chp_gas": read_costs(costs, "central gas CHP", "VOM"),
        }
        # Insert effciencies in p.u.
        parameters["efficiency"] = {
            "OCGT": read_costs(costs, "OCGT", "efficiency"),
        }

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

    Notes
    -----
    For a detailed description of the parameters see module
    :mod:`egon.data.datasets.emobility.motorized_individual_travel`.
    """

    if scenario == "eGon2035":
        parameters = {
            "motorized_individual_travel": {
                "NEP C 2035": {
                    "ev_count": 15100000,
                    "bev_mini_share": 0.1589,
                    "bev_medium_share": 0.3533,
                    "bev_luxury_share": 0.1053,
                    "phev_mini_share": 0.0984,
                    "phev_medium_share": 0.2189,
                    "phev_luxury_share": 0.0652,
                    "model_parameters": {},
                }
            }
        }

    elif scenario == "eGon100RE":
        # eGon100RE has 3 Scenario variations
        #   * allocation will always be done for all scenarios
        #   * model data will be written to tables `egon_etrago_*` only
        #     for the variation as speciefied in `datasets.yml`
        parameters = {
            "motorized_individual_travel": {
                "Reference 2050": {
                    "ev_count": 25065000,
                    "bev_mini_share": 0.1589,
                    "bev_medium_share": 0.3533,
                    "bev_luxury_share": 0.1053,
                    "phev_mini_share": 0.0984,
                    "phev_medium_share": 0.2189,
                    "phev_luxury_share": 0.0652,
                    "model_parameters": {},
                },
                "Mobility Transition 2050": {
                    "ev_count": 37745000,
                    "bev_mini_share": 0.1589,
                    "bev_medium_share": 0.3533,
                    "bev_luxury_share": 0.1053,
                    "phev_mini_share": 0.0984,
                    "phev_medium_share": 0.2189,
                    "phev_luxury_share": 0.0652,
                    "model_parameters": {},
                },
                "Electrification 2050": {
                    "ev_count": 47700000,
                    "bev_mini_share": 0.1589,
                    "bev_medium_share": 0.3533,
                    "bev_luxury_share": 0.1053,
                    "phev_mini_share": 0.0984,
                    "phev_medium_share": 0.2189,
                    "phev_luxury_share": 0.0652,
                    "model_parameters": {},
                },
            }
        }

    elif scenario == "eGon2021":
        parameters = {}

    elif scenario == "status2019":
        parameters = {
            "motorized_individual_travel": {
                "status2019": {
                    "ev_count": 200000,
                    "bev_mini_share": 0.1589,
                    "bev_medium_share": 0.3533,
                    "bev_luxury_share": 0.1053,
                    "phev_mini_share": 0.0984,
                    "phev_medium_share": 0.2189,
                    "phev_luxury_share": 0.0652,
                    "model_parameters": {},
                }
            }
        }

    else:
        print(f"Scenario name {scenario} is not valid.")
        parameters = dict()

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
        costs = read_csv(2035)

        parameters = {
            "DE_demand_reduction_residential": 0.854314018923104,
            "DE_demand_reduction_service": 0.498286864771128,
            "DE_district_heating_share": 0.14,
        }

        # Insert efficiency in p.u.
        parameters["efficiency"] = {
            "water_tank_charger": read_costs(
                costs, "water tank charger", "efficiency"
            ),
            "water_tank_discharger": read_costs(
                costs, "water tank discharger", "efficiency"
            ),
            "central_resistive_heater": read_costs(
                costs, "central resistive heater", "efficiency"
            ),
            "central_gas_boiler": read_costs(
                costs, "central gas boiler", "efficiency"
            ),
            "rural_resistive_heater": read_costs(
                costs, "decentral resistive heater", "efficiency"
            ),
            "rural_gas_boiler": read_costs(
                costs, "decentral gas boiler", "efficiency"
            ),
        }

        # Insert overnight investment costs, in EUR/MWh
        parameters["overnight_cost"] = {
            "central_water_tank": read_costs(
                costs, "central water tank storage", "investment"
            ),
            "rural_water_tank": read_costs(
                costs, "decentral water tank storage", "investment"
            ),
        }

        # Insert lifetime
        parameters["lifetime"] = {
            "central_water_tank": read_costs(
                costs, "central water tank storage", "lifetime"
            ),
            "rural_water_tank": read_costs(
                costs, "decentral water tank storage", "lifetime"
            ),
        }

        # Insert annualized capital costs
        parameters["capital_cost"] = {}

        for comp in parameters["overnight_cost"].keys():
            parameters["capital_cost"][comp] = annualize_capital_costs(
                parameters["overnight_cost"][comp],
                parameters["lifetime"][comp],
                global_settings("eGon2035")["interest_rate"],
            )

        # Insert marginal_costs in EUR/MWh
        # marginal cost can include fuel, C02 and operation and maintenance costs
        parameters["marginal_cost"] = {
            "central_heat_pump": read_costs(
                costs, "central air-sourced heat pump", "VOM"
            ),
            "central_gas_chp": read_costs(costs, "central gas CHP", "VOM"),
            "central_gas_boiler": read_costs(
                costs, "central gas boiler", "VOM"
            ),
            "central_resistive_heater": read_costs(
                costs, "central resistive heater", "VOM"
            ),
            "geo_thermal": 2.9,  # Danish Energy Agency
            "water_tank_charger": 0,  # Danish Energy Agency
            "water_tank_discharger": 0,  # Danish Energy Agency
            "rural_heat_pump": 0,  # Danish Energy Agency, Technology Data for Individual Heating Plants
        }

    elif scenario == "eGon100RE":
        costs = read_csv(2050)

        parameters = {
            "DE_demand_reduction_residential": 0.640720648501849,
            "DE_demand_reduction_service": 0.390895195300713,
            "DE_district_heating_share": 0.19,
        }

        parameters["marginal_cost"] = {
            "central_heat_pump": read_costs(
                costs, "central air-sourced heat pump", "VOM"
            ),
            "central_gas_chp": read_costs(costs, "central gas CHP", "VOM"),
            "central_gas_boiler": read_costs(
                costs, "central gas boiler", "VOM"
            ),
            "central_resistive_heater": read_costs(
                costs, "central resistive heater", "VOM"
            ),
            "geo_thermal": 2.9,  # Danish Energy Agency
            "water_tank_charger": 0,  # Danish Energy Agency
            "water_tank_discharger": 0,  # Danish Energy Agency
            "rural_heat_pump": 0,  # Danish Energy Agency, Technology Data for Individual Heating Plants
        }

        # Insert efficiency in p.u.
        parameters["efficiency"] = {
            "water_tank_charger": read_costs(
                costs, "water tank charger", "efficiency"
            ),
            "water_tank_discharger": read_costs(
                costs, "water tank discharger", "efficiency"
            ),
            "central_resistive_heater": read_costs(
                costs, "central resistive heater", "efficiency"
            ),
            "central_gas_boiler": read_costs(
                costs, "central gas boiler", "efficiency"
            ),
            "rural_resistive_heater": read_costs(
                costs, "decentral resistive heater", "efficiency"
            ),
            "rural_gas_boiler": read_costs(
                costs, "decentral gas boiler", "efficiency"
            ),
        }

        # Insert overnight investment costs, in EUR/MWh
        parameters["overnight_cost"] = {
            "central_water_tank": read_costs(
                costs, "central water tank storage", "investment"
            ),
            "rural_water_tank": read_costs(
                costs, "decentral water tank storage", "investment"
            ),
        }

        # Insert lifetime
        parameters["lifetime"] = {
            "central_water_tank": read_costs(
                costs, "central water tank storage", "lifetime"
            ),
            "rural_water_tank": read_costs(
                costs, "decentral water tank storage", "lifetime"
            ),
        }

        # Insert annualized capital costs
        parameters["capital_cost"] = {}

        for comp in parameters["overnight_cost"].keys():
            parameters["capital_cost"][comp] = annualize_capital_costs(
                parameters["overnight_cost"][comp],
                parameters["lifetime"][comp],
                global_settings("eGon100RE")["interest_rate"],
            )

    elif scenario == "eGon2021":
        parameters = {}

    elif scenario == "status2019":
        parameters = {
            "DE_demand_residential_TJ": 1658400
            + 383300,  # [TJ], space heating + hot water, source: AG Energiebilanzen 2019 (https://ag-energiebilanzen.de/wp-content/uploads/2020/10/ageb_20v_v1.pdf)
            "DE_demand_service_TJ": 567300
            + 71500,  # [TJ], space heating + hot water, source: AG Energiebilanzen 2019 (https://ag-energiebilanzen.de/wp-content/uploads/2020/10/ageb_20v_v1.pdf)
            "DE_district_heating_share": (189760 + 38248)
            / (
                1658400 + 383300 + 567300 + 71500
            ),  # [TJ], source: AG Energiebilanzen 2019 (https://ag-energiebilanzen.de/wp-content/uploads/2021/11/bilanz19d.xlsx)
        }

        costs = read_csv(2020)

        # Insert marginal_costs in EUR/MWh
        # marginal cost can include fuel, C02 and operation and maintenance costs
        parameters["marginal_cost"] = {
            "central_heat_pump": read_costs(
                costs, "central air-sourced heat pump", "VOM"
            ),
            "central_gas_chp": read_costs(costs, "central gas CHP", "VOM"),
            "central_gas_boiler": read_costs(
                costs, "central gas boiler", "VOM"
            ),
            "central_resistive_heater": read_costs(
                costs, "central resistive heater", "VOM"
            ),
            "rural_heat_pump": 0,  # Danish Energy Agency, Technology Data for Individual Heating Plants
        }

        # Insert efficiency in p.u.
        parameters["efficiency"] = {
            "central_gas_boiler": read_costs(
                costs, "central gas boiler", "efficiency"
            ),
        }

    else:
        print(f"Scenario name {scenario} is not valid.")

    return parameters
