"""
This module does sanity checks for both the eGon2035 and the eGon100RE scenario seperately where a percentage
error is given to showcase difference in output and input values. Please note that there are missing input technologies in the supply tables.
 Authors: @ALonso, @dana
"""

import pandas as pd
from egon.data import db
from egon.data.datasets import Dataset
from egon.data.datasets.electricity_demand.temporal import insert_cts_load
import egon.data.config


class SanityChecks(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="SanityChecks",
            version="0.0.1",
            dependencies=dependencies,
            tasks=(
                sanitycheck_eGon2035_electricity,
                sanitycheck_eGon2035_heat,
                #sanitycheck_eGon100RE_electricity,
                #sanitycheck_eGon100RE_electricity_storage,
                #sanitycheck_eGon100RE_heat_generator,
                #sanitycheck_eGon100RE_heat_link,
            ),
        )


def sanitycheck_eGon2035_electricity():

    """Returns print statements as sanity checks for electricity generators in
    the eGon2035 scenario.
    Parameters
    ----------
    None

    Returns
    -------
    None
    """

    # Check input and output values for the carriers "other_non_renewable",
    # "other_renewable", "reservoir", "run_of_river" and "oil"

    scn = "eGon2035"

    # Section to check generator capacities
    print(f"Sanity checks for scenario {scn}")
    print(
        "For German electricity generators the following deviations between the inputs and outputs can be observed:"
    )

    carriers_electricity = [
        "other_non_renewable",
        "other_renewable",
        "reservoir",
        "run_of_river",
        "oil",
    ]

    for carrier in carriers_electricity:

        sum_output = db.select_dataframe(
            f"""SELECT scn_name, ROUND(SUM(p_nom::numeric), 2) as output_capacity_mw
                 FROM grid.egon_etrago_generator
                 WHERE scn_name = '{scn}'
                 AND carrier IN ('{carrier}')
                 GROUP BY (scn_name);
            """
        )

        sum_input = db.select_dataframe(
            f"""SELECT carrier, ROUND(SUM(capacity::numeric), 2) as input_capacity_mw
                 FROM supply.egon_scenario_capacities
                 WHERE carrier= '{carrier}'
                 AND scenario_name ='{scn}'
                 GROUP BY (carrier);
            """
        )

        if (
            sum_output.output_capacity_mw.sum() == 0
            and sum_input.input_capacity_mw.sum() == 0
        ):
            print(
                f"No capacity for carrier '{carrier}' needed to be distributed. Everything is fine"
            )

        elif (
            sum_input.input_capacity_mw.sum() > 0
            and sum_output.output_capacity_mw.sum() == 0
        ):
            print(
                f"Error: Capacity for carrier '{carrier}' was not distributed at all!"
            )

        elif (
            sum_output.output_capacity_mw.sum() > 0
            and sum_input.input_capacity_mw.sum() == 0
        ):
            print(
                f"Error: Eventhough no input capacity was provided for carrier '{carrier}' a capacity got distributed!"
            )

        else:
            sum_input["error"] = (
                (sum_output.output_capacity_mw - sum_input.input_capacity_mw)
                / sum_input.input_capacity_mw
            ) * 100
            g = sum_input["error"].values[0]

            print(
                f"'{carrier}': "
                + str(round(g, 2)) + " %"
            )

    # Check input and output values for the carriers "solar",
    # "wind" and "biomass"

    sum_installed_gen_cap_DE = db.select_dataframe(
        """SELECT scn_name, a.carrier, ROUND(SUM(p_nom::numeric), 2) as capacity_mw, ROUND(c.capacity::numeric, 2) as target_capacity
            FROM grid.egon_etrago_generator a
            JOIN supply.egon_scenario_capacities c
            ON (c.carrier = a.carrier)
            WHERE bus IN (
                SELECT bus_id FROM grid.egon_etrago_bus
                WHERE scn_name = 'eGon2035'
                AND country = 'DE')
            AND c.scenario_name='eGon2035'
            GROUP BY (scn_name, a.carrier, c.capacity);
        """
    )

    sum_installed_gen_cap_DE["error"] = (
        (
            sum_installed_gen_cap_DE["capacity_mw"]
            - sum_installed_gen_cap_DE["target_capacity"]
        )
        / sum_installed_gen_cap_DE["target_capacity"]
    ) * 100

    sum_installed_gen_cap_DE = sum_installed_gen_cap_DE.round({"error": 2})

    carriers_electricity = [
        "wind_onshore",
        "wind_offshore",
        "solar",
        "solar_rooftop",
    ]

    for carrier in carriers_electricity:

        e = sum_installed_gen_cap_DE.loc[
            sum_installed_gen_cap_DE["carrier"] == carrier, "error"
        ].values[0]

        print(
            f"'{carrier}': "
            + str(round(e, 2)) + " %"
        )

    # Compare input and output value for biomass

    sum_installed_gen_biomass_cap_DE = db.select_dataframe(
        """SELECT scn_name, ROUND(SUM(p_nom::numeric), 2) as capacity_mw_biogas
            FROM grid.egon_etrago_generator
            WHERE bus IN (
                SELECT bus_id FROM grid.egon_etrago_bus
                WHERE scn_name = 'eGon2035'
                AND country = 'DE')
            AND carrier IN ('central_biomass_CHP_heat', 'biomass', 'industrial_biomass_CHP', 'central_biomass_CHP')
            GROUP BY (scn_name);
        """
    )

    sum_installed_gen_biomass_cap_DE["error"] = (
        (
            sum_installed_gen_biomass_cap_DE["capacity_mw_biogas"]
            - sum_installed_gen_cap_DE["target_capacity"]
        )
        / sum_installed_gen_cap_DE["target_capacity"]
    ) * 100

    e = sum_installed_gen_biomass_cap_DE["error"].values[0]

    print(
        "'biomass': "
        + str(round(e, 2)) + " %"
    )



    # Section to check storage capacities

    sum_installed_storage_cap_DE = db.select_dataframe(
        f"""SELECT scn_name, a.carrier, ROUND(SUM(p_nom::numeric), 2) as capacity_mw, ROUND(c.capacity::numeric, 2) as target_capacity
             FROM grid.egon_etrago_storage a
             JOIN supply.egon_scenario_capacities c
             ON (c.carrier = a.carrier)
             WHERE bus IN (
                 SELECT bus_id FROM grid.egon_etrago_bus
                 WHERE scn_name = '{scn}'
                 AND country = 'DE')
             AND c.scenario_name='{scn}'
             GROUP BY (scn_name, a.carrier, c.capacity);

        """
    )

    sum_installed_storage_cap_DE["Error"] = (
        (
            sum_installed_storage_cap_DE["capacity_mw"]
            - sum_installed_storage_cap_DE["target_capacity"]
        )
        / sum_installed_storage_cap_DE["target_capacity"]
    ) * 100

    e = sum_installed_storage_cap_DE["Error"].values[0]

    print(
        "The target value for carrier 'pumped_hydro' differs by "
        + str(round(e, 2))
        + f" % from the input value in scenario {scn}."
    )


    # Section to check loads

    print("For German electricity loads the following deviations between the input and output can be observed:")

    output_demand = db.select_dataframe(
        """SELECT a.scn_name, a.carrier,  ROUND((SUM((SELECT SUM(p) FROM UNNEST(b.p_set) p))/1000000)::numeric, 2) as load_twh
            FROM grid.egon_etrago_load a
            JOIN grid.egon_etrago_load_timeseries b
            ON (a.load_id = b.load_id)
            JOIN grid.egon_etrago_bus c
            ON (a.bus=c.bus_id)
            AND b.scn_name = 'eGon2035'
            AND a.scn_name = 'eGon2035'
            AND a.carrier = 'AC'
            AND c.scn_name= 'eGon2035'
            AND c.country='DE'
            GROUP BY (a.scn_name, a.carrier);

    """
    )["load_twh"].values[0]

    input_cts_ind = db.select_dataframe(
        """SELECT scenario, ROUND(SUM(demand::numeric/1000000), 2) as demand_mw_regio_cts_ind
            FROM demand.egon_demandregio_cts_ind
            WHERE scenario= 'eGon2035'
            AND year IN ('2035')
            GROUP BY (scenario);

        """
    )["demand_mw_regio_cts_ind"].values[0]

    input_hh = db.select_dataframe(
        """SELECT scenario, ROUND(SUM(demand::numeric/1000000), 2) as demand_mw_regio_hh
            FROM demand.egon_demandregio_hh
            WHERE scenario= 'eGon2035'
            AND year IN ('2035')
            GROUP BY (scenario);
        """
    )["demand_mw_regio_hh"].values[0]

    input_demand = input_hh + input_cts_ind

    e = round((output_demand - input_demand) / input_demand, 2) * 100

    print(f"electricity demand: {e} %")


def sanitycheck_eGon2035_heat():

    """Returns print statements as sanity checks for electricity generators in
    the eGon2035 scenario.
    Parameters
    ----------
    None

    Returns
    -------
    None
    """

    # Check input and output values for the carriers "other_non_renewable",
    # "other_renewable", "reservoir", "run_of_river" and "oil"

    scn = "eGon2035"

    # Section to check generator capacities
    print(f"Sanity checks for scenario {scn}")
    print(
        "For German heat demands the following deviations between the inputs and outputs can be observed:"
    )


    # Sanity checks for heat demand

    output_heat_demand = db.select_dataframe(
        """SELECT a.scn_name,  ROUND((SUM((SELECT SUM(p) FROM UNNEST(b.p_set) p))/1000000)::numeric, 2) as load_twh
            FROM grid.egon_etrago_load a
            JOIN grid.egon_etrago_load_timeseries b
            ON (a.load_id = b.load_id)
            JOIN grid.egon_etrago_bus c
            ON (a.bus=c.bus_id)
            AND b.scn_name = 'eGon2035'
            AND a.scn_name = 'eGon2035'
            AND c.scn_name= 'eGon2035'
            AND c.country='DE'
            AND a.carrier IN ('rural_heat', 'central_heat')
            GROUP BY (a.scn_name);
        """
    )["load_twh"].values[0]



    input_heat_demand = db.select_dataframe(
        """SELECT scenario, ROUND(SUM(demand::numeric/1000000), 2) as demand_mw_peta_heat
            FROM demand.egon_peta_heat
            WHERE scenario= 'eGon2035'
            GROUP BY (scenario);
        """
    )[
        "demand_mw_peta_heat"
    ].values[0]

    e_demand = (
        round(
            (output_heat_demand - input_heat_demand)
            / input_heat_demand,
            2,
        )
        * 100
    )

    print(f"heat demand: {e_demand} %")

    # Sanity checks for heat supply

    print(
        "For German heat supplies the following deviations between the inputs and outputs can be observed:"
    )

    # Comparison for central heat pumps
    heat_pump_input = db.select_dataframe(
        """SELECT carrier, ROUND(SUM(capacity::numeric), 2) as Urban_central_heat_pump_mw
            FROM supply.egon_scenario_capacities
            WHERE carrier= 'urban_central_heat_pump'
            AND scenario_name IN ('eGon2035')
            GROUP BY (carrier);
        """
    )[
        "urban_central_heat_pump_mw"
    ].values[0]

    heat_pump_output = db.select_dataframe(
        """SELECT carrier, ROUND(SUM(p_nom::numeric), 2) as Central_heat_pump_mw
            FROM grid.egon_etrago_link
            WHERE carrier= 'central_heat_pump'
            AND scn_name IN ('eGon2035')
            GROUP BY (carrier);
    """
    )["central_heat_pump_mw"].values[0]

    e_heat_pump = (
        round(
            (
                heat_pump_output
                - heat_pump_input
            )
            / heat_pump_output,
            2,
        )
        * 100
    )

    print(
        f"'central_heat_pump': {e_heat_pump} % "
    )

    # Comparison for residential heat pumps

    input_residential_heat_pump = db.select_dataframe(
        """SELECT carrier, ROUND(SUM(capacity::numeric), 2) as residential_heat_pump_mw
            FROM supply.egon_scenario_capacities
            WHERE carrier= 'residential_rural_heat_pump'
            AND scenario_name IN ('eGon2035')
            GROUP BY (carrier);
        """
    )[
        "residential_heat_pump_mw"
    ].values[0]

    output_residential_heat_pump = db.select_dataframe(
        """SELECT carrier, ROUND(SUM(p_nom::numeric), 2) as rural_heat_pump_mw
            FROM grid.egon_etrago_link
            WHERE carrier= 'rural_heat_pump'
            AND scn_name IN ('eGon2035')
            GROUP BY (carrier);
    """
    )["rural_heat_pump_mw"].values[0]

    e_residential_heat_pump = (
        round(
            (
                output_residential_heat_pump
                - input_residential_heat_pump
            )
            / input_residential_heat_pump,
            2,
        )
        * 100
    )
    print(
        f"'residential heat pumps': {e_residential_heat_pump} %"
    )

    # Comparison for resistive heater
    resistive_heater_input = db.select_dataframe(
        """SELECT carrier, ROUND(SUM(capacity::numeric), 2) as Urban_central_resistive_heater_MW
            FROM supply.egon_scenario_capacities
            WHERE carrier= 'urban_central_resistive_heater'
            AND scenario_name IN ('eGon2035')
            GROUP BY (carrier);
        """
    )[
        "urban_central_resistive_heater_mw"
    ].values[0]

    resistive_heater_output = db.select_dataframe(
        """SELECT carrier, ROUND(SUM(p_nom::numeric), 2) as Central_resistive_heater_MW
            FROM grid.egon_etrago_link
            WHERE carrier= 'central_resistive_heater'
            AND scn_name IN ('eGon2035')
            GROUP BY (carrier);
        """
    )[
        "central_resistive_heater_mw"
    ].values[0]

    e_resistive_heater = (
        round(
            (
                resistive_heater_output
                - resistive_heater_input
            )
            / resistive_heater_input,
            2,
        )
        * 100
    )

    print(
        f"'resistive heater': {e_resistive_heater} %"
    )

    # Comparison for solar thermal collectors

    input_solar_thermal = db.select_dataframe(
        """SELECT carrier, ROUND(SUM(capacity::numeric), 2) as solar_thermal_collector_mw
            FROM supply.egon_scenario_capacities
            WHERE carrier= 'urban_central_solar_thermal_collector'
            AND scenario_name IN ('eGon2035')
            GROUP BY (carrier);
        """
    )[
        "solar_thermal_collector_mw"
    ].values[0]

    output_solar_thermal = db.select_dataframe(
        """SELECT carrier, ROUND(SUM(p_nom::numeric), 2) as solar_thermal_collector_mw
            FROM grid.egon_etrago_generator
            WHERE carrier= 'solar_thermal_collector'
            AND scn_name IN ('eGon2035')
            GROUP BY (carrier);
        """
    )[
        "solar_thermal_collector_mw"
    ].values[0]

    e_solar_thermal = (
        round(
            (
                output_solar_thermal
                - input_solar_thermal
            )
            / input_solar_thermal,
            2,
        )
        * 100
    )
    print(
        f"'solar thermal collector': {e_solar_thermal} %"
    )


    # Comparison for geothermal

    input_geo_thermal = db.select_dataframe(
        """SELECT carrier, ROUND(SUM(capacity::numeric), 2) as Urban_central_geo_thermal_MW
            FROM supply.egon_scenario_capacities
            WHERE carrier= 'urban_central_geo_thermal'
            AND scenario_name IN ('eGon2035')
            GROUP BY (carrier);
        """
    )[
        "urban_central_geo_thermal_mw"
    ].values[0]

    output_geo_thermal = db.select_dataframe(
        """SELECT carrier, ROUND(SUM(p_nom::numeric), 2) as geo_thermal_MW
            FROM grid.egon_etrago_generator
            WHERE carrier= 'geo_thermal'
            AND scn_name IN ('eGon2035')
            GROUP BY (carrier);

    """
    )["geo_thermal_mw"].values[0]

    e_geo_thermal = (
        round(
            (
                output_geo_thermal
                - input_geo_thermal
            )
            / input_geo_thermal,
            2,
        )
        * 100
    )
    print(
        f"'geothermal': {e_geo_thermal} %"
    )



    # Sanity_checks_eGon100RE


# def sanitycheck_eGon100RE_electricity():

#     """Returns sanity checks for eGon100RE scenario.
#     Parameters
#     ----------
#     None

#     Returns
#     -------
#     None
#     """
#     carriers_electricity = ["onwind", "solar", "solar rooftop", "ror"]
#     for carrier in carriers_electricity:
#         sum_output = db.select_dataframe(
#             f"""SELECT scn_name, ROUND(SUM(p_nom::numeric), 2) as output_capacity_MW
#                      FROM grid.egon_etrago_generator
#                      WHERE scn_name = 'eGon100RE'
#                      AND carrier IN ('{carrier}')
#                      GROUP BY (scn_name);
#             """
#         )
#         if carrier == "onwind":
#             carrier = "wind_onshore"

#         elif carrier == "solar rooftop":
#             carrier = "solar_rooftop"

#         elif carrier == "solar":
#             carrier = "solar"
#         elif carrier == "ror":
#             carrier = "run_of_river"

#         sum_input = db.select_dataframe(
#             f""" SELECT carrier, ROUND(SUM(capacity::numeric), 2) as input_capacity_MW
#                     FROM supply.egon_scenario_capacities
#                     WHERE carrier= '{carrier}'
#                     AND scenario_name IN ('eGon100RE')
#                     GROUP BY (carrier);
#             """
#         )

#         sum_input["Error"] = (
#             (sum_output["output_capacity_mw"] - sum_input["input_capacity_mw"])
#             / sum_input["input_capacity_mw"]
#         ) * 100

#         g1 = sum_input["Error"].values[0]
#         g = round(g1, 2)

#         print(f"The target values for {carrier} differ by {g}  %")

#     # For_offwind_total

#     carriers_electricity = ["offwind-dc", "offwind-ac"]
#     for carrier in carriers_electricity:
#         if carrier == "offwind-dc" or "offwind-ac":
#             sum_output = db.select_dataframe(
#             """SELECT scn_name, ROUND(SUM(p_nom::numeric), 2) as output_capacity_MW
#                  FROM grid.egon_etrago_generator
#                  WHERE scn_name = 'eGon100RE'
#                  AND carrier IN ('offwind-dc','offwind-ac')
#                  GROUP BY (scn_name);
#             """
#             )

#         carrier = "wind_offshore"
#         sum_input = db.select_dataframe(
#             """SELECT carrier, ROUND(SUM(capacity::numeric), 2) as input_capacity_MW
#                 FROM supply.egon_scenario_capacities
#                 WHERE carrier= ('wind_offshore')
#                 AND scenario_name IN ('eGon100RE')
#                 GROUP BY (carrier);
#             """
#         )

#         sum_input["Error"] = (
#             (sum_output["output_capacity_mw"] - sum_input["input_capacity_mw"])
#             / sum_input["input_capacity_mw"]
#         ) * 100

#         g1 = sum_input["Error"].values[0]
#         g = round(g1, 2)

#         print(f"The target values for {carrier} differ by {g}  %")


# def sanitycheck_eGon100RE_electricity_storage():

#     """Returns sanity checks for heat.
#     Parameters
#     ----------
#     None

#     Returns
#     -------
#     None
#     """

#     carriers_Heating_storage_units = ["hydro", "PHS"]
#     for carrier in carriers_Heating_storage_units:
#         sum_output = db.select_dataframe(
#             f"""SELECT scn_name, ROUND(SUM(p_nom::numeric), 2) as output_capacity_MW
#                  FROM grid.egon_etrago_storage
#                  WHERE scn_name = 'eGon100RE'
#                  AND carrier IN ('{carrier}')
#                  GROUP BY (scn_name);
#             """
#         )
#     if carrier == "hydro":
#         carrier = "hydro"

#     elif carrier == "PHS":
#         carrier = "pumped_hydro"

#     sum_input = db.select_dataframe(
#         f"""SELECT carrier, ROUND(SUM(capacity::numeric), 2) as input_capacity_MW
#              FROM supply.egon_scenario_capacities
#              WHERE carrier= '{carrier}'
#              AND scenario_name IN ('eGon100RE')
#              GROUP BY (carrier);
#         """
#     )

#     sum_input["error"] = (
#         (sum_output["output_capacity_mw"] - sum_input["input_capacity_mw"])
#         / sum_input["input_capacity_mw"]
#     ) * 100

#     g1 = sum_input["error"].values[0]
#     g = round(g1, 2)
#     print(f"The target values for {carrier} differ by {g}  %")

#     # Sanity_checks_eGon100RE_Heating


# def sanitycheck_eGon100RE_heat_generator():

#     """Returns sanity checks for heat.
#     Parameters
#     ----------
#     None

#     Returns
#     -------
#     None
#     """
#     # Urban_central_solar_thermal

#     sum_output_urban_central_solar_thermal = db.select_dataframe(
#         """SELECT scn_name, ROUND(SUM(p_nom::numeric), 2) as output_capacity_MW
#             FROM grid.egon_etrago_generator
#             WHERE scn_name = 'eGon100RE'
#             AND carrier IN ('urban central solar thermal')
#             GROUP BY (scn_name);
#      """
#     )

#     sum_input_urban_central_solar_thermal = db.select_dataframe(
#         """SELECT carrier, ROUND(SUM(capacity::numeric), 2) as input_capacity_MW
#             FROM supply.egon_scenario_capacities
#             WHERE carrier= ('urban_central_solar_thermal')
#             AND scenario_name IN ('eGon100RE')
#             GROUP BY (carrier);
#         """
#     )

#     sum_input_urban_central_solar_thermal["Error"] = (
#         (
#             sum_output_urban_central_solar_thermal["output_capacity_mw"]
#             - sum_input_urban_central_solar_thermal["input_capacity_mw"]
#         )
#         / sum_input_urban_central_solar_thermal["input_capacity_mw"]
#     ) * 100

#     g1 = sum_input_urban_central_solar_thermal["Error"].values[0]
#     g = round(g1, 2)
#     print(
#         f"The target values for urban central solar thermal differ by {g}  %"
#     )

#     # Urban_central_Geo_thermal

#     sum_output_urban_central_geo_thermal = db.select_dataframe(
#         """SELECT scn_name, ROUND(SUM(p_nom::numeric), 2) as output_capacity_MW
#             FROM grid.egon_etrago_generator
#             WHERE scn_name = 'eGon100RE'
#             AND carrier IN ('urban central geo thermal')
#             GROUP BY (scn_name);
#         """
#     )

#     sum_input_urban_central_geo_thermal = db.select_dataframe(
#         """SELECT carrier, ROUND(SUM(capacity::numeric), 2) as input_capacity_MW
#             FROM supply.egon_scenario_capacities
#             WHERE carrier= ('urban_central_geo_thermal')
#             AND scenario_name IN ('eGon100RE')
#             GROUP BY (carrier);
#        """
#     )

#     sum_input_urban_central_geo_thermal["Error"] = (
#         (
#             sum_output_urban_central_geo_thermal["output_capacity_mw"]
#             - sum_input_urban_central_geo_thermal["input_capacity_mw"]
#         )
#         / sum_input_urban_central_geo_thermal["input_capacity_mw"]
#     ) * 100

#     g1 = sum_input_urban_central_geo_thermal["Error"].values[0]
#     g = round(g1, 2)

#     print(f"The target values for urban central geo thermal differ by {g}  %")

#     # For_residential_rural_solar_thermal+service_rural_solar_thermal=rural_solar_thermal

#     sum_output_rural_solar_thermal = db.select_dataframe(
#         """
#         SELECT scn_name, ROUND(SUM(p_nom::numeric), 2) as output_capacity_MW
#         FROM grid.egon_etrago_generator
#         WHERE scn_name = 'eGon100RE'
#         AND carrier IN ('residential rural solar thermal', 'service rural solar thermal')
#         GROUP BY (scn_name);
#         """
#     )

#     sum_input_rural_solar_thermal = db.select_dataframe(
#         """
#        SELECT carrier, ROUND(SUM(capacity::numeric), 2) as input_capacity_MW
#        FROM supply.egon_scenario_capacities
#        WHERE carrier= ('rural_solar_thermal)
#        AND scenario_name IN ('eGon100RE')
#        GROUP BY (carrier);
#        """
#     )

#     sum_input_rural_solar_thermal["Error"] = (
#         (
#             sum_output_rural_solar_thermal["output_capacity_mw"]
#             - sum_input_rural_solar_thermal["input_capacity_mw"]
#         )
#         / sum_input_rural_solar_thermal["input_capacity_mw"]
#     ) * 100

#     g1 = sum_input_rural_solar_thermal["Error"].values[0]
#     g = round(g1, 2)

#     print(f"The target values for rural solar thermal differ by {g}  %")


# def sanitycheck_eGon100RE_heat_link():

#     """Returns sanity checks for heat.
#     Parameters
#     ----------
#     None

#     Returns
#     -------
#     None
#     """

#     carriers_Heating_link = [
#         "urban central air heat pump",
#         "urban central resistive heater",
#         "services rural resistive heater",
#         "urban_gas",
#     ]
#     for carrier in carriers_Heating_link:
#         sum_output = db.select_dataframe(
#             f"""
#                  SELECT scn_name, ROUND(SUM(p_nom::numeric), 2) as output_capacity_MW
#                  FROM grid.egon_etrago_link
#                  WHERE scn_name = 'eGon100RE'
#                  AND carrier IN ('{carrier}')
#                  GROUP BY (scn_name);
#              """
#         )
#     if sum_output.shape[0] == 0:
#         print(f"{carrier} is not distributed correctly, please revise")

#     elif carrier == "urban central air heat pump":
#         carrier = "urban_central_air_heat_pump"

#     elif carrier == "urban central resistive heater":
#         carrier = "urban_central_resistive_heater"

#     elif carrier == "services rural resistive heater":
#         carrier = "rural_resistive_heater"

#     sum_input = db.select_dataframe(
#         f"""
#             SELECT carrier, ROUND(SUM(capacity::numeric), 2) as input_capacity_MW
#             FROM supply.egon_scenario_capacities
#             WHERE carrier= '{carrier}'
#             AND scenario_name IN ('eGon100RE')
#             GROUP BY (carrier);
#         """
#     )

#     sum_output["Error"] = (
#         (sum_output["output_capacity_mw"] - sum_input["input_capacity_mw"])
#         / sum_input["input_capacity_mw"]
#     ) * 100

#     g1 = sum_output["Error"].values[0]
#     g = round(g1, 2)

#     print(f"The target values for {carrier} differ by {g}  %")

#     # Heat_Pump_to_be_added

#     sum_output_heat_pump = db.select_dataframe(
#         """
#         SELECT scn_name, ROUND(SUM(p_nom::numeric), 2) as output_capacity_MW
#         FROM grid.egon_etrago_link
#         WHERE scn_name = 'eGon100RE'
#         AND carrier IN ('heat_pump')
#         GROUP BY (scn_name);
#     """
#     )

#     sum_input_heat_pump = db.select_dataframe(
#         """
#         SELECT carrier, ROUND(SUM(capacity::numeric), 2) as input_capacity_MW
#         FROM supply.egon_scenario_capacities
#         WHERE carrier= ('rural_heat_pump')
#         AND scenario_name IN ('eGon100RE')
#         GROUP BY (carrier);
#         """
#     )

#     sum_input_heat_pump["Error"] = (
#         (
#             sum_output_heat_pump["output_capacity_mw"]
#             - sum_input_heat_pump["input_capacity_mw"]
#         )
#         / sum_input_heat_pump["input_capacity_mw"]
#     ) * 100

#     g1 = sum_input_heat_pump["Error"].values[0]
#     g = round(g1, 2)

#     print(f"The target values for rural heat pump differ by {g}  %")
