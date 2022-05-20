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
        "wind_onshore",
        "wind_offshore",
        "solar",
        "solar_rooftop",
        "biomass",
    ]

    for carrier in carriers_electricity:

        if carrier == "biomass":
            sum_output = db.select_dataframe(
                """SELECT scn_name, ROUND(SUM(p_nom::numeric), 2) as output_capacity_mw
                    FROM grid.egon_etrago_generator
                    WHERE bus IN (
                        SELECT bus_id FROM grid.egon_etrago_bus
                        WHERE scn_name = 'eGon2035'
                        AND country = 'DE')
                    AND carrier IN ('biomass', 'industrial_biomass_CHP', 'central_biomass_CHP')
                    GROUP BY (scn_name);
                """,
                warning=False,
            )

        else:
            sum_output = db.select_dataframe(
                f"""SELECT scn_name, ROUND(SUM(p_nom::numeric), 2) as output_capacity_mw
                         FROM grid.egon_etrago_generator
                         WHERE scn_name = '{scn}'
                         AND carrier IN ('{carrier}')
                         AND bus IN
                             (SELECT bus_id
                               FROM grid.egon_etrago_bus
                               WHERE scn_name = 'eGon2035'
                               AND country = 'DE')
                         GROUP BY (scn_name);
                    """,
                warning=False,
            )

        sum_input = db.select_dataframe(
            f"""SELECT carrier, ROUND(SUM(capacity::numeric), 2) as input_capacity_mw
                     FROM supply.egon_scenario_capacities
                     WHERE carrier= '{carrier}'
                     AND scenario_name ='{scn}'
                     GROUP BY (carrier);
                """,
            warning=False,
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

            print(f"{carrier}: " + str(round(g, 2)) + " %")

    # Section to check storage capacities

    sum_installed_storage = db.select_dataframe(
        f"""SELECT scn_name, a.carrier, ROUND(SUM(p_nom::numeric), 2) as capacity_mw, ROUND(c.capacity::numeric, 2) as target_capacity
             FROM grid.egon_etrago_storage a
             JOIN supply.egon_scenario_capacities c
             ON (c.carrier = a.carrier)
             WHERE bus IN (
                 SELECT bus_id FROM grid.egon_etrago_bus
                 WHERE scn_name = '{scn}'
                 AND country = 'DE')
             AND c.scenario_name='{scn}'
             AND a.scn_name = '{scn}'
             GROUP BY (scn_name, a.carrier, c.capacity);

        """,
        warning=False,
    )

    sum_installed_storage["Error"] = (
        (
            sum_installed_storage["capacity_mw"]
            - sum_installed_storage["target_capacity"]
        )
        / sum_installed_storage["target_capacity"]
    ) * 100

    e = sum_installed_storage["Error"].values[0]

    print("pumped hydro storages: " + str(round(e, 2)) + " %")

    # Section to check loads

    print(
        "For German electricity loads the following deviations between the input and output can be observed:"
    )

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

    """,
        warning=False,
    )["load_twh"].values[0]

    input_cts_ind = db.select_dataframe(
        """SELECT scenario, ROUND(SUM(demand::numeric/1000000), 2) as demand_mw_regio_cts_ind
            FROM demand.egon_demandregio_cts_ind
            WHERE scenario= 'eGon2035'
            AND year IN ('2035')
            GROUP BY (scenario);

        """,
        warning=False,
    )["demand_mw_regio_cts_ind"].values[0]

    input_hh = db.select_dataframe(
        """SELECT scenario, ROUND(SUM(demand::numeric/1000000), 2) as demand_mw_regio_hh
            FROM demand.egon_demandregio_hh
            WHERE scenario= 'eGon2035'
            AND year IN ('2035')
            GROUP BY (scenario);
        """,
        warning=False,
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
        """,
        warning=False,
    )["load_twh"].values[0]

    input_heat_demand = db.select_dataframe(
        """SELECT scenario, ROUND(SUM(demand::numeric/1000000), 2) as demand_mw_peta_heat
            FROM demand.egon_peta_heat
            WHERE scenario= 'eGon2035'
            GROUP BY (scenario);
        """,
        warning=False,
    )["demand_mw_peta_heat"].values[0]

    e_demand = (
        round((output_heat_demand - input_heat_demand) / input_heat_demand, 2)
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
        """,
        warning=False,
    )["urban_central_heat_pump_mw"].values[0]

    heat_pump_output = db.select_dataframe(
        """SELECT carrier, ROUND(SUM(p_nom::numeric), 2) as Central_heat_pump_mw
            FROM grid.egon_etrago_link
            WHERE carrier= 'central_heat_pump'
            AND scn_name IN ('eGon2035')
            GROUP BY (carrier);
    """,
        warning=False,
    )["central_heat_pump_mw"].values[0]

    e_heat_pump = (
        round((heat_pump_output - heat_pump_input) / heat_pump_output, 2) * 100
    )

    print(f"'central_heat_pump': {e_heat_pump} % ")

    # Comparison for residential heat pumps

    input_residential_heat_pump = db.select_dataframe(
        """SELECT carrier, ROUND(SUM(capacity::numeric), 2) as residential_heat_pump_mw
            FROM supply.egon_scenario_capacities
            WHERE carrier= 'residential_rural_heat_pump'
            AND scenario_name IN ('eGon2035')
            GROUP BY (carrier);
        """,
        warning=False,
    )["residential_heat_pump_mw"].values[0]

    output_residential_heat_pump = db.select_dataframe(
        """SELECT carrier, ROUND(SUM(p_nom::numeric), 2) as rural_heat_pump_mw
            FROM grid.egon_etrago_link
            WHERE carrier= 'rural_heat_pump'
            AND scn_name IN ('eGon2035')
            GROUP BY (carrier);
    """,
        warning=False,
    )["rural_heat_pump_mw"].values[0]

    e_residential_heat_pump = (
        round(
            (output_residential_heat_pump - input_residential_heat_pump)
            / input_residential_heat_pump,
            2,
        )
        * 100
    )
    print(f"'residential heat pumps': {e_residential_heat_pump} %")

    # Comparison for resistive heater
    resistive_heater_input = db.select_dataframe(
        """SELECT carrier, ROUND(SUM(capacity::numeric), 2) as Urban_central_resistive_heater_MW
            FROM supply.egon_scenario_capacities
            WHERE carrier= 'urban_central_resistive_heater'
            AND scenario_name IN ('eGon2035')
            GROUP BY (carrier);
        """,
        warning=False,
    )["urban_central_resistive_heater_mw"].values[0]

    resistive_heater_output = db.select_dataframe(
        """SELECT carrier, ROUND(SUM(p_nom::numeric), 2) as Central_resistive_heater_MW
            FROM grid.egon_etrago_link
            WHERE carrier= 'central_resistive_heater'
            AND scn_name IN ('eGon2035')
            GROUP BY (carrier);
        """,
        warning=False,
    )["central_resistive_heater_mw"].values[0]

    e_resistive_heater = (
        round(
            (resistive_heater_output - resistive_heater_input)
            / resistive_heater_input,
            2,
        )
        * 100
    )

    print(f"'resistive heater': {e_resistive_heater} %")

    # Comparison for solar thermal collectors

    input_solar_thermal = db.select_dataframe(
        """SELECT carrier, ROUND(SUM(capacity::numeric), 2) as solar_thermal_collector_mw
            FROM supply.egon_scenario_capacities
            WHERE carrier= 'urban_central_solar_thermal_collector'
            AND scenario_name IN ('eGon2035')
            GROUP BY (carrier);
        """,
        warning=False,
    )["solar_thermal_collector_mw"].values[0]

    output_solar_thermal = db.select_dataframe(
        """SELECT carrier, ROUND(SUM(p_nom::numeric), 2) as solar_thermal_collector_mw
            FROM grid.egon_etrago_generator
            WHERE carrier= 'solar_thermal_collector'
            AND scn_name IN ('eGon2035')
            GROUP BY (carrier);
        """,
        warning=False,
    )["solar_thermal_collector_mw"].values[0]

    e_solar_thermal = (
        round(
            (output_solar_thermal - input_solar_thermal) / input_solar_thermal,
            2,
        )
        * 100
    )
    print(f"'solar thermal collector': {e_solar_thermal} %")

    # Comparison for geothermal

    input_geo_thermal = db.select_dataframe(
        """SELECT carrier, ROUND(SUM(capacity::numeric), 2) as Urban_central_geo_thermal_MW
            FROM supply.egon_scenario_capacities
            WHERE carrier= 'urban_central_geo_thermal'
            AND scenario_name IN ('eGon2035')
            GROUP BY (carrier);
        """,
        warning=False,
    )["urban_central_geo_thermal_mw"].values[0]

    output_geo_thermal = db.select_dataframe(
        """SELECT carrier, ROUND(SUM(p_nom::numeric), 2) as geo_thermal_MW
            FROM grid.egon_etrago_generator
            WHERE carrier= 'geo_thermal'
            AND scn_name IN ('eGon2035')
            GROUP BY (carrier);
    """,
        warning=False,
    )["geo_thermal_mw"].values[0]

    e_geo_thermal = (
        round((output_geo_thermal - input_geo_thermal) / input_geo_thermal, 2)
        * 100
    )
    print(f"'geothermal': {e_geo_thermal} %")
