"""
This module does sanity checks for both the eGon2035 and the eGon100RE scenario
separately where a percentage error is given to showcase difference in output
and input values. Please note that there are missing input technologies in the
supply tables.
Authors: @ALonso, @dana
"""

import numpy as np
import pandas as pd

from egon.data import db, logger
from egon.data.datasets import Dataset
from egon.data.datasets.electricity_demand_timeseries.cts_buildings import (
    EgonCtsElectricityDemandBuildingShare,
    EgonCtsHeatDemandBuildingShare,
)


class SanityChecks(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="SanityChecks",
            version="0.0.4",
            dependencies=dependencies,
            tasks={
                etrago_eGon2035_electricity,
                etrago_eGon2035_heat,
                residential_electricity_annual_sum,
                residential_electricity_hh_refinement,
            },
        )


def etrago_eGon2035_electricity():
    """Execute basic sanity checks.

    Returns print statements as sanity checks for the electricity sector in
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
    logger.info(f"Sanity checks for scenario {scn}")
    logger.info(
        "For German electricity generators the following deviations between "
        "the inputs and outputs can be observed:"
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
                """SELECT scn_name, SUM(p_nom::numeric) as output_capacity_mw
                    FROM grid.egon_etrago_generator
                    WHERE bus IN (
                        SELECT bus_id FROM grid.egon_etrago_bus
                        WHERE scn_name = 'eGon2035'
                        AND country = 'DE')
                    AND carrier IN ('biomass', 'industrial_biomass_CHP',
                    'central_biomass_CHP')
                    GROUP BY (scn_name);
                """,
                warning=False,
            )

        else:
            sum_output = db.select_dataframe(
                f"""SELECT scn_name,
                 SUM(p_nom::numeric) as output_capacity_mw
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
            f"""SELECT carrier, SUM(capacity::numeric) as input_capacity_mw
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
            logger.info(
                f"No capacity for carrier '{carrier}' needed to be"
                f" distributed. Everything is fine"
            )

        elif (
            sum_input.input_capacity_mw.sum() > 0
            and sum_output.output_capacity_mw.sum() == 0
        ):
            logger.info(
                f"Error: Capacity for carrier '{carrier}' was not distributed "
                f"at all!"
            )

        elif (
            sum_output.output_capacity_mw.sum() > 0
            and sum_input.input_capacity_mw.sum() == 0
        ):
            logger.info(
                f"Error: Eventhough no input capacity was provided for carrier"
                f"'{carrier}' a capacity got distributed!"
            )

        else:
            sum_input["error"] = (
                (sum_output.output_capacity_mw - sum_input.input_capacity_mw)
                / sum_input.input_capacity_mw
            ) * 100
            g = sum_input["error"].values[0]

            logger.info(f"{carrier}: " + str(round(g, 2)) + " %")

    # Section to check storage units

    logger.info(f"Sanity checks for scenario {scn}")
    logger.info(
        "For German electrical storage units the following deviations between"
        "the inputs and outputs can be observed:"
    )

    carriers_electricity = ["pumped_hydro"]

    for carrier in carriers_electricity:

        sum_output = db.select_dataframe(
            f"""SELECT scn_name, SUM(p_nom::numeric) as output_capacity_mw
                         FROM grid.egon_etrago_storage
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
            f"""SELECT carrier, SUM(capacity::numeric) as input_capacity_mw
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
            logger.info(
                f"No capacity for carrier '{carrier}' needed to be "
                f"distributed. Everything is fine"
            )

        elif (
            sum_input.input_capacity_mw.sum() > 0
            and sum_output.output_capacity_mw.sum() == 0
        ):
            logger.info(
                f"Error: Capacity for carrier '{carrier}' was not distributed"
                f" at all!"
            )

        elif (
            sum_output.output_capacity_mw.sum() > 0
            and sum_input.input_capacity_mw.sum() == 0
        ):
            logger.info(
                f"Error: Eventhough no input capacity was provided for carrier"
                f" '{carrier}' a capacity got distributed!"
            )

        else:
            sum_input["error"] = (
                (sum_output.output_capacity_mw - sum_input.input_capacity_mw)
                / sum_input.input_capacity_mw
            ) * 100
            g = sum_input["error"].values[0]

            logger.info(f"{carrier}: " + str(round(g, 2)) + " %")

    # Section to check loads

    logger.info(
        "For German electricity loads the following deviations between the"
        " input and output can be observed:"
    )

    output_demand = db.select_dataframe(
        """SELECT a.scn_name, a.carrier,  SUM((SELECT SUM(p)
        FROM UNNEST(b.p_set) p))/1000000::numeric as load_twh
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
        """SELECT scenario,
         SUM(demand::numeric/1000000) as demand_mw_regio_cts_ind
            FROM demand.egon_demandregio_cts_ind
            WHERE scenario= 'eGon2035'
            AND year IN ('2035')
            GROUP BY (scenario);

        """,
        warning=False,
    )["demand_mw_regio_cts_ind"].values[0]

    input_hh = db.select_dataframe(
        """SELECT scenario, SUM(demand::numeric/1000000) as demand_mw_regio_hh
            FROM demand.egon_demandregio_hh
            WHERE scenario= 'eGon2035'
            AND year IN ('2035')
            GROUP BY (scenario);
        """,
        warning=False,
    )["demand_mw_regio_hh"].values[0]

    input_demand = input_hh + input_cts_ind

    e = round((output_demand - input_demand) / input_demand, 2) * 100

    logger.info(f"electricity demand: {e} %")


def etrago_eGon2035_heat():
    """Execute basic sanity checks.

    Returns print statements as sanity checks for the heat sector in
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
    logger.info(f"Sanity checks for scenario {scn}")
    logger.info(
        "For German heat demands the following deviations between the inputs"
        " and outputs can be observed:"
    )

    # Sanity checks for heat demand

    output_heat_demand = db.select_dataframe(
        """SELECT a.scn_name,
          (SUM(
          (SELECT SUM(p) FROM UNNEST(b.p_set) p))/1000000)::numeric as load_twh
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
        """SELECT scenario, SUM(demand::numeric/1000000) as demand_mw_peta_heat
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

    logger.info(f"heat demand: {e_demand} %")

    # Sanity checks for heat supply

    logger.info(
        "For German heat supplies the following deviations between the inputs "
        "and outputs can be observed:"
    )

    # Comparison for central heat pumps
    heat_pump_input = db.select_dataframe(
        """SELECT carrier, SUM(capacity::numeric) as Urban_central_heat_pump_mw
            FROM supply.egon_scenario_capacities
            WHERE carrier= 'urban_central_heat_pump'
            AND scenario_name IN ('eGon2035')
            GROUP BY (carrier);
        """,
        warning=False,
    )["urban_central_heat_pump_mw"].values[0]

    heat_pump_output = db.select_dataframe(
        """SELECT carrier, SUM(p_nom::numeric) as Central_heat_pump_mw
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

    logger.info(f"'central_heat_pump': {e_heat_pump} % ")

    # Comparison for residential heat pumps

    input_residential_heat_pump = db.select_dataframe(
        """SELECT carrier, SUM(capacity::numeric) as residential_heat_pump_mw
            FROM supply.egon_scenario_capacities
            WHERE carrier= 'residential_rural_heat_pump'
            AND scenario_name IN ('eGon2035')
            GROUP BY (carrier);
        """,
        warning=False,
    )["residential_heat_pump_mw"].values[0]

    output_residential_heat_pump = db.select_dataframe(
        """SELECT carrier, SUM(p_nom::numeric) as rural_heat_pump_mw
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
    logger.info(f"'residential heat pumps': {e_residential_heat_pump} %")

    # Comparison for resistive heater
    resistive_heater_input = db.select_dataframe(
        """SELECT carrier,
         SUM(capacity::numeric) as Urban_central_resistive_heater_MW
            FROM supply.egon_scenario_capacities
            WHERE carrier= 'urban_central_resistive_heater'
            AND scenario_name IN ('eGon2035')
            GROUP BY (carrier);
        """,
        warning=False,
    )["urban_central_resistive_heater_mw"].values[0]

    resistive_heater_output = db.select_dataframe(
        """SELECT carrier, SUM(p_nom::numeric) as central_resistive_heater_MW
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

    logger.info(f"'resistive heater': {e_resistive_heater} %")

    # Comparison for solar thermal collectors

    input_solar_thermal = db.select_dataframe(
        """SELECT carrier, SUM(capacity::numeric) as solar_thermal_collector_mw
            FROM supply.egon_scenario_capacities
            WHERE carrier= 'urban_central_solar_thermal_collector'
            AND scenario_name IN ('eGon2035')
            GROUP BY (carrier);
        """,
        warning=False,
    )["solar_thermal_collector_mw"].values[0]

    output_solar_thermal = db.select_dataframe(
        """SELECT carrier, SUM(p_nom::numeric) as solar_thermal_collector_mw
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
    logger.info(f"'solar thermal collector': {e_solar_thermal} %")

    # Comparison for geothermal

    input_geo_thermal = db.select_dataframe(
        """SELECT carrier,
         SUM(capacity::numeric) as Urban_central_geo_thermal_MW
            FROM supply.egon_scenario_capacities
            WHERE carrier= 'urban_central_geo_thermal'
            AND scenario_name IN ('eGon2035')
            GROUP BY (carrier);
        """,
        warning=False,
    )["urban_central_geo_thermal_mw"].values[0]

    output_geo_thermal = db.select_dataframe(
        """SELECT carrier, SUM(p_nom::numeric) as geo_thermal_MW
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
    logger.info(f"'geothermal': {e_geo_thermal} %")


def residential_electricity_annual_sum(rtol=1e-5):
    """Sanity check for dataset electricity_demand_timeseries :
    Demand_Building_Assignment

    Aggregate the annual demand of all census cells at NUTS3 to compare
    with initial scaling parameters from DemandRegio.
    """

    df_nuts3_annual_sum = db.select_dataframe(
        sql="""
        SELECT dr.nuts3, dr.scenario, dr.demand_regio_sum, profiles.profile_sum
        FROM (
            SELECT scenario, SUM(demand) AS profile_sum, vg250_nuts3
            FROM demand.egon_demandregio_zensus_electricity AS egon,
             boundaries.egon_map_zensus_vg250 AS boundaries
            Where egon.zensus_population_id = boundaries.zensus_population_id
            AND sector = 'residential'
            GROUP BY vg250_nuts3, scenario
            ) AS profiles
        JOIN (
            SELECT nuts3, scenario, sum(demand) AS demand_regio_sum
            FROM demand.egon_demandregio_hh
            GROUP BY year, scenario, nuts3
              ) AS dr
        ON profiles.vg250_nuts3 = dr.nuts3 and profiles.scenario  = dr.scenario
        """
    )

    np.testing.assert_allclose(
        actual=df_nuts3_annual_sum["profile_sum"],
        desired=df_nuts3_annual_sum["demand_regio_sum"],
        rtol=rtol,
        verbose=False,
    )

    logger.info(
        "Aggregated annual residential electricity demand"
        " matches with DemandRegio at NUTS-3."
    )


def residential_electricity_hh_refinement(rtol=1e-5):
    """Sanity check for dataset electricity_demand_timeseries :
    Household Demands

    Check sum of aggregated household types after refinement method
    was applied and compare it to the original census values."""

    df_refinement = db.select_dataframe(
        sql="""
        SELECT refined.nuts3, refined.characteristics_code,
                refined.sum_refined::int, census.sum_census::int
        FROM(
            SELECT nuts3, characteristics_code, SUM(hh_10types) as sum_refined
            FROM society.egon_destatis_zensus_household_per_ha_refined
            GROUP BY nuts3, characteristics_code)
            AS refined
        JOIN(
            SELECT t.nuts3, t.characteristics_code, sum(orig) as sum_census
            FROM(
                SELECT nuts3, cell_id, characteristics_code,
                        sum(DISTINCT(hh_5types))as orig
                FROM society.egon_destatis_zensus_household_per_ha_refined
                GROUP BY cell_id, characteristics_code, nuts3) AS t
            GROUP BY t.nuts3, t.characteristics_code    ) AS census
        ON refined.nuts3 = census.nuts3
        AND refined.characteristics_code = census.characteristics_code
    """
    )

    np.testing.assert_allclose(
        actual=df_refinement["sum_refined"],
        desired=df_refinement["sum_census"],
        rtol=rtol,
        verbose=False,
    )

    logger.info("All Aggregated household types match at NUTS-3.")


def cts_electricity_demand_share(rtol=1e-5):
    """Sanity check for dataset CtsElectricityBuildings

    Check sum of aggregated cts electricity demand share which equals to one
    for every substation as the substation profile is linearly disaggregated
    to all buildings."""

    with db.session_scope() as session:
        cells_query = session.query(EgonCtsElectricityDemandBuildingShare)

    df_demand_share = pd.read_sql(
        cells_query.statement, cells_query.session.bind, index_col=None
    )

    np.testing.assert_allclose(
        actual=df_demand_share.groupby(["bus_id", "scenario"])[
            "profile_share"
        ].sum(),
        desired=1,
        rtol=rtol,
        verbose=False,
    )

    logger.info("The aggregated demand shares equal to one!.")
