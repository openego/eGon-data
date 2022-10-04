"""
This module does sanity checks for both the eGon2035 and the eGon100RE scenario
separately where a percentage error is given to showcase difference in output
and input values. Please note that there are missing input technologies in the
supply tables.
Authors: @ALonso, @dana, @nailend, @nesnoj, @khelfen
"""
from math import isclose
from pathlib import Path

from sqlalchemy import Numeric
from sqlalchemy.sql import and_, cast, func, or_
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

from egon.data import config, db, logger
from egon.data.datasets import Dataset
from egon.data.datasets.electricity_demand_timeseries.cts_buildings import (
    EgonCtsElectricityDemandBuildingShare,
    EgonCtsHeatDemandBuildingShare,
)
from egon.data.datasets.emobility.motorized_individual_travel.db_classes import (
    EgonEvCountMunicipality,
    EgonEvCountMvGridDistrict,
    EgonEvCountRegistrationDistrict,
    EgonEvMvGridDistrict,
    EgonEvPool,
    EgonEvTrip,
)
from egon.data.datasets.emobility.motorized_individual_travel.helpers import (
    DATASET_CFG,
    read_simbev_metadata_file,
)
from egon.data.datasets.etrago_setup import (
    EgonPfHvLink,
    EgonPfHvLinkTimeseries,
    EgonPfHvLoad,
    EgonPfHvLoadTimeseries,
    EgonPfHvStore,
    EgonPfHvStoreTimeseries,
)
from egon.data.datasets.power_plants.pv_rooftop_buildings import (
    municipality_data,
    osm_buildings,
    scenario_data,
)
from egon.data.datasets.scenario_parameters import get_sector_parameters
import egon.data

TESTMODE_OFF = (
    config.settings()["egon-data"]["--dataset-boundary"] == "Everything"
)


class SanityChecks(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="SanityChecks",
            version="0.0.5",
            dependencies=dependencies,
            tasks={
                etrago_eGon2035_electricity,
                etrago_eGon2035_heat,
                residential_electricity_annual_sum,
                residential_electricity_hh_refinement,
                cts_electricity_demand_share,
                cts_heat_demand_share,
                sanitycheck_emobility_mit,
                sanitycheck_pv_rooftop_buildings,
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
            print(
                f"No capacity for carrier '{carrier}' needed to be "
                f"distributed. Everything is fine"
            )

        elif (
            sum_input.input_capacity_mw.sum() > 0
            and sum_output.output_capacity_mw.sum() == 0
        ):
            print(
                f"Error: Capacity for carrier '{carrier}' was not distributed"
                f" at all!"
            )

        elif (
            sum_output.output_capacity_mw.sum() > 0
            and sum_input.input_capacity_mw.sum() == 0
        ):
            print(
                f"Error: Eventhough no input capacity was provided for carrier"
                f" '{carrier}' a capacity got distributed!"
            )

        else:
            sum_input["error"] = (
                (sum_output.output_capacity_mw - sum_input.input_capacity_mw)
                / sum_input.input_capacity_mw
            ) * 100
            g = sum_input["error"].values[0]

            print(f"{carrier}: " + str(round(g, 2)) + " %")

    # Section to check loads

    print(
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

    print(f"electricity demand: {e} %")


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
    print(f"Sanity checks for scenario {scn}")
    print(
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
    """Sanity check for dataset electricity_demand_timeseries :
    CtsBuildings

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


def cts_heat_demand_share(rtol=1e-5):
    """Sanity check for dataset electricity_demand_timeseries
    : CtsBuildings

    Check sum of aggregated cts heat demand share which equals to one
    for every substation as the substation profile is linearly disaggregated
    to all buildings."""

    with db.session_scope() as session:
        cells_query = session.query(EgonCtsHeatDemandBuildingShare)

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


def sanitycheck_pv_rooftop_buildings():
    def egon_power_plants_pv_roof_building():
        sql = """
        SELECT *
        FROM supply.egon_power_plants_pv_roof_building
        """

        return db.select_dataframe(sql, index_col="index")

    pv_roof_df = egon_power_plants_pv_roof_building()

    municipalities_gdf = municipality_data()

    osm_buildings_gdf = osm_buildings(municipalities_gdf.crs)

    merge_df = pv_roof_df.merge(
        osm_buildings_gdf[["area"]],
        how="left",
        left_on="building_id",
        right_index=True,
    )

    assert len(merge_df.loc[merge_df.area.isna()]) == 0

    scenarios = ["status_quo", "eGon2035"]

    base_path = Path(egon.data.__path__[0]).resolve()

    res_dir = base_path / "sanity_checks"

    res_dir.mkdir(parents=True, exist_ok=True)

    for scenario in scenarios:
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 8))

        scenario_df = merge_df.loc[merge_df.scenario == scenario]

        logger.info(
            scenario + " Capacity:\n" + str(scenario_df.capacity.describe())
        )

        small_gens_df = scenario_df.loc[scenario_df.capacity < 100]

        sns.histplot(data=small_gens_df, x="capacity", ax=ax1).set_title(
            scenario
        )

        sns.scatterplot(
            data=small_gens_df, x="capacity", y="area", ax=ax2
        ).set_title(scenario)

        plt.tight_layout()

        plt.savefig(
            res_dir / f"{scenario}_pv_rooftop_distribution.png",
            bbox_inches="tight",
        )

    scenarios = ["eGon2035"]  # "eGon100RE"]

    for scenario in scenarios:
        assert isclose(
            scenario_data(scenario=scenario).capacity.sum() * 1000,
            merge_df.loc[merge_df.scenario == scenario].capacity.sum(),
            rel_tol=1e-02,
        ), (
            f"{scenario_data(scenario=scenario).capacity.sum() * 1000} != "
            f"{merge_df.loc[merge_df.scenario == scenario].capacity.sum()}"
        )


def sanitycheck_emobility_mit():
    """Execute sanity checks for eMobility: motorized individual travel

    Checks data integrity for eGon2035, eGon2035_lowflex and eGon100RE scenario
    using assertions:
      1. Allocated EV numbers and EVs allocated to grid districts
      2. Trip data (original inout data from simBEV)
      3. Model data in eTraGo PF tables (grid.egon_etrago_*)

    Parameters
    ----------
    None

    Returns
    -------
    None
    """

    def check_ev_allocation():
        # Get target number for scenario
        ev_count_target = scenario_variation_parameters["ev_count"]
        print(f"  Target count: {str(ev_count_target)}")

        # Get allocated numbers
        ev_counts_dict = {}
        with db.session_scope() as session:
            for table, level in zip(
                [
                    EgonEvCountMvGridDistrict,
                    EgonEvCountMunicipality,
                    EgonEvCountRegistrationDistrict,
                ],
                ["Grid District", "Municipality", "Registration District"],
            ):
                query = session.query(
                    func.sum(
                        table.bev_mini
                        + table.bev_medium
                        + table.bev_luxury
                        + table.phev_mini
                        + table.phev_medium
                        + table.phev_luxury
                    ).label("ev_count")
                ).filter(
                    table.scenario == scenario_name,
                    table.scenario_variation == scenario_var_name,
                )

                ev_counts = pd.read_sql(
                    query.statement, query.session.bind, index_col=None
                )
                ev_counts_dict[level] = ev_counts.iloc[0].ev_count
                print(
                    f"    Count table: Total count for level {level} "
                    f"(table: {table.__table__}): "
                    f"{str(ev_counts_dict[level])}"
                )

        # Compare with scenario target (only if not in testmode)
        if TESTMODE_OFF:
            for level, count in ev_counts_dict.items():
                np.testing.assert_allclose(
                    count,
                    ev_count_target,
                    rtol=0.0001,
                    err_msg=f"EV numbers in {level} seems to be flawed.",
                )
        else:
            print("    Testmode is on, skipping sanity check...")

        # Get allocated EVs in grid districts
        with db.session_scope() as session:
            query = session.query(
                func.count(EgonEvMvGridDistrict.egon_ev_pool_ev_id).label(
                    "ev_count"
                ),
            ).filter(
                EgonEvMvGridDistrict.scenario == scenario_name,
                EgonEvMvGridDistrict.scenario_variation == scenario_var_name,
            )
        ev_count_alloc = (
            pd.read_sql(query.statement, query.session.bind, index_col=None)
            .iloc[0]
            .ev_count
        )
        print(
            f"    EVs allocated to Grid Districts "
            f"(table: {EgonEvMvGridDistrict.__table__}) total count: "
            f"{str(ev_count_alloc)}"
        )

        # Compare with scenario target (only if not in testmode)
        if TESTMODE_OFF:
            np.testing.assert_allclose(
                ev_count_alloc,
                ev_count_target,
                rtol=0.0001,
                err_msg=(
                    "EV numbers allocated to Grid Districts seems to be "
                    "flawed."
                ),
            )
        else:
            print("    Testmode is on, skipping sanity check...")

        return ev_count_alloc

    def check_trip_data():
        # Check if trips start at timestep 0 and have a max. of 35040 steps
        # (8760h in 15min steps)
        print("  Checking timeranges...")
        with db.session_scope() as session:
            query = session.query(
                func.count(EgonEvTrip.event_id).label("cnt")
            ).filter(
                or_(
                    and_(
                        EgonEvTrip.park_start > 0,
                        EgonEvTrip.simbev_event_id == 0,
                    ),
                    EgonEvTrip.park_end
                    > (60 / int(meta_run_config.stepsize)) * 8760,
                ),
                EgonEvTrip.scenario == scenario_name,
            )
        invalid_trips = pd.read_sql(
            query.statement, query.session.bind, index_col=None
        )
        np.testing.assert_equal(
            invalid_trips.iloc[0].cnt,
            0,
            err_msg=(
                f"{str(invalid_trips.iloc[0].cnt)} trips in table "
                f"{EgonEvTrip.__table__} have invalid timesteps."
            ),
        )

        # Check if charging demand can be covered by available charging energy
        # while parking
        print("  Compare charging demand with available power...")
        with db.session_scope() as session:
            query = session.query(
                func.count(EgonEvTrip.event_id).label("cnt")
            ).filter(
                func.round(
                    cast(
                        (EgonEvTrip.park_end - EgonEvTrip.park_start + 1)
                        * EgonEvTrip.charging_capacity_nominal
                        * (int(meta_run_config.stepsize) / 60),
                        Numeric,
                    ),
                    3,
                )
                < cast(EgonEvTrip.charging_demand, Numeric),
                EgonEvTrip.scenario == scenario_name,
            )
        invalid_trips = pd.read_sql(
            query.statement, query.session.bind, index_col=None
        )
        np.testing.assert_equal(
            invalid_trips.iloc[0].cnt,
            0,
            err_msg=(
                f"In {str(invalid_trips.iloc[0].cnt)} trips (table: "
                f"{EgonEvTrip.__table__}) the charging demand cannot be "
                f"covered by available charging power."
            ),
        )

    def check_model_data():
        # Check if model components were fully created
        print("  Check if all model components were created...")
        # Get MVGDs which got EV allocated
        with db.session_scope() as session:
            query = (
                session.query(
                    EgonEvMvGridDistrict.bus_id,
                )
                .filter(
                    EgonEvMvGridDistrict.scenario == scenario_name,
                    EgonEvMvGridDistrict.scenario_variation
                    == scenario_var_name,
                )
                .group_by(EgonEvMvGridDistrict.bus_id)
            )
        mvgds_with_ev = (
            pd.read_sql(query.statement, query.session.bind, index_col=None)
            .bus_id.sort_values()
            .to_list()
        )

        # Load model components
        with db.session_scope() as session:
            query = (
                session.query(
                    EgonPfHvLink.bus0.label("mvgd_bus_id"),
                    EgonPfHvLoad.bus.label("emob_bus_id"),
                    EgonPfHvLoad.load_id.label("load_id"),
                    EgonPfHvStore.store_id.label("store_id"),
                )
                .select_from(EgonPfHvLoad, EgonPfHvStore)
                .join(
                    EgonPfHvLoadTimeseries,
                    EgonPfHvLoadTimeseries.load_id == EgonPfHvLoad.load_id,
                )
                .join(
                    EgonPfHvStoreTimeseries,
                    EgonPfHvStoreTimeseries.store_id == EgonPfHvStore.store_id,
                )
                .filter(
                    EgonPfHvLoad.carrier == "land transport EV",
                    EgonPfHvLoad.scn_name == scenario_name,
                    EgonPfHvLoadTimeseries.scn_name == scenario_name,
                    EgonPfHvStore.carrier == "battery storage",
                    EgonPfHvStore.scn_name == scenario_name,
                    EgonPfHvStoreTimeseries.scn_name == scenario_name,
                    EgonPfHvLink.scn_name == scenario_name,
                    EgonPfHvLink.bus1 == EgonPfHvLoad.bus,
                    EgonPfHvLink.bus1 == EgonPfHvStore.bus,
                )
            )
        model_components = pd.read_sql(
            query.statement, query.session.bind, index_col=None
        )

        # Check number of buses with model components connected
        mvgd_buses_with_ev = model_components.loc[
            model_components.mvgd_bus_id.isin(mvgds_with_ev)
        ]
        np.testing.assert_equal(
            len(mvgds_with_ev),
            len(mvgd_buses_with_ev),
            err_msg=(
                f"Number of Grid Districts with connected model components "
                f"({str(len(mvgd_buses_with_ev))} in tables egon_etrago_*) "
                f"differ from number of Grid Districts that got EVs "
                f"allocated ({len(mvgds_with_ev)} in table "
                f"{EgonEvMvGridDistrict.__table__})."
            ),
        )

        # Check if all required components exist (if no id is NaN)
        np.testing.assert_equal(
            model_components.drop_duplicates().isna().any().any(),
            False,
            err_msg=(
                f"Some components are missing (see True values): "
                f"{model_components.drop_duplicates().isna().any()}"
            ),
        )

        # Get all model timeseries
        print("  Loading model timeseries...")
        # Get all model timeseries
        model_ts_dict = {
            "Load": {
                "carrier": "land transport EV",
                "table": EgonPfHvLoad,
                "table_ts": EgonPfHvLoadTimeseries,
                "column_id": "load_id",
                "columns_ts": ["p_set"],
                "ts": None,
            },
            "Link": {
                "carrier": "BEV charger",
                "table": EgonPfHvLink,
                "table_ts": EgonPfHvLinkTimeseries,
                "column_id": "link_id",
                "columns_ts": ["p_max_pu"],
                "ts": None,
            },
            "Store": {
                "carrier": "battery storage",
                "table": EgonPfHvStore,
                "table_ts": EgonPfHvStoreTimeseries,
                "column_id": "store_id",
                "columns_ts": ["e_min_pu", "e_max_pu"],
                "ts": None,
            },
        }

        with db.session_scope() as session:
            for node, attrs in model_ts_dict.items():
                print(f"    Loading {node} timeseries...")
                subquery = (
                    session.query(getattr(attrs["table"], attrs["column_id"]))
                    .filter(attrs["table"].carrier == attrs["carrier"])
                    .filter(attrs["table"].scn_name == scenario_name)
                    .subquery()
                )

                cols = [
                    getattr(attrs["table_ts"], c) for c in attrs["columns_ts"]
                ]
                query = session.query(
                    getattr(attrs["table_ts"], attrs["column_id"]), *cols
                ).filter(
                    getattr(attrs["table_ts"], attrs["column_id"]).in_(
                        subquery
                    ),
                    attrs["table_ts"].scn_name == scenario_name,
                )
                attrs["ts"] = pd.read_sql(
                    query.statement,
                    query.session.bind,
                    index_col=attrs["column_id"],
                )

        # Check if all timeseries have 8760 steps
        print("    Checking timeranges...")
        for node, attrs in model_ts_dict.items():
            for col in attrs["columns_ts"]:
                ts = attrs["ts"]
                invalid_ts = ts.loc[ts[col].apply(lambda _: len(_)) != 8760][
                    col
                ].apply(len)
                np.testing.assert_equal(
                    len(invalid_ts),
                    0,
                    err_msg=(
                        f"{str(len(invalid_ts))} rows in timeseries do not "
                        f"have 8760 timesteps. Table: "
                        f"{attrs['table_ts'].__table__}, Column: {col}, IDs: "
                        f"{str(list(invalid_ts.index))}"
                    ),
                )

        # Compare total energy demand in model with some approximate values
        # (per EV: 14,000 km/a, 0.17 kWh/km)
        print("  Checking energy demand in model...")
        total_energy_model = (
            model_ts_dict["Load"]["ts"].p_set.apply(lambda _: sum(_)).sum()
            / 1e6
        )
        print(f"    Total energy amount in model: {total_energy_model} TWh")
        total_energy_scenario_approx = ev_count_alloc * 14000 * 0.17 / 1e9
        print(
            f"    Total approximated energy amount in scenario: "
            f"{total_energy_scenario_approx} TWh"
        )
        np.testing.assert_allclose(
            total_energy_model,
            total_energy_scenario_approx,
            rtol=0.1,
            err_msg=(
                "The total energy amount in the model deviates heavily "
                "from the approximated value for current scenario."
            ),
        )

        # Compare total storage capacity
        print("  Checking storage capacity...")
        # Load storage capacities from model
        with db.session_scope() as session:
            query = session.query(
                func.sum(EgonPfHvStore.e_nom).label("e_nom")
            ).filter(
                EgonPfHvStore.scn_name == scenario_name,
                EgonPfHvStore.carrier == "battery storage",
            )
        storage_capacity_model = (
            pd.read_sql(
                query.statement, query.session.bind, index_col=None
            ).e_nom.sum()
            / 1e3
        )
        print(
            f"    Total storage capacity ({EgonPfHvStore.__table__}): "
            f"{round(storage_capacity_model, 1)} GWh"
        )

        # Load occurences of each EV
        with db.session_scope() as session:
            query = (
                session.query(
                    EgonEvMvGridDistrict.bus_id,
                    EgonEvPool.type,
                    func.count(EgonEvMvGridDistrict.egon_ev_pool_ev_id).label(
                        "count"
                    ),
                )
                .join(
                    EgonEvPool,
                    EgonEvPool.ev_id
                    == EgonEvMvGridDistrict.egon_ev_pool_ev_id,
                )
                .filter(
                    EgonEvMvGridDistrict.scenario == scenario_name,
                    EgonEvMvGridDistrict.scenario_variation
                    == scenario_var_name,
                    EgonEvPool.scenario == scenario_name,
                )
                .group_by(EgonEvMvGridDistrict.bus_id, EgonEvPool.type)
            )
        count_per_ev_all = pd.read_sql(
            query.statement, query.session.bind, index_col="bus_id"
        )
        count_per_ev_all["bat_cap"] = count_per_ev_all.type.map(
            meta_tech_data.battery_capacity
        )
        count_per_ev_all["bat_cap_total_MWh"] = (
            count_per_ev_all["count"] * count_per_ev_all.bat_cap / 1e3
        )
        storage_capacity_simbev = count_per_ev_all.bat_cap_total_MWh.div(
            1e3
        ).sum()
        print(
            f"    Total storage capacity (simBEV): "
            f"{round(storage_capacity_simbev, 1)} GWh"
        )

        np.testing.assert_allclose(
            storage_capacity_model,
            storage_capacity_simbev,
            rtol=0.01,
            err_msg=(
                "The total storage capacity in the model deviates heavily "
                "from the input data provided by simBEV for current scenario."
            ),
        )

        # Check SoC storage constraint: e_min_pu < e_max_pu for all timesteps
        print("  Validating SoC constraints...")
        stores_with_invalid_soc = []
        for idx, row in model_ts_dict["Store"]["ts"].iterrows():
            ts = row[["e_min_pu", "e_max_pu"]]
            x = np.array(ts.e_min_pu) > np.array(ts.e_max_pu)
            if x.any():
                stores_with_invalid_soc.append(idx)

        np.testing.assert_equal(
            len(stores_with_invalid_soc),
            0,
            err_msg=(
                f"The store constraint e_min_pu < e_max_pu does not apply "
                f"for some storages in {EgonPfHvStoreTimeseries.__table__}. "
                f"Invalid store_ids: {stores_with_invalid_soc}"
            ),
        )

    def check_model_data_lowflex_eGon2035():
        # TODO: Add eGon100RE_lowflex
        print("")
        print("SCENARIO: eGon2035_lowflex")

        # Compare driving load and charging load
        print("  Loading eGon2035 model timeseries: driving load...")
        with db.session_scope() as session:
            query = (
                session.query(
                    EgonPfHvLoad.load_id,
                    EgonPfHvLoadTimeseries.p_set,
                )
                .join(
                    EgonPfHvLoadTimeseries,
                    EgonPfHvLoadTimeseries.load_id == EgonPfHvLoad.load_id,
                )
                .filter(
                    EgonPfHvLoad.carrier == "land transport EV",
                    EgonPfHvLoad.scn_name == "eGon2035",
                    EgonPfHvLoadTimeseries.scn_name == "eGon2035",
                )
            )
        model_driving_load = pd.read_sql(
            query.statement, query.session.bind, index_col=None
        )
        driving_load = np.array(model_driving_load.p_set.to_list()).sum(axis=0)

        print(
            "  Loading eGon2035_lowflex model timeseries: dumb charging "
            "load..."
        )
        with db.session_scope() as session:
            query = (
                session.query(
                    EgonPfHvLoad.load_id,
                    EgonPfHvLoadTimeseries.p_set,
                )
                .join(
                    EgonPfHvLoadTimeseries,
                    EgonPfHvLoadTimeseries.load_id == EgonPfHvLoad.load_id,
                )
                .filter(
                    EgonPfHvLoad.carrier == "land transport EV",
                    EgonPfHvLoad.scn_name == "eGon2035_lowflex",
                    EgonPfHvLoadTimeseries.scn_name == "eGon2035_lowflex",
                )
            )
        model_charging_load_lowflex = pd.read_sql(
            query.statement, query.session.bind, index_col=None
        )
        charging_load = np.array(
            model_charging_load_lowflex.p_set.to_list()
        ).sum(axis=0)

        # Ratio of driving and charging load should be 0.9 due to charging
        # efficiency
        print("  Compare cumulative loads...")
        print(f"    Driving load (eGon2035): {driving_load.sum() / 1e6} TWh")
        print(
            f"    Dumb charging load (eGon2035_lowflex): "
            f"{charging_load.sum() / 1e6} TWh"
        )
        driving_load_theoretical = (
            float(meta_run_config.eta_cp) * charging_load.sum()
        )
        np.testing.assert_allclose(
            driving_load.sum(),
            driving_load_theoretical,
            rtol=0.01,
            err_msg=(
                f"The driving load (eGon2035) deviates by more than 1% "
                f"from the theoretical driving load calculated from charging "
                f"load (eGon2035_lowflex) with an efficiency of "
                f"{float(meta_run_config.eta_cp)}."
            ),
        )

    print("=====================================================")
    print("=== SANITY CHECKS FOR MOTORIZED INDIVIDUAL TRAVEL ===")
    print("=====================================================")

    for scenario_name in ["eGon2035", "eGon100RE"]:
        scenario_var_name = DATASET_CFG["scenario"]["variation"][scenario_name]

        print("")
        print(f"SCENARIO: {scenario_name}, VARIATION: {scenario_var_name}")

        # Load scenario params for scenario and scenario variation
        scenario_variation_parameters = get_sector_parameters(
            "mobility", scenario=scenario_name
        )["motorized_individual_travel"][scenario_var_name]

        # Load simBEV run config and tech data
        meta_run_config = read_simbev_metadata_file(
            scenario_name, "config"
        ).loc["basic"]
        meta_tech_data = read_simbev_metadata_file(scenario_name, "tech_data")

        print("")
        print("Checking EV counts...")
        ev_count_alloc = check_ev_allocation()

        print("")
        print("Checking trip data...")
        check_trip_data()

        print("")
        print("Checking model data...")
        check_model_data()

    print("")
    check_model_data_lowflex_eGon2035()

    print("=====================================================")
