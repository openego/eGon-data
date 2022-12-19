"""
This module does sanity checks for both the eGon2035 and the eGon100RE scenario
separately where a percentage error is given to showcase difference in output
and input values. Please note that there are missing input technologies in the
supply tables.
Authors: @ALonso, @dana, @nailend, @nesnoj, @khelfen
"""
import ast
from math import isclose
from pathlib import Path
import ast

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
from egon.data.datasets.emobility.motorized_individual_travel.db_classes import (  # noqa: E501
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
from egon.data.datasets.gas_grid import (
    define_gas_nodes_list,
    define_gas_pipeline_list,
    define_gas_buses_abroad,
)
from egon.data.datasets.hydrogen_etrago.storage import (
    calculate_and_map_saltcavern_storage_potential,
)
from egon.data.datasets.power_plants.pv_rooftop_buildings import (
    PV_CAP_PER_SQ_M,
    ROOF_FACTOR,
    SCENARIOS,
    load_building_data,
    scenario_data,
)
from egon.data.datasets.pypsaeursec import read_network
from egon.data.datasets.scenario_parameters import get_sector_parameters
from egon.data.datasets.storages.home_batteries import get_cbat_pbat_ratio
import egon.data

TESTMODE_OFF = (
    config.settings()["egon-data"]["--dataset-boundary"] == "Everything"
)


class SanityChecks(Dataset):
    #:
    name: str = "SanityChecks"
    #:
    version: str = "0.0.6"

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
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
                sanitycheck_home_batteries,
                etrago_eGon2035_gas_DE,
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
        "others",
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

    # Check input and output values for the carriers "others",
    # "reservoir", "run_of_river" and "oil"

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

    valid_buildings_gdf = load_building_data()

    valid_buildings_gdf = valid_buildings_gdf.assign(
        bus_id=valid_buildings_gdf.bus_id.astype(int),
        overlay_id=valid_buildings_gdf.overlay_id.astype(int),
        max_cap=valid_buildings_gdf.building_area.multiply(
            ROOF_FACTOR * PV_CAP_PER_SQ_M
        ),
    )

    merge_df = pv_roof_df.merge(
        valid_buildings_gdf[["building_area"]],
        how="left",
        left_on="building_id",
        right_index=True,
    )

    assert (
        len(merge_df.loc[merge_df.building_area.isna()]) == 0
    ), f"{len(merge_df.loc[merge_df.building_area.isna()])} != 0"

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
            data=small_gens_df, x="capacity", y="building_area", ax=ax2
        ).set_title(scenario)

        plt.tight_layout()

        plt.savefig(
            res_dir / f"{scenario}_pv_rooftop_distribution.png",
            bbox_inches="tight",
        )

    for scenario in SCENARIOS:
        if scenario == "eGon2035":
            assert isclose(
                scenario_data(scenario=scenario).capacity.sum(),
                merge_df.loc[merge_df.scenario == scenario].capacity.sum(),
                rel_tol=1e-02,
            ), (
                f"{scenario_data(scenario=scenario).capacity.sum()} != "
                f"{merge_df.loc[merge_df.scenario == scenario].capacity.sum()}"
            )
        elif scenario == "eGon100RE":
            sources = config.datasets()["solar_rooftop"]["sources"]

            target = db.select_dataframe(
                f"""
                SELECT capacity
                FROM {sources['scenario_capacities']['schema']}.
                {sources['scenario_capacities']['table']} a
                WHERE carrier = 'solar_rooftop'
                AND scenario_name = '{scenario}'
                """
            ).capacity[0]

            dataset = config.settings()["egon-data"]["--dataset-boundary"]

            if dataset == "Schleswig-Holstein":
                sources = config.datasets()["scenario_input"]["sources"]

                path = Path(
                    f"./data_bundle_egon_data/nep2035_version2021/"
                    f"{sources['eGon2035']['capacities']}"
                ).resolve()

                total_2035 = (
                    pd.read_excel(
                        path,
                        sheet_name="1.Entwurf_NEP2035_V2021",
                        index_col="Unnamed: 0",
                    ).at["PV (Aufdach)", "Summe"]
                    * 1000
                )
                sh_2035 = scenario_data(scenario="eGon2035").capacity.sum()

                share = sh_2035 / total_2035

                target *= share

            assert isclose(
                target,
                merge_df.loc[merge_df.scenario == scenario].capacity.sum(),
                rel_tol=1e-02,
            ), (
                f"{target} != "
                f"{merge_df.loc[merge_df.scenario == scenario].capacity.sum()}"
            )
        else:
            raise ValueError(f"Scenario {scenario} is not valid.")


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


def sanitycheck_home_batteries():
    # get constants
    constants = config.datasets()["home_batteries"]["constants"]
    scenarios = constants["scenarios"]
    cbat_pbat_ratio = get_cbat_pbat_ratio()

    sources = config.datasets()["home_batteries"]["sources"]
    targets = config.datasets()["home_batteries"]["targets"]

    for scenario in scenarios:
        # get home battery capacity per mv grid id
        sql = f"""
        SELECT el_capacity as p_nom, bus_id FROM
        {sources["storage"]["schema"]}
        .{sources["storage"]["table"]}
        WHERE carrier = 'home_battery'
        AND scenario = '{scenario}'
        """

        home_batteries_df = db.select_dataframe(sql, index_col="bus_id")

        home_batteries_df = home_batteries_df.assign(
            capacity=home_batteries_df.p_nom * cbat_pbat_ratio
        )

        sql = f"""
        SELECT * FROM
        {targets["home_batteries"]["schema"]}
        .{targets["home_batteries"]["table"]}
        WHERE scenario = '{scenario}'
        """

        home_batteries_buildings_df = db.select_dataframe(
            sql, index_col="index"
        )

        df = (
            home_batteries_buildings_df[["bus_id", "p_nom", "capacity"]]
            .groupby("bus_id")
            .sum()
        )

        assert (home_batteries_df.round(6) == df.round(6)).all().all()


def sanity_check_gas_buses(scn):
    """Execute sanity checks for the gas buses in Germany

    Returns print statements as sanity checks for the CH4 and
    H2_grid grid buses in Germany. The deviation is calculated between
    the number gas grid buses in the database and the original
    Scigrid_gas number of gas buses.
    Parameters
    ----------
    scn_name : str
        Name of the scenario
    """
    logger.info(f"BUSES")

    target_file = (
        Path(".") / "datasets" / "gas_data" / "data" / "IGGIELGN_Nodes.csv"
    )

    Grid_buses_list = pd.read_csv(
        target_file,
        delimiter=";",
        decimal=".",
        usecols=["country_code"],
    )

    Grid_buses_list = Grid_buses_list[
        Grid_buses_list["country_code"].str.match("DE")
    ]
    input_grid_buses = len(Grid_buses_list.index)

    for carrier in ["CH4", "H2_grid"]:

        output_grid_buses_df = db.select_dataframe(
            f"""
            SELECT bus_id
            FROM grid.egon_etrago_bus
            WHERE scn_name = '{scn}'
            AND country = 'DE'
            AND carrier = '{carrier}';
            """,
            warning=False,
        )
        output_grid_buses = len(output_grid_buses_df.index)

        e_grid_buses = (
            round(
                (output_grid_buses - input_grid_buses) / input_grid_buses,
                2,
            )
            * 100
        )
        logger.info(f"Deviation {carrier} buses: {e_grid_buses} %")


def sanity_check_CH4_stores(scn):
    """Execute sanity checks for the CH4 stores in Germany
    Returns print statements as sanity checks for the CH4 stores
    capacity in Germany. The deviation is calculated between:
      * the sum of the capacities of the stores with carrier 'CH4'
        in the database (for one scenario) and
      * the sum of:
          * the capacity the gas grid allocated to CH4 (total capacity
            in eGon2035 and capacity reduced the share of the grid
            allocated to H2 in eGon100RE) and
          * the sum of the capacities of the stores in the source
            document (Storages from the SciGRID_gas data)
    Parameters
    ----------
    scn_name : str
        Name of the scenario
    """
    output_CH4_stores = db.select_dataframe(
        f"""SELECT SUM(e_nom::numeric) as e_nom_germany
                FROM grid.egon_etrago_store
                WHERE scn_name = '{scn}'
                AND carrier = 'CH4'
                AND bus IN
                    (SELECT bus_id
                    FROM grid.egon_etrago_bus
                    WHERE scn_name = '{scn}'
                    AND country = 'DE'
                    AND carrier = 'CH4');
                """,
        warning=False,
    )["e_nom_germany"].values[0]

    target_file = (
        Path(".") / "datasets" / "gas_data" / "data" / "IGGIELGN_Storages.csv"
    )

    CH4_storages_list = pd.read_csv(
        target_file,
        delimiter=";",
        decimal=".",
        usecols=["country_code", "param"],
    )

    CH4_storages_list = CH4_storages_list[
        CH4_storages_list["country_code"].str.match("DE")
    ]

    max_workingGas_M_m3 = []
    end_year = []
    for index, row in CH4_storages_list.iterrows():
        param = ast.literal_eval(row["param"])
        end_year.append(param["end_year"])
        max_workingGas_M_m3.append(param["max_workingGas_M_m3"])
    CH4_storages_list["max_workingGas_M_m3"] = max_workingGas_M_m3
    CH4_storages_list["end_year"] = [
        float("inf") if x == None else x for x in end_year
    ]

    # Remove unused storage units
    CH4_storages_list = CH4_storages_list[
        CH4_storages_list["end_year"]
        >= get_sector_parameters("global", scn)["population_year"]
    ]

    if scn == "eGon2035":
        grid_cap = 130000
    elif scn == "eGon100RE":
        grid_cap = 13000 * (
            1
            - get_sector_parameters("gas", "eGon100RE")[
                "retrofitted_CH4pipeline-to-H2pipeline_share"
            ]
        )
    conv_factor = 10830  # gross calorific value = 39 MJ/m3 (eurogas.org)
    input_CH4_stores = (
        conv_factor * sum(CH4_storages_list["max_workingGas_M_m3"].to_list())
        + grid_cap
    )

    e_CH4_stores = (
        round(
            (output_CH4_stores - input_CH4_stores) / input_CH4_stores,
            2,
        )
        * 100
    )
    logger.info(f"Deviation CH4 stores: {e_CH4_stores} %")


def sanity_check_H2_saltcavern_stores(scn):
    """Execute sanity checks for the H2 saltcavern stores in Germany
    Returns print as sanity checks for the H2 saltcavern potential
    storage capacity in Germany. The deviation is calculated between:
      * the sum of the of the H2 saltcavern potential storage capacity
        (e_nom_max) in the database and
      * the sum of the H2 saltcavern potential storage capacity
        assumed to be the ratio of the areas of 500 m radius around
        substations in each german federal state and the estimated
        total hydrogen storage potential of the corresponding federal
        state (data from InSpEE-DS report).

    This test works also in test mode.

    Parameters
    ----------
    scn_name : str
        Name of the scenario
    """
    output_H2_stores = db.select_dataframe(
        f"""SELECT SUM(e_nom_max::numeric) as e_nom_max_germany
                FROM grid.egon_etrago_store
                WHERE scn_name = '{scn}'
                AND carrier = 'H2_underground'
                AND bus IN
                    (SELECT bus_id
                    FROM grid.egon_etrago_bus
                    WHERE scn_name = '{scn}'
                    AND country = 'DE'
                    AND carrier = 'H2_saltcavern');
                """,
        warning=False,
    )["e_nom_max_germany"].values[0]

    storage_potentials = calculate_and_map_saltcavern_storage_potential()
    storage_potentials["storage_potential"] = (
        storage_potentials["area_fraction"] * storage_potentials["potential"]
    )
    input_H2_stores = sum(storage_potentials["storage_potential"].to_list())

    e_H2_stores = (
        round(
            (output_H2_stores - input_H2_stores) / input_H2_stores,
            2,
        )
        * 100
    )
    logger.info(f"Deviation H2 saltcavern stores: {e_H2_stores} %")


def sanity_check_CH4_grid(scn):
    """Execute sanity checks for the gas grid capacity in Germany
    Returns print statements as sanity checks for the CH4 links
    (pipelines) in Germany. The deviation is calculated between
    the sum of the power (p_nom) of all the CH4 pipelines in Germany
    for one scenario in the database and the sum of the powers of the
    imported pipelines.
    In eGon100RE, the sum is reduced by the share of the grid that is
    allocated to hydrogen (share calculated by PyPSA-eur-sec).

    This test works also in test mode.

    Parameters
    ----------
    scn_name : str
        Name of the scenario
    Returns
    -------
    scn_name : float
        Sum of the power (p_nom) of all the pipelines in Germany
    """
    grid_carrier = "CH4"
    output_gas_grid = db.select_dataframe(
        f"""SELECT SUM(p_nom::numeric) as p_nom_germany
            FROM grid.egon_etrago_link
            WHERE scn_name = '{scn}'
            AND carrier = '{grid_carrier}'
            AND bus0 IN
                (SELECT bus_id
                FROM grid.egon_etrago_bus
                WHERE scn_name = '{scn}'
                AND country = 'DE'
                AND carrier = '{grid_carrier}')
            AND bus1 IN
                (SELECT bus_id
                FROM grid.egon_etrago_bus
                WHERE scn_name = '{scn}'
                AND country = 'DE'
                AND carrier = '{grid_carrier}')
                ;
            """,
        warning=False,
    )["p_nom_germany"].values[0]

    gas_nodes_list = define_gas_nodes_list()
    abroad_gas_nodes_list = define_gas_buses_abroad()
    gas_grid = define_gas_pipeline_list(gas_nodes_list, abroad_gas_nodes_list)
    gas_grid_germany = gas_grid[
        (gas_grid["country_0"] == "DE") & (gas_grid["country_1"] == "DE")
    ]
    p_nom_total = sum(gas_grid_germany["p_nom"].to_list())

    if scn == "eGon2035":
        input_gas_grid = p_nom_total
    if scn == "eGon100RE":
        input_gas_grid = p_nom_total * (
            1
            - get_sector_parameters("gas", "eGon100RE")[
                "retrofitted_CH4pipeline-to-H2pipeline_share"
            ]
        )

    e_gas_grid = (
        round(
            (output_gas_grid - input_gas_grid) / input_gas_grid,
            2,
        )
        * 100
    )
    logger.info(f"Deviation of the capacity of the CH4 grid: {e_gas_grid} %")

    return p_nom_total


def etrago_eGon2035_gas_DE():
    """Execute basic sanity checks for the gas sector in eGon2035

    Returns print statements as sanity checks for the gas sector in
    the eGon2035 scenario for the following components in Germany:
      * Buses: with the function :py:func:`sanity_check_gas_buses`
      * Loads: for the carriers 'CH4_for_industry' and 'H2_for_industry'
        the deviation is calculated between the sum of the loads in the
        database and the sum the loads in the sources document
        (opendata.ffe database)
      * Generators: the deviation is calculated between the sums of the
        nominal powers of the gas generators in the database and of
        the ones in the sources document (Biogaspartner Einspeiseatlas
        Deutschland from the dena and Productions from the SciGRID_gas
        data)
      * Stores: deviations for stores with following carriers are
        calculated:
          * 'CH4': with the function :py:func:`sanity_check_CH4_stores`
          * 'H2_underground': with the function :py:func:`sanity_check_H2_saltcavern_stores`
      * Links: with the function :py:func:`sanity_check_CH4_grid`
    """
    scn = "eGon2035"

    if TESTMODE_OFF:
        logger.info(f"Gas sanity checks for scenario {scn}")

        # Buses
        sanity_check_gas_buses(scn)

        # Loads
        logger.info(f"LOADS")

        path = Path(".") / "datasets" / "gas_data" / "demand"
        corr_file = path / "region_corr.json"
        df_corr = pd.read_json(corr_file)
        df_corr = df_corr.loc[:, ["id_region", "name_short"]]
        df_corr.set_index("id_region", inplace=True)

        for carrier in ["CH4_for_industry", "H2_for_industry"]:

            output_gas_demand = db.select_dataframe(
                f"""SELECT (SUM(
                    (SELECT SUM(p)
                    FROM UNNEST(b.p_set) p))/1000000)::numeric as load_twh
                    FROM grid.egon_etrago_load a
                    JOIN grid.egon_etrago_load_timeseries b
                    ON (a.load_id = b.load_id)
                    JOIN grid.egon_etrago_bus c
                    ON (a.bus=c.bus_id)
                    AND b.scn_name = '{scn}'
                    AND a.scn_name = '{scn}'
                    AND c.scn_name = '{scn}'
                    AND c.country = 'DE'
                    AND a.carrier = '{carrier}';
                """,
                warning=False,
            )["load_twh"].values[0]

            input_gas_demand = pd.read_json(
                path / (carrier + "_eGon2035.json")
            )
            input_gas_demand = input_gas_demand.loc[:, ["id_region", "value"]]
            input_gas_demand.set_index("id_region", inplace=True)
            input_gas_demand = pd.concat(
                [input_gas_demand, df_corr], axis=1, join="inner"
            )
            input_gas_demand["NUTS0"] = (input_gas_demand["name_short"].str)[
                0:2
            ]
            input_gas_demand = input_gas_demand[
                input_gas_demand["NUTS0"].str.match("DE")
            ]
            input_gas_demand = sum(input_gas_demand.value.to_list()) / 1000000

            e_demand = (
                round(
                    (output_gas_demand - input_gas_demand) / input_gas_demand,
                    2,
                )
                * 100
            )
            logger.info(f"Deviation {carrier}: {e_demand} %")

        # Generators
        logger.info(f"GENERATORS")
        carrier_generator = "CH4"

        output_gas_generation = db.select_dataframe(
            f"""SELECT SUM(p_nom::numeric) as p_nom_germany
                    FROM grid.egon_etrago_generator
                    WHERE scn_name = '{scn}'
                    AND carrier = '{carrier_generator}'
                    AND bus IN
                        (SELECT bus_id
                        FROM grid.egon_etrago_bus
                        WHERE scn_name = '{scn}'
                        AND country = 'DE'
                        AND carrier = '{carrier_generator}');
                    """,
            warning=False,
        )["p_nom_germany"].values[0]

        target_file = (
            Path(".")
            / "datasets"
            / "gas_data"
            / "data"
            / "IGGIELGN_Productions.csv"
        )

        NG_generators_list = pd.read_csv(
            target_file,
            delimiter=";",
            decimal=".",
            usecols=["country_code", "param"],
        )

        NG_generators_list = NG_generators_list[
            NG_generators_list["country_code"].str.match("DE")
        ]

        p_NG = 0
        for index, row in NG_generators_list.iterrows():
            param = ast.literal_eval(row["param"])
            p_NG = p_NG + param["max_supply_M_m3_per_d"]
        conversion_factor = 437.5  # MCM/day to MWh/h
        p_NG = p_NG * conversion_factor

        basename = "Biogaspartner_Einspeiseatlas_Deutschland_2021.xlsx"
        target_file = Path(".") / "datasets" / "gas_data" / basename

        conversion_factor_b = 0.01083  # m^3/h to MWh/h
        p_biogas = (
            pd.read_excel(
                target_file,
                usecols=["Einspeisung Biomethan [(N*m^3)/h)]"],
            )["Einspeisung Biomethan [(N*m^3)/h)]"].sum()
            * conversion_factor_b
        )

        input_gas_generation = p_NG + p_biogas
        e_generation = (
            round(
                (output_gas_generation - input_gas_generation)
                / input_gas_generation,
                2,
            )
            * 100
        )
        logger.info(
            f"Deviation {carrier_generator} generation: {e_generation} %"
        )

        # Stores
        logger.info(f"STORES")
        sanity_check_CH4_stores(scn)
        sanity_check_H2_saltcavern_stores(scn)

        # Links
        logger.info(f"LINKS")
        sanity_check_CH4_grid(scn)

    else:
        print("Testmode is on, skipping sanity check.")
