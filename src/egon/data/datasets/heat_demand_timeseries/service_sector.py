import os

from sqlalchemy.ext.declarative import declarative_base


import pandas as pd

from egon.data import config, db


try:
    from disaggregator import temporal
except ImportError as e:
    pass


Base = declarative_base()


def cts_demand_per_aggregation_level(aggregation_level, scenario):
    """

    Description: Create dataframe assigining the CTS demand curve to individual zensus cell
    based on their respective NUTS3 CTS curve

    Parameters
    ----------
    aggregation_level : str
        if further processing is to be done in zensus cell level 'other'
        else 'dsitrict'

    Returns
    -------
    CTS_per_district : pandas.DataFrame
        if aggregation ='district'
            NUTS3 CTS profiles assigned to individual
            zensu cells and aggregated per district heat area id
        else
            empty dataframe
    CTS_per_grid : pandas.DataFrame
        if aggregation ='district'
            NUTS3 CTS profiles assigned to individual
            zensu cells and aggregated per mv grid subst id
        else
            empty dataframe
    CTS_per_zensus : pandas.DataFrame
        if aggregation ='district'
            empty dataframe
        else
            NUTS3 CTS profiles assigned to individual
            zensu population id

    """

    demand_nuts = db.select_dataframe(
        f"""
        SELECT demand, a.zensus_population_id, b.vg250_nuts3
        FROM demand.egon_peta_heat a 
        JOIN boundaries.egon_map_zensus_vg250 b 
        ON a.zensus_population_id = b.zensus_population_id
        
        WHERE a.sector = 'service'
        AND a.scenario = '{scenario}'
        ORDER BY a.zensus_population_id
        """
    )

    if os.path.isfile("CTS_heat_demand_profile_nuts3.csv"):
        df_CTS_gas_2011 = pd.read_csv(
            "CTS_heat_demand_profile_nuts3.csv", index_col=0
        )
        df_CTS_gas_2011.columns.name = "ags_lk"
        df_CTS_gas_2011.index = pd.to_datetime(df_CTS_gas_2011.index)
        df_CTS_gas_2011 = df_CTS_gas_2011.asfreq("H")
    else:
        df_CTS_gas_2011 = temporal.disagg_temporal_gas_CTS(
            use_nuts3code=True, year=2017
        )
        df_CTS_gas_2011.to_csv("CTS_heat_demand_profile_nuts3.csv")

    ags_lk = pd.read_csv(
        os.path.join(
            os.getcwd(),
            "demandregio-disaggregator/disaggregator/disaggregator/data_in/regional",
            "t_nuts3_lk.csv",
        ),
        index_col=0,
    )
    ags_lk = ags_lk.drop(
        ags_lk.columns.difference(["natcode_nuts3", "ags_lk"]), axis=1
    )

    CTS_profile = df_CTS_gas_2011.transpose()
    CTS_profile.reset_index(inplace=True)
    CTS_profile.ags_lk = CTS_profile.ags_lk.astype(int)
    CTS_profile = pd.merge(CTS_profile, ags_lk, on="ags_lk", how="inner")
    CTS_profile.set_index("natcode_nuts3", inplace=True)
    CTS_profile.drop("ags_lk", axis=1, inplace=True)

    CTS_per_zensus = pd.merge(
        demand_nuts[["zensus_population_id", "vg250_nuts3"]],
        CTS_profile,
        left_on="vg250_nuts3",
        right_on=CTS_profile.index,
        how="left",
    )

    CTS_per_zensus = CTS_per_zensus.drop("vg250_nuts3", axis=1)

    if aggregation_level == "district":
        district_heating = db.select_dataframe(
            f"""
            SELECT area_id, zensus_population_id
            FROM demand.egon_map_zensus_district_heating_areas
            WHERE scenario = '{scenario}'
            """
        )

        CTS_per_district = pd.merge(
            CTS_per_zensus,
            district_heating,
            on="zensus_population_id",
            how="inner",
        )
        CTS_per_district.set_index("area_id", inplace=True)
        CTS_per_district.drop("zensus_population_id", axis=1, inplace=True)

        CTS_per_district = CTS_per_district.groupby(lambda x: x, axis=0).sum()
        CTS_per_district = CTS_per_district.transpose()
        CTS_per_district = CTS_per_district.apply(lambda x: x / x.sum())
        CTS_per_district.columns.name = "area_id"
        CTS_per_district.reset_index(drop=True, inplace=True)

        # mv_grid = mv_grid.set_index("zensus_population_id")
        district_heating = district_heating.set_index("zensus_population_id")

        mv_grid_ind = db.select_dataframe(
            f"""
            SELECT bus_id, a.zensus_population_id
            FROM boundaries.egon_map_zensus_grid_districts a

				JOIN demand.egon_peta_heat c
				ON a.zensus_population_id = c.zensus_population_id 

				WHERE c.scenario = '{scenario}'
				AND c.sector = 'service'
            """
        )

        mv_grid_ind = mv_grid_ind[
            ~mv_grid_ind.zensus_population_id.isin(
                district_heating.index.values
            )
        ]
        CTS_per_grid = pd.merge(
            CTS_per_zensus,
            mv_grid_ind,
            on="zensus_population_id",
            how="inner",
        )
        CTS_per_grid.set_index("bus_id", inplace=True)
        CTS_per_grid.drop("zensus_population_id", axis=1, inplace=True)

        CTS_per_grid = CTS_per_grid.groupby(lambda x: x, axis=0).sum()
        CTS_per_grid = CTS_per_grid.transpose()
        CTS_per_grid = CTS_per_grid.apply(lambda x: x / x.sum())
        CTS_per_grid.columns.name = "bus_id"
        CTS_per_grid.reset_index(drop=True, inplace=True)

        CTS_per_zensus = pd.DataFrame()

    else:
        CTS_per_district = pd.DataFrame()
        CTS_per_grid = pd.DataFrame()
        CTS_per_zensus.set_index("zensus_population_id", inplace=True)

        CTS_per_zensus = CTS_per_zensus.groupby(lambda x: x, axis=0).sum()
        CTS_per_zensus = CTS_per_zensus.transpose()
        CTS_per_zensus = CTS_per_zensus.apply(lambda x: x / x.sum())
        CTS_per_zensus.columns.name = "zensus_population_id"
        CTS_per_zensus.reset_index(drop=True, inplace=True)

    return CTS_per_district, CTS_per_grid, CTS_per_zensus


def CTS_demand_scale(aggregation_level):
    """

    Description: caling the demand curves to the annual demand of the respective aggregation level


    Parameters
    ----------
    aggregation_level : str
        aggregation_level : str
        if further processing is to be done in zensus cell level 'other'
        else 'dsitrict'

    Returns
    -------
    CTS_per_district : pandas.DataFrame
        if aggregation ='district'
            Profiles scaled up to annual demand
        else
            0
    CTS_per_grid : pandas.DataFrame
        if aggregation ='district'
            Profiles scaled up to annual demandd
        else
            0
    CTS_per_zensus : pandas.DataFrame
        if aggregation ='district'
            0
        else
           Profiles scaled up to annual demand

    """
    scenarios = config.settings()["egon-data"]["--scenarios"]

    CTS_district = pd.DataFrame()
    CTS_grid = pd.DataFrame()
    CTS_zensus = pd.DataFrame()

    for scenario in scenarios:
        (
            CTS_per_district,
            CTS_per_grid,
            CTS_per_zensus,
        ) = cts_demand_per_aggregation_level(aggregation_level, scenario)
        CTS_per_district = CTS_per_district.transpose()
        CTS_per_grid = CTS_per_grid.transpose()
        CTS_per_zensus = CTS_per_zensus.transpose()

        demand = db.select_dataframe(
            f"""
                SELECT demand, zensus_population_id
                FROM demand.egon_peta_heat                
                WHERE sector = 'service'
                AND scenario = '{scenario}'
                ORDER BY zensus_population_id
                """
        )

        if aggregation_level == "district":
            district_heating = db.select_dataframe(
                f"""
                SELECT area_id, zensus_population_id
                FROM demand.egon_map_zensus_district_heating_areas
                WHERE scenario = '{scenario}'
                """
            )

            CTS_demands_district = pd.merge(
                demand,
                district_heating,
                on="zensus_population_id",
                how="inner",
            )
            CTS_demands_district.drop(
                "zensus_population_id", axis=1, inplace=True
            )
            CTS_demands_district = CTS_demands_district.groupby(
                "area_id"
            ).sum()

            CTS_per_district = pd.merge(
                CTS_per_district,
                CTS_demands_district[["demand"]],
                how="inner",
                right_on=CTS_per_district.index,
                left_on=CTS_demands_district.index,
            )

            CTS_per_district = CTS_per_district.rename(
                columns={"key_0": "area_id"}
            )
            CTS_per_district.set_index("area_id", inplace=True)

            CTS_per_district = CTS_per_district[
                CTS_per_district.columns[:-1]
            ].multiply(CTS_per_district.demand, axis=0)

            CTS_per_district.insert(0, "scenario", scenario)

            CTS_district = pd.concat(
                [CTS_district, CTS_per_district], ignore_index=True
            )
            CTS_district = CTS_district.sort_index()

            mv_grid_ind = db.select_dataframe(
                f"""
                SELECT bus_id, a.zensus_population_id
                FROM boundaries.egon_map_zensus_grid_districts a

				JOIN demand.egon_peta_heat c
				ON a.zensus_population_id = c.zensus_population_id 

				WHERE c.scenario = '{scenario}'
				AND c.sector = 'service'
                """
            )

            mv_grid_ind = mv_grid_ind[
                ~mv_grid_ind.zensus_population_id.isin(
                    district_heating.zensus_population_id.values
                )
            ]

            CTS_demands_grid = pd.merge(
                demand,
                mv_grid_ind[["bus_id", "zensus_population_id"]],
                on="zensus_population_id",
                how="inner",
            )

            CTS_demands_grid.drop("zensus_population_id", axis=1, inplace=True)
            CTS_demands_grid = CTS_demands_grid.groupby("bus_id").sum()

            CTS_per_grid = pd.merge(
                CTS_per_grid,
                CTS_demands_grid[["demand"]],
                how="inner",
                right_on=CTS_per_grid.index,
                left_on=CTS_demands_grid.index,
            )

            CTS_per_grid = CTS_per_grid.rename(columns={"key_0": "bus_id"})
            CTS_per_grid.set_index("bus_id", inplace=True)

            CTS_per_grid = CTS_per_grid[CTS_per_grid.columns[:-1]].multiply(
                CTS_per_grid.demand, axis=0
            )

            CTS_per_grid.insert(0, "scenario", scenario)

            CTS_grid = pd.concat([CTS_grid, CTS_per_grid])
            CTS_grid = CTS_grid.sort_index()

            CTS_per_zensus = 0

        else:
            CTS_per_district = 0
            CTS_per_grid = 0

            CTS_per_zensus = pd.merge(
                CTS_per_zensus,
                demand,
                how="inner",
                right_on=CTS_per_zensus.index,
                left_on=demand.zensus_population_id,
            )
            CTS_per_zensus = CTS_per_zensus.drop("key_0", axis=1)
            CTS_per_zensus.set_index("zensus_population_id", inplace=True)

            CTS_per_zensus = CTS_per_zensus[
                CTS_per_zensus.columns[:-1]
            ].multiply(CTS_per_zensus.demand, axis=0)
            CTS_per_zensus.insert(0, "scenario", scenario)

            CTS_per_zensus.reset_index(inplace=True)

            CTS_zensus = pd.concat([CTS_zensus, CTS_per_grid])
            CTS_zensus = CTS_zensus.set_index("bus_id")
            CTS_zensus = CTS_zensus.sort_index()

    return CTS_district, CTS_grid, CTS_zensus
