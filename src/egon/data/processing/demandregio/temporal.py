"""The central module containing all code dealing with processing
timeseries data using demandregio

"""

import pandas as pd
import egon.data.config
from egon.data import db

def calc_load_curve(share_wz, annual_demand=1):
    """ Create aggregated demand curve for service sector

    Parameters
    ----------
    share_wz : pandas.Series or pandas.DataFrame
        Share of annual demand per cts branch
    annual_demand : float or pandas.Series, optional
        Annual demand in MWh. The default is 1.

    Returns
    -------
    pandas.Series or pandas.DataFrame
        Annual load curve of combindes cts branches

    """
    year = 2011

    sources = (egon.data.config.datasets()
               ['electrical_load_curves_cts']['sources'])

    # Select normalizes load curves per cts branch
    df_select = db.select_dataframe(
        f"""SELECT wz, load_curve
        FROM {sources['demandregio_timeseries']['schema']}.
            {sources['demandregio_timeseries']['table']}
        WHERE year = {year}""",
        index_col='wz').transpose()

    # Cretae timeindex for each hour of the selected year
    idx = pd.DatetimeIndex(pd.date_range(
        start=f'01/01/{year}', end=f'01/01/{year+1}', freq='H', closed='left'))

    # Inizalize DataFrame for load curves
    df = pd.DataFrame(index=idx, columns=df_select.columns)

    # Import load curves to Dataframe
    for col in df.columns:
        df[col] = df_select[col].load_curve

    # If shares per cts branch is a DataFrame (e.g. shares per substation)
    # demand curves are created for each row
    if type(share_wz) == pd.core.frame.DataFrame:
        result = pd.DataFrame(columns=share_wz.index)
        for i, row in share_wz.iterrows():
            result[i] = df[row.index].mul(row).sum(axis=1).mul(
                annual_demand[i])
    else:
        result = df[share_wz.index].mul(share_wz).sum(axis=1).mul(
            annual_demand)

    # Return load curve considering shares of cts branches and annual demand
    return result

def calc_load_curves_cts(scenario):
    """Temporal disaggregate electrical cts demand per substation.


    Parameters
    ----------
    scenario : str
        Scenario name.

    Returns
    -------
    pandas.DataFrame
        Demand timeseries of cts per substation id

    """

    sources = (egon.data.config.datasets()
               ['electrical_load_curves_cts']['sources'])

    # Select demands per cts branch and nuts3-region
    demands_nuts = db.select_dataframe(
            f"""SELECT nuts3, wz, demand
            FROM {sources['demandregio_cts']['schema']}.
            {sources['demandregio_cts']['table']}
            WHERE scenario = '{scenario}'
            AND demand > 0
            AND wz IN (
                SELECT wz FROM
                {sources['demandregio_wz']['schema']}.
                {sources['demandregio_wz']['table']}
                WHERE sector = 'CTS')
            """).set_index(['nuts3', 'wz'])

    # Select cts demands per zensus cell including nuts3-region and substation
    demands_zensus = db.select_dataframe(
            f"""SELECT a.zensus_population_id, a.demand,
            b.vg250_nuts3 as nuts3,
            c.subst_id
            FROM {sources['zensus_electricity']['schema']}.
            {sources['zensus_electricity']['table']} a
            INNER JOIN
            {sources['map_vg250']['schema']}.{sources['map_vg250']['table']} b
            ON (a.zensus_population_id = b.zensus_population_id)
            INNER JOIN
            {sources['map_grid_districts']['schema']}.
            {sources['map_grid_districts']['table']} c
            ON (a.zensus_population_id = c.zensus_population_id)
            WHERE a.scenario = '{scenario}'
            AND a.sector = 'service'
            """, index_col='zensus_population_id')

    # Calculate shares of cts branches per nuts3-region
    nuts3_share_wz = demands_nuts.groupby('nuts3').apply(
        lambda grp: grp/grp.sum())

    # Calculate shares of cts branches per zensus cell
    for wz in demands_nuts.index.get_level_values('wz').unique():
        demands_zensus[wz] = 0
        share = nuts3_share_wz[
            nuts3_share_wz.index.get_level_values('wz') == wz
            ].reset_index().set_index('nuts3').demand
        idx = demands_zensus.index[demands_zensus.nuts3.isin(share.index)]
        demands_zensus.loc[idx,wz] = share[
            demands_zensus.nuts3[idx].values].values

    # Calculate shares of cts branches per hvmv substation
    share_subst = demands_zensus.drop(
        'demand', axis=1).groupby('subst_id').mean()

    # Calculate cts annual demand per hvmv substation
    annual_demand_subst = demands_zensus.groupby('subst_id').demand.sum()

    # Return electrical load curves per hvmv substation
    return calc_load_curve(share_subst, annual_demand_subst)


def insert_cts_load():

    for scenario in ['eGon2035', 'eGon100RE']:
        # Calculate cts load curves per mv substation
        data = calc_load_curves_cts(scenario)

        # Initalize pandas.DataFrame for pf table
        load_df = pd.DataFrame(index=data.columns, columns=['p_set'])
        load_df.p_set = data.transpose().values.tolist()

    # TODO: match substation_id to etrago bus:
        #https://github.com/openego/data_processing/blob/5edca414212ccb4b1df6b046bf916bc2ebb7b4c6/dataprocessing/sql_snippets/ego_dp_substation_otg.sql




