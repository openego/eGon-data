import pandas as pd
import numpy as np
from itertools import permutations
from sqlalchemy.sql import func

from egon.data import db
from egon.data.datasets.scenario_parameters import (
    get_sector_parameters,
)
from egon.data.datasets.zensus_vg250 import (
    Vg250Gem,
    Vg250GemPopulation,
    MapZensusVg250,
    DestatisZensusPopulationPerHaInsideGermany
)
from egon.data.datasets.mv_grid_districts import MvGridDistricts
from egon.data.datasets.zensus_mv_grid_districts import MapZensusGridDistricts
from egon.data.datasets.emobility.motorized_individual_travel.helpers import (
    COLUMNS_KBA,
    CONFIG_EV,
    read_kba_data,
    read_rs7_data
)


def fix_missing_ags_municipality_regiostar(muns, rs7_data):
    """Check if all AGS of municipality dataset are included in RegioStaR7
    dataset and vice versa.

    As of Dec 2021, some municipalities are not included int he RegioStaR7
    dataset. This is mostly caused by incorporations of a municipality by
    another municipality. This is fixed by assining a RS7 id from another
    municipality with similar AGS (most likely a neighboured one).

    Missing entries in the municipality dataset is printed but not fixed
    as it doesn't result in bad data. Nevertheless, consider to update the
    municipality/VG250 dataset.

    Parameters
    ----------
    muns : pandas.DataFrame
        Municipality data
    rs7_data : pandas.DataFrame
        RegioStaR7 data

    Returns
    -------
    pandas.DataFrame
        Fixed RegioStaR7 data
    """

    if len(muns.ags.unique()) != len(rs7_data.ags):
        print('==========> Number of AGS differ between VG250 and RS7, trying to fix this...')

        # Get AGS differences
        ags_datasets = {
            'RS7': rs7_data.ags,
            'VG250': muns.ags
        }
        ags_datasets_missing = {k: [] for k in ags_datasets.keys()}
        perm = permutations(ags_datasets.items())
        for (name1, ags1), (name2, ags2) in perm:
            print(f'  Checking if all AGS of {name1} are in {name2}...')
            missing = [_ for _ in ags1 if _ not in ags2.to_list()]
            if len(missing) > 0:
                ags_datasets_missing[name2] = missing
                print(f'    AGS in {name1} but not in {name2}: ', missing)
            else:
                print('    OK')

        print('')

        # Try to fix
        for name, missing in ags_datasets_missing.items():
            if len(missing) > 0:
                # RS7 entries missing: use RS7 number from mun with similar AGS
                if name == 'RS7':
                    for ags in missing:
                        similar_entry = rs7_data[
                            round((rs7_data.ags.div(10))) == round(ags / 10)
                            ].iloc[0]
                        if len(similar_entry) > 0:
                            print(f'Adding dataset from VG250 to RS7 based upon AGS {ags}.')
                            similar_entry.ags = ags
                            rs7_data = rs7_data.append(similar_entry)
                        print('Consider to update RS7.')
                # VG250 entries missing:
                elif name == 'VG250':
                    print('Cannot guess VG250 entries. This error does not '
                          'result in bad data but consider to update VG250.')

        if len(muns.ags.unique()) != len(rs7_data.ags):
            print('==========> AGS could not be fixed!')
        else:
            print('==========> AGS were fixed!')

    return rs7_data


def validate_results():
    """Validates some of the produced results"""
    raise NotImplementedError


def calc_evs_per_reg_district(
    scenario_variation_parameters,
    kba_data
):
    """Calculate EVs per registration district

    Parameters
    ----------
    scenario_variation_parameters : dict
        Parameters of scenario variation
    kba_data : pandas.DataFrame
        Vehicle registration data for registration district

    Returns
    -------
    pandas.DataFrame
        EVs per registration district
    """

    scenario_variation_parameters['mini_share'] = (
        scenario_variation_parameters['BEV_mini_share'] +
        scenario_variation_parameters['PHEV_mini_share']
    )
    scenario_variation_parameters['medium_share'] = (
        scenario_variation_parameters['BEV_medium_share'] +
        scenario_variation_parameters['PHEV_medium_share']
    )
    scenario_variation_parameters['luxury_share'] = (
        scenario_variation_parameters['BEV_luxury_share'] +
        scenario_variation_parameters['PHEV_luxury_share']
    )

    factor_dict = dict()
    factor_dict['mini_factor'] = (scenario_variation_parameters['mini_share'] *
                                  scenario_variation_parameters['EV_count'] /
                                  kba_data.mini.sum())
    factor_dict['medium_factor'] = (scenario_variation_parameters['medium_share'] *
                                    scenario_variation_parameters['EV_count'] /
                                    kba_data.medium.sum())
    factor_dict['luxury_factor'] = (scenario_variation_parameters['luxury_share'] *
                                    scenario_variation_parameters['EV_count'] /
                                    kba_data.luxury.sum())

    # Define shares and factors
    ev_data = kba_data.copy()

    for tech, params in CONFIG_EV.items():
        ev_data[tech] = (
            kba_data[params['column']] *
            factor_dict[params['factor']] *
            scenario_variation_parameters[params['tech_share']] /
            scenario_variation_parameters[params['share']]
        ).round().astype('int')

    ev_data.drop(
        columns=[_ for _ in COLUMNS_KBA if _ != 'reg_district'],
        inplace=True
    )

    return ev_data


def calc_evs_per_municipality(ev_data, rs7_data):
    """Calculate EVs per municipality

    Parameters
    ----------
    ev_data : pandas.DataFrame
        EVs per regstration district
    rs7_data : pandas.DataFrame
        RegioStaR7 data
    """
    with db.session_scope() as session:
        query = session.query(
            Vg250GemPopulation.ags_0.label('ags'),
            Vg250GemPopulation.gen,
            Vg250GemPopulation.population_total.label('pop')
        )

    muns = pd.read_sql(
        query.statement,
        query.session.bind,
        index_col=None).astype({'ags': 'int64'})

    muns['ags_district'] = muns.ags.multiply(1/1000).apply(
        np.floor).astype('int')

    # Manual fix of Trier-Saarburg: Introduce new `ags_reg_district`
    # for correct allocation of mun to registration district
    # (Zulassungsbezirk), see above for background.
    muns['ags_reg_district'] = muns['ags_district']
    muns.loc[muns['ags_reg_district'] == 7235,
             'ags_reg_district'] = 7211

    # Remove multiple municipality entries (due to 'gf' in VG250)
    # by summing up population
    muns = muns[['ags', 'gen', 'ags_reg_district', 'pop']].groupby(
        ['ags', 'gen', 'ags_reg_district']
    ).sum().reset_index()

    # Add population of registration district
    pop_per_reg_district = muns[['ags_reg_district', 'pop']].groupby(
        'ags_reg_district'
    ).sum().rename(
        columns={'pop': 'pop_district'}
    ).reset_index()

    # Fix missing ags in mun data
    rs7_data = fix_missing_ags_municipality_regiostar(muns, rs7_data)

    # Merge municipality, EV data and pop per district
    ev_data_muns = muns.merge(
        ev_data,
        on='ags_reg_district'
    ).merge(
        pop_per_reg_district,
        on='ags_reg_district')

    # Disaggregate EV numbers to municipality
    for tech in ev_data[CONFIG_EV.keys()]:
        ev_data_muns[tech] = round(ev_data_muns[tech] *
                                   ev_data_muns['pop'] /
                                   ev_data_muns['pop_district']).astype('int')

    # TODO: data check

    # Filter columns
    cols = ['ags']
    cols.extend(CONFIG_EV.keys())
    ev_data_muns = ev_data_muns[cols]

    # Merge RS7 data
    ev_data_muns = ev_data_muns.merge(rs7_data[['ags', 'RS7_id']], on='ags')

    # TODO: Data check
    # ev_data_muns[[_ for _ in ev_columns.keys()]].sum().sum()
    # check if merge matching is succesful for all ags_ZB
    # len(ev_data_muns[ev_data_muns.isna().any(axis=1)]) == 0

    return ev_data_muns


def calc_evs_per_grid_district(ev_data_muns):
    """Calculate EVs per grid district by using population weighting

    Parameters
    ----------
    ev_data_muns : pandas.DataFrame
        EV data for municipalities

    Returns
    -------
    pandas.DataFrame
        EV data for grid districts
    """

    # Read MVGDs with intersecting muns and aggregate pop for each
    # municipality part
    with db.session_scope() as session:
        query_pop_per_mvgd = session.query(
            MvGridDistricts.bus_id,
            Vg250Gem.ags,
            func.sum(
                DestatisZensusPopulationPerHaInsideGermany.population
            ).label('pop')
        ).select_from(
            MapZensusGridDistricts
        ).join(
            MvGridDistricts,
            MapZensusGridDistricts.bus_id == MvGridDistricts.bus_id
        ).join(
            DestatisZensusPopulationPerHaInsideGermany,
            MapZensusGridDistricts.zensus_population_id ==
            DestatisZensusPopulationPerHaInsideGermany.id
        ).join(
            MapZensusVg250,
            MapZensusGridDistricts.zensus_population_id ==
            MapZensusVg250.zensus_population_id
        ).join(
            Vg250Gem,
            MapZensusVg250.vg250_municipality_id == Vg250Gem.id
        ).group_by(
            MvGridDistricts.bus_id, Vg250Gem.ags
        ).order_by(
            Vg250Gem.ags
        )

    mvgd_pop_per_mun = pd.read_sql(
        query_pop_per_mvgd.statement,
        query_pop_per_mvgd.session.bind,
        index_col=None).astype(
            {'bus_id': 'int64',
             'pop': 'int64',
             'ags': 'int64'}
        )

    # Calc population share of each municipality in MVGD
    mvgd_pop_per_mun_in_mvgd = mvgd_pop_per_mun.groupby(
        ['bus_id', 'ags']
    ).agg({'pop': 'sum'})

    # Calc relative and absolute population shares:
    # * pop_mun_in_mvgd: pop share of mun which intersects with MVGD
    # * pop_share_mun_in_mvgd: relative pop share of mun which
    #   intersects with MVGD
    # * pop_mun_total: total pop of mun
    # * pop_mun_in_mvgd_of_mun_total: relative pop share of mun which
    #   intersects with MVGD in relation to total pop of mun
    mvgd_pop_per_mun_in_mvgd = mvgd_pop_per_mun_in_mvgd.groupby(
        level=0
    ).apply(
        lambda x: x / float(x.sum())
    ).reset_index().rename(
        columns={'pop': 'pop_share_mun_in_mvgd'}
    ).merge(
        mvgd_pop_per_mun_in_mvgd.reset_index(),
        on=['bus_id', 'ags'], how='left'
    ).rename(
        columns={'pop': 'pop_mun_in_mvgd'}
    ).merge(
        mvgd_pop_per_mun[['ags', 'pop']].groupby('ags').agg({'pop': 'sum'}),
        on='ags', how='left'
    ).rename(
        columns={'pop': 'pop_mun_total'}
    )
    mvgd_pop_per_mun_in_mvgd['pop_mun_in_mvgd_of_mun_total'] = (
        mvgd_pop_per_mun_in_mvgd['pop_mun_in_mvgd'] /
        mvgd_pop_per_mun_in_mvgd['pop_mun_total']
    )

    # Merge EV data
    ev_data_mvgds = mvgd_pop_per_mun_in_mvgd.merge(
        ev_data_muns,
        on='ags', how='left'
    ).sort_values(['bus_id', 'ags'])

    # Calc EVs per MVGD by using EV from mun and share of mun's pop
    # that is located within MVGD
    for tech in ev_data_mvgds[CONFIG_EV.keys()]:
        ev_data_mvgds[tech] = round(
            ev_data_mvgds[tech] *
            ev_data_mvgds['pop_mun_in_mvgd_of_mun_total']
        ).fillna(0).astype('int')

    # Set RS7 id for MVGD by using the RS7 id from the mun with the
    # highest share in population
    rs7_data_mvgds = ev_data_mvgds[
        ['bus_id', 'pop_mun_in_mvgd', 'RS7_id']
    ].groupby(['bus_id', 'RS7_id']).sum().sort_values(
        ['bus_id', 'pop_mun_in_mvgd'],
        ascending=False,
        na_position='last').reset_index().drop_duplicates(
        'bus_id',
        keep='first')[['bus_id', 'RS7_id']]

    # Join RS7 id and select columns
    columns = ['bus_id'] + [_ for _ in CONFIG_EV.keys()]
    ev_data_mvgds = ev_data_mvgds[columns].groupby('bus_id').agg('sum').merge(
        rs7_data_mvgds, on='bus_id', how='left')

    # TODO: data check
    #ev_data_mvgds[[_ for _ in CONFIG_EV.keys()]].sum().sum()
    #ev_data_mvgds.RS7_id.isna().any()

    return ev_data_mvgds


def allocate_evs():
    """Allocate electric vehicles to different spatial levels.

    Accocation uses today's vehicles registration data per registration
    district from KBA and scales scenario's EV targets (BEV and PHEV)
    linearly using population. Furthermore, a RegioStaR7 code (BMVI) is
    assigned.

    Levels:
    * districts of registration
    * municipalities
    * grid districts

    Parameters
    ----------

    Returns
    -------

    """
    # TODO: allow for import SH only
    # TODO: dataset = egon.data.config.settings()["egon-data"]["--dataset-boundary"]
    # Import
    kba_data = read_kba_data()
    rs7_data = read_rs7_data()

    # TODO: Add support for scenario variations
    #for scenario_name in ["eGon2035", "eGon100RE"]:
    for scenario_name in ["eGon2035"]:

        # Load scenario params
        scenario_parameters = get_sector_parameters(
            "mobility",
            scenario=scenario_name
        )["motorized_individual_travel"]#.get("NEP C 2035")

        print(f"========== SCENARIO: {scenario_name} ==========")

        # Go through scenario variations
        for (scenario_variation_name,
             scenario_variation_parameters) in scenario_parameters.items():

            print(f"===== SCENARIO VARIATION: {scenario_variation_name} =====")

            # Calc EV data per registration district
            ev_data = calc_evs_per_reg_district(
                scenario_variation_parameters,
                kba_data
            )
            print(ev_data)

            # Calc EV data per municpality
            ev_data_muns = calc_evs_per_municipality(
                ev_data,
                rs7_data
            )
            print(ev_data_muns)

            # Calc EV data per grid district
            ev_data_mvgds = calc_evs_per_grid_district(
                ev_data_muns
            )
            print(ev_data_mvgds)
