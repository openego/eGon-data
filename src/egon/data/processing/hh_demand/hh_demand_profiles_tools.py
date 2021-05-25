#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np

from itertools import cycle
import random


def clean(x):
    """clean dataset convert '.' and '-' to str(0). remove brackets. table will be converted to int/floats afterwards"""
    x = str(x).replace('-', str(0))
    x = str(x).replace('.', str(0))
    x = x.strip('()')
    return x


def get_hh_dist(df_zensus, hh_types, multi_adjust=True, relative=True):
    """group zensus data to fit Demand-Profile-Generator (DPG) format.
    Parameters
    ----------
    df_zensus: pd.DataFrame
        containing zensus data
    hh_types: dict
        mapping of zensus groups to DPG groups
    multi-adjust: bool
        if True, splits DPG-group 'OO' into 3 subgroups and uses distribution factor derived by
        table II in https://www.researchgate.net/publication/273775902_Erzeugung_zeitlich_hochaufgeloster_Stromlastprofile_fur_verschiedene_Haushaltstypen
    relative: bool
        if True produces relative values
    Returns
    ----------
    df_hh_types: pd.DataFrame
        distribution of people by household type and regional-resolution
    !data still needs to be converted from amount of people to amount of households!
     """
    # adjust multi with/without kids via eurostat share as not clearly derivable without infos about share of kids
    if multi_adjust:
        adjust = {'SR': 1, 'SO': 1, 'SK': 1, 'PR': 1, 'PO': 1, 'P1': 1, 'P2': 1, 'P3': 1, 'OR': 1,
                  'OO': 0.703, 'O1': 0.216, 'O2': 0.081, }
    else:
        adjust = {'SR': 1, 'SO': 1, 'SK': 1, 'PR': 1, 'PO': 1, 'P1': 1, 'P2': 1, 'P3': 1, 'OR': 1,
                  'OO': 1, 'O1': 0, 'O2': 0, }

    df_hh_types = pd.DataFrame(
        ({hhtype: adjust[hhtype] * df_zensus.loc[countries, codes].sum() for hhtype, codes in hh_types.items()}
         for countries in df_zensus.index), index=df_zensus.index)
    # drop zero columns
    df_hh_types = df_hh_types.loc[:, (df_hh_types != 0).any(axis=0)]
    if relative:
        # normalize
        df_hh_types = df_hh_types.div(df_hh_types.sum(axis=1), axis=0)
    return df_hh_types.T


def inhabitants_to_households(df_people_by_householdtypes_abs, mapping_people_in_households):
    """converts distribution of peoples living in types of households to distribution of household types by using
    a people-in-household mapping. results are rounded to int (ceiled) to full households.

    Parameters
    ----------
    df_people_by_householdtypes_abs: pd.DataFrame
        distribution of people living in households
    mapping_people_in_households: dict
        mapping of people living in certain types of households
    Returns
    ----------
    df_households_by_type: pd.DataFrame
        distribution of households type

    """
    # compare categories and remove form mapping if to many
    diff = set(df_people_by_householdtypes_abs.index) ^ set(mapping_people_in_households.keys())

    if bool(diff):
        for key in diff:
            mapping_people_in_households = dict(mapping_people_in_households)
            del mapping_people_in_households[key]
        print(f'Removed {diff} from mapping!')

    # divide amount of people by people in household types
    df_households_by_type = df_people_by_householdtypes_abs.div(mapping_people_in_households, axis=0)
    # TODO: check @ Guido
    # round up households
    df_households_by_type = df_households_by_type.apply(np.ceil)

    return df_households_by_type


def process_nuts1_zensus_data(df_zensus):
    """group, remove and reorder categories wich are not needed for demand-profile-generator (DPG)
    Kids (<15) are excluded as they are also excluded in DPG origin dataset.
    Adults (15<65)
    Seniors (<65)
    """
    # Group data to fit Load Profile Generator categories
    # define kids/adults/seniors
    kids = ['Unter 3', '3 - 5', '6 - 14']  # < 15
    adults = ['15 - 17', '18 - 24', '25 - 29', '30 - 39', '40 - 49', '50 - 64']  # 15 < x <65
    seniors = ['65 - 74', '75 und älter']  # >65

    # sum groups of kids, adults and seniors and concat
    df_kids = df_zensus.loc[:, (slice(None), kids)].groupby(level=0, axis=1).sum()
    df_adults = df_zensus.loc[:, (slice(None), adults)].groupby(level=0, axis=1).sum()
    df_seniors = df_zensus.loc[:, (slice(None), seniors)].groupby(level=0, axis=1).sum()
    df_zensus = pd.concat([df_kids, df_adults, df_seniors], axis=1, keys=['Kids', 'Adults', 'Seniors'],
                          names=['age', 'persons'])

    # reduce column names to state only
    mapping_state = {i: i.split()[1] for i in df_zensus.index.get_level_values(level=0)}

    # rename index
    df_zensus = df_zensus.rename(index=mapping_state, level=0)
    # rename axis
    df_zensus = df_zensus.rename_axis(['state', 'type'])
    # unstack
    df_zensus = df_zensus.unstack()
    # reorder levels
    df_zensus = df_zensus.reorder_levels(order=['type', 'persons', 'age'], axis=1)

    return df_zensus


def get_cell_demand_profile_ids(df_cell, pool_size, df_profiles):
    """generates tuple of hh_type and random sample(without replacement) profile ids for cell"""
    # maybe use instead
    # np.random.default_rng().integers(low=0, high=pool_size[hh_type], size=sq) instead of random.sample
    # use random.choice() if with replacement
    # list of sample ids per hh_type in cell
    cell_profile_ids = [(hh_type, random.sample(range(pool_size[hh_type]), k=sq)) \
                        for hh_type, sq in zip(df_cell['hh_type'],
                                               df_cell['hh_10types'].astype(int))]

    # format to lists of tuples (hh_type, id)
    cell_profile_ids = [list(zip(cycle([hh_type]), ids)) for hh_type, ids in cell_profile_ids]
    # reduce to list
    cell_profile_ids = [a for b in cell_profile_ids for a in b]

    return cell_profile_ids


# can be parallelized with grouping df_zensus_cells by grid_id/nuts3/nuts1
def get_cell_demand_metadata(df_zensus_cells, df_profiles):
    """generate table including demand profile ids for each cell using get_cell_demand_profile_ids"""

    df_cell_demand_metadata = pd.DataFrame(index=df_zensus_cells.grid_id.unique(),
                                           columns=['cell_profile_ids', 'nuts3', 'nuts1', '2035_factor',
                                                    '2050_factor', ])
    # 'peak_loads_hh', 'peak_load_cell',
    df_cell_demand_metadata = df_cell_demand_metadata.rename_axis('cell_id')

    pool_size = df_profiles.groupby(level=0, axis=1).size()

    for cell_id, df_cell in df_zensus_cells.groupby(by='grid_id'):
        # FIXME
        # ! runden der Haushaltszahlen auf int
        # ! kein zurücklegen innerhalb einer Zelle ?! -> das is ok.
        cell_profile_ids = get_cell_demand_profile_ids(df_cell, pool_size, df_profiles)

        df_cell_demand_metadata.at[cell_id, 'cell_profile_ids'] = cell_profile_ids
        df_cell_demand_metadata.at[cell_id, 'nuts3'] = df_cell.loc[:, 'nuts3'].unique()[0]
        df_cell_demand_metadata.at[cell_id, 'nuts1'] = df_cell.loc[:, 'nuts1'].unique()[0]

    return df_cell_demand_metadata


# can be parallelized with grouping df_zensus_cells by grid_id/nuts3/nuts1
def adjust_to_demand_regio_nuts3_annual(df_cell_demand_metadata, df_profiles, df_demand_regio):
    """computes the profile scaling factor by accumulated nuts3 cells and demand_regio data"""
    for nuts3_id, df_nuts3 in df_cell_demand_metadata.groupby(by='nuts3'):
        nuts3_cell_ids = df_nuts3.index
        nuts3_profile_ids = df_nuts3.loc[:, 'cell_profile_ids'].sum()

        # take all profiles of one nuts3, aggregate and sum
        # profiles in Wh
        nuts3_profiles_sum_annual = df_profiles.loc[:, nuts3_profile_ids].sum().sum()

        # Scaling Factor
        # ##############
        # demand regio in MWh
        # profiles in Wh
        df_cell_demand_metadata.loc[nuts3_cell_ids, '2035_factor'] = df_demand_regio.loc[
                                                                         (2035, nuts3_id), 'demand_mwha'] * 1e3 / (
                                                                                 nuts3_profiles_sum_annual / 1e3)
        df_cell_demand_metadata.loc[nuts3_cell_ids, '2050_factor'] = df_demand_regio.loc[
                                                                         (2050, nuts3_id), 'demand_mwha'] * 1e3 / (
                                                                                 nuts3_profiles_sum_annual / 1e3)

    return df_cell_demand_metadata


def get_load_area_max_load(df_profiles, df_cell_demand_metadata, load_area_ids, year):
    """get max value of load area demand profile"""
    timesteps = len(df_profiles)
    full_load = pd.Series(data=np.zeros(timesteps), dtype=np.float64, index=range(timesteps))
    load_area_meta = df_cell_demand_metadata.loc[load_area_ids, ['cell_profile_ids', 'nuts3', f'{year}_factor']]
    for (nuts3, factor), df in load_area_meta.groupby(by=['nuts3', f'{year}_factor']):
        part_load = df_profiles.loc[:, df['cell_profile_ids'].sum()].sum(axis=1) * factor / 1e3  # profiles in Wh
        full_load = full_load.add(part_load)
    return full_load.max()  #, full_load.idxmax()


def download_files():
    """
    1. 'h0_profiles.h5'
        Households demand profiles generated by Fraunhofer IWES
        Methodology is described in: https://www.researchgate.net/publication/273775902_Erzeugung_zeitlich_hochaufgeloster_Stromlastprofile_fur_verschiedene_Haushaltstypen
        used and further describer in the thesisis by:
            1. Jonas Haack
                "Auswirkungen verschiedener Haushaltslastprofile auf PV-Batterie-Systeme" (confidential)
            2. Simon Ruben Drauz
                "Synthesis of a heat and electrical load profile for single and multi-family houses used for subsequent
                 performance tests of a multi-component energy system"
                http://dx.doi.org/10.13140/RG.2.2.13959.14248

    2. 'Zensus2011_Personen.csv' (does not exist in this format anymore but in different format)
        Dataset describing the amount of people living by a certain types of family-types, age-classes,
        sex and size of household in Germany in state-resolution.
        Data from: https://ergebnisse2011.zensus2022.de/datenbank/online
        - Search for: "1000A-2029"
        - or choose topic: "Bevölkerung kompakt"
        - Choose table code: "1000A-2029" with title "Personen: Alter (11 Altersklassen)/Geschlecht/Größe des
        privaten Haushalts - Typ des privaten Haushalts (nach Familien/Lebensform)"
        - Change setting "GEOLK1" to "Bundesländer (16)"
        higher resolution "Landkreise und kreisfreie Städte (412)" only accessible after registration.
    """
