#!/usr/bin/env python
# coding: utf-8

import os
import pandas as pd
from egon.data import db

from egon.data.processing.hh_demand import hh_demand_profiles_tools as hh_tools


if __name__ == "__main__":
    # Loadprofilesdump
    # ###################
    # in Wh
    # TODO: > to/from SQL?
    # filed needs to be placed manually in directory
    file = 'h0_profiles.h5'
    file = os.path.join(os.path.realpath(file))
    df_profiles = pd.read_hdf(file)

    # set multiindex to HH_types
    df_profiles.columns = pd.MultiIndex.from_arrays([df_profiles.columns.str[:2], df_profiles.columns.str[3:]])


    # Load Zensus data at nuts-level
    # ###################
    # TODO: > to/from SQL?
    # filed needs to be placed manually in directory
    file = 'Zensus2011_Personen.csv'
    file = os.path.join(os.path.realpath(file))
    df_zensus = pd.read_csv(file, sep=';', decimal='.', skiprows=5, skipfooter=7,
                            index_col=[0, 1], header=[0, 1], encoding='latin1', engine='python')

    # clean data
    df_zensus = df_zensus.applymap(hh_tools.clean).applymap(int)
    # preprocess nuts1 zensus data
    df_zensus = hh_tools.process_nuts1_zensus_data(df_zensus)

    # ## Household distribution
    # - Adults living in househould type
    # - number of kids not included even if in housholdtype name
    # **! The Eurostat data only gives the amount of adults/seniors, excluding the amount of kids <15**
    # eurostat is used for demand-profile-generator @fraunhofer

    # hh shares mapping zensus to eurostat
    hh_types = {'SR': [('Einpersonenhaushalte (Singlehaushalte)', 'Insgesamt', 'Seniors'),
                       ('Alleinerziehende Elternteile', 'Insgesamt', 'Seniors')],  # Single Seniors Single Parents Seniors
                'SO': [('Einpersonenhaushalte (Singlehaushalte)', 'Insgesamt', 'Adults')],  # Single Adults
                'SK': [('Alleinerziehende Elternteile', 'Insgesamt', 'Adults')],  # Single Parents Adult
                'PR': [('Paare ohne Kind(er)', '2 Personen', 'Seniors'),
                       ('Mehrpersonenhaushalte ohne Kernfamilie', '2 Personen', 'Seniors')],
                # Couples without Kids Senior & same sex couples & shared flat seniors
                'PO': [('Paare ohne Kind(er)', '2 Personen', 'Adults'),
                       ('Mehrpersonenhaushalte ohne Kernfamilie', '2 Personen', 'Adults')],
                # Couples without Kids adults & same sex couples & shared flat adults
                'P1': [('Paare mit Kind(ern)', '3 Personen', 'Adults')],
                'P2': [('Paare mit Kind(ern)', '4 Personen', 'Adults')],
                'P3': [('Paare mit Kind(ern)', '5 Personen', 'Adults'),
                       ('Paare mit Kind(ern)', '6 und mehr Personen', 'Adults')],
                'OR': [('Mehrpersonenhaushalte ohne Kernfamilie', '3 Personen', 'Seniors'),
                       ('Mehrpersonenhaushalte ohne Kernfamilie', '4 Personen', 'Seniors'),
                       ('Mehrpersonenhaushalte ohne Kernfamilie', '5 Personen', 'Seniors'),
                       ('Mehrpersonenhaushalte ohne Kernfamilie', '6 und mehr Personen', 'Seniors'),
                       ('Paare mit Kind(ern)', '3 Personen', 'Seniors'),
                       ('Paare ohne Kind(er)', '3 Personen', 'Seniors'),
                       ('Paare mit Kind(ern)', '4 Personen', 'Seniors'),
                       ('Paare ohne Kind(er)', '4 Personen', 'Seniors'),
                       ('Paare mit Kind(ern)', '5 Personen', 'Seniors'),
                       ('Paare ohne Kind(er)', '5 Personen', 'Seniors'),
                       ('Paare mit Kind(ern)', '6 und mehr Personen', 'Seniors'),
                       ('Paare ohne Kind(er)', '6 und mehr Personen', 'Seniors')],  # no info about share of kids

                # OO, O1, O2 have the same amount, as no information about the share of kids within zensus data set.
                # if needed the total amount can be corrected in the hh_tools.get_hh_dist function
                # using multi_adjust=True option
                'OO': [('Mehrpersonenhaushalte ohne Kernfamilie', '3 Personen', 'Adults'),
                       ('Mehrpersonenhaushalte ohne Kernfamilie', '4 Personen', 'Adults'),
                       ('Mehrpersonenhaushalte ohne Kernfamilie', '5 Personen', 'Adults'),
                       ('Mehrpersonenhaushalte ohne Kernfamilie', '6 und mehr Personen', 'Adults'),
                       ('Paare ohne Kind(er)', '3 Personen', 'Adults'),
                       ('Paare ohne Kind(er)', '4 Personen', 'Adults'),
                       ('Paare ohne Kind(er)', '5 Personen', 'Adults'),
                       ('Paare ohne Kind(er)', '6 und mehr Personen', 'Adults')],  # no info about share of kids
                # TODO: maybe remove following lines if not needed

                #             'O1': [('Mehrpersonenhaushalte ohne Kernfamilie', '3 Personen', 'Adults'),
                #                    ('Mehrpersonenhaushalte ohne Kernfamilie', '4 Personen', 'Adults'),
                #                    ('Mehrpersonenhaushalte ohne Kernfamilie', '5 Personen', 'Adults'),
                #                    ('Mehrpersonenhaushalte ohne Kernfamilie', '6 und mehr Personen', 'Adults'),
                #                    ('Paare ohne Kind(er)', '3 Personen', 'Adults'),
                #                    ('Paare ohne Kind(er)', '4 Personen', 'Adults'),
                #                    ('Paare ohne Kind(er)', '5 Personen', 'Adults'),
                #                    ('Paare ohne Kind(er)', '6 und mehr Personen', 'Adults')],  # no info about share of kids
                #             'O2': [('Mehrpersonenhaushalte ohne Kernfamilie', '3 Personen', 'Adults'),
                #                    ('Mehrpersonenhaushalte ohne Kernfamilie', '4 Personen', 'Adults'),
                #                    ('Mehrpersonenhaushalte ohne Kernfamilie', '5 Personen', 'Adults'),
                #                    ('Mehrpersonenhaushalte ohne Kernfamilie', '6 und mehr Personen', 'Adults'),
                #                    ('Paare ohne Kind(er)', '3 Personen', 'Adults'),
                #                    ('Paare ohne Kind(er)', '4 Personen', 'Adults'),
                #                    ('Paare ohne Kind(er)', '5 Personen', 'Adults'),
                #                    ('Paare ohne Kind(er)', '6 und mehr Personen', 'Adults')]
                }

    # distribution of people by household @eurostats
    # df_hh_types_D = pd.Series({'SR': 0.083, 'SO': 0.158, 'SK': 0.022,
    #                            'PR': 0.145, 'PO': 0.203, 'P1': 0.081, 'P2': 0.077, 'P3': 0.024,
    #                            'OR': 0.023, 'OO': 0.13, 'O1': 0.04, 'O2': 0.015})

    # hh_tools.get_hh_dist without eurostat adjustment for O1-03 Groups in absolute values
    df_hh_types_nad_abs = hh_tools.get_hh_dist(df_zensus, hh_types, multi_adjust=False, relative=False)

    # #########################
    # FIXME:
    # mapping needs to be adjusted for OR, OO, O1, O2
    # O1, O2 are not used anymore
    # influence of OO and OR -parameter to overall household-sum rather small
    # ###########################

    mapping_people_in_households = {'SR': 1,
                                    'SO': 1,
                                    'SK': 1,  # kids are excluded
                                    'PR': 2,
                                    'PO': 2,
                                    'P1': 2,  # kids are excluded
                                    'P2': 2,  # ""
                                    'P3': 2,  # ""
                                    'OR': 4,  # parameter needs to be re/defined
                                    'OO': 4,  # ""
                                    #                                 'O1': 4,  # ""
                                    #                                 'O2': 4,  # ""
                                    }
    # derivate households data from inhabitants data by compound number of people per household type
    df_dist_households = hh_tools.inhabitants_to_households(df_hh_types_nad_abs, mapping_people_in_households)

    # FIXME:
    # compare df_dist_households.sum() here with values from other source

    # TODO: direct db.engine to configuration file
    # engine = db.engine()
    # SQL - Access Zensus household data cell-level
    df_households_typ = db.select_dataframe(sql="""
                SELECT grid_id, attribute, characteristics_code, characteristics_text, quantity
                FROM society.destatis_zensus_household_per_ha
                WHERE attribute = 'HHTYP_FAM' """)
    df_households_typ = df_households_typ.drop(columns=['attribute', 'characteristics_text'])
    df_households_typ = df_households_typ.rename(columns={'quantity': 'hh_5types'})

    mapping_zensus_hh_subgroups = {1: ['SR', 'SO'],
                                   2: ['PR', 'PO'],
                                   3: ['SK'],
                                   4: ['P1', 'P2', 'P3'],
                                   5: ['OR', 'OO'],
                                   }

    for value in mapping_zensus_hh_subgroups.values():
        df_dist_households.loc[value] = df_dist_households.loc[value].div(df_dist_households.loc[value].sum())

    # SQL- create table to map cells to nuts3 and nuts1
    df_grid_id = db.select_dataframe(sql="""
                        SELECT pop.grid_id, pop.gid, vg250.vg250_nuts3 as nuts3, lan.nuts as nuts1, lan.gen
                        FROM society.destatis_zensus_population_per_ha_inside_germany as pop
                        LEFT JOIN boundaries.egon_map_zensus_vg250 as vg250
                        ON (pop.gid=vg250.zensus_population_id)
                        LEFT JOIN boundaries.vg250_lan as lan
                        ON (LEFT(vg250.vg250_nuts3, 3)=lan.nuts) """)
    df_grid_id = df_grid_id.drop_duplicates()
    df_grid_id = df_grid_id.reset_index(drop=True)

    # merge nuts info to zensus cell level data
    # how='inner' is used as ids of unpopulated areas are removed df_grid_id or earliers tables. see here:
    # https://github.com/openego/eGon-data/blob/59195926e41c8bd6d1ca8426957b97f33ef27bcc/src/egon/data/importing/zensus/__init__.py#L418-L449
    df_households_typ = pd.merge(df_households_typ, df_grid_id[['grid_id', 'gen', 'nuts1', 'nuts3']],
                                 left_on='grid_id', right_on='grid_id', how='inner')

    # Merge Zensus nuts level household data with zensus cell level by dividing hh-groups with mapping_zensus_hh_subgroups
    df_zensus_cells = pd.DataFrame()
    for (country, code), df_country_type in df_households_typ.groupby(['gen', 'characteristics_code']):

        # iterate over zenus_country subgroups
        for typ in mapping_zensus_hh_subgroups[code]:
            df_country_type['hh_type'] = typ
            df_country_type['factor'] = df_dist_households.loc[typ, country]
            df_country_type['hh_10types'] = df_country_type['hh_5types'] * df_dist_households.loc[typ, country]
            df_zensus_cells = df_zensus_cells.append(df_country_type, ignore_index=True)

    df_zensus_cells = df_zensus_cells.sort_values(by=['grid_id', 'characteristics_code']).reset_index(drop=True)

    # change profile numbers to int
    df_profiles.columns = pd.MultiIndex.from_tuples([(a, int(b)) for a, b in df_profiles.columns])

    pool_size = df_profiles.groupby(level=0, axis=1).size()

    df_demand_regio = db.select_dataframe(sql="""
                            SELECT year, nuts3, SUM (demand) as demand_mWha
                            FROM demand.egon_demandregio_hh as egon_d
                            GROUP BY nuts3, year
                            ORDER BY year""", index_col=['year', 'nuts3'])

    # # testcase
    # test_data = df_zensus_cells.groupby('nuts3').get_group('DEF03')
    # test_data = pd.concat([df_zensus_cells.groupby('nuts3').get_group('DEF03'),
    #                        df_zensus_cells.groupby('nuts3').get_group('DEF06')])
    #
    # df_cell_demand_metadata = hh_tools.get_cell_demand_metadata(test_data, df_profiles)
    # df_cell_demand_metadata = hh_tools.adjust_to_demand_regio_nuts3_annual(df_cell_demand_metadata, df_profiles, df_demand_regio)
    #
    #
    #
    # import random
    # load_area_cell_ids = random.sample(list(df_cell_demand_metadata.index), 100)
    # max_value_load_area = hh_tools.get_load_area_max_load(df_profiles, df_cell_demand_metadata, load_area_cell_ids, 2035)
    # # print(df_cell_demand_metadata.shape)
    # print(max_value_load_area)


