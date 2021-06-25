# -*- coding: utf-8 -*-

# This script is part of eGon-data using a fork of PyPSA-Eur-Sec.

# license text - to be added.

"""
Adjustment of heat assumptions for scenario generation with PyPSA-Eur-Sec.

This module adjustes the national heat demands and district heating shares in
Germany and other European countries.
"""

import egon.data.config
# from urllib.request import urlretrieve
# import geopandas as gpd

# import numpy as np
import pandas as pd

# in forked ogenego pypsa-eur-sec repository
# stand alone or in build_energy_totals.py

def adjust_heat_demand_column(sector, heat_type, scenario_data, energy_totals):
    """
    Adjust one energy_totals column containing heat demands.

    Adjust the heat demands for European coutries using the provided data.

    Parameters
    ----------
        sector: str
            'Residential' or 'Services'
        heat_type: str
            'hot water' or 'space heating'
        scenario_data: pandas dataframe
            new absolute values (delivered energy) used to replace the current
            values in enregy_totals
        energy_totals: pandas dataframe
            latest version of energy_totals to be adjusted

    Returns
    -------
        merged: pandas dataframe

    Notes
    -----
        None.
    """

    # Interrupt the process, when the sector is not defined correctly
    assert (
        (sector != 'Residential' or sector != 'Services')
    ), """The chosen sector for heat demand adjustment is not equal to
        'Residential' or 'Services'. Please adjust."""

    # Interrupt the process, when the heat_type is not defined correctly
    assert (
        (heat_type != 'hot water' or heat_type != 'space heating')
    ), """The chosen heat_type for heat demand adjustment is not equal to
        'hot water' or 'space heating'. Please adjust."""

    # CHECK THE QUALITY OF scenario_data; How?

    if (sector == 'Residential' and heat_type == 'space heating'):
        column = 'total residential space'
    elif (sector == 'Residential' and heat_type == 'hot water'):
            column = 'total residential water'
    elif (sector == 'Services' and heat_type == 'space heating'):
        column = 'total services space'
    elif (sector == 'Services' and heat_type == 'hot water'):
        column = 'total services water'

    # merge and set index
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.set_index.html
    merged = energy_totals.merge(scenario_data, how="left",
                                     on=['country'])

    print("No Hotmaps heat demand data available for: " + sector + " "
          + heat_type + " in:")
    print(merged.loc[(merged[heat_type].isna()), heat_type].index)
    # not listed in the filtered Hotmaps dataset -> NoData in data columns
    # reductions as in Bulgaria, for
    # AL = Albania, BA = Bosnia and Herzegovina, ME = Montenegro,
    # MK = Macedonia, RS = Serbia;
    # CH = Switzerland and NO = Norway: will be corrected once again afterwards
    correction_factor_balkans = (merged.loc['BG', heat_type] /
                         merged.loc['BG', column])

    merged.loc[(merged[heat_type].isna()), heat_type] = (
        merged[column] * correction_factor_balkans)

    # reductions in Norway as in Sweden and in Switzerland as in Austria
    # https://kanoki.org/2019/07/17/pandas-how-to-replace-values-based-on-conditions/
    correction_factor_CH = (merged.loc['AT', heat_type] /
                            merged.loc['AT', column])
    # residential space heating: 0.5701152717470834
    # services space heating: 0.4688016563499976
    merged.loc[(merged.index == 'CH'), heat_type] = (
        merged[column] * correction_factor_CH)

    correction_factor_NO = (merged.loc['SE', heat_type] /
                            merged.loc['SE', column])
    # residential space heating
    # 0.767567818434578, Denmark: 0.7173879729007566,
    # Finland: 1.1224712803733468 -> Why? Is so, according to hotmaps data.
    # services space heating
    # 0.7332911994237652, Denmark: 0.7238984460901043,
    # Finland: 0.7013480970368443
    merged.loc[(merged.index == 'NO'), heat_type] = (
        merged[column] * correction_factor_NO)

    merged.loc[:,column] = (
        merged.loc[:, heat_type])

    merged.drop(heat_type, inplace=True, axis=1 )

    return merged


def adjust_energy_totals():
    """
    Adjust future heat demands for scenario generation

    Adjust the heat demand for European countries using hotmaps data, the
    assumptions for Germany are changed afterwards again.

    Parameters
    ----------
        None

    Returns
    -------
        energy_totals: pandas dataframe

    ToDo
    -------
        Doublecheck and document the data and assumptions: Europe and DE;
        (figures: status-quo - 2050, etc.)

        Understand what happends with the not corrected/adjusted columns when
        PyPSA-Eur-Sec runs.

        Metadata?

    Notes
    -----
        Overwriting 2011 Final energy consumption data with 2050
        heat demands for space heating and hot water heating using Hotmaps
        data. The heat demands for domestic hot water heating in an couple of
        countries increase until 2050 compared to 2011 / 2015. Nevertheless,
        the scenario is now taken as a packages, even if it is kind of a blackbox.
        The usage of alternatives have been considered:
        - the Thermal Energy Service of the JRC IDEES data or
        - Hotmaps data for 2012 or 2015 hot water heat demands;
            2011 data not available.

        The new data are the heat demands of the builings
        - including the heat demand parts satisfied with ambient air and solar
        thermal heat)
        - excluding the losses, as PyPSA-Eur-Sec adds the losses of heat
        generators.

        Remark regarding the heat conversion losses (in buildings):
        Hotmaps did not provide any conversion factors. Other studies assume
        very low losses in 2050. Therefore, the most important thing to get
        right is the (not-)consideration of ambient heat.

        Hotmaps definition of Final Energy Consumption (FEC) = ?
        It includes the ambient heat (ambient air and solarthermal heat).

        As Hotmaps does not provide data for all studied countries (Balkan
        states, Switcherland and Norway) the data provided by PyPSA-Eur-Sec
        (based on Eurostat data) are adjusted in the function
        adjust_heat_demand_column using a correction factor.
    """

    # Load the energy_totals (to be adjusted) for the weather year
    # (here: 2011, FEC)
    energy_totals = pd.read_csv('/data/energy_totals.csv').set_index('country')

    # temp for testing only:
    # energy_totals = pd.read_csv(
    #     'run-pypsa-eur-sec/pypsa-eur-sec/data/energy_totals.csv'
    #     ).set_index('country')

    # Load the hotmaps data for 2050 (current policy scenario)

    # Get information from data configuration file about the hotmaps data
    data_config = egon.data.config.datasets()
    hotmaps_heatdemands_orig = data_config[
        "hotmaps_current_policy_scenario_heat_demands_buildings"][
        "original_data"
    ]
    # path to the downloaded heat demand secenario data
    filepath = hotmaps_heatdemands_orig["target"]["path"]

    # temp for testing only
    # filepath = ('/home/liv/Documents/eGo_n/Code/eGon-data/egon-data-test/scen_current_building_demand.csv')

    df_hotmaps = pd.read_csv(filepath)
    # df_hotmaps.Type.unique()
    # excluding 'cooling', 'auxiliary energy demand' and years not being 2050
    df_filtered = df_hotmaps[(df_hotmaps['Scenario'] == "current") & (
        df_hotmaps['Year'] == 2050) & (
        (df_hotmaps['Type'] == "hot water") |
        (df_hotmaps['Type'] == "space heating"))]

    # df_filtered.Type
    # df_filtered.head()
    # alternative:
    # df_filtered2 = df_hotmaps.query('Type == "hot water" | Type == "space heating" & Year == 2050')

    # df_filtered.columns
    # df_filtered.Supertype.unique()
    # df_filtered.Technology.unique()
    # df_filtered.Fuel.unique()

    # Try to avoid loops

    # https://jamesrledoux.com/code/group-by-aggregate-pandas
    heat_demands_2050 = df_filtered.groupby(['NUTS0_code', 'Supertype', 'Type',
                                             'Unit'])['Value'].sum()
    heat_demands_2050.columns = ['Value']
    heat_demands_2050 = heat_demands_2050.reset_index()
    heat_demands_2050['Value'] /= 1000

    # not used in the end:
    # pivot_hotmaps_test = (pd.pivot_table(heat_demands_2050, values="Value",
    #                                       index=['NUTS0_code'],
    #                                       columns=['Supertype', "Type"],
    #                                       aggfunc=np.sum)
    #                       ).reset_index()

    pivot_hotmaps = heat_demands_2050.pivot(index='NUTS0_code',
                                            columns=['Supertype', "Type"],
                                            values='Value')

    # extract residential heat demand data
    residential_demands = (pivot_hotmaps['Residential'].rename_axis(None).
                           reset_index().rename({'index': 'country'}, axis=1)
                           ).set_index('country')

    # residential space heating
    res_space_heating = residential_demands.drop('hot water',
                                                 inplace=False, axis=1)
    energy_totals = adjust_heat_demand_column('Residential', 'space heating',
                              res_space_heating, energy_totals)
    # residential hot water
    res_hot_water = residential_demands.drop('space heating',
                                                 inplace=False, axis=1)
    energy_totals = adjust_heat_demand_column('Residential', 'hot water',
                                                res_hot_water, energy_totals)

    # Do the same for service sector heat demands, after summing up the
    # private and the public sector parts first
    # pivot_hotmaps.columns

    ser_temp =  pivot_hotmaps.drop('Residential', axis = 1,
                                   level = 0).unstack().reset_index().rename(
                                       {0: 'Value'}, axis=1)
    # ser_temp.columns
    grouped = ser_temp.groupby(['NUTS0_code', 'Type'])['Value'].sum()
    grouped.columns = ['Value']
    grouped = grouped.reset_index()

    pivot_ser = grouped.pivot(index='NUTS0_code', columns=["Type"],
                              values='Value')

    service_demands = pivot_ser.rename_axis(None).reset_index().rename(
        {'index': 'country'}, axis=1).set_index('country')

    # services space heating
    ser_space_heating = service_demands.drop('hot water',
                                                  inplace=False, axis=1 )
    energy_totals = adjust_heat_demand_column('Services', 'space heating',
                                              ser_space_heating, energy_totals)

    # services hot water
    ser_hot_water = service_demands.drop('space heating',
                                                 inplace=False, axis=1)
    energy_totals = adjust_heat_demand_column('Services', 'hot water',
                                              ser_hot_water, energy_totals)


    # Adjustments for Germany
    # accoording to the Max Scenario also used in the Netzentwicklungsplan

    # residential space heating
    # basierend auf BMWi-Zielszenario, Energiereferenzprognose, Seite 261
    # residential hot water
    # basierend auf BMWi-Zielszenario, Energiereferenzprognose, Seite  QUELLE
    # conversion from PJ to TWh and from Final energy demand (including ambient
    # heat) to delivered energy using "Nutzungsgrade"
    # Entwicklung der Energiemärkte – Energiereferenzprognose, page: 267 + 268
    # https://www.prognos.com/uploads/tx_atwpubdb/140716_Langfassung_583_Seiten_Energiereferenzprognose_2014.pdf
    # 96% or 128% (excl. ambient air)
    energy_totals.loc[(energy_totals.index == 'DE'
                       ), 'total residential space'] = 845 * 0.96 /60/60*1000
    # 94% (no value given excl. ambient air)
    energy_totals.loc[(energy_totals.index == 'DE'
                       ), 'total residential water'] = 226 * 0.94 /60/60*1000
    # service space heating
    # basierend auf BMWi-Zielszenario, Energiereferenzprognose, Seite QUELLE
    # service hot water
    # basierend auf BMWi-Zielszenario, Energiereferenzprognose, Seite  QUELLE
    # "Nutzungsgrad" not given, assumption: as for residential sector
    energy_totals.loc[(energy_totals.index == 'DE'
                       ), 'total services space'] = 151 * 0.96 /60/60*1000
    # 94% (no value given excl. ambient air)
    energy_totals.loc[(energy_totals.index == 'DE'
                       ), 'total services water'] = 185 * 0.94 /60/60*1000


    """
    IMPORTANT INTERFACE REMARKS
    'electricity residential space' and 'electricity residential water' and
    'electricity services space' and 'electricity services water'
    are adjusted at a different place. Where? Please add a cross reference.

    The following columns are not updated and should not be used. Can we delete
    them to be on the safe side?
    'total residential cooking', 'electricity residential cooking'
    'total residential', 'electricity residential'
    'total services cooking', electricity services cooking',
    'total services', 'electricity services'
    """

    return energy_totals


def adjust_CO2_totals():
    """
    DO WE CORRECT THE EMISSIONS???

    Returns
    -------
    None.

    """

    return None

def adjust_district_heating_shares():
    """
    Adjust future district heating shares for the scenario generation

    Adjust the district heating shares for European countries using hotmaps
    data. The assumption for Germany is changed afterwards again.

    Parameters
    ----------
        None

    Returns
    -------
        None

    ToDo
    ----
         Implementation: Load the of the csv file and change the data.

    Notes
    -----
        None.

    """
    # implementation is still missing

    return None

