# -*- coding: utf-8 -*-
"""
The central module containing all code dealing with importing CH4 production data
"""
#import os
import ast
import pandas as pd
import geopandas as gpd
import numpy as np
import geopandas

from egon.data import db
from egon.data.config import settings
from egon.data.datasets import Dataset
from urllib.request import urlretrieve
from pathlib import Path

class CH4Production(Dataset):
     def __init__(self, dependencies):
         super().__init__(
             name="CH4Production",
             version="0.0.5",
             dependencies=dependencies,
             tasks=(import_gas_generators),
         )


def load_NG_generators():
    """Define the natural CH4 production units in Germany

    Returns
    -------
    CH4_generators_list :
        Dataframe containing the natural gas production units in Germany

    """
    target_file = (
            Path(".") /
            "datasets" /
            "gas_data" /
            "data" /
            "IGGIELGN_Productions.csv")

    NG_generators_list = pd.read_csv(target_file,
                               delimiter=';', decimal='.',
                               usecols = ['lat', 'long', 'country_code','param'])

    NG_generators_list = NG_generators_list[ NG_generators_list['country_code'].str.match('DE')]

    # Cut data to federal state if in testmode
    NUTS1 = []
    for index, row in NG_generators_list.iterrows():
        param = ast.literal_eval(row['param'])
        NUTS1.append(param['nuts_id_1'])
    NG_generators_list = NG_generators_list.assign(NUTS1 = NUTS1)

    boundary = settings()['egon-data']['--dataset-boundary']
    if boundary != 'Everything':
        map_states = {'Baden-Württemberg':'DE1', 'Nordrhein-Westfalen': 'DEA',
                'Hessen': 'DE7', 'Brandenburg': 'DE4', 'Bremen':'DE5',
                'Rheinland-Pfalz': 'DEB', 'Sachsen-Anhalt': 'DEE',
                'Schleswig-Holstein':'DEF', 'Mecklenburg-Vorpommern': 'DE8',
                'Thüringen': 'DEG', 'Niedersachsen': 'DE9',
                'Sachsen': 'DED', 'Hamburg': 'DE6', 'Saarland': 'DEC',
                'Berlin': 'DE3', 'Bayern': 'DE2'}

        NG_generators_list = NG_generators_list[NG_generators_list['NUTS1'].isin([map_states[boundary], np.nan])]

    NG_generators_list = NG_generators_list.rename(columns={'lat': 'y','long': 'x'})
    NG_generators_list = geopandas.GeoDataFrame(NG_generators_list,
                                                geometry=geopandas.points_from_xy(NG_generators_list['x'],
                                                                                  NG_generators_list['y']))
    NG_generators_list = NG_generators_list.rename(columns={'geometry': 'geom'}).set_geometry('geom', crs=4326)

    # Insert p_nom
    # Total production in Germany
    Total_NG_extracted_2035 = 36 # [TWh] Netzentwicklungsplan Gas 2020–2030
    Total_NG_capacity_2035 = Total_NG_extracted_2035 * 1000000 / (24 * 365)

    # Regionalization of the production
    share = []
    for index, row in NG_generators_list.iterrows():
        param = ast.literal_eval(row['param'])
        share.append(param['max_supply_M_m3_per_d'])

    NG_generators_list['p_nom'] = [(i/(sum(share)) * Total_NG_capacity_2035) for i in share]

    # Remove useless columns
    NG_generators_list = NG_generators_list.drop(columns=['x', 'y', 'param', 'country_code', 'NUTS1'])

    return NG_generators_list


def load_biogas_generators():
    """Define the biogas production units in Germany

    Returns
    -------
    CH4_generators_list :
        Dataframe containing the biogas production units in Germany

    """
    # Download file
    basename = "Biogaspartner_Einspeiseatlas_Deutschland_2021.xlsx"
    url = "https://www.biogaspartner.de/fileadmin/Biogaspartner/Dokumente/Einspeiseatlas/" + basename
    target_file = (
            Path(".") /
            "datasets" /
            "gas_data" /
            basename)

    urlretrieve(url, target_file)

    # Read-in data from csv-file
    biogas_generators_list = pd.read_excel(target_file,
                               usecols = ['Koordinaten', 'Einspeisung Biomethan [(N*m^3)/h)]'])

    x = []
    y = []
    for index, row in biogas_generators_list.iterrows():
        coordinates = row['Koordinaten'].split(',')
        y.append(coordinates[0])
        x.append(coordinates[1])
    biogas_generators_list['x'] = x
    biogas_generators_list['y'] = y

    biogas_generators_list = geopandas.GeoDataFrame(biogas_generators_list,
                                                    geometry=geopandas.points_from_xy(biogas_generators_list['x'],
                                                                                      biogas_generators_list['y']))
    biogas_generators_list = biogas_generators_list.rename(columns={'geometry': 'geom'}).set_geometry('geom', crs=4326)

    # Connect to local database
    engine = db.engine()

    # Cut data to federal state if in testmode
    boundary = settings()['egon-data']['--dataset-boundary']
    if boundary != 'Everything':
        db.execute_sql(
            """
              DROP TABLE IF EXISTS grid.egon_biogas_generator CASCADE;
            """)
        biogas_generators_list.to_postgis('egon_biogas_generator',
                              engine,
                              schema ='grid',
                              index = False,
                              if_exists = 'replace')

        sql = '''SELECT *
            FROM grid.egon_biogas_generator, boundaries.vg250_sta_union  as vg
            WHERE ST_Transform(vg.geometry,4326) && egon_biogas_generator.geom
            AND ST_Contains(ST_Transform(vg.geometry,4326), egon_biogas_generator.geom)'''

        biogas_generators_list = gpd.GeoDataFrame.from_postgis(sql, con=engine, geom_col="geom", crs=4326)
        biogas_generators_list = biogas_generators_list.drop(columns=['id', 'bez', 'area_ha', 'geometry'])
        db.execute_sql(
            """
              DROP TABLE IF EXISTS grid.egon_biogas_generator CASCADE;
            """)

    # Insert p_nom
    # Total production in Germany
    Total_biogas_extracted_2035 = 10 # [TWh] Netzentwicklungsplan Gas 2020–2030
    Total_biogas_capacity_2035 = Total_biogas_extracted_2035 * 1000000 / (24 * 365)

    # Regionalization of the production
    biogas_generators_list['p_nom'] = (biogas_generators_list['Einspeisung Biomethan [(N*m^3)/h)]']
                                       / biogas_generators_list['Einspeisung Biomethan [(N*m^3)/h)]'].sum()
                                       * Total_biogas_capacity_2035)
    # Remove useless columns
    biogas_generators_list = biogas_generators_list.drop(columns=['x', 'y', 'Koordinaten',
                                                                  'Einspeisung Biomethan [(N*m^3)/h)]'])
    return biogas_generators_list


def assign_ch4_bus_id(dataframe):
    """Assigns bus_ids (for CH4 buses) to points (contained in a dataframe) according to location
    Parameters
    ----------
    dataframe : pandas.DataFrame cointaining points

    Returns
    -------
    power_plants : pandas.DataFrame
        Power plants (including voltage level) and bus_id
    """

    CH4_voronoi = db.select_geodataframe(
        """
        SELECT * FROM grid.egon_voronoi_ch4
        """, epsg=4326)

    res = gpd.sjoin(dataframe, CH4_voronoi)
    res['bus'] = res['bus_id']
    res = res.drop(columns=['index_right', 'id'])

    # Assert that all power plants have a bus_id
    assert res.bus.notnull().all(), "Some points are not attached to a CH4 bus."

    return res

def assign_h2_bus_id(dataframe):
    """Assigns bus_ids (for H2 buses) to points (contained in a dataframe) according to location
    Parameters
    ----------
    dataframe : pandas.DataFrame cointaining points

    Returns
    -------
    power_plants : pandas.DataFrame
        Power plants (including voltage level) and bus_id
    """

    H2_voronoi = db.select_geodataframe(
        """
        SELECT * FROM grid.egon_voronoi_h2
        """, epsg=4326)

    res = gpd.sjoin(dataframe, H2_voronoi)
    res['bus'] = res['bus_id']
    res = res.drop(columns=['index_right', 'id'])

    # Assert that all power plants have a bus_id
    assert res.bus.notnull().all(), "Some points are not attached to a H2 bus."

    return res


def import_gas_generators():
    """Insert list of gas production units in database

    Returns
    -------
     None.
    """
    # Connect to local database
    engine = db.engine()

    # Clean table
    db.execute_sql(
        """
    DELETE FROM grid.egon_etrago_generator WHERE "carrier" = 'CH4';
    """
    )

    # Select next id value
    new_id = db.next_etrago_id('generator')

    CH4_generators_list = pd.concat([load_NG_generators(), load_biogas_generators()])
    CH4_generators_list['generator_id'] = range(new_id, new_id + len(CH4_generators_list))

    # Add missing columns
#    CH4_generators_list['p_set_fixed'] = CH4_generators_list['p_nom']
    c = {'scn_name':'eGon2035', 'carrier':'CH4'}
    CH4_generators_list = CH4_generators_list.assign(**c)

    CH4_generators_list =  CH4_generators_list.reset_index(drop=True)

    # Match to associated CH4 bus
    CH4_generators_list = assign_ch4_bus_id(CH4_generators_list)

    # Remove useless columns
    CH4_generators_list = CH4_generators_list.drop(columns=['geom', 'bus_id'])

    # Insert data to db
    CH4_generators_list.to_sql('egon_etrago_generator',
                              engine,
                              schema ='grid',
                              index = False,
                              if_exists = 'append')
