# -*- coding: utf-8 -*-
"""
Test if the capacities of MaStR and NEP are matching to use them later as key.
"""
import pandas as pd
import geopandas
from egon.data import db, config
from egon.data.processing.power_plants import (
    assign_voltage_level, assign_bus_id, assign_gas_bus_id)
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, String, Float, Integer, Sequence
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import JSONB
from geoalchemy2 import Geometry
Base = declarative_base()

class EgonChp(Base):
    __tablename__ = "egon_chp"
    __table_args__ = {"schema": "supply"}
    id = Column(Integer, Sequence("chp_seq"), primary_key=True)
    sources = Column(JSONB)
    source_id = Column(JSONB)
    carrier = Column(String)
    use_case = Column(String)
    el_capacity = Column(Float)
    th_capacity = Column(Float)
    electrical_bus_id = Column(Integer)
    heat_bus_id = Column(Integer)
    gas_bus_id = Column(Integer)
    voltage_level = Column(Integer)
    scenario = Column(String)
    geom = Column(Geometry("POINT", 4326))

def create_tables():
    """Create tables for chp data
    Returns
    -------
    None.
    """

    db.execute_sql("CREATE SCHEMA IF NOT EXISTS supply;")
    engine = db.engine()
    EgonChp.__table__.drop(bind=engine, checkfirst=True)
    EgonChp.__table__.create(bind=engine, checkfirst=True)

def map_carrier_nep_mastr():
    """Map carriers from NEP to carriers from MaStR

    Returns
    -------
    pandas.Series
        List of mapped carriers

    """
    return (
        pd.Series(data={
        'Abfall': "Sonstige_Energietraeger",
        'Erdgas': 'Erdgas',
        'Sonstige\nEnergieträger': "Sonstige_Energietraeger",
        'Steinkohle': 'Steinkohle',
        'Kuppelgase': 'Kuppelgase',
        'Mineralöl-\nprodukte': 'Mineraloelprodukte'

        }))

def map_carrier_egon_mastr():
    """Map carriers from MaStR to carriers used in egon-data

    Returns
    -------
    pandas.Series
        List of mapped carriers

    """
    return (
        pd.Series(data={
            'Steinkohle': 'coal',
            'Erdgas': 'gas',
            'Kuppelgase': 'gas',
            'Mineraloelprodukte': 'oil',
            'NichtBiogenerAbfall': 'other_non_renewable',
            'AndereGase': 'other_non_renewable',
            'Waerme': 'other_non_renewable',
            'Sonstige_Energietraeger': 'other_non_renewable',
            }))

def map_carrier_matching():
    """Map carriers from NEP to carriers from MaStR used in matching

    Returns
    -------
    pandas.Series
        List of mapped carriers

    """
    return (
        pd.Series(
        data = {
        'Kuppelgase': ['Erdgas', 'AndereGase','Mineraloelprodukte' ],
        'Sonstige_Energietraeger':[
                    'Erdgas', 'AndereGase','Mineraloelprodukte',
                    'Waerme','NichtBiogenerAbfall'],
        'Mineraloelprodukte':['Erdgas', 'Mineraloelprodukte' ],
        'Erdgas': ['Erdgas'],
        'Steinkohle': ['Steinkohle']}))

#####################################   NEP treatment   #################################
def select_chp_from_nep():
    """ Select CHP plants with location from NEP's list of power plants

    Returns
    -------
    pandas.DataFrame
        CHP plants from NEP list

    """
    sources = config.datasets()["chp_location"]["sources"]

    # Select CHP plants with geolocation from list of conventional power plants
    chp_NEP_data = db.select_dataframe(
        f"""
        SELECT bnetza_id, name, carrier, chp, postcode, capacity,
           c2035_chp, c2035_capacity
        FROM {sources['list_conv_pp']['schema']}.
        {sources['list_conv_pp']['table']}
        WHERE bnetza_id != 'KW<10 MW'
        AND (chp = 'Ja' OR c2035_chp = 'Ja')
        AND c2035_capacity > 0
        AND postcode != 'None'
        """
        )

    # Removing CHP out of Germany
    chp_NEP_data['postcode'] = chp_NEP_data['postcode'].astype(str)
    chp_NEP_data = chp_NEP_data[ ~ chp_NEP_data['postcode'].str.contains('A')]
    chp_NEP_data = chp_NEP_data[ ~ chp_NEP_data['postcode'].str.contains('L')]
    chp_NEP_data = chp_NEP_data[ ~ chp_NEP_data['postcode'].str.contains('nan') ]

    # Remove the subunits from the bnetza_id
    chp_NEP_data['bnetza_id'] = chp_NEP_data['bnetza_id'].str[0:7]

    # Update carrier to match to MaStR
    map_carrier = map_carrier_nep_mastr()
    chp_NEP_data['carrier'] = map_carrier[chp_NEP_data['carrier'].values].values

    # Initalize DataFrame
    chp_NEP = pd.DataFrame(
        columns = ['name', 'postcode', 'carrier', 'capacity',
                   'c2035_capacity', 'c2035_chp'])

    # Insert rows from list without a name
    chp_NEP = chp_NEP.append(
        chp_NEP_data[chp_NEP_data.name.isnull()].loc[:, [
            'name', 'postcode', 'carrier', 'capacity',
            'c2035_capacity',  'c2035_chp']])
    # Insert rows from list with a name
    chp_NEP = chp_NEP.append(
        chp_NEP_data.groupby(
            ['carrier', 'name', 'postcode', 'c2035_chp']
            )['capacity','c2035_capacity'].sum().reset_index()).reset_index()

    return chp_NEP.drop('index', axis=1)

#####################################   MaStR treatment   #################################
def select_chp_from_mastr():
    """Select combustion CHP plants from MaStR

    Returns
    -------
    MaStR_konv : pd.DataFrame
        CHP plants from MaStR

    """

    sources = config.datasets()["chp_location"]["sources"]

    # Read-in data from MaStR
    MaStR_konv = pd.read_csv(
        sources["mastr_combustion"],
        delimiter = ',',
        usecols = ['Nettonennleistung',
                    'EinheitMastrNummer',
                    'Kraftwerksnummer',
                    'Energietraeger',
                    'Postleitzahl',
                    'Laengengrad',
                    'Breitengrad',
                    'ThermischeNutzleistung',
                    'EinheitBetriebsstatus',
                    'LokationMastrNummer'])

    # Rename columns
    MaStR_konv = MaStR_konv.rename(columns={ 'Kraftwerksnummer': 'bnetza_id',
                                      'Energietraeger': 'energietraeger_Ma',
                                      'Postleitzahl': 'plz_Ma',
                                      'Laengengrad': 'longitude',
                                      'Breitengrad': 'latitude'})

    # Select only CHP plants which are in operation
    MaStR_konv = MaStR_konv[MaStR_konv.EinheitBetriebsstatus=='InBetrieb']

    # Insert geometry column
    MaStR_konv = MaStR_konv[ ~ ( MaStR_konv['longitude'].isnull()) ]
    MaStR_konv = geopandas.GeoDataFrame(
        MaStR_konv, geometry=geopandas.points_from_xy(
            MaStR_konv['longitude'], MaStR_konv['latitude']))

    # Drop individual CHP
    MaStR_konv = MaStR_konv[(MaStR_konv['Nettonennleistung'] >= 100)]

    # Drop rows without post code and update datatype of postcode
    MaStR_konv = MaStR_konv[~MaStR_konv['plz_Ma'].isnull()]
    MaStR_konv['plz_Ma'] = MaStR_konv['plz_Ma'].astype(int)

    # Calculate power in MW
    MaStR_konv.loc[:, 'Nettonennleistung'] *=1e-3
    MaStR_konv.loc[:, 'ThermischeNutzleistung'] *=1e-3

    return MaStR_konv


# ############################################   Match with plz and K   ############################################
def match_nep_chp(chp_NEP, MaStR_konv, chp_NEP_matched, buffer_capacity=0.1):
    """ Match CHP plants from MaStR to list of power plants from NEP

    Parameters
    ----------
    chp_NEP : pandas.DataFrame
        CHP plants from NEP which are not matched to MaStR
    MaStR_konv : pandas.DataFrame
        CHP plants from MaStR which are not matched to NEP
    chp_NEP_matched : pandas.DataFrame
        Already matched CHP
    buffer_capacity : float, optional
        Maximum difference in capacity in p.u. The default is 0.1.

    Returns
    -------
    chp_NEP_matched : pandas.DataFrame
        Matched CHP
    MaStR_konv : pandas.DataFrame
        CHP plants from MaStR which are not matched to NEP
    chp_NEP : pandas.DataFrame
        CHP plants from NEP which are not matched to MaStR

    """

    for ET in chp_NEP['carrier'].unique():

        print('**********************  ' + ET + '  **********************')
        map_carrier = map_carrier_matching()

        carrier_egon = map_carrier_egon_mastr()

        for index, row in chp_NEP[
                (chp_NEP['carrier'] == ET)
                & (chp_NEP['postcode'] != 'None')].iterrows():
            # Select plants from MaStR that match carrier, PLZ
            # and have a similar capacity
            selected = MaStR_konv[
                (MaStR_konv.Nettonennleistung<= row['capacity']
                 * (1+buffer_capacity))
                & (MaStR_konv.Nettonennleistung>= row['capacity']
                   * (1-buffer_capacity))
                & (MaStR_konv.plz_Ma==int(row['postcode']))
                & MaStR_konv.energietraeger_Ma.isin(map_carrier[ET])]

            # If a plant could be matched, add this to chp_NEP_matched
            if len(selected) > 0:
                chp_NEP_matched = chp_NEP_matched.append(
                    geopandas.GeoDataFrame(
                        data = {
                            'source': 'MaStR scaled with NEP 2021 list',
                            'MaStRNummer': selected.EinheitMastrNummer.head(1),
                            'carrier': (
                                carrier_egon[ET] if row.c2035_chp=='Nein'
                                else 'gas'),
                            'chp': True,
                            'el_capacity': row.c2035_capacity,
                            'th_capacity': selected.ThermischeNutzleistung.head(1),
                            'scenario': 'eGon2035',
                            'geometry': selected.geometry.head(1),
                            'voltage_level': selected.voltage_level.head(1)
                        }))

                # Drop matched CHP from chp_NEP and MaStR_konv
                chp_NEP = chp_NEP.drop(index)
                MaStR_konv = MaStR_konv.drop(selected.index)

    return chp_NEP_matched, MaStR_konv, chp_NEP

def match_chp(chp_NEP, MaStR_konv, chp_NEP_matched, consider_carrier=True):
    """ Match CHP plants from MaStR to list of power plants from NEP

    Parameters
    ----------
    chp_NEP : pandas.DataFrame
        CHP plants from NEP which are not matched to MaStR
    MaStR_konv : pandas.DataFrame
        CHP plants from MaStR which are not matched to NEP
    chp_NEP_matched : pandas.DataFrame
        Already matched CHP
    consider_carrier : boolean, optional
        Choose if carrier is considered in matching. The default is True.

    Returns
    -------
    chp_NEP_matched : pandas.DataFrame
        Matched CHP
    MaStR_konv : pandas.DataFrame
        CHP plants from MaStR which are not matched to NEP
    chp_NEP : pandas.DataFrame
        CHP plants from NEP which are not matched to MaStR

    """

    map_carrier = map_carrier_matching()

    carrier_egon = map_carrier_egon_mastr()

    for i, row in chp_NEP.iterrows():
        if consider_carrier:
            # Select MaStR power plants with the same carrier and PLZ
            selected_plants = MaStR_konv[
                            (MaStR_konv.energietraeger_Ma.isin(
                                map_carrier[row.carrier]))
                            &(MaStR_konv.plz_Ma==int(row.postcode))
                            &(MaStR_konv.Nettonennleistung>50)]
        else:
            # Select MaStR power plants with the same PLZ
            selected_plants = MaStR_konv[
                            (MaStR_konv.plz_Ma==int(row.postcode))
                            &(MaStR_konv.Nettonennleistung>50)]

        selected_plants.loc[:, 'Nettonennleistung'] = (
                        row.c2035_capacity * selected_plants.Nettonennleistung/
                        selected_plants.Nettonennleistung.sum())

        # If a plant could be matched, add this to chp_NEP_matched
        chp_NEP_matched = chp_NEP_matched.append(
            geopandas.GeoDataFrame(
                data = {
                    'source': 'MaStR scaled with NEP 2021',
                    'MaStRNummer': selected_plants.EinheitMastrNummer,
                    'carrier': (
                        carrier_egon[row.carrier] if row.c2035_chp=='Nein'
                        else 'gas'),
                    'chp': True,
                    'el_capacity': selected_plants.Nettonennleistung,
                    'th_capacity': selected_plants.ThermischeNutzleistung,
                    'scenario': 'eGon2035',
                    'geometry': selected_plants.geometry,
                    'voltage_level': selected_plants.voltage_level
                    }))

        # Drop matched CHP from chp_NEP and MaStR_konv
        if len(selected_plants) > 0:
            chp_NEP = chp_NEP.drop(i)
            MaStR_konv = MaStR_konv.drop(selected_plants.index)

    return chp_NEP_matched, chp_NEP, MaStR_konv

################################################### Final table ###################################################
def insert_chp_egon2035():
    """ Insert large CHP plants for eGon2035 considering NEP and MaStR

    Returns
    -------
    None.

    """
    
    create_tables()
    
    target = config.datasets()["chp_location"]["targets"]["power_plants"]

    # Select CHP from NEP list
    chp_NEP = select_chp_from_nep()

    # Select CHP from MaStR
    MaStR_konv = select_chp_from_mastr()

    # Assign voltage level to MaStR
    MaStR_konv['voltage_level'] = assign_voltage_level(
        MaStR_konv, config.datasets()["chp_location"])

    # Initalize DataFrame for match CHPs
    chp_NEP_matched = geopandas.GeoDataFrame(
        columns = [
            'carrier','chp','el_capacity','th_capacity', 'scenario','geometry',
            'MaStRNummer', 'source', 'voltage_level'])

    # Match CHP from NEP list using PLZ, carrier and capacity
    chp_NEP_matched, MaStR_konv, chp_NEP = match_nep_chp(
        chp_NEP, MaStR_konv, chp_NEP_matched, buffer_capacity=0.1)

    # Aggregate units from MaStR to one power plant
    MaStR_konv = MaStR_konv.groupby(
        ['plz_Ma', 'longitude', 'latitude','energietraeger_Ma', 'voltage_level']
        )[['Nettonennleistung', 'ThermischeNutzleistung', 'EinheitMastrNummer'
           ]].sum(numeric_only=False).reset_index()
    MaStR_konv['geometry'] = geopandas.points_from_xy(
        MaStR_konv['longitude'], MaStR_konv['latitude'])

    # Match CHP from NEP list with aggregated MaStR units
    chp_NEP_matched, MaStR_konv, chp_NEP = match_nep_chp(
        chp_NEP, MaStR_konv, chp_NEP_matched, buffer_capacity=0.1)

    # If some CHP's are not matched, drop capacity constraint
    chp_NEP_matched, chp_NEP, MaStR_konv = match_chp(
        chp_NEP, MaStR_konv, chp_NEP_matched, consider_carrier=True)

    # If some CHP's are not matched, drop carrier constraint
    chp_NEP_matched, chp_NEP, MaStR_konv = match_chp(
        chp_NEP, MaStR_konv, chp_NEP_matched, consider_carrier=False)

    # Prepare geometry for database import
    chp_NEP_matched["geometry_wkt"] = chp_NEP_matched["geometry"].apply(
        lambda geom: geom.wkt)

    print(f"{chp_NEP_matched.el_capacity.sum()} MW matched")
    print(f"{chp_NEP.c2035_capacity.sum()} MW not matched")

    # Aggregate chp per location and carrier
    insert_chp = chp_NEP_matched.groupby(["carrier", "geometry_wkt", "voltage_level"])[
        ['el_capacity', 'th_capacity', 'geometry',
           'MaStRNummer', 'source']].sum(numeric_only=False).reset_index()
    insert_chp.loc[:, 'geometry'] = chp_NEP_matched.set_index('geometry_wkt').loc[
        insert_chp.set_index('geometry_wkt').index, 'geometry'].unique()
    insert_chp.crs = "EPSG:4326"
    insert_chp_c = insert_chp.copy()
    
    # Assign bus_id
    insert_chp['bus_id'] = assign_bus_id(
        insert_chp, config.datasets()["chp_location"]).bus_id
    
    # Assign gas bus_id
    insert_chp['gas_bus_id'] = assign_gas_bus_id(insert_chp_c).gas_bus_id

    # Delete existing CHP in the target table
    db.execute_sql(
        f""" DELETE FROM {target['schema']}.{target['table']}
        WHERE carrier IN ('gas', 'other_non_renewable', 'oil')
        AND scenario='eGon2035';""")

    # Insert into target table
    session = sessionmaker(bind=db.engine())()
    for i, row in insert_chp.iterrows():
        entry = EgonChp(
                sources={
                    "chp": "MaStR",
                    "el_capacity": row.source,
                    "th_capacity": "MaStR",
                },
                source_id={"MastrNummer": row.MaStRNummer},
                carrier=row.carrier,
                el_capacity=row.el_capacity,
                th_capacity= row.th_capacity,
                voltage_level = row.voltage_level,
                electrical_bus_id = row.bus_id,
                gas_bus_id = row.gas_bus_id,
                scenario='eGon2035',
                geom=f"SRID=4326;POINT({row.geometry.x} {row.geometry.y})",
            )
        session.add(entry)
    session.commit()

