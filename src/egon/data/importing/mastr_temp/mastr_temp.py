#!/usr/bin/env python3

import pandas as pd
from egon.data import db
from sqlalchemy import Column, String, Float, Integer
from sqlalchemy.ext.declarative import declarative_base

### temporary import mastr dumps to local database
Base = declarative_base()

class BnetzaMastrWind(Base):
        __tablename__ = 'bnetza_mastr_wind'
        __table_args__ = {'schema': 'model_draft', 'extend_existing':True}

        EinheitMastrNummer = Column(String(50), primary_key=True)
        DatumLetzeAktualisierung = Column(String(50))
        Name = Column(String(100))
        Standort = Column(String(100))
        Bruttoleistung = Column(Float)
        Erzeugungsleistung = Column(Float)
        EinheitBetriebsstatus = Column(String(50))
        StatisikFlag = Column(String(50))
        StatisikFlag_unit = Column(String(50))
        Laengengrad = Column(Float)
        Breitengrad = Column(Float)
        DatumEndgueltigeStilllegung = Column(String(50))
        Seelage = Column(String(50))
        Hersteller = Column(String(50))
        Typenbezeichnung = Column(String(50))
        Nabenhoehe = Column(Float)
        Rotordurchmesser = Column(Float)

class BnetzaMastrSolar(Base):
        __tablename__ = 'bnetza_mastr_solar'
        __table_args__ = {'schema': 'model_draft', 'extend_existing':True}

        EinheitMastrNummer = Column(String(50), primary_key=True)
        DatumLetzeAktualisierung = Column(String(50))
        Name = Column(String(100))
        Standort = Column(String(100))
        Bruttoleistung = Column(Float)
        Erzeugungsleistung = Column(Float)
        EinheitBetriebsstatus = Column(String(50))
        StatisikFlag = Column(String(50))
        StatisikFlag_unit = Column(String(50))
        Laengengrad = Column(Float)
        Breitengrad = Column(Float)

class BnetzaMastrBiomass(Base):
        __tablename__ = 'bnetza_mastr_biomass'
        __table_args__ = {'schema': 'model_draft', 'extend_existing':True}

        EinheitMastrNummer = Column(String(50), primary_key=True)
        DatumLetzeAktualisierung = Column(String(50))
        Name = Column(String(100))
        Standort = Column(String(100))
        Bruttoleistung = Column(Float)
        Erzeugungsleistung = Column(Float)
        EinheitBetriebsstatus = Column(String(50))
        StatisikFlag = Column(String(50))
        StatisikFlag_unit = Column(String(50))
        Laengengrad = Column(Float)
        Breitengrad = Column(Float)


class BnetzaMastrCombustion(Base):
        __tablename__ = 'bnetza_mastr_combustion'
        __table_args__ = {'schema': 'model_draft', 'extend_existing':True}

        EinheitMastrNummer = Column(String(50), primary_key=True)
        DatumLetzeAktualisierung = Column(String(50))
        Name = Column(String(100))
        Standort = Column(String(100))
        Bruttoleistung = Column(Float)
        Erzeugungsleistung = Column(Float)
        EinheitBetriebsstatus = Column(String(50))
        StatisikFlag = Column(String(50))
        StatisikFlag_unit = Column(String(50))
        Laengengrad = Column(Float)
        Breitengrad = Column(Float)

class BnetzaMastrHydro(Base):
        __tablename__ = 'bnetza_mastr_hydro'
        __table_args__ = {'schema': 'model_draft', 'extend_existing':True}

        EinheitMastrNummer = Column(String(50), primary_key=True)
        DatumLetzeAktualisierung = Column(String(50))
        Name = Column(String(100))
        Standort = Column(String(100))
        Bruttoleistung = Column(Float)
        Erzeugungsleistung = Column(Float)
        EinheitBetriebsstatus = Column(String(50))
        StatisikFlag = Column(String(50))
        StatisikFlag_unit = Column(String(50))
        Laengengrad = Column(Float)
        Breitengrad = Column(Float)

class BnetzaMastrGsgk(Base):
        __tablename__ = 'bnetza_mastr_gsgk'
        __table_args__ = {'schema': 'model_draft', 'extend_existing':True}

        EinheitMastrNummer = Column(String(50), primary_key=True)
        DatumLetzeAktualisierung = Column(String(50))
        Name = Column(String(100))
        Standort = Column(String(100))
        Bruttoleistung = Column(Float)
        Erzeugungsleistung = Column(Float)
        EinheitBetriebsstatus = Column(String(50))
        StatisikFlag = Column(String(50))
        StatisikFlag_unit = Column(String(50))
        Laengengrad = Column(Float)
        Breitengrad = Column(Float)


def insert_data_mastr():

    engine = db.engine()

    for carrier in ['wind', 'solar', 'biomass', 'combustion', 'gsgk', 'hydro']:

        path = '2020-11-15_sample-10000/bnetza_mastr_' + carrier + '_raw.csv'

        df = pd.read_csv(path).set_index('EinheitMastrNummer')

        df.to_sql('bnetza_mastr_' + carrier,
                  engine,
                  schema='model_draft',
                  if_exists='replace')

