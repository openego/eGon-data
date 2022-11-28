"""Import MaStR dataset and write to DB tables"""
from geoalchemy2 import Geometry
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Float,
    Integer,
    Sequence,
    String,
)
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd

import egon.data.config

Base = declarative_base()


class EgonPowerPlantsPv(Base):
    __tablename__ = "egon_power_plants_pv"
    __table_args__ = {"schema": "supply"}

    id = Column(Integer, Sequence("pp_pv_seq"), primary_key=True)
    bus_id = Column(Integer, nullable=True)  # Grid district id
    gens_id = Column(String, nullable=True)  # EinheitMastrNummer

    status = Column(String, nullable=True)  # EinheitBetriebsstatus
    commissioning_date = Column(DateTime, nullable=True)  # Inbetriebnahmedatum
    postcode = Column(String(5), nullable=True)  # Postleitzahl
    city = Column(String(50), nullable=True)  # Ort
    federal_state = Column(String(31), nullable=True)  # Bundesland

    site_type = Column(String(69), nullable=True)  # Lage
    usage_sector = Column(String(36), nullable=True)  # Nutzungsbereich
    orientation_primary = Column(String(11), nullable=True)  # Hauptausrichtung
    orientation_primary_angle = Column(
        String(18), nullable=True
    )  # HauptausrichtungNeigungswinkel
    orientation_secondary = Column(
        String(11), nullable=True
    )  # Nebenausrichtung
    orientation_secondary_angle = Column(
        String(18), nullable=True
    )  # NebenausrichtungNeigungswinkel
    orientation_uniform = Column(
        Boolean, nullable=True
    )  # EinheitlicheAusrichtungUndNeigungswinkel
    module_count = Column(Float, nullable=True)  # AnzahlModule

    capacity = Column(Float, nullable=True)  # Nettonennleistung
    capacity_inverter = Column(
        Float, nullable=True
    )  # ZugeordneteWirkleistungWechselrichter in MW
    feedin_type = Column(String(47), nullable=True)  # Einspeisungsart
    voltage_level = Column(Integer, nullable=True)

    geom = Column(Geometry("POINT", 4326), index=True, nullable=True)


class EgonPowerPlantsWind(Base):
    __tablename__ = "egon_power_plants_wind"
    __table_args__ = {"schema": "supply"}

    id = Column(Integer, Sequence("pp_wind_seq"), primary_key=True)
    bus_id = Column(Integer, nullable=True)  # Grid district id
    gens_id = Column(String, nullable=True)  # EinheitMastrNummer

    status = Column(String, nullable=True)  # EinheitBetriebsstatus
    commissioning_date = Column(DateTime, nullable=True)  # Inbetriebnahmedatum
    postcode = Column(String(5), nullable=True)  # Postleitzahl
    city = Column(String(50), nullable=True)  # Ort
    federal_state = Column(String(31), nullable=True)  # Bundesland

    site_type = Column(String(17), nullable=True)  # Lage
    manufacturer_name = Column(String(100), nullable=True)  # Hersteller
    type_name = Column(String(100), nullable=True)  # Typenbezeichnung
    hub_height = Column(Float, nullable=True)  # Nabenhoehe
    rotor_diameter = Column(Float, nullable=True)  # Rotordurchmesser

    capacity = Column(Float, nullable=True)  # Nettonennleistung
    feedin_type = Column(String(47), nullable=True)  # Einspeisungsart
    voltage_level = Column(Integer, nullable=True)

    geom = Column(Geometry("POINT", 4326), index=True, nullable=True)


class EgonPowerPlantsBiomass(Base):
    __tablename__ = "egon_power_plants_biomass"
    __table_args__ = {"schema": "supply"}

    id = Column(Integer, Sequence("pp_biomass_seq"), primary_key=True)
    bus_id = Column(Integer, nullable=True)  # Grid district id
    gens_id = Column(String, nullable=True)  # EinheitMastrNummer

    status = Column(String, nullable=True)  # EinheitBetriebsstatus
    commissioning_date = Column(DateTime, nullable=True)  # Inbetriebnahmedatum
    postcode = Column(String(5), nullable=True)  # Postleitzahl
    city = Column(String(50), nullable=True)  # Ort
    federal_state = Column(String(31), nullable=True)  # Bundesland

    technology = Column(String(45), nullable=True)  # Technologie
    fuel_name = Column(String(52), nullable=True)  # Hauptbrennstoff
    fuel_type = Column(String(19), nullable=True)  # Biomasseart

    capacity = Column(Float, nullable=True)  # Nettonennleistung
    th_capacity = Column(Float, nullable=True)  # ThermischeNutzleistung
    feedin_type = Column(String(47), nullable=True)  # Einspeisungsart
    voltage_level = Column(Integer, nullable=True)

    geom = Column(Geometry("POINT", 4326), index=True, nullable=True)


class EgonPowerPlantsHydro(Base):
    __tablename__ = "egon_power_plants_hydro"
    __table_args__ = {"schema": "supply"}

    id = Column(Integer, Sequence("pp_hydro_seq"), primary_key=True)
    bus_id = Column(Integer, nullable=True)  # Grid district id
    gens_id = Column(String, nullable=True)  # EinheitMastrNummer

    status = Column(String, nullable=True)  # EinheitBetriebsstatus
    commissioning_date = Column(DateTime, nullable=True)  # Inbetriebnahmedatum
    postcode = Column(String(5), nullable=True)  # Postleitzahl
    city = Column(String(50), nullable=True)  # Ort
    federal_state = Column(String(31), nullable=True)  # Bundesland

    type = Column(Integer, nullable=True)  # ArtDerWasserkraftanlage
    water_origin = Column(String(20), nullable=True)  # ArtDesZuflusses

    capacity = Column(Float, nullable=True)  # Nettonennleistung
    feedin_type = Column(String(47), nullable=True)  # Einspeisungsart
    voltage_level = Column(Integer, nullable=True)

    geom = Column(Geometry("POINT", 4326), index=True, nullable=True)


def import_mastr():
    cfg = egon.data.config.datasets()["power_plants"]

    cols_mapping = {
        "all": {
            "EinheitMastrNummer": "gens_id",
            "EinheitBetriebsstatus": "status",
            "Inbetriebnahmedatum": "commissioning_date",
            "Postleitzahl": "postcode",
            "Ort": "city",
            "Bundesland": "federal_state",
            "Nettonennleistung": "capacity",
            "Einspeisungsart": "feedin_type",
        },
        "pv": {
            "Lage": "site_type",
            "Nutzungsbereich": "usage_sector",
            "Hauptausrichtung": "orientation_primary",
            "HauptausrichtungNeigungswinkel": "orientation_primary_angle",
            "Nebenausrichtung": "orientation_secondary",
            "NebenausrichtungNeigungswinkel": "orientation_secondary_angle",
            "EinheitlicheAusrichtungUndNeigungswinkel": "orientation_uniform",
            "AnzahlModule": "module_count",
            "ZugeordneteWirkleistungWechselrichter": "capacity_inverter",
        },
        "wind": {
            "Lage": "site_type",
            "Hersteller": "manufacturer_name",
            "Typenbezeichnung": "type_name",
            "Nabenhoehe": "hub_height",
            "Rotordurchmesser": "rotor_diameter",
        },
        "biomass": {
            "Technologie": "technology",
            "Hauptbrennstoff": "fuel_name",
            "Biomasseart": "fuel_type",
            "ThermischeNutzleistung": "th_capacity",
        },
        "hydro": {
            "ArtDerWasserkraftanlage": "type",
            "ArtDesZuflusses": "water_origin",
        },
    }

    source_files = {
        "pv": cfg["sources"]["mastr_pv"],
        "wind": cfg["sources"]["mastr_wind"],
        "biomass": cfg["sources"]["mastr_biomass"],
        "hydro": cfg["sources"]["mastr_hydro"],
    }
    target_tables = {
        "pv": EgonPowerPlantsPv,
        "wind": EgonPowerPlantsWind,
        "biomass": EgonPowerPlantsBiomass,
        "hydro": EgonPowerPlantsHydro,
    }

    # import locations
    locations = pd.read_csv(cfg["sources"]["mastr_location"], index_col=None)

    # import units
    technologies = ["pv", "wind", "biomass", "hydro"]
    for tech in technologies:
        units = pd.read_csv(
            source_files[tech],
            usecols=(
                list(cols_mapping["all"].keys())
                + list(cols_mapping[tech].keys())
            ),
            index_col=None,
        ).rename(columns=cols_mapping)
