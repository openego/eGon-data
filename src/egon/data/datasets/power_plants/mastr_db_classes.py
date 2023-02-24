from geoalchemy2 import Geometry
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    Integer,
    Sequence,
    String,
)
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class EgonMastrGeocoded(Base):
    __tablename__ = "egon_mastr_geocoded"
    __table_args__ = {"schema": "supply"}

    index = Column(
        Integer, Sequence("mastr_geocoded_seq"), primary_key=True, index=True
    )
    zip_and_municipality = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    altitude = Column(Float)
    geometry = Column(Geometry("POINT", 4326))


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
    municipality = Column(String, nullable=True)  # Gemeinde
    federal_state = Column(String(31), nullable=True)  # Bundesland
    site = Column(String, nullable=True)  # Standort
    zip_and_municipality = Column(String, nullable=True)

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
    voltage_level_inferred = Column(Boolean, nullable=True)

    geometry_geocoded = Column(Boolean)

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
    municipality = Column(String, nullable=True)  # Gemeinde
    federal_state = Column(String(31), nullable=True)  # Bundesland
    zip_and_municipality = Column(String, nullable=True)

    site_type = Column(String(17), nullable=True)  # Lage
    manufacturer_name = Column(String(100), nullable=True)  # Hersteller
    type_name = Column(String(100), nullable=True)  # Typenbezeichnung
    hub_height = Column(Float, nullable=True)  # Nabenhoehe
    rotor_diameter = Column(Float, nullable=True)  # Rotordurchmesser

    capacity = Column(Float, nullable=True)  # Nettonennleistung
    feedin_type = Column(String(47), nullable=True)  # Einspeisungsart
    voltage_level = Column(Integer, nullable=True)
    voltage_level_inferred = Column(Boolean, nullable=True)

    geometry_geocoded = Column(Boolean)

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
    municipality = Column(String, nullable=True)  # Gemeinde
    federal_state = Column(String(31), nullable=True)  # Bundesland
    zip_and_municipality = Column(String, nullable=True)

    technology = Column(String(45), nullable=True)  # Technologie
    fuel_name = Column(String(52), nullable=True)  # Hauptbrennstoff
    fuel_type = Column(String(19), nullable=True)  # Biomasseart

    capacity = Column(Float, nullable=True)  # Nettonennleistung
    th_capacity = Column(Float, nullable=True)  # ThermischeNutzleistung
    feedin_type = Column(String(47), nullable=True)  # Einspeisungsart
    voltage_level = Column(Integer, nullable=True)
    voltage_level_inferred = Column(Boolean, nullable=True)

    geometry_geocoded = Column(Boolean)

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
    municipality = Column(String, nullable=True)  # Gemeinde
    federal_state = Column(String(31), nullable=True)  # Bundesland
    zip_and_municipality = Column(String, nullable=True)

    plant_type = Column(String(39), nullable=True)  # ArtDerWasserkraftanlage
    water_origin = Column(String(20), nullable=True)  # ArtDesZuflusses

    capacity = Column(Float, nullable=True)  # Nettonennleistung
    feedin_type = Column(String(47), nullable=True)  # Einspeisungsart
    voltage_level = Column(Integer, nullable=True)
    voltage_level_inferred = Column(Boolean, nullable=True)

    geometry_geocoded = Column(Boolean)

    geom = Column(Geometry("POINT", 4326), index=True, nullable=True)
