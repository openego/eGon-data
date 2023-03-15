import datetime
import json

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

from egon.data import config, db
from egon.data.metadata import (
    context,
    generate_resource_fields_from_db_table,
    license_dedl,
    meta_metadata,
)

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
    main_fuel = Column(String(52), nullable=True)  # Hauptbrennstoff
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


class EgonPowerPlantsCombustion(Base):
    __tablename__ = "egon_power_plants_combustion"
    __table_args__ = {"schema": "supply"}

    id = Column(Integer, Sequence("pp_combustion_seq"), primary_key=True)
    bus_id = Column(Integer, nullable=True)  # Grid district id
    gens_id = Column(String, nullable=True)  # EinheitMastrNummer

    status = Column(String, nullable=True)  # EinheitBetriebsstatus
    commissioning_date = Column(DateTime, nullable=True)  # Inbetriebnahmedatum
    postcode = Column(String(5), nullable=True)  # Postleitzahl
    city = Column(String(50), nullable=True)  # Ort
    municipality = Column(String, nullable=True)  # Gemeinde
    federal_state = Column(String(31), nullable=True)  # Bundesland
    zip_and_municipality = Column(String, nullable=True)

    carrier = Column(String)  # Energietraeger
    main_fuel = Column(String)  # Hauptbrennstoff
    other_main_fuel = Column(String)  # WeitererHauptbrennstoff
    technology = Column(String)  # Technologie

    capacity = Column(Float, nullable=True)  # Nettonennleistung
    th_capacity = Column(Float, nullable=True)  # ThermischeNutzleistung
    feedin_type = Column(String(47), nullable=True)  # Einspeisungsart
    voltage_level = Column(Integer, nullable=True)
    voltage_level_inferred = Column(Boolean, nullable=True)

    geometry_geocoded = Column(Boolean)
    geom = Column(Geometry("POINT", 4326), index=True, nullable=True)


class EgonPowerPlantsGsgk(Base):
    __tablename__ = "egon_power_plants_gsgk"
    __table_args__ = {"schema": "supply"}

    id = Column(Integer, Sequence("pp_gsgk_seq"), primary_key=True)
    bus_id = Column(Integer, nullable=True)  # Grid district id
    gens_id = Column(String, nullable=True)  # EinheitMastrNummer

    status = Column(String, nullable=True)  # EinheitBetriebsstatus
    commissioning_date = Column(DateTime, nullable=True)  # Inbetriebnahmedatum
    postcode = Column(String(5), nullable=True)  # Postleitzahl
    city = Column(String(50), nullable=True)  # Ort
    municipality = Column(String, nullable=True)  # Gemeinde
    federal_state = Column(String(31), nullable=True)  # Bundesland
    zip_and_municipality = Column(String, nullable=True)

    carrier = Column(String)  # Energietraeger
    technology = Column(String)  # Technologie

    capacity = Column(Float, nullable=True)  # Nettonennleistung
    th_capacity = Column(Float, nullable=True)  # ThermischeNutzleistung
    feedin_type = Column(String(47), nullable=True)  # Einspeisungsart
    voltage_level = Column(Integer, nullable=True)
    voltage_level_inferred = Column(Boolean, nullable=True)

    geometry_geocoded = Column(Boolean)
    geom = Column(Geometry("POINT", 4326), index=True, nullable=True)


class EgonPowerPlantsNuclear(Base):
    __tablename__ = "egon_power_plants_nuclear"
    __table_args__ = {"schema": "supply"}

    id = Column(Integer, Sequence("pp_nuclear_seq"), primary_key=True)
    bus_id = Column(Integer, nullable=True)  # Grid district id
    gens_id = Column(String, nullable=True)  # EinheitMastrNummer

    status = Column(String, nullable=True)  # EinheitBetriebsstatus
    commissioning_date = Column(DateTime, nullable=True)  # Inbetriebnahmedatum
    postcode = Column(String(5), nullable=True)  # Postleitzahl
    city = Column(String(50), nullable=True)  # Ort
    municipality = Column(String, nullable=True)  # Gemeinde
    federal_state = Column(String(31), nullable=True)  # Bundesland
    zip_and_municipality = Column(String, nullable=True)

    carrier = Column(String)  # Energietraeger
    technology = Column(String)  # Technologie

    capacity = Column(Float, nullable=True)  # Nettonennleistung
    th_capacity = Column(Float, nullable=True)  # ThermischeNutzleistung
    feedin_type = Column(String(47), nullable=True)  # Einspeisungsart
    voltage_level = Column(Integer, nullable=True)
    voltage_level_inferred = Column(Boolean, nullable=True)

    geometry_geocoded = Column(Boolean)
    geom = Column(Geometry("POINT", 4326), index=True, nullable=True)


class EgonPowerPlantsStorage(Base):
    __tablename__ = "egon_power_plants_storage"
    __table_args__ = {"schema": "supply"}

    id = Column(Integer, Sequence("pp_storage_seq"), primary_key=True)
    bus_id = Column(Integer, nullable=True)  # Grid district id
    gens_id = Column(String, nullable=True)  # EinheitMastrNummer

    status = Column(String, nullable=True)  # EinheitBetriebsstatus
    commissioning_date = Column(DateTime, nullable=True)  # Inbetriebnahmedatum
    postcode = Column(String(5), nullable=True)  # Postleitzahl
    city = Column(String(50), nullable=True)  # Ort
    municipality = Column(String, nullable=True)  # Gemeinde
    federal_state = Column(String(31), nullable=True)  # Bundesland
    zip_and_municipality = Column(String, nullable=True)

    carrier = Column(String)  # Energietraeger
    technology = Column(String)  # Technologie
    battery_type = Column(String)  # Batterietechnologie
    pump_storage_type = Column(String)  # Pumpspeichertechnologie

    capacity = Column(Float, nullable=True)  # Nettonennleistung
    th_capacity = Column(Float, nullable=True)  # ThermischeNutzleistung
    feedin_type = Column(String(47), nullable=True)  # Einspeisungsart
    voltage_level = Column(Integer, nullable=True)
    voltage_level_inferred = Column(Boolean, nullable=True)

    geometry_geocoded = Column(Boolean)
    geom = Column(Geometry("POINT", 4326), index=True, nullable=True)


def add_metadata():
    technologies = config.datasets()["mastr_new"]["technologies"]

    target_tables = {
        "pv": EgonPowerPlantsPv,
        "wind": EgonPowerPlantsWind,
        "biomass": EgonPowerPlantsBiomass,
        "hydro": EgonPowerPlantsHydro,
        "combustion": EgonPowerPlantsCombustion,
        "gsgk": EgonPowerPlantsGsgk,
        "nuclear": EgonPowerPlantsNuclear,
        "storage": EgonPowerPlantsStorage,
    }

    for technology in technologies:
        target_table = target_tables[technology]

        meta = {
            "name": (
                f"{target_table.__table_args__['schema']}."
                f"{target_table.__tablename__}"
            ),
            "title": f"eGon {technology} power plants",
            "id": "WILL_BE_SET_AT_PUBLICATION",
            "description": (
                f"eGon {technology} power plants status quo derived from MaStR"
            ),
            "language": "en-US",
            "keywords": [technology, "mastr"],
            "publicationDate": datetime.date.today().isoformat(),
            "context": context(),
            "spatial": {
                "location": "none",
                "extent": "Germany",
                "resolution": "1 m",
            },
            "temporal": {
                "referenceDate": (
                    config.datasets()["mastr_new"]["egon2021_date_max"].split(
                        " "
                    )[0]
                ),
                "timeseries": {
                    "start": "",
                    "end": "",
                    "resolution": "",
                    "alignment": "",
                    "aggregationType": "",
                },
            },
            "sources": [
                {
                    "title": "Data bundle for egon-data",
                    "description": (
                        "Data bundle for egon-data: A transparent and "
                        "reproducible data processing pipeline for energy "
                        "system modeling"
                    ),
                    "path": (
                        "https://sandbox.zenodo.org/record/"
                        "1167119#.Y_dWM4CZMVM"
                    ),
                    "licenses": [license_dedl(attribution="© Cußmann, Ilka")],
                },
                {
                    "title": (
                        "open-MaStR power unit registry for eGo^n project"
                    ),
                    "description": (
                        "Data from Marktstammdatenregister (MaStR) data using "
                        "the data dump from 2022-11-17 for eGon-data."
                    ),
                    "path": "https://sandbox.zenodo.org/record/1132987",
                    "licenses": [license_dedl(attribution="© Amme, Jonathan")],
                },
            ],
            "licenses": [license_dedl(attribution="© eGon development team")],
            "contributors": [
                {
                    "title": "khelfen",
                    "email": "Kilian.Helfenbein@rl-institut.de",
                    "date": "2023-03-15",
                    "object": "metadata",
                    "comment": "Create metadata",
                }
            ],
            "resources": [
                {
                    "profile": "tabular-data-resource",
                    "name": (
                        f"{target_table.__table_args__['schema']}."
                        f"{target_table.__tablename__}"
                    ),
                    "path": "None",
                    "format": "PostgreSQL",
                    "encoding": "UTF-8",
                    "schema": {
                        "fields": generate_resource_fields_from_db_table(
                            target_table.__table_args__["schema"],
                            target_table.__tablename__,
                        ),
                        "primaryKey": "id",
                    },
                    "dialect": {"delimiter": "", "decimalSeparator": ""},
                }
            ],
            "review": {"path": "", "badge": ""},
            "metaMetadata": meta_metadata(),
            "_comment": {
                "metadata": (
                    "Metadata documentation and explanation (https://github."
                    "com/OpenEnergyPlatform/oemetadata/blob/master/metadata/"
                    "v141/metadata_key_description.md)"
                ),
                "dates": (
                    "Dates and time must follow the ISO8601 including time "
                    "zone (YYYY-MM-DD or YYYY-MM-DDThh:mm:ss±hh)"
                ),
                "units": "Use a space between numbers and units (100 m)",
                "languages": (
                    "Languages must follow the IETF (BCP47) format (en-GB, "
                    "en-US, de-DE)"
                ),
                "licenses": (
                    "License name must follow the SPDX License List "
                    "(https://spdx.org/licenses/)"
                ),
                "review": (
                    "Following the OEP Data Review (https://github.com/"
                    "OpenEnergyPlatform/data-preprocessing/wiki)"
                ),
                "none": "If not applicable use (none)",
            },
        }

        db.submit_comment(
            f"'{json.dumps(meta)}'",
            target_table.__table_args__["schema"],
            target_table.__tablename__,
        )
