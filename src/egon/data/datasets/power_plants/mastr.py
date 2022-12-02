"""Import MaStR dataset and write to DB tables

Data dump from Marktstammdatenregister (2022-11-17) is imported into the
database. Only some technologies are taken into account and written to the
following tables:

* PV: table `supply.egon_power_plants_pv`
* wind turbines: table `supply.egon_power_plants_wind`
* biomass/biogas plants: table `supply.egon_power_plants_biomass`
* hydro plants: table `supply.egon_power_plants_hydro`

Empty data in columns `voltage_level`, `bus_id` (all tables), and `plant_type`
(supply.egon_power_plants_hydro) is indicated by the value -1.

The data is used especially for the generation of status quo grids by ding0.
"""
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
import geopandas as gpd
import pandas as pd

from egon.data import db
from egon.data.datasets.mastr import WORKING_DIR_MASTR_NEW
import egon.data.config

Base = declarative_base()

TESTMODE_OFF = (
    egon.data.config.settings()["egon-data"]["--dataset-boundary"]
    == "Everything"
)


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

    plant_type = Column(String(39), nullable=True)  # ArtDerWasserkraftanlage
    water_origin = Column(String(20), nullable=True)  # ArtDesZuflusses

    capacity = Column(Float, nullable=True)  # Nettonennleistung
    feedin_type = Column(String(47), nullable=True)  # Einspeisungsart
    voltage_level = Column(Integer, nullable=True)

    geom = Column(Geometry("POINT", 4326), index=True, nullable=True)


def import_mastr() -> None:
    """Import MaStR data into database"""
    engine = db.engine()
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
            "zugeordneteWirkleistungWechselrichter": "capacity_inverter",
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
            "ArtDerWasserkraftanlage": "plant_type",
            "ArtDesZuflusses": "water_origin",
        },
    }

    source_files = {
        "pv": WORKING_DIR_MASTR_NEW / cfg["sources"]["mastr_pv"],
        "wind": WORKING_DIR_MASTR_NEW / cfg["sources"]["mastr_wind"],
        "biomass": WORKING_DIR_MASTR_NEW / cfg["sources"]["mastr_biomass"],
        "hydro": WORKING_DIR_MASTR_NEW / cfg["sources"]["mastr_hydro"],
    }
    target_tables = {
        "pv": EgonPowerPlantsPv,
        "wind": EgonPowerPlantsWind,
        "biomass": EgonPowerPlantsBiomass,
        "hydro": EgonPowerPlantsHydro,
    }
    vlevel_mapping = {
        "Höchstspannung": 1,
        "UmspannungZurHochspannung": 2,
        "Hochspannung": 3,
        "UmspannungZurMittelspannung": 4,
        "Mittelspannung": 5,
        "UmspannungZurNiederspannung": 6,
        "Niederspannung": 7,
    }

    # import locations
    locations = pd.read_csv(
        WORKING_DIR_MASTR_NEW / cfg["sources"]["mastr_location"],
        index_col=None,
    )

    # import grid districts
    mv_grid_districts = db.select_geodataframe(
        f"""
        SELECT * FROM {cfg['sources']['egon_mv_grid_district']}
        """,
        epsg=4326,
    )

    # import units
    technologies = ["pv", "wind", "biomass", "hydro"]
    for tech in technologies:
        # read units
        print(f"Importing MaStR dataset: {tech}:")
        print("  Reading CSV and filtering data...")
        units = pd.read_csv(
            source_files[tech],
            usecols=(
                ["LokationMastrNummer", "Laengengrad", "Breitengrad", "Land"]
                + list(cols_mapping["all"].keys())
                + list(cols_mapping[tech].keys())
            ),
            index_col=None,
            dtype={"Postleitzahl": str},
        ).rename(columns=cols_mapping)

        # drop units outside of Germany
        len_old = len(units)
        units = units.loc[units.Land == "Deutschland"]
        print(f"    {len_old-len(units)} units outside of Germany dropped...")

        # filter for SH units if in testmode
        if not TESTMODE_OFF:
            print(
                """    TESTMODE:
                Dropping all units outside of Schleswig-Holstein...
                """
            )
            units = units.loc[units.Bundesland == "SchleswigHolstein"]

        # merge and rename voltage level
        print("  Merging with locations and allocate voltage level...")
        units = units.merge(
            locations[["MaStRNummer", "Spannungsebene"]],
            left_on="LokationMastrNummer",
            right_on="MaStRNummer",
            how="left",
        )
        units["voltage_level"] = units.Spannungsebene.replace(vlevel_mapping)

        # add geometry
        print("  Adding geometries...")
        units = gpd.GeoDataFrame(
            units,
            geometry=gpd.points_from_xy(
                units["Laengengrad"], units["Breitengrad"], crs=4326
            ),
            crs=4326,
        )
        units_wo_geom = len(
            units.loc[(units.Laengengrad.isna() | units.Laengengrad.isna())]
        )
        print(
            f"    {units_wo_geom}/{len(units)} units do not have a geometry!"
        )

        # drop unnecessary and rename columns
        print("  Reformatting...")
        units.drop(
            columns=[
                "LokationMastrNummer",
                "MaStRNummer",
                "Laengengrad",
                "Breitengrad",
                "Spannungsebene",
                "Land",
            ],
            inplace=True,
        )
        mapping = cols_mapping["all"].copy()
        mapping.update(cols_mapping[tech])
        mapping.update({"geometry": "geom"})
        units.rename(columns=mapping, inplace=True)
        units["voltage_level"] = units.voltage_level.fillna(-1).astype(int)

        units.set_geometry("geom", inplace=True)
        units["id"] = range(0, len(units))

        # assign bus ids
        print("  Assigning bus ids...")
        units = (
            units.loc[~units.geom.x.isna()]
            .sjoin(mv_grid_districts[["bus_id", "geom"]], how="left")
            .drop(columns=["index_right"])
        )
        units["bus_id"] = units.bus_id.fillna(-1).astype(int)

        # write to DB
        print("  Writing to DB...")
        units.to_postgis(
            name=target_tables[tech].__tablename__,
            con=engine,
            if_exists="append",
            schema=target_tables[tech].__table_args__["schema"],
        )
