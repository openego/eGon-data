"""Import MaStR dataset and write to DB tables

Data dump from Marktstammdatenregister (2022-11-17) is imported into the
database. Only some technologies are taken into account and written to the
following tables:

* PV: table `supply.egon_power_plants_pv`
* wind turbines: table `supply.egon_power_plants_wind`
* biomass/biogas plants: table `supply.egon_power_plants_biomass`
* hydro plants: table `supply.egon_power_plants_hydro`

Handling of empty source data in MaStr dump:
* `voltage_level`: inferred based on nominal power (`capacity`) using the
  ranges from
  https://redmine.iks.cs.ovgu.de/oe/projects/ego-n/wiki/Definition_of_thresholds_for_voltage_level_assignment
  which results in True in column `voltage_level_inferred`. Remaining datasets
  are set to -1 (which only occurs if `capacity` is empty).
* `supply.egon_power_plants_*.bus_id`: set to -1 (only if not within grid
  districts or no geom available, e.g. for units with nom. power <30 kW)
* `supply.egon_power_plants_hydro.plant_type`: NaN

The data is used especially for the generation of status quo grids by ding0.
"""
from __future__ import annotations

from pathlib import Path

from loguru import logger
import geopandas as gpd
import numpy as np
import pandas as pd

from egon.data import config, db
from egon.data.datasets.mastr import WORKING_DIR_MASTR_NEW
from egon.data.datasets.power_plants.mastr_db_classes import (
    add_metadata,
    EgonMastrGeocoded,
    EgonPowerPlantsBiomass,
    EgonPowerPlantsCombustion,
    EgonPowerPlantsGsgk,
    EgonPowerPlantsHydro,
    EgonPowerPlantsNuclear,
    EgonPowerPlantsPv,
    EgonPowerPlantsStorage,
    EgonPowerPlantsWind,
)
from egon.data.datasets.power_plants.pv_rooftop_buildings import (
    federal_state_data,
)

TESTMODE_OFF = (
    config.settings()["egon-data"]["--dataset-boundary"] == "Everything"
)


def isfloat(num: str):
    """
    Determine if string can be converted to float.
    Parameters
    -----------
    num : str
        String to parse.
    Returns
    -------
    bool
        Returns True in string can be parsed to float.
    """
    try:
        float(num)
        return True
    except ValueError:
        return False


def zip_and_municipality_from_standort(
    standort: str,
) -> tuple[str, bool]:
    """
    Get zip code and municipality from Standort string split into a list.
    Parameters
    -----------
    standort : str
        Standort as given from MaStR data.
    Returns
    -------
    str
        Standort with only the zip code and municipality
        as well a ', Germany' added.
    """
    standort_list = standort.split()

    found = False
    count = 0

    for count, elem in enumerate(standort_list):
        if len(elem) != 5:
            continue
        if not elem.isnumeric():
            continue

        found = True

        break

    if found:
        cleaned_str = " ".join(standort_list[count:])

        return cleaned_str, found

    logger.warning(
        "Couldn't identify zip code. This entry will be dropped."
        f" Original standort: {standort}."
    )

    return standort, found


def infer_voltage_level(
    units_gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """
    Infer nan values in voltage level derived from generator capacity to
    the power plants.

    Parameters
    -----------
    units_gdf : geopandas.GeoDataFrame
        GeoDataFrame containing units with voltage levels from MaStR
    Returnsunits_gdf: gpd.GeoDataFrame
    -------
    geopandas.GeoDataFrame
        GeoDataFrame containing units all having assigned a voltage level.
    """

    def voltage_levels(p: float) -> int:
        if p <= 100:
            return 7
        elif p <= 200:
            return 6
        elif p <= 5500:
            return 5
        elif p <= 20000:
            return 4
        elif p <= 120000:
            return 3
        return 1

    units_gdf["voltage_level_inferred"] = False
    mask = units_gdf.voltage_level.isna()
    units_gdf.loc[mask, "voltage_level_inferred"] = True
    units_gdf.loc[mask, "voltage_level"] = units_gdf.loc[
        mask
    ].Nettonennleistung.apply(voltage_levels)

    return units_gdf


def import_mastr() -> None:
    """Import MaStR data into database"""
    engine = db.engine()

    # import geocoded data
    cfg = config.datasets()["mastr_new"]
    path_parts = cfg["geocoding_path"]
    path = Path(*["."] + path_parts).resolve()
    path = list(path.iterdir())[0]

    deposit_id_geocoding = int(path.parts[-1].split(".")[0].split("_")[-1])
    deposit_id_mastr = cfg["deposit_id"]

    if deposit_id_geocoding != deposit_id_mastr:
        raise AssertionError(
            f"The zenodo (sandbox) deposit ID {deposit_id_mastr} for the MaStR"
            f" dataset is not matching with the geocoding version "
            f"{deposit_id_geocoding}. Make sure to hermonize the data. When "
            f"the MaStR dataset is updated also update the geocoding and "
            f"update the egon data bundle. The geocoding can be done using: "
            f"https://github.com/RLI-sandbox/mastr-geocoding"
        )

    geocoding_gdf = gpd.read_file(path)

    # remove failed requests
    geocoding_gdf = geocoding_gdf.loc[geocoding_gdf.geometry.is_valid]

    EgonMastrGeocoded.__table__.drop(bind=engine, checkfirst=True)
    EgonMastrGeocoded.__table__.create(bind=engine, checkfirst=True)

    geocoding_gdf.to_postgis(
        name=EgonMastrGeocoded.__tablename__,
        con=engine,
        if_exists="append",
        schema=EgonMastrGeocoded.__table_args__["schema"],
        index=True,
    )

    cfg = config.datasets()["power_plants"]

    cols_mapping = {
        "all": {
            "EinheitMastrNummer": "gens_id",
            "EinheitBetriebsstatus": "status",
            "Inbetriebnahmedatum": "commissioning_date",
            "Postleitzahl": "postcode",
            "Ort": "city",
            "Gemeinde": "municipality",
            "Bundesland": "federal_state",
            "Nettonennleistung": "capacity",
            "Einspeisungsart": "feedin_type",
        },
        "pv": {
            "Lage": "site_type",
            "Standort": "site",
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
            "Hauptbrennstoff": "main_fuel",
            "Biomasseart": "fuel_type",
            "ThermischeNutzleistung": "th_capacity",
        },
        "hydro": {
            "ArtDerWasserkraftanlage": "plant_type",
            "ArtDesZuflusses": "water_origin",
        },
        "combustion": {
            "Energietraeger": "carrier",
            "Hauptbrennstoff": "main_fuel",
            "WeitererHauptbrennstoff": "other_main_fuel",
            "Technologie": "technology",
            "ThermischeNutzleistung": "th_capacity",
        },
        "gsgk": {
            "Energietraeger": "carrier",
            "Technologie": "technology",
        },
        "nuclear": {
            "Energietraeger": "carrier",
            "Technologie": "technology",
        },
        "storage": {
            "Energietraeger": "carrier",
            "Technologie": "technology",
            "Batterietechnologie": "battery_type",
            "Pumpspeichertechnologie": "pump_storage_type",
        },
    }

    source_files = {
        "pv": WORKING_DIR_MASTR_NEW / cfg["sources"]["mastr_pv"],
        "wind": WORKING_DIR_MASTR_NEW / cfg["sources"]["mastr_wind"],
        "biomass": WORKING_DIR_MASTR_NEW / cfg["sources"]["mastr_biomass"],
        "hydro": WORKING_DIR_MASTR_NEW / cfg["sources"]["mastr_hydro"],
        "combustion": WORKING_DIR_MASTR_NEW
        / cfg["sources"]["mastr_combustion"],
        "gsgk": WORKING_DIR_MASTR_NEW / cfg["sources"]["mastr_gsgk"],
        "nuclear": WORKING_DIR_MASTR_NEW / cfg["sources"]["mastr_nuclear"],
        "storage": WORKING_DIR_MASTR_NEW / cfg["sources"]["mastr_storage"],
    }

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
    technologies = [
        "pv",
        "wind",
        "biomass",
        "hydro",
        "combustion",
        "gsgk",
        "nuclear",
        "storage",
    ]

    for tech in technologies:
        # read units
        logger.info(f"===== Importing MaStR dataset: {tech} =====")
        logger.debug("Reading CSV and filtering data...")
        units = pd.read_csv(
            source_files[tech],
            usecols=(
                ["LokationMastrNummer", "Laengengrad", "Breitengrad", "Land"]
                + list(cols_mapping["all"].keys())
                + list(cols_mapping[tech].keys())
            ),
            index_col=None,
            dtype={"Postleitzahl": str},
            low_memory=False,
        ).rename(columns=cols_mapping)

        # drop units outside of Germany
        len_old = len(units)
        units = units.loc[units.Land == "Deutschland"]
        logger.debug(
            f"{len_old - len(units)} units outside of Germany dropped..."
        )

        # get boundary
        boundary = (
            federal_state_data(geocoding_gdf.crs).dissolve().at[0, "geom"]
        )

        # drop units installed after reference date from cfg
        # (eGon2021 scenario)
        len_old = len(units)
        ts = pd.Timestamp(
            config.datasets()["mastr_new"]["status2023_date_max"]
        )
        units = units.loc[pd.to_datetime(units.Inbetriebnahmedatum) <= ts]
        logger.debug(
            f"{len_old - len(units)} units installed after {ts} dropped..."
        )

        # drop not operating units
        len_old = len(units)
        units = units.loc[
            units.EinheitBetriebsstatus.isin(
                ["InBetrieb", "VoruebergehendStillgelegt"]
            )
        ]
        logger.debug(f"{len_old - len(units)} not operating units dropped...")

        # filter for SH units if in testmode
        if not TESTMODE_OFF:
            logger.info(
                "TESTMODE: Dropping all units outside of Schleswig-Holstein..."
            )
            units = units.loc[units.Bundesland == "SchleswigHolstein"]

        # merge and rename voltage level
        logger.debug("Merging with locations and allocate voltage level...")
        units = units.merge(
            locations[["MaStRNummer", "Spannungsebene"]],
            left_on="LokationMastrNummer",
            right_on="MaStRNummer",
            how="left",
        )
        # convert voltage levels to numbers
        units["voltage_level"] = units.Spannungsebene.replace(vlevel_mapping)
        # set voltage level for nan values
        units = infer_voltage_level(units)

        # add geometry
        logger.debug("Adding geometries...")
        units = gpd.GeoDataFrame(
            units,
            geometry=gpd.points_from_xy(
                units["Laengengrad"], units["Breitengrad"], crs=4326
            ),
            crs=4326,
        )

        units["geometry_geocoded"] = (
            units.Laengengrad.isna() | units.Laengengrad.isna()
        )

        units.loc[~units.geometry_geocoded, "geometry_geocoded"] = ~units.loc[
            ~units.geometry_geocoded, "geometry"
        ].is_valid

        units_wo_geom = units["geometry_geocoded"].sum()

        logger.debug(
            f"{units_wo_geom}/{len(units)} units do not have a geometry!"
            " Adding geocoding results."
        )

        # determine zip and municipality string
        mask = (
            units.Postleitzahl.apply(isfloat)
            & ~units.Postleitzahl.isna()
            & ~units.Gemeinde.isna()
        )
        units["zip_and_municipality"] = np.nan
        ok_units = units.loc[mask]

        units.loc[mask, "zip_and_municipality"] = (
            ok_units.Postleitzahl.astype(int).astype(str).str.zfill(5)
            + " "
            + ok_units.Gemeinde.astype(str).str.rstrip().str.lstrip()
            + ", Deutschland"
        )

        # get zip and municipality from Standort
        parse_df = units.loc[~mask]

        if not parse_df.empty and "Standort" in parse_df.columns:
            init_len = len(parse_df)

            logger.info(
                f"Parsing ZIP code and municipality from Standort for "
                f"{init_len} values for {tech}."
            )

            parse_df[["zip_and_municipality", "drop_this"]] = (
                parse_df.Standort.astype(str)
                .apply(zip_and_municipality_from_standort)
                .tolist()
            )

            parse_df = parse_df.loc[parse_df.drop_this]

            if not parse_df.empty:
                units.loc[
                    parse_df.index, "zip_and_municipality"
                ] = parse_df.zip_and_municipality

        # add geocoding to missing
        units = units.merge(
            right=geocoding_gdf[["zip_and_municipality", "geometry"]].rename(
                columns={"geometry": "temp"}
            ),
            how="left",
            on="zip_and_municipality",
        )

        units.loc[units.geometry_geocoded, "geometry"] = units.loc[
            units.geometry_geocoded, "temp"
        ]

        init_len = len(units)

        logger.info(
            "Dropping units outside boundary by geometry or without geometry"
            "..."
        )

        units.dropna(subset=["geometry"], inplace=True)

        units = units.loc[units.geometry.within(boundary)]

        if init_len > 0:
            logger.debug(
                f"{init_len - len(units)}/{init_len} "
                f"({((init_len - len(units)) / init_len) * 100: g} %) dropped."
            )

        # drop unnecessary and rename columns
        logger.debug("Reformatting...")
        units.drop(
            columns=[
                "LokationMastrNummer",
                "MaStRNummer",
                "Laengengrad",
                "Breitengrad",
                "Spannungsebene",
                "Land",
                "temp",
            ],
            inplace=True,
        )
        mapping = cols_mapping["all"].copy()
        mapping.update(cols_mapping[tech])
        mapping.update({"geometry": "geom"})
        units.rename(columns=mapping, inplace=True)
        units["voltage_level"] = units.voltage_level.fillna(-1).astype(int)

        units.set_geometry("geom", inplace=True)
        units["id"] = range(len(units))

        # change capacity unit: kW to MW
        units["capacity"] = units["capacity"] / 1e3
        if "capacity_inverter" in units.columns:
            units["capacity_inverter"] = units["capacity_inverter"] / 1e3
        if "th_capacity" in units.columns:
            units["th_capacity"] = units["th_capacity"] / 1e3

        # assign bus ids
        logger.debug("Assigning bus ids...")
        units = units.assign(
            bus_id=units.loc[~units.geom.x.isna()]
            .sjoin(mv_grid_districts[["bus_id", "geom"]], how="left")
            .drop(columns=["index_right"])
            .bus_id
        )
        units["bus_id"] = units.bus_id.fillna(-1).astype(int)

        # write to DB
        logger.info(f"Writing {len(units)} units to DB...")

        units.to_postgis(
            name=target_tables[tech].__tablename__,
            con=engine,
            if_exists="append",
            schema=target_tables[tech].__table_args__["schema"],
        )

    add_metadata()
