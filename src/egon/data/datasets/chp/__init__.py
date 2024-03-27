"""
The central module containing all code dealing with combined heat and power
(CHP) plants.
"""

from pathlib import Path

from geoalchemy2 import Geometry
from shapely.ops import nearest_points
from sqlalchemy import Boolean, Column, Float, Integer, Sequence, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import geopandas as gpd
import pandas as pd
import pypsa

from egon.data import config, db
from egon.data.datasets import Dataset, wrapped_partial
from egon.data.datasets.chp.match_nep import insert_large_chp, map_carrier
from egon.data.datasets.chp.small_chp import (
    assign_use_case,
    existing_chp_smaller_10mw,
    extension_per_federal_state,
    extension_to_areas,
    select_target,
)
from egon.data.datasets.mastr import (
    WORKING_DIR_MASTR_OLD,
    WORKING_DIR_MASTR_NEW,
)
from egon.data.datasets.power_plants import (
    assign_bus_id,
    assign_voltage_level,
    filter_mastr_geometry,
    scale_prox2now,
)

Base = declarative_base()


class EgonChp(Base):
    __tablename__ = "egon_chp_plants"
    __table_args__ = {"schema": "supply"}
    id = Column(Integer, Sequence("chp_seq"), primary_key=True)
    sources = Column(JSONB)
    source_id = Column(JSONB)
    carrier = Column(String)
    district_heating = Column(Boolean)
    el_capacity = Column(Float)
    th_capacity = Column(Float)
    electrical_bus_id = Column(Integer)
    district_heating_area_id = Column(Integer)
    ch4_bus_id = Column(Integer)
    voltage_level = Column(Integer)
    scenario = Column(String)
    geom = Column(Geometry("POINT", 4326))


class EgonMaStRConventinalWithoutChp(Base):
    __tablename__ = "egon_mastr_conventional_without_chp"
    __table_args__ = {"schema": "supply"}
    id = Column(Integer, Sequence("mastr_conventional_seq"), primary_key=True)
    EinheitMastrNummer = Column(String)
    carrier = Column(String)
    el_capacity = Column(Float)
    plz = Column(Integer)
    city = Column(String)
    federal_state = Column(String)
    geometry = Column(Geometry("POINT", 4326))


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
    EgonMaStRConventinalWithoutChp.__table__.drop(bind=engine, checkfirst=True)
    EgonMaStRConventinalWithoutChp.__table__.create(
        bind=engine, checkfirst=True
    )


def nearest(
    row,
    df,
    centroid=False,
    row_geom_col="geometry",
    df_geom_col="geometry",
    src_column=None,
):
    """
    Finds the nearest point and returns the specified column values

    Parameters
    ----------
    row : pandas.Series
        Data to which the nearest data of df is assigned.
    df : pandas.DataFrame
        Data which includes all options for the nearest neighbor alogrithm.
    centroid : boolean
        Use centroid geoemtry. The default is False.
    row_geom_col : str, optional
        Name of row's geometry column. The default is 'geometry'.
    df_geom_col : str, optional
        Name of df's geometry column. The default is 'geometry'.
    src_column : str, optional
        Name of returned df column. The default is None.

    Returns
    -------
    value : pandas.Series
        Values of specified column of df

    """

    if centroid:
        unary_union = df.centroid.unary_union
    else:
        unary_union = df[df_geom_col].unary_union
    # Find the geometry that is closest
    nearest = (
        df[df_geom_col] == nearest_points(row[row_geom_col], unary_union)[1]
    )

    # Get the corresponding value from df (matching is based on the geometry)
    value = df[nearest][src_column].values[0]

    return value


def assign_heat_bus():
    """Selects heat_bus for chps used in district heating.

    Parameters
    ----------
    scenario : str, optional
        Name of the corresponding scenario. The default is 'eGon2035'.

    Returns
    -------
    None.

    """
    sources = config.datasets()["chp_location"]["sources"]
    target = config.datasets()["chp_location"]["targets"]["chp_table"]

    for scenario in config.settings()["egon-data"]["--scenarios"]:
        # Select CHP with use_case = 'district_heating'
        chp = db.select_geodataframe(
            f"""
            SELECT * FROM
            {target['schema']}.{target['table']}
            WHERE scenario = '{scenario}'
            AND district_heating = True
            """,
            index_col="id",
            epsg=4326,
        )

        # Select district heating areas and their centroid
        district_heating = db.select_geodataframe(
            f"""
            SELECT area_id, ST_Centroid(geom_polygon) as geom
            FROM
            {sources['district_heating_areas']['schema']}.
            {sources['district_heating_areas']['table']}
            WHERE scenario = '{scenario}'
            """,
            epsg=4326,
        )

        # Assign district heating area_id to district_heating_chp
        # According to nearest centroid of district heating area
        chp["district_heating_area_id"] = chp.apply(
            nearest,
            df=district_heating,
            row_geom_col="geom",
            df_geom_col="geom",
            centroid=True,
            src_column="area_id",
            axis=1,
        )

        # Drop district heating CHP without heat_bus_id
        db.execute_sql(
            f"""
            DELETE FROM {target['schema']}.{target['table']}
            WHERE scenario = '{scenario}'
            AND district_heating = True
            """
        )

        # Insert district heating CHP with heat_bus_id
        session = sessionmaker(bind=db.engine())()
        for i, row in chp.iterrows():
            if row.carrier != "biomass":
                entry = EgonChp(
                    id=i,
                    sources=row.sources,
                    source_id=row.source_id,
                    carrier=row.carrier,
                    el_capacity=row.el_capacity,
                    th_capacity=row.th_capacity,
                    electrical_bus_id=row.electrical_bus_id,
                    ch4_bus_id=row.ch4_bus_id,
                    district_heating_area_id=row.district_heating_area_id,
                    district_heating=row.district_heating,
                    voltage_level=row.voltage_level,
                    scenario=scenario,
                    geom=f"SRID=4326;POINT({row.geom.x} {row.geom.y})",
                )
            else:
                entry = EgonChp(
                    id=i,
                    sources=row.sources,
                    source_id=row.source_id,
                    carrier=row.carrier,
                    el_capacity=row.el_capacity,
                    th_capacity=row.th_capacity,
                    electrical_bus_id=row.electrical_bus_id,
                    district_heating_area_id=row.district_heating_area_id,
                    district_heating=row.district_heating,
                    voltage_level=row.voltage_level,
                    scenario=scenario,
                    geom=f"SRID=4326;POINT({row.geom.x} {row.geom.y})",
                )
            session.add(entry)
        session.commit()


def insert_biomass_chp(scenario):
    """Insert biomass chp plants of future scenario

    Parameters
    ----------
    scenario : str
        Name of scenario.

    Returns
    -------
    None.

    """
    cfg = config.datasets()["chp_location"]

    # import target values from NEP 2021, scneario C 2035
    target = select_target("biomass", scenario)

    # import data for MaStR
    mastr = pd.read_csv(
        WORKING_DIR_MASTR_OLD / cfg["sources"]["mastr_biomass"]
    ).query("EinheitBetriebsstatus=='InBetrieb'")

    # Drop entries without federal state or 'AusschließlichWirtschaftszone'
    mastr = mastr[
        mastr.Bundesland.isin(
            pd.read_sql(
                f"""SELECT DISTINCT ON (gen)
        REPLACE(REPLACE(gen, '-', ''), 'ü', 'ue') as states
        FROM {cfg['sources']['vg250_lan']['schema']}.
        {cfg['sources']['vg250_lan']['table']}""",
                con=db.engine(),
            ).states.values
        )
    ]

    # Scaling will be done per federal state in case of eGon2035 scenario.
    if scenario == "eGon2035":
        level = "federal_state"
    else:
        level = "country"
    # Choose only entries with valid geometries inside DE/test mode
    mastr_loc = filter_mastr_geometry(mastr).set_geometry("geometry")

    # Scale capacities to meet target values
    mastr_loc = scale_prox2now(mastr_loc, target, level=level)

    # Assign bus_id
    if len(mastr_loc) > 0:
        mastr_loc["voltage_level"] = assign_voltage_level(
            mastr_loc, cfg, WORKING_DIR_MASTR_OLD
        )
        mastr_loc = assign_bus_id(mastr_loc, cfg)
    mastr_loc = assign_use_case(mastr_loc, cfg["sources"], scenario)

    # Insert entries with location
    session = sessionmaker(bind=db.engine())()
    for i, row in mastr_loc.iterrows():
        if row.ThermischeNutzleistung > 0:
            entry = EgonChp(
                sources={
                    "chp": "MaStR",
                    "el_capacity": "MaStR scaled with NEP 2021",
                    "th_capacity": "MaStR",
                },
                source_id={"MastrNummer": row.EinheitMastrNummer},
                carrier="biomass",
                el_capacity=row.Nettonennleistung,
                th_capacity=row.ThermischeNutzleistung / 1000,
                scenario=scenario,
                district_heating=row.district_heating,
                electrical_bus_id=row.bus_id,
                voltage_level=row.voltage_level,
                geom=f"SRID=4326;POINT({row.Laengengrad} {row.Breitengrad})",
            )
            session.add(entry)
    session.commit()


def insert_chp_statusquo(scn="status2019"):
    cfg = config.datasets()["chp_location"]

    # import data for MaStR
    mastr = pd.read_csv(
        WORKING_DIR_MASTR_NEW / "bnetza_mastr_combustion_cleaned.csv"
    )

    mastr_biomass = pd.read_csv(
        WORKING_DIR_MASTR_NEW / "bnetza_mastr_biomass_cleaned.csv"
    )

    mastr = pd.concat([mastr, mastr_biomass]).reset_index(drop=True)

    mastr = mastr.loc[mastr.ThermischeNutzleistung > 0]

    mastr = mastr.loc[
        mastr.Energietraeger.isin(
            [
                "Erdgas",
                "Mineralölprodukte",
                "andere Gase",
                "nicht biogener Abfall",
                "Braunkohle",
                "Steinkohle",
                "Biomasse",
            ]
        )
    ]

    mastr.Inbetriebnahmedatum = pd.to_datetime(mastr.Inbetriebnahmedatum)
    mastr.DatumEndgueltigeStilllegung = pd.to_datetime(
        mastr.DatumEndgueltigeStilllegung
    )
    mastr = mastr.loc[
        mastr.Inbetriebnahmedatum
        <= config.datasets()["mastr_new"][f"{scn}_date_max"]
    ]

    mastr = mastr.loc[
        (
            mastr.DatumEndgueltigeStilllegung
            >= config.datasets()["mastr_new"][f"{scn}_date_max"]
        )
        | (mastr.DatumEndgueltigeStilllegung.isnull())
    ]

    mastr.groupby("Energietraeger").Nettonennleistung.sum().mul(1e-6)

    geom_municipalities = db.select_geodataframe(
        """
        SELECT gen, ST_UNION(geometry) as geom
        FROM boundaries.vg250_gem
        GROUP BY gen
        """
    ).set_index("gen")

    # Assing Laengengrad and Breitengrad to chps without location data
    # based on the centroid of the municipaltiy
    idx_no_location = mastr[
        (mastr.Laengengrad.isnull())
        & (mastr.Gemeinde.isin(geom_municipalities.index))
    ].index

    mastr.loc[idx_no_location, "Laengengrad"] = (
        geom_municipalities.to_crs(epsg="4326").centroid.x.loc[
            mastr.Gemeinde[idx_no_location]
        ]
    ).values

    mastr.loc[idx_no_location, "Breitengrad"] = (
        geom_municipalities.to_crs(epsg="4326").centroid.y.loc[
            mastr.Gemeinde[idx_no_location]
        ]
    ).values

    if (
        config.settings()["egon-data"]["--dataset-boundary"]
        == "Schleswig-Holstein"
    ):
        dropped_capacity = mastr[
            (mastr.Laengengrad.isnull())
            & (mastr.Bundesland == "SchleswigHolstein")
        ].Nettonennleistung.sum()

    else:
        dropped_capacity = mastr[
            (mastr.Laengengrad.isnull())
        ].Nettonennleistung.sum()

    print(
        f"""
          CHPs with a total installed electrical capacity of {dropped_capacity} kW are dropped
          because of missing or wrong location data
          """
    )

    mastr = mastr[~mastr.Laengengrad.isnull()]
    mastr = filter_mastr_geometry(mastr).set_geometry("geometry")

    # Assign bus_id
    if len(mastr) > 0:
        mastr["voltage_level"] = assign_voltage_level(
            mastr, cfg, WORKING_DIR_MASTR_NEW
        )

        gas_bus_id = db.assign_gas_bus_id(mastr, scn, "CH4").bus

        mastr = assign_bus_id(mastr, cfg, drop_missing=True)

        mastr["gas_bus_id"] = gas_bus_id

    mastr = assign_use_case(mastr, cfg["sources"], scn)

    # Insert entries with location
    session = sessionmaker(bind=db.engine())()
    for i, row in mastr.iterrows():
        if row.ThermischeNutzleistung > 0:
            entry = EgonChp(
                sources={
                    "chp": "MaStR",
                    "el_capacity": "MaStR",
                    "th_capacity": "MaStR",
                },
                source_id={"MastrNummer": row.EinheitMastrNummer},
                carrier=map_carrier().loc[row.Energietraeger],
                el_capacity=row.Nettonennleistung / 1000,
                th_capacity=row.ThermischeNutzleistung / 1000,
                scenario=scn,
                district_heating=row.district_heating,
                electrical_bus_id=row.bus_id,
                ch4_bus_id=row.gas_bus_id,
                voltage_level=row.voltage_level,
                geom=f"SRID=4326;POINT({row.Laengengrad} {row.Breitengrad})",
            )
            session.add(entry)
    session.commit()


def insert_chp_egon2035():
    """Insert CHP plants for eGon2035 considering NEP and MaStR data

    Returns
    -------
    None.

    """

    sources = config.datasets()["chp_location"]["sources"]

    targets = config.datasets()["chp_location"]["targets"]

    insert_biomass_chp("eGon2035")

    # Insert large CHPs based on NEP's list of conventional power plants
    MaStR_konv = insert_large_chp(sources, targets["chp_table"], EgonChp)

    # Insert smaller CHPs (< 10MW) based on existing locations from MaStR
    existing_chp_smaller_10mw(sources, MaStR_konv, EgonChp)

    gpd.GeoDataFrame(
        MaStR_konv[
            [
                "EinheitMastrNummer",
                "el_capacity",
                "geometry",
                "carrier",
                "plz",
                "city",
                "federal_state",
            ]
        ]
    ).to_postgis(
        targets["mastr_conventional_without_chp"]["table"],
        schema=targets["mastr_conventional_without_chp"]["schema"],
        con=db.engine(),
        if_exists="replace",
    )


def extension_BW():
    extension_per_federal_state("BadenWuerttemberg", EgonChp)


def extension_BY():
    extension_per_federal_state("Bayern", EgonChp)


def extension_HB():
    extension_per_federal_state("Bremen", EgonChp)


def extension_BB():
    extension_per_federal_state("Brandenburg", EgonChp)


def extension_HH():
    extension_per_federal_state("Hamburg", EgonChp)


def extension_HE():
    extension_per_federal_state("Hessen", EgonChp)


def extension_MV():
    extension_per_federal_state("MecklenburgVorpommern", EgonChp)


def extension_NS():
    extension_per_federal_state("Niedersachsen", EgonChp)


def extension_NW():
    extension_per_federal_state("NordrheinWestfalen", EgonChp)


def extension_SN():
    extension_per_federal_state("Sachsen", EgonChp)


def extension_TH():
    extension_per_federal_state("Thueringen", EgonChp)


def extension_SL():
    extension_per_federal_state("Saarland", EgonChp)


def extension_ST():
    extension_per_federal_state("SachsenAnhalt", EgonChp)


def extension_RP():
    extension_per_federal_state("RheinlandPfalz", EgonChp)


def extension_BE():
    extension_per_federal_state("Berlin", EgonChp)


def extension_SH():
    extension_per_federal_state("SchleswigHolstein", EgonChp)


def insert_chp_egon100re():
    """Insert CHP plants for eGon100RE considering results from pypsa-eur-sec

    Returns
    -------
    None.

    """

    sources = config.datasets()["chp_location"]["sources"]

    db.execute_sql(
        f"""
        DELETE FROM {EgonChp.__table__.schema}.{EgonChp.__table__.name}
        WHERE scenario = 'eGon100RE'
        """
    )

    # select target values from pypsa-eur-sec
    additional_capacity = db.select_dataframe(
        """
        SELECT capacity
        FROM supply.egon_scenario_capacities
        WHERE scenario_name = 'eGon100RE'
        AND carrier = 'urban_central_gas_CHP'
        """
    ).capacity[0]

    if config.settings()["egon-data"]["--dataset-boundary"] != "Everything":
        additional_capacity /= 16
    target_file = (
        Path(".")
        / "data_bundle_egon_data"
        / "pypsa_eur_sec"
        / "2022-07-26-egondata-integration"
        / "postnetworks"
        / "elec_s_37_lv2.0__Co2L0-1H-T-H-B-I-dist1_2050.nc"
    )

    network = pypsa.Network(str(target_file))
    chp_index = "DE0 0 urban central gas CHP"

    standard_chp_th = 10
    standard_chp_el = (
        standard_chp_th
        * network.links.loc[chp_index, "efficiency"]
        / network.links.loc[chp_index, "efficiency2"]
    )

    areas = db.select_geodataframe(
        f"""
            SELECT
            residential_and_service_demand as demand, area_id,
            ST_Transform(ST_PointOnSurface(geom_polygon), 4326)  as geom
            FROM
            {sources['district_heating_areas']['schema']}.
            {sources['district_heating_areas']['table']}
            WHERE scenario = 'eGon100RE'
            """
    )

    existing_chp = pd.DataFrame(
        data={
            "el_capacity": standard_chp_el,
            "th_capacity": standard_chp_th,
            "voltage_level": 5,
        },
        index=range(1),
    )

    flh = (
        network.links_t.p0[chp_index].sum()
        / network.links.p_nom_opt[chp_index]
    )

    extension_to_areas(
        areas,
        additional_capacity,
        existing_chp,
        flh,
        EgonChp,
        district_heating=True,
        scenario="eGon100RE",
    )


tasks = (create_tables,)

insert_per_scenario = set()

if "status2019" in config.settings()["egon-data"]["--scenarios"]:
    insert_per_scenario.add(
        wrapped_partial(insert_chp_statusquo, scn="status2019", postfix="_2019")
    )

if "status2023" in config.settings()["egon-data"]["--scenarios"]:
    insert_per_scenario.add(
        wrapped_partial(insert_chp_statusquo, scn="status2023", postfix="_2023")
    )

if "eGon2035" in config.settings()["egon-data"]["--scenarios"]:
    insert_per_scenario.add(insert_chp_egon2035)

if "eGon100RE" in config.settings()["egon-data"]["--scenarios"]:
    insert_per_scenario.add(insert_chp_egon100re)

tasks = tasks + (insert_per_scenario, assign_heat_bus)

extension = set()

if "eGon2035" in config.settings()["egon-data"]["--scenarios"]:
    # Add one task per federal state for small CHP extension
    if (
        config.settings()["egon-data"]["--dataset-boundary"]
        == "Schleswig-Holstein"
    ):
        extension = extension_SH
    else:
        extension = {
            extension_BW,
            extension_BY,
            extension_HB,
            extension_BB,
            extension_HE,
            extension_MV,
            extension_NS,
            extension_NW,
            extension_SH,
            extension_HH,
            extension_RP,
            extension_SL,
            extension_SN,
            extension_ST,
            extension_TH,
            extension_BE,
        }

if extension != set():
    tasks = tasks + (extension,)


class Chp(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="Chp", version="0.0.8", dependencies=dependencies, tasks=tasks
        )
