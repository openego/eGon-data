"""The central module containing all code dealing with importing data from
Netzentwicklungsplan 2035, Version 2031, Szenario C
"""

from pathlib import Path

from sqlalchemy import Column, Float, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import numpy as np
import pandas as pd
import yaml

from egon.data import db
from egon.data.config import settings
from egon.data.datasets import Dataset
import egon.data.config

# will be later imported from another file
Base = declarative_base()


class EgonScenarioCapacities(Base):
    __tablename__ = "egon_scenario_capacities"
    __table_args__ = {"schema": "supply"}
    index = Column(Integer, primary_key=True)
    component = Column(String(25))
    carrier = Column(String(50))
    capacity = Column(Float)
    nuts = Column(String(12))
    scenario_name = Column(String(50))


class NEP2021ConvPowerPlants(Base):
    __tablename__ = "egon_nep_2021_conventional_powerplants"
    __table_args__ = {"schema": "supply"}
    index = Column(String(50), primary_key=True)
    bnetza_id = Column(String(50))
    name = Column(String(100))
    name_unit = Column(String(50))
    carrier_nep = Column(String(50))
    carrier = Column(String(12))
    chp = Column(String(12))
    postcode = Column(String(12))
    city = Column(String(50))
    federal_state = Column(String(12))
    commissioned = Column(String(12))
    status = Column(String(50))
    capacity = Column(Float)
    a2035_chp = Column(String(12))
    a2035_capacity = Column(Float)
    b2035_chp = Column(String(12))
    b2035_capacity = Column(Float)
    c2035_chp = Column(String(12))
    c2035_capacity = Column(Float)
    b2040_chp = Column(String(12))
    b2040_capacity = Column(Float)


def create_table():
    """Create input tables for scenario setup

    Returns
    -------
    None.

    """

    engine = db.engine()
    db.execute_sql("CREATE SCHEMA IF NOT EXISTS supply;")
    EgonScenarioCapacities.__table__.drop(bind=engine, checkfirst=True)
    NEP2021ConvPowerPlants.__table__.drop(bind=engine, checkfirst=True)
    EgonScenarioCapacities.__table__.create(bind=engine, checkfirst=True)
    NEP2021ConvPowerPlants.__table__.create(bind=engine, checkfirst=True)


def nuts_mapping():
    nuts_mapping = {
        "BW": "DE1",
        "NW": "DEA",
        "HE": "DE7",
        "BB": "DE4",
        "HB": "DE5",
        "RP": "DEB",
        "ST": "DEE",
        "SH": "DEF",
        "MV": "DE8",
        "TH": "DEG",
        "NI": "DE9",
        "SN": "DED",
        "HH": "DE6",
        "SL": "DEC",
        "BE": "DE3",
        "BY": "DE2",
    }

    return nuts_mapping


def insert_capacities_status2019():
    """Insert capacity of rural heat pumps for status2019

    Returns
    -------
    None.

    """

    targets = egon.data.config.datasets()["scenario_input"]["targets"]

    # Delete rows if already exist
    db.execute_sql(
        f"""
        DELETE FROM
        {targets['scenario_capacities']['schema']}.
        {targets['scenario_capacities']['table']}
        WHERE scenario_name = 'status2019'
        """
    )

    # Rural heat capacity for 2019 according to NEP 2035, version 2021
    rural_heat_capacity = 1e6 * 5e-3

    if settings()["egon-data"]["--dataset-boundary"] != "Everything":
        rural_heat_capacity *= population_share()

    db.execute_sql(
        f"""
        INSERT INTO 
        {targets['scenario_capacities']['schema']}.
        {targets['scenario_capacities']['table']}
        (component, carrier, capacity, nuts, scenario_name)
        VALUES (
            'link',
            'residential_rural_heat_pump',
            {rural_heat_capacity},
            'DE',
            'status2019'            
            )
        """
    )

    # Include small storages for scenario2019
    small_storages = 600  # MW for Germany

    db.execute_sql(
        f"""
        INSERT INTO 
        {targets['scenario_capacities']['schema']}.
        {targets['scenario_capacities']['table']}
        (component, carrier, capacity, nuts, scenario_name)
        VALUES (
            'storage_units',
            'battery',
            {small_storages},
            'DE',
            'status2019'         
            )
        """
    )


def insert_capacities_per_federal_state_nep():
    """Inserts installed capacities per federal state accordning to
    NEP 2035 (version 2021), scenario 2035 C

    Returns
    -------
    None.

    """

    sources = egon.data.config.datasets()["scenario_input"]["sources"]
    targets = egon.data.config.datasets()["scenario_input"]["targets"]

    # Connect to local database
    engine = db.engine()

    # Delete rows if already exist
    db.execute_sql(
        f"""
        DELETE FROM
        {targets['scenario_capacities']['schema']}.
        {targets['scenario_capacities']['table']}
        WHERE scenario_name = 'eGon2035'
        AND nuts != 'DE'
        """
    )

    # read-in installed capacities per federal state of germany
    target_file = (
        Path(".")
        / "data_bundle_egon_data"
        / "nep2035_version2021"
        / sources["eGon2035"]["capacities"]
    )

    df = pd.read_excel(
        target_file,
        sheet_name="1.Entwurf_NEP2035_V2021",
        index_col="Unnamed: 0",
    )

    df_draft = pd.read_excel(
        target_file,
        sheet_name="Entwurf_des_Szenariorahmens",
        index_col="Unnamed: 0",
    )

    # Import data on wind offshore capacities
    df_windoff = pd.read_excel(
        target_file,
        sheet_name="WInd_Offshore_NEP",
    ).dropna(subset=["Bundesland", "Netzverknuepfungspunkt"])

    # Remove trailing whitespace from column Bundesland
    df_windoff["Bundesland"] = df_windoff["Bundesland"].str.strip()

    # Group and sum capacities per federal state
    df_windoff_fs = (
        df_windoff[["Bundesland", "C 2035"]].groupby(["Bundesland"]).sum()
    )

    # List federal state with an assigned wind offshore capacity
    index_list = list(df_windoff_fs.index.values)

    # Overwrite capacities in df_windoff with more accurate values from
    # df_windoff_fs

    for state in index_list:
        df.at["Wind offshore", state] = (
            df_windoff_fs.at[state, "C 2035"] / 1000
        )

    # sort NEP-carriers:
    rename_carrier = {
        "Wind onshore": "wind_onshore",
        "Wind offshore": "wind_offshore",
        "sonstige Konventionelle": "others",
        "Speicherwasser": "reservoir",
        "Laufwasser": "run_of_river",
        "Biomasse": "biomass",
        "Erdgas": "gas",
        "Kuppelgas": "gas",
        "PV (Aufdach)": "solar_rooftop",
        "PV (Freiflaeche)": "solar",
        "Pumpspeicher": "pumped_hydro",
        "sonstige EE": "others",
        "Oel": "oil",
        "Haushaltswaermepumpen": "residential_rural_heat_pump",
        "KWK < 10 MW": "small_chp",
    }
    # 'Elektromobilitaet gesamt': 'transport',
    # 'Elektromobilitaet privat': 'transport'}

    # nuts1 to federal state in Germany
    map_nuts = pd.read_sql(
        f"""
        SELECT DISTINCT ON (nuts) gen, nuts
        FROM {sources['boundaries']['schema']}.{sources['boundaries']['table']}
        """,
        engine,
        index_col="gen",
    )

    insert_data = pd.DataFrame()

    scaled_carriers = [
        "Haushaltswaermepumpen",
        "PV (Aufdach)",
        "PV (Freiflaeche)",
    ]

    for bl in map_nuts.index:
        data = pd.DataFrame(df[bl])

        # if distribution to federal states is not provided,
        # use data from draft of scenario report
        for c in scaled_carriers:
            data.loc[c, bl] = (
                df_draft.loc[c, bl]
                / df_draft.loc[c, "Summe"]
                * df.loc[c, "Summe"]
            )

        # split hydro into run of river and reservoir
        # according to draft of scenario report
        if data.loc["Lauf- und Speicherwasser", bl] > 0:
            for c in ["Speicherwasser", "Laufwasser"]:
                data.loc[c, bl] = (
                    data.loc["Lauf- und Speicherwasser", bl]
                    * df_draft.loc[c, bl]
                    / df_draft.loc[["Speicherwasser", "Laufwasser"], bl].sum()
                )

        data["carrier"] = data.index.map(rename_carrier)
        data = data.groupby(data.carrier)[bl].sum().reset_index()
        data["component"] = "generator"
        data["nuts"] = map_nuts.nuts[bl]
        data["scenario_name"] = "eGon2035"

        # According to NEP, each heatpump has 5kW_el installed capacity
        # source: Entwurf des Szenariorahmens NEP 2035, version 2021, page 47
        data.loc[data.carrier == "residential_rural_heat_pump", bl] *= 5e-6
        data.loc[
            data.carrier == "residential_rural_heat_pump", "component"
        ] = "link"

        data = data.rename(columns={bl: "capacity"})

        # convert GW to MW
        data.capacity *= 1e3

        insert_data = pd.concat([insert_data, data])

    # Get aggregated capacities from nep's power plant list for certain carrier

    carriers = ["oil", "other_non_renewable", "pumped_hydro"]

    capacities_list = aggr_nep_capacities(carriers)

    # Filter by carrier
    updated = insert_data[insert_data["carrier"].isin(carriers)]

    # Merge to replace capacities for carriers "oil", "other_non_renewable" and
    # "pumped_hydro"
    updated = (
        updated.merge(capacities_list, on=["carrier", "nuts"], how="left")
        .fillna(0)
        .drop(["capacity"], axis=1)
        .rename(columns={"c2035_capacity": "capacity"})
    )

    # Remove updated entries from df
    original = insert_data[~insert_data["carrier"].isin(carriers)]

    # Join dfs
    insert_data = pd.concat([original, updated])

    # Insert data to db
    insert_data.to_sql(
        targets["scenario_capacities"]["table"],
        engine,
        schema=targets["scenario_capacities"]["schema"],
        if_exists="append",
        index=insert_data.index,
    )

    # Add district heating data accordning to energy and full load hours
    district_heating_input()


def population_share():
    """Calulate share of population in testmode

    Returns
    -------
    float
        Share of population in testmode

    """

    sources = egon.data.config.datasets()["scenario_input"]["sources"]

    return (
        pd.read_sql(
            f"""
            SELECT SUM(population)
            FROM {sources['zensus_population']['schema']}.
            {sources['zensus_population']['table']}
            WHERE population>0
            """,
            con=db.engine(),
        )["sum"][0]
        / 80324282
    )


def aggr_nep_capacities(carriers):
    """Aggregates capacities from NEP power plants list by carrier and federal
    state

    Returns
    -------
    pandas.Dataframe
        Dataframe with capacities per federal state and carrier

    """
    # Get list of power plants from nep
    nep_capacities = insert_nep_list_powerplants(export=False)[
        ["federal_state", "carrier", "c2035_capacity"]
    ]

    # Sum up capacities per federal state and carrier
    capacities_list = (
        nep_capacities.groupby(["federal_state", "carrier"])["c2035_capacity"]
        .sum()
        .to_frame()
        .reset_index()
    )

    # Neglect entries with carriers not in argument
    capacities_list = capacities_list[capacities_list.carrier.isin(carriers)]

    # Include NUTS code
    capacities_list["nuts"] = capacities_list.federal_state.map(nuts_mapping())

    # Drop entries for foreign plants with nan values and federal_state column
    capacities_list = capacities_list.dropna(subset=["nuts"]).drop(
        columns=["federal_state"]
    )

    return capacities_list


def map_carrier():
    """Map carriers from NEP and Marktstammdatenregister to carriers from eGon

    Returns
    -------
    pandas.Series
        List of mapped carriers

    """
    return pd.Series(
        data={
            "Abfall": "others",
            "Erdgas": "gas",
            "Sonstige\nEnergieträger": "others",
            "Steinkohle": "coal",
            "Kuppelgase": "gas",
            "Mineralöl-\nprodukte": "oil",
            "Braunkohle": "lignite",
            "Waerme": "others",
            "Mineraloelprodukte": "oil",
            "Mineralölprodukte": "oil",
            "NichtBiogenerAbfall": "others",
            "nicht biogener Abfall": "others",
            "AndereGase": "gas",
            "andere Gase": "gas",
            "Sonstige_Energietraeger": "others",
            "Kernenergie": "nuclear",
            "Pumpspeicher": "pumped_hydro",
            "Mineralöl-\nProdukte": "oil",
            "Biomasse": "biomass",
        }
    )


def insert_nep_list_powerplants(export=True):
    """Insert list of conventional powerplants attached to the approval
    of the scenario report by BNetzA

    Parameters
    ----------
    export : bool
        Choose if nep list should be exported to the data
        base. The default is True.
        If export=False a data frame will be returned

    Returns
    -------
    kw_liste_nep : pandas.DataFrame
        List of conventional power plants from nep if export=False
    """

    sources = egon.data.config.datasets()["scenario_input"]["sources"]
    targets = egon.data.config.datasets()["scenario_input"]["targets"]

    # Connect to local database
    engine = db.engine()

    # Read-in data from csv-file
    target_file = (
        Path(".")
        / "data_bundle_egon_data"
        / "nep2035_version2021"
        / sources["eGon2035"]["list_conv_pp"]
    )

    kw_liste_nep = pd.read_csv(target_file, delimiter=";", decimal=",")

    # Adjust column names
    kw_liste_nep = kw_liste_nep.rename(
        columns={
            "BNetzA-ID": "bnetza_id",
            "Kraftwerksname": "name",
            "Blockname": "name_unit",
            "Energieträger": "carrier_nep",
            "KWK\nJa/Nein": "chp",
            "PLZ": "postcode",
            "Ort": "city",
            "Bundesland/\nLand": "federal_state",
            "Inbetrieb-\nnahmejahr": "commissioned",
            "Status": "status",
            "el. Leistung\n06.02.2020": "capacity",
            "A 2035:\nKWK-Ersatz": "a2035_chp",
            "A 2035:\nLeistung": "a2035_capacity",
            "B 2035\nKWK-Ersatz": "b2035_chp",
            "B 2035:\nLeistung": "b2035_capacity",
            "C 2035:\nKWK-Ersatz": "c2035_chp",
            "C 2035:\nLeistung": "c2035_capacity",
            "B 2040:\nKWK-Ersatz": "b2040_chp",
            "B 2040:\nLeistung": "b2040_capacity",
        }
    )

    # Cut data to federal state if in testmode
    boundary = settings()["egon-data"]["--dataset-boundary"]
    if boundary != "Everything":
        map_states = {
            "Baden-Württemberg": "BW",
            "Nordrhein-Westfalen": "NW",
            "Hessen": "HE",
            "Brandenburg": "BB",
            "Bremen": "HB",
            "Rheinland-Pfalz": "RP",
            "Sachsen-Anhalt": "ST",
            "Schleswig-Holstein": "SH",
            "Mecklenburg-Vorpommern": "MV",
            "Thüringen": "TH",
            "Niedersachsen": "NI",
            "Sachsen": "SN",
            "Hamburg": "HH",
            "Saarland": "SL",
            "Berlin": "BE",
            "Bayern": "BY",
        }

        kw_liste_nep = kw_liste_nep[
            kw_liste_nep.federal_state.isin([map_states[boundary], np.nan])
        ]

        for col in [
            "capacity",
            "a2035_capacity",
            "b2035_capacity",
            "c2035_capacity",
            "b2040_capacity",
        ]:
            kw_liste_nep.loc[
                kw_liste_nep[kw_liste_nep.federal_state.isnull()].index, col
            ] *= population_share()

    kw_liste_nep["carrier"] = map_carrier()[kw_liste_nep.carrier_nep].values

    if export is True:
        # Insert data to db
        kw_liste_nep.to_sql(
            targets["nep_conventional_powerplants"]["table"],
            engine,
            schema=targets["nep_conventional_powerplants"]["schema"],
            if_exists="replace",
        )
    else:
        return kw_liste_nep


def district_heating_input():
    """Imports data for district heating networks in Germany

    Returns
    -------
    None.

    """

    sources = egon.data.config.datasets()["scenario_input"]["sources"]

    # import data to dataframe
    file = (
        Path(".")
        / "data_bundle_egon_data"
        / "nep2035_version2021"
        / sources["eGon2035"]["capacities"]
    )
    df = pd.read_excel(
        file, sheet_name="Kurzstudie_KWK", dtype={"Wert": float}
    )
    df.set_index(["Energietraeger", "Name"], inplace=True)

    # Scale values to population share in testmode
    if settings()["egon-data"]["--dataset-boundary"] != "Everything":
        df.loc[
            pd.IndexSlice[:, "Fernwaermeerzeugung"], "Wert"
        ] *= population_share()

    # Connect to database
    engine = db.engine()
    session = sessionmaker(bind=engine)()

    # insert heatpumps and resistive heater as link
    for c in ["Grosswaermepumpe", "Elektrodenheizkessel"]:
        entry = EgonScenarioCapacities(
            component="link",
            scenario_name="eGon2035",
            nuts="DE",
            carrier="urban_central_"
            + ("heat_pump" if c == "Grosswaermepumpe" else "resistive_heater"),
            capacity=df.loc[(c, "Fernwaermeerzeugung"), "Wert"]
            * 1e6
            / df.loc[(c, "Volllaststunden"), "Wert"]
            / df.loc[(c, "Wirkungsgrad"), "Wert"],
        )

        session.add(entry)

    # insert solar- and geothermal as generator
    for c in ["Geothermie", "Solarthermie"]:
        entry = EgonScenarioCapacities(
            component="generator",
            scenario_name="eGon2035",
            nuts="DE",
            carrier="urban_central_"
            + (
                "solar_thermal_collector"
                if c == "Solarthermie"
                else "geo_thermal"
            ),
            capacity=df.loc[(c, "Fernwaermeerzeugung"), "Wert"]
            * 1e6
            / df.loc[(c, "Volllaststunden"), "Wert"],
        )

        session.add(entry)

    session.commit()


def insert_data_nep():
    """Overall function for importing scenario input data for eGon2035 scenario

    Returns
    -------
    None.

    """

    insert_nep_list_powerplants(export=True)

    insert_capacities_per_federal_state_nep()


def eGon100_capacities(year="2045"):
    """Inserts installed capacities for the eGon100 scenario

    Returns
    -------
    None.

    """

    sources = egon.data.config.datasets()["scenario_input"]["sources"]
    targets = egon.data.config.datasets()["scenario_input"]["targets"]

    # read-in installed capacities
    cwd = Path(".")

    if egon.data.config.settings()["egon-data"]["--run-pypsa-eur"]:
        filepath = cwd / "run-pypsa-eur"
        pypsa_eur_repos = filepath / "pypsa-eur"
        # Read YAML file
        pes_egonconfig = pypsa_eur_repos / "config" / "config.yaml"
        with open(pes_egonconfig, "r") as stream:
            data_config = yaml.safe_load(stream)

        target_file = (
            pypsa_eur_repos
            / "results"
            / data_config["run"]["name"]
            / "csvs"
            / sources["eGon100RE"]["capacities"]
        )

    else:
        target_file = (
            cwd
            / "data_bundle_powerd_data"
            / "pypsa_eur"
            / "2024-08-02-egondata-integration"
            / "csvs"
            / sources["eGon100RE"]["capacities"]
        )

    scn_path_years = ["2025", "2030", "2035", "2045"]
    scn_path_years.remove(year)

    df = pd.read_csv(target_file, skiprows=3)
    df = df.drop(columns=scn_path_years, errors="ignore")
    df.columns = ["component", "country", "carrier", "p_nom"]
    df = df[~df["country"].isna()]

    df.set_index("carrier", inplace=True)

    df = df[df.country.str[:2] == "DE"]

    # Drop country column
    df.drop("country", axis=1, inplace=True)

    # Drop copmponents which will be optimized in eGo
    unused_carrier = [
        "BEV charger",
        "DAC",
        "H2 Electrolysis",
        "electricity distribution grid",
        "home battery charger",
        "home battery discharger",
        "H2",
        "Li ion",
        "home battery",
        "residential rural water tanks charger",
        "residential rural water tanks discharger",
        "services rural water tanks charger",
        "services rural water tanks discharger",
        "residential rural water tanks",
        "services rural water tanks",
        "urban central water tanks",
        "urban central water tanks charger",
        "urban central water tanks discharger",
        "H2 Fuel Cell",
    ]

    df = df[~df.index.isin(unused_carrier)]

    df.index = df.index.str.replace(" ", "_")

    # Aggregate offshore wind
    df = pd.concat(
        [
            df,
            pd.DataFrame(
                index=["wind_offshore"],
                data={
                    "p_nom": (df.p_nom["offwind-ac"] + df.p_nom["offwind-dc"]),
                    "component": df.component["offwind-ac"],
                },
            ),
        ]
    )
    df = df.drop(["offwind-ac", "offwind-dc"])

    # Aggregate technologies with and without carbon_capture (CC)
    for carrier in ["SMR", "urban_central_gas_CHP"]:
        df.p_nom[carrier] += df.p_nom[f"{carrier}_CC"]
        df = df.drop([f"{carrier}_CC"])

    # Aggregate residential and services rural heat supply
    for merge_carrier in [
        "rural_resistive_heater",
        "rural_ground_heat_pump",
        "rural_gas_boiler",
        "rural_solar_thermal",
    ]:
        if f"residential_{merge_carrier}" in df.index:
            df = pd.concat(
                [
                    df,
                    pd.DataFrame(
                        index=[merge_carrier],
                        data={
                            "p_nom": (
                                df.p_nom[f"residential_{merge_carrier}"]
                                + df.p_nom[f"services_{merge_carrier}"]
                            ),
                            "component": df.component[
                                f"residential_{merge_carrier}"
                            ],
                        },
                    ),
                ]
            )
            df = df.drop(
                [f"residential_{merge_carrier}", f"services_{merge_carrier}"]
            )

    # Rename carriers
    df.rename(
        {
            "onwind": "wind_onshore",
            "ror": "run_of_river",
            "PHS": "pumped_hydro",
            "OCGT": "gas",
            "rural_ground_heat_pump": "residential_rural_heat_pump",
            "urban_central_air_heat_pump": "urban_central_heat_pump",
            "urban_central_solar_thermal": (
                "urban_central_solar_thermal_collector"
            ),
        },
        inplace=True,
    )

    # Reset index
    df = df.reset_index()

    # Rename columns
    df.rename(
        {"p_nom": "capacity", "index": "carrier"}, axis="columns", inplace=True
    )

    df["scenario_name"] = "eGon100RE"
    df["nuts"] = "DE"
    df["capacity"] = df["capacity"].fillna(0)

    db.execute_sql(
        f"""
        DELETE FROM
        {targets['scenario_capacities']['schema']}.{targets['scenario_capacities']['table']}
        WHERE scenario_name='eGon100RE'
        """
    )

    df.to_sql(
        targets["scenario_capacities"]["table"],
        schema=targets["scenario_capacities"]["schema"],
        con=db.engine(),
        if_exists="append",
        index=False,
    )


tasks = (create_table,)

if "status2019" in egon.data.config.settings()["egon-data"]["--scenarios"]:
    tasks = tasks + (insert_capacities_status2019, insert_data_nep)

if (
    "eGon2035" in egon.data.config.settings()["egon-data"]["--scenarios"]
) and not (
    "status2019" in egon.data.config.settings()["egon-data"]["--scenarios"]
):
    tasks = tasks + (insert_data_nep,)

if "eGon100RE" in egon.data.config.settings()["egon-data"]["--scenarios"]:
    tasks = tasks + (eGon100_capacities,)


class ScenarioCapacities(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="ScenarioCapacities",
            version="0.0.15",
            dependencies=dependencies,
            tasks=tasks,
        )
