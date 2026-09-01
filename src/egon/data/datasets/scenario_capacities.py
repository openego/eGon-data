"""The central module containing all code dealing with importing data from
Netzentwicklungsplan 2035, Version 2021, Szenario C, and
Netzentwicklungsplan 2037/2045, Version 2025, Szenario C
"""

from pathlib import Path
import datetime
import json
import time

from sqlalchemy import Column, Float, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import numpy as np
import pandas as pd
import yaml

from egon.data import config, db
from egon.data.datasets import (
    Dataset,
    DatasetSources,
    DatasetTargets,
)
from egon.data.metadata import (
    context,
    generate_resource_fields_from_sqla_model,
    license_ccby,
    meta_metadata,
    sources,
)

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

class NEPConvPowerPlants(Base):
    __tablename__ = "egon_nep_conventional_powerplants"
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
    #from here it is new entries of the NEP2025 Kraftwerksliste
    c2037_capacity = Column(Float)
    c2045_capacity = Column(Float)
    technology = Column(String(24))
    carrier_nep_2037 = Column(String(24))
    carrier_nep_2045 = Column(String(12)) # only hydrogen 
    tso = Column(String(12))
    operator = Column(String(124))
    mastr_id = Column(String(24))
    scenario = Column(String(124))
    
    

def create_table():
    """Create input tables for scenario setup

    Returns
    -------
    None.

    """

    engine = db.engine()
    db.execute_sql("CREATE SCHEMA IF NOT EXISTS supply;")
    EgonScenarioCapacities.__table__.drop(bind=engine, checkfirst=True)
    NEPConvPowerPlants.__table__.drop(bind=engine, checkfirst=True)
    EgonScenarioCapacities.__table__.create(bind=engine, checkfirst=True)
    NEPConvPowerPlants.__table__.create(bind=engine, checkfirst=True)


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


def insert_capacities_status_quo(scenario: str) -> None:
    """Insert capacity of rural heat pumps and small storage for status quo

    Returns
    -------
    None.

    """
    targets = ScenarioCapacities.targets
    # Delete rows if already exist
    db.execute_sql(f"""
        DELETE FROM {targets.tables['scenario_capacities']}
        WHERE scenario_name = '{scenario}'
        """)

    rural_heat_capacity = {
        # Convert heat pump count to MW installed capacity
        # assuming 5 kW_el per heat pump (source: Entwurf des Szenariorahmens NEP 2035,
        # version 2021, page 47)
        # Rural heat capacity for 2024 according to NEP 2037/2045, version 2025, table 1
        "status2024": 2e6 * 5e-3,
    }[scenario]

    if config.settings()["egon-data"]["--dataset-boundary"] != "Everything":
        rural_heat_capacity *= population_share()

    db.execute_sql(f"""
        INSERT INTO {targets.tables['scenario_capacities']}
        (component, carrier, capacity, nuts, scenario_name)
        VALUES (
            'link',
            'rural_heat_pump',
            {rural_heat_capacity},
            'DE',
            '{scenario}'
            )
        """)

    # Include small storages for status2024
    small_storages = {
        # MW for Germany
        # small storage capacity for 2024 according to NEP 2037/2045, version 2025, table 1
        "status2024": 9900,
    }[scenario]

    db.execute_sql(f"""
        INSERT INTO {targets.tables['scenario_capacities']}
        (component, carrier, capacity, nuts, scenario_name)
        VALUES (
            'storage_units',
            'battery',
            {small_storages},
            'DE',
            '{scenario}'
            )
        """)


def insert_capacities_per_federal_state_nep():
    """Inserts installed capacities per federal state according to
    NEP 2035 (version 2021), scenario 2035 C, and
    NEP 2037/2045 (version 2025), scenario 2037/2045 C 

    Returns
    -------
    None.

    """
    sources = ScenarioCapacities.sources
    targets = ScenarioCapacities.targets
    # Connect to local database
    engine = db.engine()
    
    # Delete rows if already exist
    db.execute_sql(f"""
        DELETE FROM {targets.tables['scenario_capacities']}
        WHERE scenario_name IN ('eGon2035', 'reGon2037', 'reGon2045')
        AND nuts != 'DE'
        """)
    
    # kicks statusquo entries from list
    scenarios = [s for s in config.settings()["egon-data"]["--scenarios"] if "status" not in str(s).lower()]
    
    # Scenario-specific configuration for file paths, sheet names and wind offshore column
    scenario_config = {
        "eGon2035": {
            "file": sources.files["eGon2035_capacities"],
            "sheet_main": "1.Entwurf_NEP2035_V2021",
            "sheet_draft": "Entwurf_des_Szenariorahmens",
            "windoff_col": "C 2035",
        },
        "reGon2037": {
            "file": sources.files["reGon_capacities"],
            "sheet_main": "2.Entwurf_NEP2037C_V2025",
            "sheet_draft": "Entwurf_des_Szenariorahmens2037",
            "windoff_col": "C 2037",
        },
        "reGon2045": {
            "file": sources.files["reGon_capacities"],
            "sheet_main": "2.Entwurf_NEP2045C_V2025",
            "sheet_draft": "Entwurf_des_Szenariorahmens2045",
            "windoff_col": "C 2045",
        },
    }
    
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
        "PV-Batteriespeicher": "battery",
        "Großbatteriespeicher": "BESS",
    }
    # 'Elektromobilitaet gesamt': 'transport',
    # 'Elektromobilitaet privat': 'transport'}

    # nuts1 to federal state in Germany
    map_nuts = pd.read_sql(
        f"""
        SELECT DISTINCT ON (nuts) gen, nuts
        FROM {sources.tables['boundaries']}
        """,
        engine,
        index_col="gen",
    )

    scaled_carriers = [
        "Haushaltswaermepumpen",
        "PV (Aufdach)",
        "PV (Freiflaeche)",
    ]
    
    insert_data = pd.DataFrame()
    for scenario in scenarios:
        if scenario not in scenario_config:
            continue
    
        cfg = scenario_config[scenario]
        target_file = Path(".") / cfg["file"]
        
        # Load main NEP capacity table and draft scenario report
        df = pd.read_excel(target_file, sheet_name=cfg["sheet_main"], index_col="Unnamed: 0")
        df_draft = pd.read_excel(target_file, sheet_name=cfg["sheet_draft"], index_col="Unnamed: 0")
        
        # Load wind offshore data and drop rows without federal state or grid connection point
        df_windoff = pd.read_excel(
            target_file, sheet_name="WInd_Offshore_NEP"
        ).dropna(subset=["Bundesland", "Netzverknuepfungspunkt"])
        # Remove trailing whitespace from federal state column
        df_windoff["Bundesland"] = df_windoff["Bundesland"].str.strip()
        # Sum wind offshore capacities per federal state
        df_windoff_fs = df_windoff[["Bundesland", cfg["windoff_col"]]].groupby("Bundesland").sum()
        
        # Overwrite wind offshore capacities in main df with more accurate
        # values from the wind offshore sheet (convert MW to GW)
        for state in df_windoff_fs.index:
            df.at["Wind offshore", state] = df_windoff_fs.at[state, cfg["windoff_col"]] / 1000
    
        for bl in map_nuts.index:
            # Extract capacity data for the current federal state
            data = pd.DataFrame(df[bl])
            
            # For carriers without direct federal state distribution in the final
            # NEP report, scale the draft distribution to match the final national total
            for c in scaled_carriers:
                data.loc[c, bl] = df_draft.loc[c, bl] / df_draft.loc[c, "Summe"] * df.loc[c, "Summe"]
    
            # Split combined hydro entry into run of river and reservoir
            # using the distribution from the draft scenario report
            if data.loc["Lauf- und Speicherwasser", bl] > 0:
                for c in ["Speicherwasser", "Laufwasser"]:
                    data.loc[c, bl] = (
                        data.loc["Lauf- und Speicherwasser", bl]
                        * df_draft.loc[c, bl]
                        / df_draft.loc[["Speicherwasser", "Laufwasser"], bl].sum()
                    )
                    
            # Map NEP carrier names to internal names and aggregate by carrier
            data["carrier"] = data.index.map(rename_carrier)
            data = data.groupby(data.carrier)[bl].sum().reset_index()
            
            # Add metadata columns
            data["component"] = "generator"
            data["nuts"] = map_nuts.nuts[bl]
            data["scenario"] = scenario
            
            # Convert heat pump count to GW installed capacity
            # assuming 5 kW_el per heat pump (source: Entwurf des Szenariorahmens NEP 2035,
            # version 2021, page 47)
            data.loc[data.carrier == "residential_rural_heat_pump", bl] *= 5e-6
            data.loc[data.carrier == "residential_rural_heat_pump", "component"] = "link"
            
            # Rename capacity column and convert from GW to MW
            data = data.rename(columns={bl: "capacity"})
            data.capacity *= 1e3
            
            # Append federal state data to the full insert DataFrame
            insert_data = pd.concat([insert_data, data])
    
    # Nothing was collected because none of the scenarios covered by the
    # NEP report (eGon2035, reGon2037, reGon2045) is part of the configured
    # scenarios
    if not insert_data.empty:
        # Get aggregated capacities from nep's power plant list for
        # certain carrier
        carriers = ["oil", "others", "pumped_hydro"]

        capacities_list = aggr_nep_capacities(carriers)

        # Filter by carrier
        updated = insert_data[insert_data["carrier"].isin(carriers)]

        # Merge to replace capacities for carriers "oil", "others" and
        # "pumped_hydro"
        updated = (
            updated.merge(capacities_list, on=["carrier", "nuts", "scenario"], how="left")
            .fillna(0)
            .drop(["capacity_x"], axis=1)
            .rename(columns={"capacity_y": "capacity"})
        )

        # Remove updated entries from df
        original = insert_data[~insert_data["carrier"].isin(carriers)]

        # Join dfs
        insert_data = pd.concat([original, updated])
        insert_data = insert_data.rename(columns={"scenario": "scenario_name"})

        # Insert data to db
        insert_data.to_sql(
            targets.get_table_name("scenario_capacities"),
            engine,
            schema=targets.get_table_schema("scenario_capacities"),
            if_exists="append",
            index=insert_data.index,
        )

    # Add district heating data according to energy and full load hours
    district_heating_input()

def population_share():
    """Calulate share of population in testmode

    Returns
    -------
    float
        Share of population in testmode

    """
    sources = ScenarioCapacities.sources
    return (
        pd.read_sql(
            f"""
            SELECT SUM(population)
            FROM {sources.tables['zensus_population']}
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
    # Depending on the configured scenarios, only one of the NEP power plant
    # lists may have been loaded, so not all capacity columns are present.
    # Missing ones are added as NaN, which yields empty per-scenario
    # aggregations below.
    nep_capacities = insert_nep_list_powerplants(export=False).reindex(
        columns=[
            "federal_state",
            "scenario",
            "carrier",
            "c2035_capacity",
            "c2037_capacity",
            "c2045_capacity",
        ]
    )

    # Sum up capacities per federal state and carrier for eGon2035
    capacities_list_eGon = (
        nep_capacities[nep_capacities["scenario"] == "eGon2035"]
        .groupby(["federal_state", "carrier", "scenario"])["c2035_capacity"]
        .sum()
        .to_frame()
        .reset_index()
        .rename(columns={"c2035_capacity": "capacity"})
    )
    
    # Sum up capacities per federal state and carrier for reGon scenarios
    capacities_list_reGon2037 = (
        nep_capacities[nep_capacities["scenario"] == "reGon"]
        .groupby(["federal_state", "carrier", "scenario"])["c2037_capacity"]
        .sum()
        .to_frame()
        .reset_index()
        .rename(columns={"c2037_capacity": "capacity"})
    )
    
    
    # Neglect entries with carriers not in argument
    capacities_list_eGon = capacities_list_eGon[capacities_list_eGon.carrier.isin(carriers)]
    capacities_list_reGon2037 = capacities_list_reGon2037[capacities_list_reGon2037.carrier.isin(carriers)]
     
    # Include NUTS code
    capacities_list_eGon["nuts"] = capacities_list_eGon.federal_state.map(nuts_mapping())
    capacities_list_reGon2037["nuts"] = capacities_list_reGon2037.federal_state.map(nuts_mapping())
    
    # works as capacities for 2037 and 2045 are the same
    capacities_list_reGon2037["scenario"] = "reGon2037"
    capacities_list_reGon2045 = capacities_list_reGon2037.copy()
    capacities_list_reGon2045["scenario"] = "reGon2045"
    
    capacities_list = pd.concat([capacities_list_eGon, capacities_list_reGon2037, capacities_list_reGon2045], ignore_index=True)
    
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
            "Sonstige": "others",
            "Erdgas/Wasserstoff": "gas",
            "Wasserstoff": "hydrogen",
            "Wasser": "pumped_hydro",
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
    sources = ScenarioCapacities.sources
    targets = ScenarioCapacities.targets

    # Connect to local database
    engine = db.engine()
    # Initialize both DataFrames as empty upfront.
    # This guarantees they exist later,
    # for their concat further down.
    kw_liste_nep21 = pd.DataFrame()
    kw_liste_nep25 = pd.DataFrame()
    # kicks statusquo entries from list
    scenarios = [s for s in config.settings()["egon-data"]["--scenarios"] if "status" not in str(s).lower()]

    # Nothing to do if none of the scenarios covered by the NEP power plant
    # lists (eGon2035, reGon2037, reGon2045) is part of the configured
    # scenarios
    if not any(s in scenarios for s in ["eGon2035", "reGon2037", "reGon2045"]):
        if export:
            return
        return pd.DataFrame(
            columns=[
                "federal_state",
                "scenario",
                "carrier",
                "c2035_capacity",
                "c2037_capacity",
                "c2045_capacity",
            ]
        )

    # iterates over all scenarios except for status_quo scenarios
    ran = False
    for scenario in scenarios:
        if scenario == "eGon2035":
            # Read-in data from csv-file
            target_file = Path(".") / sources.files["eGon2035_list_conv_pp"]
            kw_liste_nep21 = pd.read_csv(target_file, delimiter=";", decimal=",")
            
            # Adjust column names for Kraftwerksliste_zum_Szenariorahmen from the NEP2021
            kw_liste_nep21 = kw_liste_nep21.rename(
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
            # add scenario column
            kw_liste_nep21["scenario"] = "eGon2035"
            
        elif scenario in ["reGon2037", "reGon2045"] and not ran:
            # Read-in data from csv-file
            target_file = Path(".") / sources.files["reGon_list_conv_pp"]
            kw_liste_nep25 = pd.read_excel(target_file, decimal=",")
            
            # Adjust column names for Kraftwerksliste_Szenariorahmen from the NEP2025
            kw_liste_nep25 = kw_liste_nep25.rename(
                columns={
                    "MaStR-ID": "mastr_id",
                    "ÜNB" : "tso",
                    "Betreiber": "operator",
                    "Kraftwerksname": "name",
                    "Blockname": "name_unit",
                    "Energieträger": "carrier_nep",
                    "Technologie": "technology",
                    "KWK": "chp", # original column name from NEP2021 changed in NEP2025
                    "PLZ": "postcode",
                    "Ort": "city",
                    "Bundesland": "federal_state",
                    "Inbetriebnahmejahr": "commissioned",
                    "Status aktuell": "status", # original column name from NEP2021 changed in NEP2025
                    "Nettonennleistung [MW] 31.12.2023": "capacity",
                    "Leistung [MW] 2037": "c2037_capacity",
                    "Energieträger 2037": "carrier_nep_2037",
                    "Leistung [MW] 2045": "c2045_capacity",
                    "Energieträger 2045": "carrier_nep_2045",
                }
            )
            
            # rename federal states to shortcuts
            kw_liste_nep25["federal_state"].replace({
                "Baden-Wuerttemberg": "BW",
                "Nordrhein-Westfalen": "NW",
                "Hessen": "HE",
                "Brandenburg": "BB",
                "Bremen": "HB",
                "Rheinland-Pfalz": "RP",
                "Sachsen-Anhalt": "ST",
                "Schleswig-Holstein": "SH",
                "Mecklenburg-Vorpommern": "MV",
                "Thueringen": "TH",
                "Niedersachsen": "NI",
                "Sachsen": "SN",
                "Hamburg": "HH",
                "Saarland": "SL",
                "Berlin": "BE",
                "Bayern": "BY",
            }, inplace = True)
            
            
            # add scenario column
            kw_liste_nep25["scenario"] = "reGon"
            
            ran = True # ensures the data is only loaded once
        
    
    kw_liste_nep = pd.concat([kw_liste_nep21, kw_liste_nep25], ignore_index=True)

    # Cut data to federal state if in testmode
    boundary = config.settings()["egon-data"]["--dataset-boundary"]
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
        
        # scale all capacity to the respective population share
        # Only columns of the NEP lists that were actually loaded are
        # present, depending on the configured scenarios
        for col in [
            c
            for c in [
                "capacity",
                "a2035_capacity",
                "b2035_capacity",
                "c2035_capacity",
                "b2040_capacity",
                "c2037_capacity",
                "c2045_capacity",
            ]
            if c in kw_liste_nep.columns
        ]:
            kw_liste_nep.loc[
                kw_liste_nep[kw_liste_nep.federal_state.isnull()].index, col
            ] *= population_share()
    
    # Map NEP carrier names to internal eGon-data carrier names
    kw_liste_nep["carrier"] = map_carrier()[kw_liste_nep.carrier_nep].values
    kw_liste_nep["chp"] = kw_liste_nep["chp"].replace("ja", "Ja")

    if export is True:
        # Insert data to db
        kw_liste_nep.to_sql(
            targets.get_table_name("nep_conventional_powerplants"),
            engine,
            schema=targets.get_table_schema("nep_conventional_powerplants"),
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
    sources = ScenarioCapacities.sources
    targets = ScenarioCapacities.targets

    # The eGon2035 scenario has its own capacities file, while reGon2037
    # and reGon2045 share the same "Kurzstudie_KWK" sheet (analogous to
    # how reGon2045 reuses reGon2037's aggregated NEP capacities, see
    # aggr_nep_capacities()).
    file_per_scenario = {
        "eGon2035": sources.files["eGon2035_capacities"],
        "reGon2037": sources.files["reGon_capacities"],
        "reGon2045": sources.files["reGon_capacities"],
    }

    scenarios = [
        s
        for s in config.settings()["egon-data"]["--scenarios"]
        if s in file_per_scenario
    ]

    # Connect to database
    engine = db.engine()
    session = sessionmaker(bind=engine)()

    for scenario in scenarios:
        # Delete rows if already existing to keep this function idempotent.
        # Scoped to these four carriers only, since other functions write
        # other rows for the same scenario_name.
        db.execute_sql(
            f"""
            DELETE FROM {targets.tables['scenario_capacities']}
            WHERE scenario_name = '{scenario}'
            AND carrier IN (
                'urban_central_heat_pump',
                'urban_central_resistive_heater',
                'urban_central_geo_thermal',
                'urban_central_solar_thermal_collector')
            """
        )

        # import data to dataframe
        file = Path(".") / file_per_scenario[scenario]
        #TODO:Umgang mit der Kurzstudie_KWK diskutieren
        df = pd.read_excel(
            file, sheet_name="Kurzstudie_KWK", dtype={"Wert": float}
        )
        df.set_index(["Energietraeger", "Name"], inplace=True)

        # Scale values to population share in testmode
        if (
            config.settings()["egon-data"]["--dataset-boundary"]
            != "Everything"
        ):
            df.loc[
                pd.IndexSlice[:, "Fernwaermeerzeugung"], "Wert"
            ] *= population_share()

        # insert heatpumps and resistive heater as link
        for c in ["Grosswaermepumpe", "Elektrodenheizkessel"]:
            entry = EgonScenarioCapacities(
                component="link",
                scenario_name=scenario,
                nuts="DE",
                carrier="urban_central_"
                + (
                    "heat_pump"
                    if c == "Grosswaermepumpe"
                    else "resistive_heater"
                ),
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
                scenario_name=scenario,
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


def insert_capacities_status_quo_scn():
    """Insert status quo capacities for all configured status quo scenarios

    Returns
    -------
    None.

    """
    for scenario in config.settings()["egon-data"]["--scenarios"]:
        if "status" in scenario:
            insert_capacities_status_quo(scenario)


def insert_data_nep():
    """Overall function for importing scenario input data for eGon2035,
    reGon2037 and reGon2045 scenarios

    Returns
    -------
    None.

    """

    insert_nep_list_powerplants(export=True)

    insert_capacities_per_federal_state_nep()



def add_metadata():
    """Add metdata to supply.egon_scenario_capacities

    Returns
    -------
    None.

    """

    # Import column names and datatypes
    fields = pd.DataFrame(
        generate_resource_fields_from_sqla_model(EgonScenarioCapacities)
    ).set_index("name")

    # Set descriptions and units
    fields.loc["index", "description"] = "Index"
    fields.loc["component", "description"] = (
        "Name of representative PyPSA component"
    )
    fields.loc["carrier", "description"] = "Name of carrier"
    fields.loc["capacity", "description"] = "Installed capacity"
    fields.loc["capacity", "unit"] = "MW"
    fields.loc["nuts", "description"] = (
        "NUTS region, either federal state or Germany"
    )
    fields.loc["scenario_name", "description"] = (
        "Name of corresponding reGon scenario"
    )

    # Reformat pandas.DataFrame to dict
    fields = fields.reset_index().to_dict(orient="records")

    meta = {
        "name": "supply.egon_scenario_capacities",
        "title": "reGon scenario capacities",
        "id": "WILL_BE_SET_AT_PUBLICATION",
        "description": (
            "Installed capacities of scenarios used in the reGon project"
        ),
        "language": ["de-DE"],
        "publicationDate": datetime.date.today().isoformat(),
        "context": context(),
        "spatial": {
            "location": None,
            "extent": "Germany",
            "resolution": None,
        },
        "sources": [
            sources()["nep2025"],
            sources()["vg250"],
            sources()["zensus"],
            sources()["egon-data"],
        ],
        "licenses": [
            license_ccby(
                "© Übertragungsnetzbetreiber; "
                "© Bundesamt für Kartographie und Geodäsie 2020 (Daten verändert); "
                "© Statistische Ämter des Bundes und der Länder 2014; "
                "© Jonathan Amme, Clara Büttner, Ilka Cußmann, Julian Endres, Carlos Epia, Stephan Günther, Ulf Müller, Amélia Nadal, Guido Pleßmann, Francesco Witte",
            )
        ],
        "contributors": [
            {
                "title": "Clara Büttner",
                "email": "http://github.com/ClaraBuettner",
                "date": time.strftime("%Y-%m-%d"),
                "object": None,
                "comment": "Imported data",
            },
        ],
        "resources": [
            {
                "profile": "tabular-data-resource",
                "name": "supply.egon_scenario_capacities",
                "path": None,
                "format": "PostgreSQL",
                "encoding": "UTF-8",
                "schema": {
                    "fields": fields,
                    "primaryKey": ["index"],
                    "foreignKeys": [],
                },
                "dialect": {"delimiter": None, "decimalSeparator": "."},
            }
        ],
        "metaMetadata": meta_metadata(),
    }

    # Create json dump
    meta_json = "'" + json.dumps(meta) + "'"

    # Add metadata as a comment to the table
    db.submit_comment(
        meta_json,
        EgonScenarioCapacities.__table__.schema,
        EgonScenarioCapacities.__table__.name,
    )


tasks = (
    create_table,
    insert_capacities_status_quo_scn,
    insert_data_nep,
    add_metadata,
)


class ScenarioCapacities(Dataset):
    """
    Create and fill table with installed generation capacities in Germany

    This dataset creates and fills a table with the installed generation capacities in
    Germany in a lower spatial resolution (either per federal state or on national level).
    This data is coming from external sources (e.g. German grid developement plan for scenario eGon2035).
    The table is in downstream datasets used to define target values for the installed capacities.


    *Dependencies*
      * :py:func:`Setup <egon.data.datasets.database.setup>`
      * :py:class:`PypsaEurSec <egon.data.datasets.pypsaeursec.PypsaEurSec>`
      * :py:class:`Vg250 <egon.data.datasets.vg250.Vg250>`
      * :py:class:`DataBundle <egon.data.datasets.data_bundle.DataBundle>`
      * :py:class:`ZensusPopulation <egon.data.datasets.zensus.ZensusPopulation>`


    *Resulting tables*
      * :py:class:`supply.egon_scenario_capacities <egon.data.datasets.scenario_capacities.EgonScenarioCapacities>` is created and filled
      * :py:class:`supply.egon_nep_conventional_powerplants <egon.data.datasets.scenario_capacities.NEPConvPowerPlants>` is created and filled

    """

    #:
    name: str = "ScenarioCapacities"
    #:
    version: str = "0.0.24"
    sources = DatasetSources(
        files={
            "eGon2035_capacities": "data_bundle_egon_data/nep2035_version2021/NEP2035_V2021_scnC2035.xlsx",
            "eGon2035_list_conv_pp": "data_bundle_egon_data/nep2035_version2021/Kraftwerksliste_NEP_2021_konv.csv",
            "reGon_capacities": "data_bundle_egon_data/nep2037_version2025/NEP2037_V2025_scnC2037.xlsx",
            "reGon_list_conv_pp": "data_bundle_egon_data/nep2037_version2025/Kraftwerksliste_Szenariorahmen.xlsx",
        },
        tables={
            "boundaries": "boundaries.vg250_lan",
            "zensus_population": "society.destatis_zensus_population_per_ha",
        },
    )

    targets = DatasetTargets(
        tables={
            "scenario_capacities": "supply.egon_scenario_capacities",
            "nep_conventional_powerplants": "supply.egon_nep_conventional_powerplants",
        }
    )

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=tasks,
        )
