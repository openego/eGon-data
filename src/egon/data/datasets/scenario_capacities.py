"""The central module containing all code dealing with importing data from
Netzentwicklungsplan 2035, Version 2021, Szenario C, and
Netzentwicklungsplan 2037/2045, Version 2025, Szenario C
"""

from functools import lru_cache
from pathlib import Path
from urllib.request import urlretrieve
import datetime
import json
import time

from sqlalchemy import Boolean, Column, Float, Integer, String
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
    # NEP 2025 only, see load_nep2025_power_plant_list()
    h2_site = Column(Boolean)
    h2_conversion = Column(Boolean)


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
            'home_battery',
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
        AND (nuts != 'DE' OR carrier IN ('home_battery', 'BESS'))
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
        "PV-Batteriespeicher": "home_battery",
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

    # 'PV-Batteriespeicher' (home battery) and 'Großbatteriespeicher' (BESS)
    # have no federal-state breakdown at all in the eGon2035 source file -
    # neither in the main sheet (only the national 'Summe' is filled) nor
    # in the draft report (no matching row exists there either), so the
    # scaled_carriers mechanism above cannot be used for them. Handled
    # below via a single national (nuts='DE') row instead, analogous to
    # how status2024's small-storage capacity is inserted in
    # insert_capacities_status_quo(). Not needed for reGon2037/reGon2045,
    # whose (newer) source file already has real per-federal-state values
    # for both carriers.
    national_only_carriers = ["PV-Batteriespeicher", "Großbatteriespeicher"]

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

        # Insert 'national_only_carriers' as a single nuts='DE' row instead
        # of a per-federal-state breakdown (see comment above their
        # definition)
        if scenario == "eGon2035":
            insert_data = pd.concat([
                insert_data,
                pd.DataFrame({
                    "carrier": [rename_carrier[c] for c in national_only_carriers],
                    "capacity": [
                        df.loc[c, "Summe"] * 1e3 for c in national_only_carriers
                    ],
                    "component": "storage_units",
                    "nuts": "DE",
                    "scenario": scenario,
                }),
            ])

        for bl in map_nuts.index:
            # Extract capacity data for the current federal state
            data = pd.DataFrame(df[bl])

            # Handled above as a single national row instead, see comment
            # at national_only_carriers' definition
            if scenario == "eGon2035":
                data = data.drop(index=national_only_carriers, errors="ignore")

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

            # Storage carriers, mislabeled "generator" by the default above
            data.loc[
                data.carrier.isin(["home_battery", "BESS", "pumped_hydro"]),
                "component",
            ] = "storage_units"

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

        # Hydrogen power plants per federal state (reGon scenarios), inside
        # the dataset boundary
        hydrogen = pd.DataFrame(
            [
                {
                    "carrier": "hydrogen",
                    "capacity": capacity * 1e3,
                    "component": "generator",
                    "nuts": nuts_mapping()[state],
                    "scenario": scenario,
                }
                for scenario, per_state in NEP2025_HYDROGEN_POWER_PLANTS.items()
                if scenario in scenarios
                for state, capacity in per_state.items()
                if nuts_mapping()[state] in map_nuts.nuts.values
            ]
        )
        insert_data = pd.concat([insert_data, hydrogen])
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

    # Sum up capacities per federal state and carrier for the reGon
    # scenarios, from the capacity column of their target year
    capacities_list_reGon = {
        scenario: (
            nep_capacities[nep_capacities["scenario"] == "reGon"]
            .groupby(["federal_state", "carrier"])[column]
            .sum()
            .to_frame()
            .reset_index()
            .rename(columns={column: "capacity"})
            .assign(scenario=scenario)
        )
        for scenario, column in [
            ("reGon2037", "c2037_capacity"),
            ("reGon2045", "c2045_capacity"),
        ]
    }
    capacities_list_reGon2037 = capacities_list_reGon["reGon2037"]
    capacities_list_reGon2045 = capacities_list_reGon["reGon2045"]

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
            "Dampf": "others",
        }
    )


# Federal states as written in the lists of the NEP 2025 and in the MaStR
FEDERAL_STATE_CODES = {
    "Baden-Württemberg": "BW",
    "BadenWuerttemberg": "BW",
    "Bayern": "BY",
    "Berlin": "BE",
    "Brandenburg": "BB",
    "Bremen": "HB",
    "Hamburg": "HH",
    "Hessen": "HE",
    "Mecklenburg-Vorpommern": "MV",
    "MecklenburgVorpommern": "MV",
    "Niedersachsen": "NI",
    "Nordrhein-Westfalen": "NW",
    "NordrheinWestfalen": "NW",
    "Rheinland-Pfalz": "RP",
    "RheinlandPfalz": "RP",
    "Saarland": "SL",
    "Sachsen": "SN",
    "Sachsen-Anhalt": "ST",
    "SachsenAnhalt": "ST",
    "Schleswig-Holstein": "SH",
    "SchleswigHolstein": "SH",
    "Thüringen": "TH",
    "Thueringen": "TH",
}

# Net capacity of H2 power plants per federal state [GW], scenario C: NEP
# Strom 2037/2045 (2025), 2. Entwurf, Kap. 2, Abb. 15 (2037) and 18 (2045)
# https://www.netzentwicklungsplan.de/sites/default/files/2026-03/NEP_2037_2045_V2025_2_Entwurf_Kap2.pdf
NEP2025_HYDROGEN_POWER_PLANTS = {
    "reGon2037": {
        "BW": 1.8,
        "BY": 5.8,
        "BE": 1.3,
        "BB": 2.1,
        "HB": 0.0,
        "HH": 0.2,
        "HE": 2.5,
        "MV": 0.2,
        "NI": 2.9,
        "NW": 17.6,
        "RP": 0.3,
        "SL": 2.1,
        "SN": 2.1,
        "ST": 0.9,
        "SH": 0.4,
        "TH": 0.2,
    },
    "reGon2045": {
        "BW": 7.9,
        "BY": 12.2,
        "BE": 2.5,
        "BB": 3.3,
        "HB": 0.5,
        "HH": 1.3,
        "HE": 5.1,
        "MV": 0.8,
        "NI": 8.3,
        "NW": 25.7,
        "RP": 2.2,
        "SL": 2.7,
        "SN": 3.6,
        "ST": 2.7,
        "SH": 1.4,
        "TH": 1.1,
    },
}

# Units of the approved list with a deleted MaStR number: new number
NEP2025_MASTR_REPLACEMENTS = {
    # Kraftwerksgruppe Pfreimd (ENGIE): Pfreimd T1, R1, R2, R3
    "SEE935182416977": "SEE909599581535",
    "SEE942667598431": "SEE991322361361",
    "SEE930411510563": "SEE972377801989",
    "SEE968359064886": "SEE925690509520",
    # AVA Velsen (Saarbrücken)
    "SEE938035990372": "SEE959496642280",
}

# Reserve units of the approved list (not in the market model of the NEP
# Strom, Kap. 2, p. 29); status: Kraftwerksliste of the BNetzA, 26 June 2026
# https://www.bundesnetzagentur.de/DE/Fachthemen/ElektrizitaetundGas/Versorgungssicherheit/Erzeugungskapazitaeten/Kraftwerksliste/_DL/Kraftwerksliste.xlsx
NEP2025_RESERVE_UNITS = {
    # Kapazitätsreserve (§ 13e EnWG)
    "SEE923304040681": "Gersteinwerk F GT (F1)",
    "SEE932787342328": "Gersteinwerk G GT (G1)",
    "SEE908672656115": "Gersteinwerk G DT (G2)",
    "SEE964242179781": "Gersteinwerk K",
    "SEE930596800480": "Gasturbinenkraftwerk Ahrensfelde GT A",
    "SEE929797382345": "Gasturbinenkraftwerk Ahrensfelde GT B",
    "SEE988046628214": "Gasturbinenkraftwerk Ahrensfelde GT C",
    "SEE923527691592": "Gasturbinenkraftwerk Ahrensfelde GT D",
    "SEE957496380690": "Gasturbinenkraftwerk Thyrow GT B",
    "SEE980421575656": "Gasturbinenkraftwerk Thyrow GT C",
    "SEE989393516094": "Gasturbinenkraftwerk Thyrow GT D",
    "SEE983877705974": "Gasturbinenkraftwerk Thyrow GT E",
    "SEE976333173899": "Gaskraftwerk Landesbergen - Gasturbine",
    # Netzreserve (§ 13b EnWG)
    "SEE988182827533": "Staudinger 4",
    "SEE968136280119": "Rheinhafen-Dampfkraftwerk RDK 4S DT",
    "SEE934927915690": "Rheinhafen-Dampfkraftwerk RDK 4S GT",
    "SEE963398776042": "Darmstadt GT11",
    "SEE996136363488": "Darmstadt GT12",
    # besonderes netztechnisches Betriebsmittel (§ 11 (3) EnWG)
    "SEE916274994887": "bnBm Gaskraftwerk Leipheim",
}

# Location (state, postcode, municipality) of the units without MaStR number
NEP2025_LOCATIONS = {
    # Natural gas new builds (§§ 38/39 GasNZV), municipality of the name
    "BHKW Profen Village": ("ST", "06729", "Elsteraue"),
    "Rechenzentrum Frechen": ("NW", "50226", "Frechen"),
    "GKW Hanau": ("HE", "63450", "Hanau"),
    "Voerde Schleusenstraße": ("NW", "46562", "Voerde"),
    "Steag Herne Block 4": ("NW", "44649", "Herne"),
    "GuD Marbach": ("BW", "71672", "Marbach am Neckar"),
    # Other plants. Source, unless stated otherwise: location of the units
    # of the same plant in the MaStR (dump 2025-02-09, raw files)
    "MHKW": ("HB", "28219", "Bremen"),  # swb Entsorgung, MHKW_Gen4 49.2 MW
    "Blockdammweg/Klingenberg": ("BE", "10317", "Berlin"),  # HKW Klingenberg
    "Reuter West": ("BE", "13599", "Berlin"),  # HKW Reuter West
    "FHKW Ludwigshafen": ("RP", "67063", "Ludwigshafen"),
    "Erzhausen": ("NI", "37574", "Einbeck"),  # PSW Erzhausen, 200 MW
    "Rudolf-Fettweis-Werk Oberstufe": ("BW", "76596", "Forbach"),
    "Rudolf-Fettweis-Werk Unterstufe": ("BW", "76596", "Forbach"),
    # Planned next to the Jochenstein plant (MaStR: Untergriesbach)
    "Pumpspeicherwerk Riedl": ("BY", "94107", "Untergriesbach"),
    # Klärschlammverbrennung at the Köhlbrandhöft treatment plant, source:
    # https://de.wikipedia.org/wiki/VERA_Kl%C3%A4rschlammverbrennung
    "VERA": ("HH", "20457", "Hamburg"),
    # Planned pumped hydro in Einöden near Flintsbach am Inn, source:
    # https://psw-einoeden.de/
    "Einoeden": ("BY", "83126", "Flintsbach am Inn"),
    # Planned pumped hydro Leutenberg/Probstzella (Vattenfall), source:
    # https://group.vattenfall.com/de/newsroom/pressemitteilungen/2022/vattenfall-erwirbt-projektgesellschaft-fur-pumpspeicherkraftwerk-in-thuringen
    "PSW Leutenberg": ("TH", "07338", "Leutenberg"),
    # Naturstromspeicher Gaildorf, halted in 2024 but part of the approved
    # list, source: https://de.wikipedia.org/wiki/Naturstromspeicher_Gaildorf
    "Gaildorf": ("BW", "74405", "Gaildorf"),
}


def download_nep2025_power_plant_list():
    """
    Download the approved list of power plants of the NEP 2025 (Annex 1 of
    the approval of the Szenariorahmen) and Annexes 2 and 3 of the
    Szenariorahmen Gas/Wasserstoff 2025

    Returns
    -------
    None

    """
    scenarios = config.settings()["egon-data"]["--scenarios"]
    if not any(s in scenarios for s in ["reGon2037", "reGon2045"]):
        return

    for key in [
        "reGon_list_conv_pp",
        "reGon_h2_market_survey",
        "reGon_gas_power_plants",
    ]:
        target_file = Path(ScenarioCapacities.sources.files[key])
        if target_file.is_file():
            continue

        target_file.parent.mkdir(parents=True, exist_ok=True)
        urlretrieve(ScenarioCapacities.sources.urls[key], target_file)


def read_mastr_units(file, unit_ids, columns):
    """Read the given units from a MaStR file

    Parameters
    ----------
    file : str
        Path of the MaStR file
    unit_ids : list
        MaStR numbers of the units (EinheitMastrNummer)
    columns : list
        Columns to read besides EinheitMastrNummer

    Returns
    -------
    pandas.DataFrame
        Units found in the file

    """
    # The storage file is large, so it is read in chunks
    return pd.concat(
        chunk[chunk.EinheitMastrNummer.isin(unit_ids)]
        for chunk in pd.read_csv(
            file,
            usecols=["EinheitMastrNummer"] + columns,
            dtype={"Postleitzahl": str},
            chunksize=500_000,
        )
    )


def read_nep2025_power_plant_list():
    """Return a copy of the approved list of power plants of the NEP 2025

    See :py:func:`load_nep2025_power_plant_list`. The list is read once
    per process, as it is needed twice in :py:func:`insert_data_nep`.

    Returns
    -------
    pandas.DataFrame
        Power plants of the list

    """
    return load_nep2025_power_plant_list().copy()


@lru_cache(maxsize=1)
def load_nep2025_power_plant_list():
    """
    Read the approved list of power plants of the NEP 2025

    Scenario C of Annex 1 of the approval of the Szenariorahmen
    2025-2037/2045, without the reserve plants
    (:py:data:`NEP2025_RESERVE_UNITS`), located with the MaStR. The flags
    ``h2_site`` (natural gas units, Annex 3) and ``h2_conversion`` (hydrogen
    projects, Annex 2) of the Szenariorahmen Gas/Wasserstoff 2025 link the
    hydrogen projects to the natural gas units they replace.

    Returns
    -------
    pandas.DataFrame
        Power plants in the format of
        :py:class:`NEPConvPowerPlants
        <egon.data.datasets.scenario_capacities.NEPConvPowerPlants>`

    """
    sources = ScenarioCapacities.sources
    file = sources.files["reGon_list_conv_pp"]

    gas = pd.read_excel(file, sheet_name="Erdgas-Kraftwerke").rename(
        columns={
            "MaStR-Nr. der Stromerzeugungseinheit": "mastr_id",
            "Anzeige-Name der Stromerzeugungseinheit": "name",
            "Anlagenbetreiber": "operator",
            "2037 Szenario 2 / B / C [MWel]": "c2037_capacity",
        }
    )
    gas = gas[~gas.mastr_id.str.contains("Dummy")]
    gas["carrier_nep"] = "Erdgas"
    # scenario C has no natural gas plants in 2045
    gas["c2045_capacity"] = 0.0
    # The reserve plants are not market plants of the NEP Strom
    reserve = gas.mastr_id.isin(NEP2025_RESERVE_UNITS)
    print(
        "Reserve plants of the NEP 2025 list not used in 2037: "
        f"{gas.loc[reserve, 'c2037_capacity'].sum():.0f} MW, "
        f"{', '.join(gas.loc[reserve, 'mastr_id'].map(NEP2025_RESERVE_UNITS))}."
    )
    gas.loc[reserve, "c2037_capacity"] = 0.0

    # Sites reported in the hydrogen market survey. Units in planning
    # (§§ 38/39 GasNZV) have no MaStR number in Annex 3 and are skipped.
    gas_plants = pd.read_excel(
        sources.files["reGon_gas_power_plants"], header=1
    )
    h2_sites = gas_plants.loc[
        gas_plants["Standort in Marktabfrage Wasserstoff gemeldet"].eq("x"),
        "MaStR-Nr. der Stromerzeugungseinheit",
    ].dropna()
    gas["h2_site"] = gas.mastr_id.isin(h2_sites)

    others = pd.read_excel(
        file, sheet_name="Sonstige Kraftwerke (Strom-NEP)"
    ).rename(
        columns={
            "MaStR-ID": "mastr_id",
            "Anlagenbetreiber": "operator",
            "Anlagenname": "name",
            "Blockname": "name_unit",
            "Energieträger": "carrier_nep",
            "2037 Szenario A / B / C [MWel]": "c2037_capacity",
            "2045 Szenario A / B / C [MWel]": "c2045_capacity",
        }
    )

    hydrogen = pd.read_excel(file, sheet_name="Wasserstoff-Kraftwerke").rename(
        columns={
            "Projektnummer": "name",
            "Bundesland": "federal_state",
            "2037 Szenario 2 / B / C [MWel]": "c2037_capacity",
            "2045 Szenario 2 / B / C [MWel]": "c2045_capacity",
        }
    )
    hydrogen["carrier_nep"] = "Wasserstoff"
    hydrogen["federal_state"] = hydrogen.federal_state.map(FEDERAL_STATE_CODES)
    hydrogen["chp"] = "Nein"

    # Projects that replace a natural gas plant. The survey has seven rows
    # per project with the same flag.
    survey = pd.read_excel(sources.files["reGon_h2_market_survey"], header=1)
    conversion = (
        survey.groupby("Projekt-\nnummer")["Reduzierung des\nMethanbedarfs"]
        .first()
        .eq("ja")
    )
    hydrogen["h2_conversion"] = (
        hydrogen.name.map(conversion).fillna(False).astype(bool)
    )

    units = pd.concat([gas, others], ignore_index=True)
    units["name"] = units.name.str.strip()
    units["mastr_id"] = units.mastr_id.replace(NEP2025_MASTR_REPLACEMENTS)

    # Add the data of the units from the MaStR
    columns = [
        "Bundesland",
        "Postleitzahl",
        "Ort",
        "Inbetriebnahmedatum",
        "EinheitBetriebsstatus",
        "Nettonennleistung",
    ]
    mastr = pd.concat(
        [
            read_mastr_units(
                sources.files["mastr_combustion"],
                units.mastr_id,
                columns + ["ThermischeNutzleistung"],
            ),
            read_mastr_units(
                sources.files["mastr_hydro"], units.mastr_id, columns
            ),
            read_mastr_units(
                sources.files["mastr_storage"], units.mastr_id, columns
            ),
        ]
    ).drop_duplicates(subset="EinheitMastrNummer")

    units = units.merge(
        mastr, left_on="mastr_id", right_on="EinheitMastrNummer", how="left"
    )
    units["federal_state"] = units.Bundesland.map(FEDERAL_STATE_CODES)
    units["postcode"] = units.Postleitzahl
    units["city"] = units.Ort
    units["commissioned"] = units.Inbetriebnahmedatum.str[:4]
    units["status"] = units.EinheitBetriebsstatus
    # MaStR capacities are given in kW
    units["capacity"] = units.Nettonennleistung / 1e3
    units["chp"] = np.where(
        units.ThermischeNutzleistung.fillna(0) > 0, "Ja", "Nein"
    )

    for name, (state, postcode, city) in NEP2025_LOCATIONS.items():
        located = (units.name == name) & units.federal_state.isnull()
        units.loc[located, "federal_state"] = state
        units.loc[located, "postcode"] = postcode
        units.loc[located, "city"] = city
        units.loc[located, "status"] = np.where(
            units.loc[located, "mastr_id"] == "Neubau §§38/39 GasNZV",
            "Neubau §§38/39 GasNZV",
            "not in the MaStR",
        )

    # Units without German federal state (abroad or unlocated) are dropped
    missing = units.federal_state.isnull()
    if missing.any():
        dropped = units[
            missing & (units.c2037_capacity + units.c2045_capacity > 0)
        ]
        print(
            "Units of the NEP 2025 list without location in Germany are "
            "dropped (MW in 2037 per carrier): "
            f"{dropped.groupby('carrier_nep').c2037_capacity.sum().round().to_dict()}; "
            f"units: {', '.join(dropped.name.astype(str))}"
        )
        units = units[~missing]

    kw_liste = pd.concat([units, hydrogen], ignore_index=True)
    kw_liste["scenario"] = "reGon"

    return kw_liste[
        [
            "mastr_id",
            "name",
            "name_unit",
            "operator",
            "carrier_nep",
            "chp",
            "postcode",
            "city",
            "federal_state",
            "commissioned",
            "status",
            "capacity",
            "c2037_capacity",
            "c2045_capacity",
            "scenario",
            "h2_site",
            "h2_conversion",
        ]
    ]


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
            kw_liste_nep25 = read_nep2025_power_plant_list()

            ran = True  # ensures the data is only loaded once

    kw_liste_nep = pd.concat(
        [kw_liste_nep21, kw_liste_nep25], ignore_index=True
    )

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

    # The NEP2021 list uses "Ja"/"Nein", the NEP2025 list uses "ja"/"nein".
    # Downstream queries filter on the capitalized form, so normalize both.
    kw_liste_nep["chp"] = kw_liste_nep["chp"].replace(
        {"ja": "Ja", "nein": "Nein"}
    )
    # Convert postcode column to string to avoid issues with data types
    kw_liste_nep["postcode"] = kw_liste_nep["postcode"].astype("string")

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
    download_nep2025_power_plant_list,
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
      * :py:func:`mastr_data_setup <egon.data.datasets.mastr.mastr_data_setup>`


    *Resulting tables*
      * :py:class:`supply.egon_scenario_capacities <egon.data.datasets.scenario_capacities.EgonScenarioCapacities>` is created and filled
      * :py:class:`supply.egon_nep_conventional_powerplants <egon.data.datasets.scenario_capacities.NEPConvPowerPlants>` is created and filled

    """

    #:
    name: str = "ScenarioCapacities"
    #:
    version: str = "0.0.25"
    sources = DatasetSources(
        files={
            "eGon2035_capacities": "data_bundle_egon_data/NEP/NEP_V2021_scnC2035.xlsx",
            "eGon2035_list_conv_pp": "data_bundle_egon_data/NEP/Kraftwerksliste_NEP_V2021_konv.csv",
            "reGon_capacities": "data_bundle_egon_data/NEP/NEP_V2025_scnC2037.xlsx",
            "reGon_list_conv_pp": "nep_2025/Anlage1_Standorte_Kraftwerke.xlsx",
            "reGon_h2_market_survey": "nep_2025/SR_Anlage2Gas.xlsx",
            "reGon_gas_power_plants": "nep_2025/SR_Anlage3Gas.xlsx",
            "mastr_combustion": "./bnetza_mastr/dump_2025-02-09/bnetza_mastr_combustion_cleaned.csv",
            "mastr_hydro": "./bnetza_mastr/dump_2025-02-09/bnetza_mastr_hydro_cleaned.csv",
            "mastr_storage": "./bnetza_mastr/dump_2025-02-09/bnetza_mastr_storage_cleaned.csv",
        },
        urls={
            "reGon_list_conv_pp": "https://www.netzentwicklungsplan.de/sites/default/files/2025-05/Anlage1_Standorte_Kraftwerke_0.xlsx",
            "reGon_h2_market_survey": "https://ko-nep.de/wp-content/uploads/2024/03/SR_Anlage2Gas.xlsx",
            "reGon_gas_power_plants": "https://ko-nep.de/wp-content/uploads/2024/03/SR_Anlage3Gas.xlsx",
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
