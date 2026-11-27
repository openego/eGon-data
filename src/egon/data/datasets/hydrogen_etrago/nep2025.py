"""
Data of the Netzentwicklungsplan Gas und Wasserstoff 2025

Revised draft of 1 June 2026 and its annexes:
https://ko-nep.de/wp-content/uploads/2024/02/2026_06_01_Ueberarbeiteter_Entwurf_NEP_Gas_Wasserstoff_2025.pdf
The Excel annexes are read as published. The tables only published in the PDF
are read from NEP_Gas_H2_2025_tables.xlsx of the data bundle (one sheet per
table). Each function names its table or annex; the use of the data is
described in the documentation of the gas grids, supply and stores.
"""

from functools import lru_cache
from pathlib import Path
from urllib.request import urlretrieve

import pandas as pd

# Files of the NEP Gas und Wasserstoff in the data bundle; missing published
# files are downloaded from their URL
BUNDLE_FOLDER = Path("data_bundle_egon_data", "NEP", "NEP_gas_H2")
DOWNLOAD_FOLDER = Path("datasets", "h2_data")
URLS = {
    "Anlage1_Umsetzungsstand_Methan_Wasserstoff.xlsx": (
        "https://ko-nep.de/wp-content/uploads/2024/02/"
        "Anlage1_Umsetzungsstand_Methan_Wasserstoff.xlsx"
    ),
    "Inbetriebnahmedaten.xlsx": (
        "https://www.bundesnetzagentur.de/DE/Fachthemen/ElektrizitaetundGas/"
        "NEP/Gas/DL/Inbetriebnahmedaten.xlsx"
    ),
    "Anlage3_Netzausbauvorschlag_Wasserstoff.xlsx": (
        "https://ko-nep.de/wp-content/uploads/2024/02/"
        "Anlage3_Netzausbauvorschlag_Wasserstoff.xlsx"
    ),
    "SR_Anlage2Gas.xlsx": (
        "https://ko-nep.de/wp-content/uploads/2024/03/SR_Anlage2Gas.xlsx"
    ),
}
# Tables of the PDF, transcribed (only in the data bundle)
TABLES = "NEP_Gas_H2_2025_tables.xlsx"


def source_file(name):
    """
    Path of a file of the NEP Gas und Wasserstoff

    The file of the data bundle, else the file downloaded from its URL.

    Parameters
    ----------
    name : str
        Name of the file

    Returns
    -------
    pathlib.Path
        Path of the file

    """
    path = BUNDLE_FOLDER / name
    if path.is_file():
        return path
    if name not in URLS:
        raise FileNotFoundError(
            f"{path} is missing; it is only available in the data bundle."
        )
    path = DOWNLOAD_FOLDER / name
    if not path.is_file():
        path.parent.mkdir(parents=True, exist_ok=True)
        urlretrieve(URLS[name], path)
    return path


@lru_cache(maxsize=None)
def _table(sheet):
    """Sheet of NEP_Gas_H2_2025_tables.xlsx"""
    return pd.read_excel(source_file(TABLES), sheet_name=sheet, dtype=object)


def _rows(sheet, columns):
    """Rows of a sheet as tuples of Python values (empty cells: None)"""
    df = _table(sheet)[columns].astype(object)
    return [
        tuple(None if pd.isna(v) else v for v in row)
        for row in df.itertuples(index=False)
    ]


@lru_cache(maxsize=None)
def kernnetz_commissioning_year():
    """
    Commissioning year of the Kernnetz measures

    Latest planned commissioning of their NEP measures (Anlage 1b), assigned
    by the Kernnetz-ID of Tabelle 11, Tabelle 13 and Anhang 9. Measures without
    an entry keep the year of the FNB-Gas list.

    Returns
    -------
    dict
        Year per Kernnetz-ID ("Antrags-ID" of the FNB-Gas lists)

    """
    status = pd.read_excel(
        source_file("Anlage1_Umsetzungsstand_Methan_Wasserstoff.xlsx"),
        sheet_name="Anlage 1b - Umsetzungsstand H2",
        header=2,
    )
    planned = status.filter(like="Planerische").iloc[:, 0].astype(str)
    year = (
        pd.to_numeric(planned.str[-4:], errors="coerce")
        .groupby(status["Kürzel"])
        .max()
    )

    ids = _table("Kernnetz_NEP_IDs")
    ids = ids[ids["kernnetz_id"] != "---"]
    # "KLN009-01, KLN010-01" and version letters ("KLU057-01a")
    ids = ids.assign(
        kernnetz_id=ids["kernnetz_id"].str.split(", "),
        measure=ids["nep_id"].str.extract(r"^(H2-\d+)", expand=False),
    ).explode("kernnetz_id")
    ids["kernnetz_id"] = ids["kernnetz_id"].str.replace(
        r"[a-z]$", "", regex=True
    )
    ids["year"] = ids["measure"].map(year)

    return (
        ids.dropna(subset=["year"])
        .groupby("kernnetz_id")["year"]
        .max()
        .astype(int)
        .to_dict()
    )


# Scenario of the NEP used for 2045 (scenario 2, "O45-Strom", numbering of
# the approved Szenariorahmen)
NEP_SCENARIO_2045 = "2"


def nep_h2_network_2045():
    """
    Additional H2 sections 2045 (Anhang 6, p. 187-191, 193 sections)

    Returns
    -------
    list of tuple
        (number, start, end, kind, length [km], DN, DP [bar], NEP scenarios
        as text, e.g. "123")

    """
    sections = _rows(
        "Anhang6_H2_2045",
        [
            "number",
            "start",
            "end",
            "kind",
            "length_km",
            "DN",
            "DP_bar",
            "scenarios",
        ],
    )
    # The scenarios stay text even if a spreadsheet turned them into numbers
    return [
        (*section[:7], str(section[7]).split(".")[0]) for section in sections
    ]


# Criteria of Anlage 3 of the measures used (modelling result 2037 of
# NEP_SCENARIO_2045, section 6.4.2)
NEP_H2_MEASURES_CRITERIA = ("H2(4)", "H2(5)", "H2(6)")


@lru_cache(maxsize=None)
def nep_h2_measures_criterion():
    """
    Criterion of the H2 measures beyond the core network (Anlage 3)

    H2(4) and H2(6) are part of the network proposal, H2(5) counts only if
    the measure is part of scenario NEP_SCENARIO_2045 in 2037.

    Returns
    -------
    dict
        Criterion per NEP id

    """
    criteria = pd.read_excel(
        source_file("Anlage3_Netzausbauvorschlag_Wasserstoff.xlsx"), header=2
    )
    criteria.columns = criteria.columns.map(lambda c: str(c).split(":")[0])
    in_scenario = criteria[f"Szenario {NEP_SCENARIO_2045} 2037"].eq("Ja")
    criterion = pd.Series(index=criteria.index, dtype=object)
    for name in ["H2(4)", "H2(6)"]:
        criterion[criteria[name].eq("x")] = name
    criterion[criteria["H2(5)"].eq("x") & in_scenario] = "H2(5)"
    return criterion.dropna().groupby(criteria["Kürzel"]).first().to_dict()


def nep_h2_measures_2037():
    """
    H2 measures up to 2037 beyond the core network (Anlage 3); lengths and
    DN/DP are partly assumed, see the documentation of the gas grids

    Returns
    -------
    list of tuple
        (NEP id, start, end, length [km], DN, DP [bar], commissioning year of
        the NEP)

    """
    return _rows(
        "Massnahmen_H2_2037",
        [
            "nep_id",
            "start",
            "end",
            "length_km",
            "DN",
            "DP_bar",
            "commissioning",
        ],
    )


@lru_cache(maxsize=None)
def nep_h2_measures_commissioning():
    """
    Commissioning year of the H2 measures beyond the core network

    BNetzA list "Inbetriebnahmedaten" (June 2026); measures without a date
    keep the commissioning of the NEP, see :py:func:`nep_h2_measures_2037`.

    Returns
    -------
    dict
        Year per NEP id

    """
    dates = pd.read_excel(source_file("Inbetriebnahmedaten.xlsx"))
    year = pd.to_numeric(
        dates.iloc[:, 3].astype(str).str[-4:], errors="coerce"
    )
    return year.groupby(dates.iloc[:, 0]).max().dropna().astype(int).to_dict()


def nep_kernnetz_not_in_2045():
    """
    Kernnetz measures of the modelling result 2037 not in the network 2045
    of scenario 2 (Tabelle 37, p. 147)

    Returns
    -------
    list of str
        Kernnetz-IDs

    """
    changes = _table("Tabelle37_2045")
    return changes.loc[
        changes["change"] == "not in 2045", "kernnetz_id"
    ].tolist()


def nep_kernnetz_dn_2045():
    """
    DN of Kernnetz measures modelled differently by scenario 2 (Tabelle 37)

    Returns
    -------
    dict
        DN per Kernnetz-ID

    """
    changes = _table("Tabelle37_2045")
    changes = changes[changes["change"] == "DN"]
    return dict(zip(changes["kernnetz_id"], changes["DN"].astype(int)))


def nep_h2_border_points():
    """
    H2 cross-border points, scenario 2 (Tabellen 22, 31 and 36)

    Returns
    -------
    list of tuple
        (GÜP, country, share, border node, entry 2037, exit 2037, entry
        2045, exit 2045) in GWh/h (Brennwert)

    """
    return _rows(
        "GUEP_H2",
        [
            "point",
            "country",
            "share",
            "border_node",
            "entry_2037",
            "exit_2037",
            "entry_2045",
            "exit_2045",
        ],
    )


def nep_h2_lng_terminals():
    """
    H2 imports via converted LNG terminals (Tabelle 36, p. 143)

    Returns
    -------
    list of tuple
        (terminal, border node, entry 2037, entry 2045) in GWh/h

    """
    return _rows(
        "LNG_H2", ["terminal", "border_node", "entry_2037", "entry_2045"]
    )


# Ratio of the higher to the lower heating value of H2 (approval of the
# Szenariorahmen Gas und Wasserstoff 2025, p. 23)
NEP_H2_HHV_PER_LHV = 1.18


def nep_h2_storage():
    """
    H2 storage of scenario 2 (Tabellen 20, 21, 34 and 35)

    Returns
    -------
    dict
        (withdrawal, injection) in GWh/h (Brennwert) per year

    """
    return {
        year: (withdrawal, injection)
        for year, withdrawal, injection in _rows(
            "Speicher_Importe_H2",
            ["year", "storage_withdrawal", "storage_injection"],
        )
    }


def nep_h2_storage_dunkelflaute():
    """
    Additional H2 withdrawal of scenario 2 in the load case "Dunkelflaute"
    and the federal states of its four storage sites (p. 110)

    Returns
    -------
    dict
        (withdrawal in GWh/h (Brennwert), list of states) per year

    """
    return {
        year: (withdrawal, states.split(", "))
        for year, withdrawal, states in _rows(
            "Dunkelflaute_H2", ["year", "withdrawal", "states"]
        )
    }


# Project status of the market survey counted for 2037 (from the design
# planning on); 2045 counts all projects
SURVEY_STATUS_2037 = (
    "Entwurfsplanung/ Raumordnungsverfahren",
    "Detailplanung und Genehmigungsverfahren",
    "Beschaffung/ Bauvorbereitung und Montage/Bau",
    "Inbetriebnahme/ Projektabschluss/Fertigstellung",
)


@lru_cache(maxsize=None)
def _survey_projects(
    category, quantity="Einspeiseleistung H2 in MW", sector=None
):
    """
    H2 projects of the market survey per federal state (Szenariorahmen
    Anlage 2, values of 2035)

    Parameters
    ----------
    category : str
        Category of the survey ("Speicher", "Einspeisung" or "Ausspeisung")
    quantity : str
        Quantity of the survey (column "Angaben in Brennwert")
    sector : str, optional
        Only the projects with an exit to this sector, e.g. "Industrie"

    Returns
    -------
    dict
        Value [MW or MWh, Brennwert] per federal state, for 2037 and 2045

    """
    survey = pd.read_excel(
        source_file("SR_Anlage2Gas.xlsx"), sheet_name=0, header=1
    )
    survey.columns = survey.columns.map(str)
    selected = (survey["Kategorie"] == category) & (
        survey["Angaben in Brennwert"] == quantity
    )
    if sector is not None:
        selected &= survey[f"Ausspeisung\nSektor {sector}"] == "ja"
    survey = survey[selected]
    capacity = pd.to_numeric(survey["2035"], errors="coerce")
    projects = {}
    for year, status in [
        (2037, survey["Projektstatus"].isin(SURVEY_STATUS_2037)),
        (2045, survey["Projektstatus"].notna()),
    ]:
        state = (
            capacity[status].groupby(survey.loc[status, "Bundesland"]).sum()
        )
        projects[year] = state[state > 0].to_dict()
    return projects


def nep_h2_industry_projects():
    """
    Industrial H2 demand of the market survey per federal state [MWh,
    Brennwert] (Szenariorahmen Anlage 2, category "Ausspeisung", sector
    industry)

    Returns
    -------
    dict
        Demand per federal state, for 2037 and 2045

    """
    return _survey_projects(
        "Ausspeisung", "Ausspeisemenge H2 in MWh", "Industrie"
    )


def nep_h2_storage_projects():
    """
    H2 storage projects of the market survey per federal state [MW,
    Brennwert] (Szenariorahmen Anlage 2, category "Speicher")

    Returns
    -------
    dict
        Capacity per federal state, for 2037 and 2045

    """
    return _survey_projects("Speicher")


def nep_h2_other_imports():
    """
    Other H2 imports of scenario 2 (LH2 and derivatives), Tabellen 21 and 35

    Returns
    -------
    dict
        Import in GWh/h (Brennwert) per year

    """
    return dict(_rows("Speicher_Importe_H2", ["year", "other_imports"]))


def nep_h2_other_import_projects():
    """
    Other H2 import projects of the market survey per federal state [MW,
    Brennwert] (Szenariorahmen Anlage 2, category "Einspeisung")

    Returns
    -------
    dict
        Capacity per federal state, for 2037 and 2045

    """
    return _survey_projects("Einspeisung")


def nep_h2_other_import_sites():
    """
    Sites of the other H2 imports per federal state (nodes of
    h2_grid_nodes.csv; model assignment)

    Returns
    -------
    dict
        List of sites per federal state

    """
    return {
        state: sites.split(", ")
        for state, sites in _rows("Importstandorte_H2", ["state", "sites"])
    }


def nep_ch4_lng_terminals():
    """
    LNG terminals for methane: clusters of the approval of the
    Szenariorahmen (pp. 87-89) shared equally by their terminals

    Returns
    -------
    dict
        Capacity [GWh/h] per terminal

    """
    capacity = {}
    for terminals, cluster in _rows(
        "LNG_Cluster_CH4", ["terminals", "capacity"]
    ):
        terminals = terminals.split(", ")
        for terminal in terminals:
            capacity[terminal] = capacity.get(terminal, 0) + cluster / len(
                terminals
            )
    return capacity


def nep_ch4_lng_use():
    """
    Share of the LNG capacity used in the peak load case (Tabelle 19)

    Returns
    -------
    dict
        Share per year

    """
    return dict(_rows("LNG_Nutzung_CH4", ["year", "share"]))
