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


# Kernnetz measures dropped by the NEP without replacement (Anhang 2; the
# dropped KLN001-01, KLN025-01 and KLU045-01 are replaced and kept as proxies)
DROPPED_KERNNETZ_MEASURES = []


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


# Year of the methane network below
NEP_CH4_NETWORK_YEAR = 2045


def nep_ch4_network_2045():
    """
    Sections of the methane network 2045 (Anhang 5)

    Returns
    -------
    list of tuple
        (number in Anhang 5, start, end, length in km, nominal diameter DN
        in mm as printed in the NEP, text such as "800-1200")

    """
    return _rows(
        "Anhang5_CH4_2045", ["number", "start", "end", "length_km", "DN"]
    )


# Coordinates (lon, lat, EPSG:4326) of the end points of the sections
NEP_CH4_NETWORK_2045_NODES = {
    "Aalen": (10.0930, 48.8376),  # OpenStreetMap
    "Achim": (9.0531, 53.0079),
    "Anwalting": (10.9404, 48.4581),  # OpenStreetMap
    "Appeldorn": (6.3510, 51.7241),  # OpenStreetMap
    "Bad Lauchstädt": (11.8675, 51.3867),
    "Basel": (7.5878, 47.5581),  # OpenStreetMap
    "Bergheim": (6.6521, 50.9464),
    "Bernburg": (11.7409, 51.7923),
    "Bierwang": (12.3827, 48.1271),  # OpenStreetMap
    "Binsheim": (6.6992, 51.5124),  # OpenStreetMap
    "Blankenloch": (8.4699, 49.0663),  # OpenStreetMap
    "Bocholtz": (6.0065, 50.8185),  # OpenStreetMap
    "Brandov": (13.3907, 50.6320),  # OpenStreetMap
    "Brensbach": (8.8993, 49.7695),  # OpenStreetMap
    "Bunde": (7.2729, 53.1814),
    "Cörmigk": (11.8415, 51.7265),
    "Datteln": (7.3386, 51.6515),  # OpenStreetMap
    "Dornum": (7.4279, 53.6469),  # OpenStreetMap
    "Dorsten": (6.9643, 51.6599),
    "Drohne": (8.3392, 52.4321),
    "Dürrlewang": (9.1183, 48.7196),  # OpenStreetMap
    "Eisborn": (7.8839, 51.3871),  # OpenStreetMap
    # OpenStreetMap, Elbe crossing near Hetlingen
    "Elbe Süd": (9.6373, 53.6077),
    "Ellund": (9.3106, 54.7965),
    "Elten": (6.1622, 51.8720),
    "Emden": (7.2086, 53.3627),
    "Emsbüren": (7.3031, 52.3938),
    "Epe": (7.0379, 52.1834),
    "Essingen": (10.0277, 48.8081),  # OpenStreetMap
    "Eynatten": (6.1233, 50.7135),
    "Finsing": (11.8253, 48.2162),
    "Fockbek": (9.5964, 54.3058),
    "Folmhusen": (7.4788, 53.1741),
    "Ganderkesee": (8.5412, 53.0357),
    "Gernsheim": (8.5112, 49.7466),  # OpenStreetMap
    "Glehn": (6.5788, 51.1657),
    "Grenzhof": (8.5956, 49.4181),  # OpenStreetMap
    "Großkrotzenburg": (8.9501, 50.0892),  # OpenStreetMap, KW Staudinger
    "Haiming": (12.8868, 48.2134),
    "Hamborn": (6.7737, 51.4973),
    "Hamm": (7.8200, 51.6763),
    "Heek": (7.1009, 52.1185),
    "Heidenau": (9.6560, 53.3121),
    "Heilbronn": (9.2113, 49.1423),
    "Hennen": (7.6503, 51.4443),  # OpenStreetMap
    "Herchenrode": (8.7383, 49.7607),  # OpenStreetMap
    "Herne": (7.2200, 51.5380),  # OpenStreetMap
    "Hoeningen": (6.6915, 51.0923),  # OpenStreetMap
    "Hünxe": (6.7660, 51.6415),  # OpenStreetMap
    "Kirrlach": (8.5408, 49.2444),  # OpenStreetMap
    "Klein Offenseth": (9.6840, 53.7847),
    "Kolshorn": (9.9551, 52.4224),
    "Kötz": (10.2750, 48.4061),
    "Lampertheim": (8.4668, 49.6048),
    "Lasów": (15.0294, 51.2284),  # OpenStreetMap
    "Lauchhammer": (13.7401, 51.4978),  # OpenStreetMap
    "Leipheim": (10.2214, 48.4487),  # OpenStreetMap
    "Lindau": (9.7011, 47.5577),
    "Lingen": (7.3187, 52.5252),
    "Lintorf": (6.8312, 51.3333),  # OpenStreetMap
    "Lippendorf": (12.3760, 51.1822),  # OpenStreetMap, KW Lippendorf
    "Lubmin": (13.6150, 54.1341),
    "Medelsheim": (7.2675, 49.1437),
    "Mehrum": (10.1006, 52.3168),  # OpenStreetMap
    "Metterzimmern": (9.1017, 48.9622),  # OpenStreetMap
    "Milzau": (11.9021, 51.3747),
    "Mittelbrunn": (7.5491, 49.3708),  # OpenStreetMap
    "Oberkappel": (13.7705, 48.5528),  # OpenStreetMap
    "Ochtrup": (7.1887, 52.2091),
    "Oude Statenzijl": (7.2036, 53.1725),
    "Paffrath": (7.1007, 51.0006),
    "Porz": (7.0951, 50.8799),  # OpenStreetMap
    "Quarnstedt": (9.7847, 53.9542),
    "Rehden": (8.4828, 52.6110),
    "Reichertsheim": (12.2875, 48.1986),  # OpenStreetMap
    "Rostock": (12.1186, 54.0872),
    "Rothenstadt": (12.1381, 49.6337),
    "Scharenstetten": (9.8496, 48.5142),  # OpenStreetMap
    "Schwandorf": (12.0866, 49.3153),  # OpenStreetMap
    "Stockum": (7.6920, 51.6726),  # OpenStreetMap
    "Stolberg": (6.2308, 50.7914),  # OpenStreetMap
    "Sülstorf": (11.3767, 53.5109),  # OpenStreetMap
    "Tunsel": (7.6695, 47.9039),  # OpenStreetMap
    "Uelsen": (6.8861, 52.4938),  # OpenStreetMap
    "Waidhaus": (12.4952, 49.6428),
    "Wallach": (6.5749, 51.5944),
    "Wallbach": (7.9028, 47.5599),  # OpenStreetMap
    "Wardenburg": (8.1978, 53.0602),
    "Weißensberg": (9.7211, 47.5959),  # OpenStreetMap
    "Werne": (7.6286, 51.6795),
    "Wertingen": (10.6831, 48.5591),
    "Wiernsheim": (8.8986, 48.8854),  # OpenStreetMap
    "Würselen": (6.1341, 50.8179),  # OpenStreetMap
    "Überackern": (12.8794, 48.2047),
}

# Year of the hydrogen network below
NEP_H2_NETWORK_YEAR = 2045

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


# Coordinates (lon, lat) of the end points of NEP_H2_NETWORK_2045
# (h2_grid_nodes.csv, otherwise OpenStreetMap, September 2026)
NEP_H2_NETWORK_2045_NODES = {
    "Aachen": (6.2056, 50.7611),  # OpenStreetMap
    "Achim": (9.0531, 53.0079),
    "Ahlten": (9.9131, 52.3691),
    "Ahrem": (6.7614, 50.7857),  # OpenStreetMap
    "Alsdorf": (6.1621, 50.8772),  # OpenStreetMap
    "Altenlingen": (7.2991, 52.5415),  # OpenStreetMap
    "Amerdingen": (10.4862, 48.7270),  # OpenStreetMap
    "Anwalting": (10.9404, 48.4581),
    "Arresting": (11.7368, 48.8579),  # OpenStreetMap
    "Au am Rhein": (8.2381, 48.9517),  # OpenStreetMap
    "Augsburg": (10.8980, 48.3690),  # OpenStreetMap
    "Bad Bentheim": (7.1609, 52.3014),
    "Bad Godesberg": (7.1563, 50.6851),  # OpenStreetMap
    "Bad Lauchstädt": (11.8675, 51.3867),
    "Barßel": (7.7496, 53.1681),
    "Beckum": (7.8936, 51.3557),  # OpenStreetMap
    "Bernau": (13.5881, 52.6787),  # OpenStreetMap
    "Biemenhorst": (6.6237, 51.8181),  # OpenStreetMap
    "Bierwang": (12.3827, 48.1271),
    "Bischofsheim": (8.3548, 49.9899),  # OpenStreetMap
    "Blumberg": (13.6161, 52.6028),
    "Bocholt": (6.6149, 51.8383),  # OpenStreetMap
    "Bocholtz": (6.0065, 50.8185),
    "Bochum": (7.2197, 51.4818),  # OpenStreetMap
    "Bochum-Westpark": (7.1990, 51.4814),  # OpenStreetMap
    "Bockum": (7.2826, 51.6747),  # OpenStreetMap
    "Bolzum": (9.9446, 52.2968),  # OpenStreetMap
    "Bonn-Rheinaue": (7.1469, 50.7062),  # OpenStreetMap
    "Borken": (6.8583, 51.8445),  # OpenStreetMap
    "Breitbrunn": (12.1539, 48.0429),  # OpenStreetMap
    "Broichweiden": (6.1655, 50.8247),  # OpenStreetMap
    "Brunsbüttel": (9.1376, 53.9003),
    "Brüser Berg": (7.0549, 50.6983),  # OpenStreetMap
    "Buckow": (14.0762, 52.5672),  # OpenStreetMap
    "Bunde": (7.2729, 53.1814),
    "Burghausen": (12.8329, 48.1589),  # OpenStreetMap
    "Börnicke": (13.6383, 52.6627),  # OpenStreetMap
    "Büttgen": (6.6082, 51.1970),  # OpenStreetMap
    "Castrop-Rauxel": (7.3106, 51.5646),  # OpenStreetMap
    "Clenze": (10.9343, 52.9517),  # OpenStreetMap
    "Cloppenburg": (8.0439, 52.8461),  # OpenStreetMap
    "Crailsheim": (10.0720, 49.1366),  # OpenStreetMap
    "Deutschneudorf": (13.4638, 50.6034),
    "Diesenbach": (12.1173, 49.1317),  # OpenStreetMap
    "Dorsten": (6.9643, 51.6599),
    "Dortmund-Scharnhorst": (7.5386, 51.5528),  # OpenStreetMap
    "Drohne": (8.3392, 52.4321),
    "Duisburg": (6.7596, 51.4350),  # OpenStreetMap
    "Duisburg Hafen": (6.7430, 51.4499),  # OpenStreetMap
    "Duisburg-Baerl": (6.6748, 51.4934),  # OpenStreetMap
    "Dörnigheim": (8.8393, 50.1334),  # OpenStreetMap
    "Dülmen": (7.2791, 51.8284),  # OpenStreetMap
    "Düren": (6.4821, 50.8032),  # OpenStreetMap
    "Eckartsweier": (7.8538, 48.5294),  # OpenStreetMap
    "Edesbüttel": (10.6257, 52.4055),
    "Egenstedt": (9.9941, 52.1010),
    "Egmating": (11.7945, 48.0029),  # OpenStreetMap
    "Elbe Nord": (9.6027, 53.6085),  # h2_grid_nodes.csv: Elbe-Nord
    "Elbe Süd": (9.5629, 53.5954),  # h2_grid_nodes.csv: Elbe-Süd
    "Ellund": (9.3106, 54.7965),
    "Elsfleth": (8.4598, 53.2378),
    "Elsterwerda": (13.5205, 51.4615),  # OpenStreetMap
    "Emden": (7.2086, 53.3627),
    "Emsbüren": (7.3031, 52.3938),
    "Ennigerloh": (8.0256, 51.8360),  # OpenStreetMap
    "Epe": (7.0379, 52.1834),
    "Essen-Süd": (7.0231, 51.4393),  # OpenStreetMap
    "Essingen": (10.0277, 48.8081),
    "Etzel": (7.8839, 53.4590),  # OpenStreetMap
    "Euskirchen": (6.7871, 50.6613),  # OpenStreetMap
    "Eynatten": (6.1233, 50.7135),
    "Finsing": (11.8253, 48.2162),
    "Flößberg": (12.5892, 51.1234),  # OpenStreetMap
    "Fockbek": (9.5964, 54.3058),
    "Folmhusen": (7.4788, 53.1741),
    "Forchheim": (11.6834, 48.8267),
    "Frankenthal": (8.3541, 49.5353),  # OpenStreetMap
    "Freienohl": (8.1706, 51.3752),  # OpenStreetMap
    "Friemersheim": (6.7063, 51.3880),  # OpenStreetMap
    "Fröndenberg": (7.7615, 51.4712),  # OpenStreetMap
    "Georgsmarienhütte": (8.0514, 52.2022),
    "Gernsheim": (8.5112, 49.7466),
    "Godesberg": (7.1563, 50.6851),  # OpenStreetMap
    "Gronau": (7.1185, 52.1969),  # OpenStreetMap
    "Groß Gießen": (9.8938, 52.1952),  # OpenStreetMap
    "Gröben": (12.4265, 48.1282),  # OpenStreetMap
    "Gut Melschede": (7.9268, 51.3557),  # OpenStreetMap
    # OpenStreetMap, Fröndenberg-Ostbüren (ambiguous)
    "Haarweg": (7.7633, 51.4988),
    "Hallendorf": (10.3776, 52.1533),  # OpenStreetMap
    "Hamborn": (6.7737, 51.4973),
    "Heist": (9.6582, 53.6523),
    "Helmste": (9.4825, 53.5227),  # OpenStreetMap
    "Herchenrode": (8.7383, 49.7607),
    "Herne": (7.2200, 51.5380),
    "Herne Hochlarmark": (7.1853, 51.5688),  # OpenStreetMap
    "Herzogenrath": (6.0951, 50.8685),  # OpenStreetMap
    "Hiltrop": (7.2596, 51.5154),  # OpenStreetMap
    "Hinterschwarzenberg": (10.4650, 47.6753),  # OpenStreetMap
    "Hittistetten": (10.0963, 48.3284),
    "Holzwickede": (7.6186, 51.5007),  # OpenStreetMap
    "Homberg (Duisburg)": (6.7038, 51.4542),  # OpenStreetMap
    "Huntorf": (8.3870, 53.1951),
    "Hövel": (7.9255, 51.3691),  # OpenStreetMap
    "Hügelheim": (7.6215, 47.8290),  # OpenStreetMap
    "Hünxe": (6.7660, 51.6415),
    "Hüsingen": (7.7403, 47.6307),  # OpenStreetMap
    "Ingolstadt": (11.4250, 48.7630),  # OpenStreetMap
    "Inzenham": (12.2282, 47.8806),  # OpenStreetMap
    "Karlsruhe": (8.4058, 49.0070),
    "Katzdorf": (12.1026, 49.2461),  # OpenStreetMap
    "Kempten": (10.3169, 47.7267),  # OpenStreetMap
    "Kiefersfelden": (12.1888, 47.6138),  # OpenStreetMap
    "Kienbaum": (13.9580, 52.4478),
    "Kissing": (10.9895, 48.2979),  # OpenStreetMap
    "Klein Offenseth": (9.6840, 53.7847),
    "Kolshorn": (9.9551, 52.4224),
    "Kötz": (10.2750, 48.4061),
    "Laer": (7.2716, 51.4690),  # OpenStreetMap
    "Lampertheim": (8.4668, 49.6048),
    "Langerwehe": (6.3681, 50.8020),  # OpenStreetMap
    "Lauchhammer": (13.7401, 51.4978),
    "Leer": (7.4568, 53.2332),  # OpenStreetMap
    "Legden": (7.1049, 52.0308),  # h2_grid_nodes.csv: Ledgen
    "Lemförde": (8.3747, 52.4658),
    "Lenglern": (9.8734, 51.5861),  # OpenStreetMap
    "Lindau": (9.7011, 47.5577),
    "Lintorf": (6.8312, 51.3333),
    "Löchgau": (9.1099, 49.0013),
    "Marl": (7.1030, 51.6807),
    "Mengede": (7.3670, 51.5749),  # OpenStreetMap
    "Merzdorf": (13.0183, 50.9212),  # OpenStreetMap
    "Michelbach": (10.1173, 49.2358),  # OpenStreetMap
    "Mittelbrunn": (7.5510, 49.3723),
    "Münchsmünster": (11.6877, 48.7610),
    "Neukirchen": (6.6818, 51.1225),  # OpenStreetMap
    "Neuss": (6.6916, 51.1982),  # OpenStreetMap
    "Niederbonsfeld": (7.1310, 51.3875),  # OpenStreetMap
    "Niedereimer": (8.0474, 51.4205),  # OpenStreetMap
    "Niederhohndorf": (12.4675, 50.7522),
    "Niederschelden": (7.9706, 50.8445),  # OpenStreetMap
    "Nüttermoor": (7.4352, 53.2622),
    "Oberisling": (12.1056, 48.9834),  # OpenStreetMap
    "Oberkappel": (13.7705, 48.5528),
    "Obermichelbach": (10.9106, 49.5274),  # OpenStreetMap
    "Oberpframmern": (11.8136, 48.0220),  # OpenStreetMap
    "Ochtrup": (7.1887, 52.2091),
    "Oelde": (8.1455, 51.8261),  # OpenStreetMap
    "Oer Erkenschwick": (7.2606, 51.6399),  # OpenStreetMap
    "Olfen": (7.3788, 51.7076),  # OpenStreetMap
    "Olpe": (8.1655, 51.3557),  # OpenStreetMap
    "Oude Statenzijl": (7.2036, 53.1725),
    "Peine": (10.2345, 52.3216),
    "Pfronten": (10.5580, 47.5814),  # OpenStreetMap
    "Plackweg": (8.1372, 51.4131),  # OpenStreetMap
    "Quarnstedt": (9.7847, 53.9542),
    "Querenburg": (7.2712, 51.4500),  # OpenStreetMap
    "Radevormwald": (7.3571, 51.2029),  # OpenStreetMap
    "Rapen": (7.2790, 51.6445),  # OpenStreetMap
    "Rastede": (8.1984, 53.2504),
    "Recklinghausen": (7.1986, 51.6132),
    "Reckrod": (9.7951, 50.7749),
    "Regensburg": (12.0975, 49.0195),  # OpenStreetMap
    "Rehden": (8.4828, 52.6110),
    "Reinhardshofen": (10.6514, 49.6251),  # OpenStreetMap
    "Reiningen": (8.3419, 52.4467),
    "Remich": (6.3676, 49.5442),  # OpenStreetMap
    "Rimpar": (9.9619, 49.8555),
    "Roelsdorf": (6.4623, 50.7901),  # OpenStreetMap
    "Ronneburg": (12.1809, 50.8631),  # OpenStreetMap
    "Rothenstadt": (12.1381, 49.6337),
    "Rückersdorf": (12.2189, 50.8217),
    "Sannerz": (9.5907, 50.3224),  # OpenStreetMap
    "Sayda": (13.4194, 50.7124),  # OpenStreetMap
    "Scharenstetten": (9.8496, 48.5142),
    "Schlüchtern": (9.5613, 50.3517),  # OpenStreetMap
    "Schnaitsee": (12.3665, 48.0699),
    "Schwarzach": (8.0460, 48.7471),  # OpenStreetMap
    "Selikum": (6.7028, 51.1738),  # OpenStreetMap
    "Sonsbeck": (6.3760, 51.6094),  # OpenStreetMap
    "Spreetal": (14.3396, 51.5052),
    "St. Hubert": (6.4515, 51.3825),
    "Stade": (9.4960, 53.5791),
    "Steinbrink": (8.7387, 52.4788),  # OpenStreetMap
    "Steinfeld": (8.2171, 52.5874),  # OpenStreetMap
    "Steinitz": (11.1146, 52.8253),  # OpenStreetMap
    "Stiepel": (7.2409, 51.4273),  # OpenStreetMap
    "Stockum": (7.6920, 51.6726),
    "Stolberg": (6.2308, 50.7914),
    "Telgte": (7.7849, 51.9835),  # OpenStreetMap
    "Uelde": (8.3152, 51.5153),  # OpenStreetMap
    "Ueldener Haar": (8.3152, 51.5153),  # OpenStreetMap
    "Uellendahl": (7.1558, 51.2806),  # OpenStreetMap
    "Ulm": (9.9912, 48.3985),  # OpenStreetMap
    "Vinnhorst": (9.7053, 52.4181),
    "Visbek": (8.3120, 52.8336),  # h2_grid_nodes.csv: Visbeck
    "Vitzeroda": (10.0710, 50.8885),  # OpenStreetMap
    "Voerde": (6.6846, 51.6024),  # OpenStreetMap
    "Voigtei": (8.9207, 52.6056),
    "Waidhaus": (12.4952, 49.6428),
    "Waldenburg": (9.6430, 49.1894),  # OpenStreetMap
    "Wallbach": (7.9028, 47.5599),
    "Walldorf": (8.5818, 50.0045),  # OpenStreetMap
    "Walle": (10.4497, 52.3382),  # OpenStreetMap
    "Wardenburg": (8.1978, 53.0602),
    "Weier": (7.9191, 48.4973),  # OpenStreetMap
    "Weine": (8.5112, 51.5418),  # OpenStreetMap
    "Weisweiler": (6.3166, 50.8280),
    "Werne": (7.6286, 51.6795),
    "Wertingen": (10.6831, 48.5591),
    "Wettringen": (7.3142, 52.2095),
    "Wickede": (7.8696, 51.4960),  # OpenStreetMap
    "Wiernsheim": (8.8986, 48.8854),
    "Wilhelmshaven": (8.1141, 53.5297),
    "Willstätt": (7.8935, 48.5415),  # OpenStreetMap
    "Windberg": (12.7459, 48.9413),  # OpenStreetMap
    "Wirtheim": (9.2639, 50.2227),  # OpenStreetMap
    "Witten": (7.3351, 51.4370),  # OpenStreetMap
    "Wolfersberg": (11.8140, 48.0483),  # OpenStreetMap
    "Zons": (6.8448, 51.1232),  # OpenStreetMap
    "Zopp": (6.1375, 50.8690),  # OpenStreetMap
}

# Commissioning year of the H2 measures beyond the core network (p. 171)
NEP_H2_MEASURES_YEAR = 2036

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


# H2 measures up to 2037 beyond the core network (Anlage 3): (NEP id, start,
# end, kind, length [km], DN, DP [bar]); lengths and DN/DP are partly
# derived, see the documentation of the gas grids
NEP_H2_MEASURES_2037 = [
    (
        "H2-1201",
        "Strohreit",
        "Reitmehring",
        "new",
        4.5,
        400,
        70,
    ),  # default
    (
        "H2-1202",
        "Wertingen",
        "Augsburg/Klärwerk",
        "new",
        26.3,
        600,
        70,
    ),  # default
    (
        "H2-1214",
        "Broichweiden",
        "Aachen",
        "new",
        8.8,
        400,
        70,
    ),  # default
    (
        "H2-1215",
        "Kirchpütz",
        "Weisweiler",
        "new",
        7.1,
        400,
        70,
    ),  # default
    (
        "H2-1216",
        "Breinig",
        "Broichweiden",
        "new",
        12.8,
        400,
        70,
    ),  # default
    (
        "H2-1218",
        "Nievenheim",
        "Neuss",
        "new",
        12.4,
        400,
        70,
    ),  # default
    (
        "H2-1219",
        "Neuss",
        "Neuss Hafen",
        "new",
        1.7,
        400,
        70,
    ),  # default
    (
        "H2-203",
        "Deffingen",
        "Wasserburg",
        "new",
        2.2,
        400,
        70,
    ),  # default
    (
        "H2-205",
        "Münchsmünster",
        "Regensburg",
        "new",
        48.4,
        600,
        70,
    ),  # default
    ("H2-206", "Ulm", "Augsburg", "new", 78.2, 400, 68),  # SciGRID
    ("H2-209", "Kötz", "Günzburg", "new", 8.1, 400, 70),  # default
    ("H2-231", "Freiburg", "Weier", "new", 60.6, 600, 70),  # default
    (
        "H2-233",
        "Uedener Bruch",
        "Wardt",
        "conversion",
        6.8,
        200,
        70,
    ),  # KLU121-01
    ("H2-244", "Aachen", "Soers", "new", 10.7, 400, 70),  # default
    (
        "H2-211",
        "Kadeltshofen",
        "Weißenhorn",
        "conversion",
        9.8,
        400,
        70,
    ),  # default
    (
        "H2-213",
        "Weißenhorn",
        "Bellenberg",
        "conversion",
        8.5,
        400,
        70,
    ),  # default
    (
        "H2-218",
        "Birlinghoven",
        "Beuel",
        "conversion",
        8.0,
        400,
        70,
    ),  # default
    (
        "H2-224",
        "Dernbach Elgendorf",
        "Bendorf",
        "conversion",
        18.3,
        600,
        70,
    ),  # default
    (
        "H2-226",
        "Niederkassel",
        "Wesseling",
        "conversion",
        5.1,
        400,
        70,
    ),  # default
    (
        "H2-248",
        "Dormagen",
        "Nievenheim",
        "conversion",
        5.7,
        400,
        70,
    ),  # default
    (
        "H2-249",
        "Neuss Hafen",
        "Düsseldorf Hafen",
        "conversion",
        2.8,
        400,
        70,
    ),  # default
    (
        "H2-1203",
        "Augsburg Nord",
        "Augsburg Mitte",
        "new",
        2.9,
        400,
        70,
    ),  # default
    (
        "H2-1207",
        "Elbe Süd",
        "Achim",
        "new",
        85.7,
        1000,
        70,
    ),  # SciGRID
    (
        "H2-1208",
        "Brunsbüttel",
        "Heist",
        "new",
        51.2,
        600,
        70,
    ),  # default
    ("H2-1209", "Heist", "Elbe Nord", "new", 7.1, 400, 70),  # default
    (
        "H2-1212",
        "Borken",
        "Borken-Gemen",
        "new",
        2.1,
        400,
        70,
    ),  # default
    (
        "H2-208",
        "Einmuß",
        "Kelheim",
        "conversion",
        8.2,
        400,
        70,
    ),  # default
    (
        "H2-210",
        "Breitbrunn",
        "Bierwang",
        "conversion",
        18.6,
        600,
        70,
    ),  # default
    (
        "H2-216",
        "Mittelbrunn",
        "Au am Rhein",
        "conversion",
        79.0,
        1000,
        68,
    ),  # SciGRID
    (
        "H2-219",
        "Drohne",
        "Werne",
        "conversion",
        111.8,
        600,
        70,
    ),  # default
    (
        "H2-220",
        "Lauterbach",
        "Scheidt",
        "conversion",
        124.6,
        900,
        70,
    ),  # SciGRID
    (
        "H2-221",
        "Büchelberg",
        "Karlsruhe",
        "conversion",
        10.4,
        400,
        70,
    ),  # default
    (
        "H2-222",
        "Etzel",
        "Wardenburg",
        "conversion",
        57.0,
        1000,
        84,
    ),  # SciGRID
    (
        "H2-223",
        "Wardenburg",
        "Drohne",
        "conversion",
        75.2,
        900,
        70,
    ),  # SciGRID
    (
        "H2-227",
        "Lauterbach",
        "Vitzeroda",
        "conversion",
        67.6,
        800,
        80,
    ),  # SciGRID
    (
        "H2-228",
        "Schwandorf",
        "Windberg",
        "conversion",
        71.9,
        1000,
        68,
    ),  # SciGRID
    (
        "H2-229",
        "Steinitz",
        "Wedringen",
        "conversion",
        73.1,
        900,
        55,
    ),  # SciGRID
    (
        "H2-230",
        "Au am Rhein",
        "Leonberg",
        "conversion",
        73.7,
        350,
        62,
    ),  # SciGRID
    (
        "H2-232",
        "Kirchhausen",
        "Waldenburg",
        "conversion",
        45.9,
        600,
        70,
    ),  # default
    ("H2-234", "Wardt", "Vissel", "new", 4.2, 400, 70),  # default
    ("H2-235", "Wardt", "Vissel", "new", 4.2, 400, 70),  # default
    (
        "H2-236",
        "Vissel",
        "Hamminkeln",
        "new",
        9.4,
        400,
        70,
    ),  # default
    ("H2-237", "Praest", "Bocholt", "new", 21.7, 600, 70),  # default
    (
        "H2-238",
        "Hamminkeln",
        "Coesfeld",
        "new",
        54.2,
        600,
        70,
    ),  # default
    (
        "H2-243",
        "Broichweiden",
        "Stolberg",
        "new",
        6.9,
        400,
        70,
    ),  # default
    (
        "H2-245",
        "Kirchpütz",
        "Alsdorf",
        "new",
        8.7,
        400,
        70,
    ),  # default
    ("H2-250", "Voerde", "Hünxe", "new", 8.3, 400, 70),  # default
]


@lru_cache(maxsize=None)
def nep_h2_measures_commissioning():
    """
    Commissioning year of the H2 measures beyond the core network

    BNetzA list "Inbetriebnahmedaten" (June 2026); measures without a date
    get NEP_H2_MEASURES_YEAR.

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


# Coordinates (lon, lat) of the end points of NEP_H2_MEASURES_2037
# (h2_grid_nodes.csv or the lists above, otherwise OpenStreetMap)
NEP_H2_MEASURES_2037_NODES = {
    "Aachen": (6.2056, 50.7611),
    "Achim": (9.0531, 53.0079),
    "Alsdorf": (6.1621, 50.8772),
    "Au am Rhein": (8.2381, 48.9517),
    "Augsburg": (10.8980, 48.3690),
    "Augsburg Mitte": (10.8979, 48.3690),  # OpenStreetMap
    "Augsburg Nord": (10.8763, 48.3860),  # OpenStreetMap
    "Augsburg/Klärwerk": (10.8841, 48.4055),  # OpenStreetMap
    "Bellenberg": (10.0939, 48.2562),  # OpenStreetMap
    "Bendorf": (7.5733, 50.4238),  # OpenStreetMap
    "Beuel": (7.1245, 50.7387),  # OpenStreetMap
    "Bierwang": (12.3827, 48.1271),
    "Birlinghoven": (7.2198, 50.7536),
    "Bocholt": (6.6149, 51.8383),
    "Borken": (6.8583, 51.8445),
    "Borken-Gemen": (6.8643, 51.8606),  # OpenStreetMap
    "Breinig": (6.2172, 50.7318),  # OpenStreetMap
    "Breitbrunn": (12.1539, 48.0429),
    "Broichweiden": (6.1655, 50.8247),
    "Brunsbüttel": (9.1376, 53.9003),
    "Büchelberg": (8.1719, 49.0208),  # OpenStreetMap
    "Coesfeld": (7.1667, 51.9500),
    "Deffingen": (10.2967, 48.4354),  # OpenStreetMap
    "Dernbach Elgendorf": (7.7885, 50.4565),  # OpenStreetMap
    "Dormagen": (6.8303, 51.0939),
    "Drohne": (8.3392, 52.4321),
    "Düsseldorf Hafen": (6.7336, 51.2170),  # OpenStreetMap
    "Einmuß": (11.9446, 48.8509),  # OpenStreetMap
    "Elbe Nord": (9.6027, 53.6085),
    "Elbe Süd": (9.5629, 53.5954),
    "Etzel": (7.8839, 53.4590),
    "Freiburg": (7.8132, 48.0341),
    "Günzburg": (10.2745, 48.4690),  # OpenStreetMap
    "Hamminkeln": (6.5909, 51.7307),  # OpenStreetMap
    "Heist": (9.6582, 53.6523),
    "Hünxe": (6.7660, 51.6415),
    "Kadeltshofen": (10.1495, 48.3798),  # OpenStreetMap
    "Karlsruhe": (8.4058, 49.0070),
    "Kelheim": (11.8723, 48.9185),  # OpenStreetMap
    "Kirchhausen": (9.1103, 49.1845),  # OpenStreetMap
    "Kirchpütz": (6.2295, 50.8249),  # OpenStreetMap
    "Kötz": (10.2750, 48.4061),
    "Lauterbach": (9.3675, 50.6454),  # OpenStreetMap
    "Leonberg": (9.0150, 48.8013),  # OpenStreetMap
    "Mittelbrunn": (7.5510, 49.3723),
    "Münchsmünster": (11.6877, 48.7610),
    "Neuss": (6.6916, 51.1982),
    "Neuss Hafen": (6.7009, 51.2106),  # OpenStreetMap
    "Niederkassel": (7.0388, 50.8161),
    "Nievenheim": (6.7684, 51.1153),  # OpenStreetMap
    "Praest": (6.3446, 51.8232),
    "Regensburg": (12.0975, 49.0195),
    "Reitmehring": (12.1894, 48.0612),  # OpenStreetMap
    "Scheidt": (7.9066, 50.3470),  # OpenStreetMap
    "Schwandorf": (12.0866, 49.3153),
    "Soers": (6.0878, 50.7975),  # OpenStreetMap
    "Steinitz": (11.1146, 52.8253),
    "Stolberg": (6.2308, 50.7914),
    "Strohreit": (12.2174, 48.0907),  # OpenStreetMap
    "Uedener Bruch": (6.3253, 51.6538),
    "Ulm": (9.9912, 48.3985),
    "Vissel": (6.4846, 51.7001),  # OpenStreetMap
    "Vitzeroda": (10.0710, 50.8885),
    "Voerde": (6.6846, 51.6024),
    "Waldenburg": (9.6430, 49.1894),
    "Wardenburg": (8.1978, 53.0602),
    "Wardt": (6.4350, 51.6902),
    "Wasserburg": (10.2704, 48.4363),  # OpenStreetMap
    "Wedringen": (11.4658, 52.2734),
    "Weier": (7.9191, 48.4973),
    "Weisweiler": (6.3166, 50.8280),
    "Weißenhorn": (10.1601, 48.3045),  # OpenStreetMap
    "Werne": (7.6286, 51.6795),
    "Wertingen": (10.6831, 48.5591),
    "Wesseling": (6.9785, 50.8254),
    "Windberg": (12.7459, 48.9413),
}


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


# Additional withdrawal in GWh/h (Brennwert) of scenario 2 in the load case
# "Dunkelflaute" of 2037 (NEP p. 110: 2.4 GW from four storage sites)
NEP_H2_STORAGE_DUNKELFLAUTE = {2037: 2.4}

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


# Federal states of the four "Dunkelflaute" storage sites (p. 110)
NEP_H2_STORAGE_DUNKELFLAUTE_STATES = [
    "Bayern",
    "Brandenburg/Berlin",
    "Niedersachsen/Bremen",
    "Thüringen",
]


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


# Sites (x, y) of the other H2 imports per federal state: LNG terminals in
# the coastal states, otherwise the largest chemical site (assumption)
NEP_H2_OTHER_IMPORT_SITES = {
    "Niedersachsen/Bremen": {
        "Wilhelmshaven": (8.114101, 53.52972),
        "Stade": (9.496045, 53.579073),
    },
    "Schleswig-Holstein/Hamburg": {"Brunsbüttel": (9.137629, 53.900299)},
    "Mecklenburg-Vorpommern": {
        "Rostock": (12.118621, 54.087185),
        "Lubmin": (13.615004, 54.13412),
    },
    "Nordrhein-Westfalen": {"Marl": (7.103009, 51.680693)},
    "Sachsen-Anhalt": {"Leuna": (12.019319, 51.323344)},
    "Brandenburg/Berlin": {"Schwedt": (14.24524, 53.090217)},
    "Sachsen": {"Böhlen": (12.387644, 51.201337)},
    "Bayern": {"Burghausen": (12.8331, 48.1693)},
}

# Sites (x, y) of the LNG terminals for methane
NEP_CH4_LNG_TERMINAL_SITES = {
    "Wilhelmshaven": (8.114101, 53.52972),
    "Brunsbüttel": (9.137629, 53.900299),
    "Stade": (9.496045, 53.579073),
    "Lubmin": (13.615004, 54.13412),
    "Mukran": (13.5856, 54.4937),
    "Rostock": (12.118621, 54.087185),
}


def nep_ch4_lng_terminals():
    """
    LNG terminals for methane: clusters of the approval of the
    Szenariorahmen (pp. 87-89) shared equally by their terminals

    Returns
    -------
    list of tuple
        (name, x, y, capacity [GWh/h])

    """
    capacity = dict.fromkeys(NEP_CH4_LNG_TERMINAL_SITES, 0)
    for terminals, cluster in _rows(
        "LNG_Cluster_CH4", ["terminals", "capacity"]
    ):
        terminals = terminals.split(", ")
        for terminal in terminals:
            capacity[terminal] += cluster / len(terminals)
    return [
        (name, x, y, capacity[name])
        for name, (x, y) in NEP_CH4_LNG_TERMINAL_SITES.items()
    ]


def nep_ch4_lng_use():
    """
    Share of the LNG capacity used in the peak load case (Tabelle 19)

    Returns
    -------
    dict
        Share per year

    """
    return dict(_rows("LNG_Nutzung_CH4", ["year", "share"]))
