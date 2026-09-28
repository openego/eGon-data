"""
Data transcribed by hand from the Netzentwicklungsplan Gas und Wasserstoff 2025

Revised draft of 1 June 2026 and its annexes:
https://ko-nep.de/wp-content/uploads/2024/02/2026_06_01_Ueberarbeiteter_Entwurf_NEP_Gas_Wasserstoff_2025.pdf
Each constant names its table or annex; the use of the data is described in
the documentation of the gas grids, supply and stores.
"""

# Kernnetz measures dropped by the NEP without replacement (Anhang 2; the
# dropped KLN001-01, KLN025-01 and KLU045-01 are replaced and kept as proxies)
DROPPED_KERNNETZ_MEASURES = []

# Commissioning year of the Kernnetz measures (Anlage 1b), NEP id in the
# comment; measures without an entry keep the year of the FNB-Gas list
KERNNETZ_COMMISSIONING_YEAR = {
    "KLN002-01": 2035,  # H2-1002
    "KLN003-01": 2034,  # H2-1003
    "KLN004-01": 2028,  # H2-1004
    "KLN005-01": 2035,  # H2-1005
    "KLN006-01": 2035,  # H2-1006
    "KLN007-01": 2035,  # H2-1007
    "KLN008-01": 2028,  # H2-1008
    "KLN010-01": 2030,  # H2-1010
    "KLN011-01": 2032,  # H2-1011
    "KLN012-01": 2032,  # H2-1012
    "KLN013-01": 2029,  # H2-1013
    "KLN014-01": 2030,  # H2-1014
    "KLN015-01": 2027,  # H2-1015
    "KLN016-01": 2027,  # H2-1016
    "KLN017-01": 2031,  # H2-1017
    "KLN019-01": 2029,  # H2-1019
    "KLN020-01": 2031,  # H2-1020
    "KLN021-01": 2029,  # H2-1021
    "KLN022-01": 2033,  # H2-1022
    "KLN023-01": 2028,  # H2-1023
    "KLN026-01": 2035,  # H2-1026
    "KLN027-01": 2030,  # H2-1027
    "KLN028-01": 2033,  # H2-1028
    "KLN029-01": 2032,  # H2-1029
    "KLN030-01": 2034,  # H2-1030
    "KLN033-01": 2027,  # H2-1033
    "KLN034-01": 2027,  # H2-1034
    "KLN035-01": 2027,  # H2-1035
    "KLN036-01": 2027,  # H2-1036
    "KLN037-01": 2030,  # H2-1037
    "KLN038-01": 2030,  # H2-1038
    "KLN039-01": 2035,  # H2-1039
    "KLN040-01": 2035,  # H2-1040
    "KLN042-01": 2035,  # H2-1042
    "KLN043-01": 2035,  # H2-1043
    "KLN044-01": 2032,  # H2-1044
    "KLN045-01": 2030,  # H2-1045
    "KLN046-01": 2030,  # H2-1046
    "KLN047-01": 2032,  # H2-1047
    "KLN048-01": 2025,  # H2-1048
    "KLN049-01": 2027,  # H2-1049
    "KLN050-01": 2027,  # H2-1050
    "KLN052-01": 2035,  # H2-1052
    "KLN053-01": 2035,  # H2-1053
    "KLN054-01": 2035,  # H2-1054
    "KLN055-01": 2034,  # H2-1055
    "KLN056-01": 2034,  # H2-1056
    "KLN057-01": 2033,  # H2-1057
    "KLN058-01": 2034,  # H2-1058
    "KLN059-01": 2034,  # H2-1059
    "KLN060-01": 2033,  # H2-1060
    "KLN061-01": 2034,  # H2-1061
    "KLN062-01": 2034,  # H2-1062
    "KLN063-01": 2034,  # H2-1063
    "KLN064-01": 2029,  # H2-1064
    "KLN065-01": 2029,  # H2-1065
    "KLN066-01": 2027,  # H2-1066
    "KLN067-01": 2028,  # H2-1067
    "KLN073-01": 2033,  # H2-1073
    "KLN074-01": 2029,  # H2-1074
    "KLN075-01": 2029,  # H2-1075
    "KLN076-01": 2034,  # H2-1076
    "KLN077-01": 2034,  # H2-1077
    "KLN078-01": 2034,  # H2-1078
    "KLN079-01": 2034,  # H2-1079
    "KLN081-01": 2034,  # H2-1081
    "KLN082-01": 2030,  # H2-1082
    "KLN083-01": 2033,  # H2-1083
    "KLN084-01": 2030,  # H2-1084
    "KLN085-01": 2027,  # H2-1085
    "KLN086-01": 2029,  # H2-1086
    "KLN087-01": 2032,  # H2-1087
    "KLN088-01": 2028,  # H2-1088
    "KLN089-01": 2030,  # H2-1089
    "KLN090-01": 2030,  # H2-1090
    "KLN091-01": 2030,  # H2-1091
    "KLN092-01": 2035,  # H2-1092
    "KLN093-01": 2030,  # H2-1093
    "KLN094-01": 2035,  # H2-1094
    "KLN095-01": 2030,  # H2-1095
    "KLN096-01": 2035,  # H2-1096
    "KLN097-01": 2035,  # H2-1097
    "KLN098-01": 2035,  # H2-1098
    "KLN099-01": 2030,  # H2-1099
    "KLN100-01": 2035,  # H2-1100
    "KLN102-01": 2033,  # H2-1102
    "KLN103-01": 2033,  # H2-1103
    "KLN104-01": 2035,  # H2-1104
    "KLN105-01": 2025,  # H2-1105
    "KLN107-01": 2029,  # H2-1107
    "KLU001-01": 2032,  # H2-001
    "KLU002-01": 2028,  # H2-002
    "KLU003-01": 2032,  # H2-003
    "KLU004-01": 2026,  # H2-004
    "KLU005-01": 2032,  # H2-005
    "KLU006-01": 2030,  # H2-006
    "KLU007-01": 2032,  # H2-007
    "KLU008-01": 2032,  # H2-008
    "KLU009-01": 2028,  # H2-009
    "KLU010-01": 2030,  # H2-010
    "KLU011-01": 2035,  # H2-011
    "KLU012-01": 2028,  # H2-012
    "KLU013-01": 2030,  # H2-023
    "KLU014-01": 2030,  # H2-023
    "KLU023-01": 2030,  # H2-023
    "KLU024-01": 2030,  # H2-024
    "KLU027-01": 2027,  # H2-027
    "KLU029-01": 2029,  # H2-029
    "KLU030-01": 2029,  # H2-030
    "KLU031-01": 2027,  # H2-031
    "KLU032-01": 2027,  # H2-032
    "KLU033-01": 2027,  # H2-033
    "KLU034-01": 2034,  # H2-034
    "KLU035-01": 2034,  # H2-035
    "KLU036-01": 2032,  # H2-036
    "KLU037-01": 2027,  # H2-038
    "KLU038-01": 2027,  # H2-038
    "KLU039-01": 2031,  # H2-050
    "KLU040-01": 2029,  # H2-040
    "KLU041-01": 2034,  # H2-041
    "KLU042-01": 2029,  # H2-042
    "KLU043-01": 2027,  # H2-043
    "KLU044-01": 2027,  # H2-044
    "KLU046-01": 2029,  # H2-046
    "KLU047-01": 2034,  # H2-047
    "KLU048-01": 2029,  # H2-048
    "KLU049-01": 2029,  # H2-049
    "KLU050-01": 2031,  # H2-050
    "KLU051-01": 2026,  # H2-051
    "KLU052-01": 2026,  # H2-052
    "KLU056-01": 2036,  # H2-056
    "KLU059-01": 2032,  # H2-059
    "KLU062-01": 2036,  # H2-062
    "KLU063-01": 2032,  # H2-063
    "KLU064-01": 2032,  # H2-064
    "KLU065-01": 2035,  # H2-065
    "KLU066-01": 2025,  # H2-066
    "KLU067-01": 2035,  # H2-067
    "KLU068-01": 2035,  # H2-068
    "KLU069-01": 2035,  # H2-069
    "KLU070-01": 2030,  # H2-070
    "KLU071-01": 2030,  # H2-071
    "KLU072-01": 2030,  # H2-072
    "KLU073-01": 2032,  # H2-073
    "KLU074-01": 2031,  # H2-074
    "KLU075-01": 2032,  # H2-075
    "KLU076-01": 2032,  # H2-076
    "KLU077-01": 2032,  # H2-077
    "KLU078-01": 2032,  # H2-078
    "KLU080-01": 2032,  # H2-080
    "KLU081-01": 2032,  # H2-081
    "KLU082-01": 2032,  # H2-082
    "KLU083-01": 2032,  # H2-083
    "KLU084-01": 2034,  # H2-084
    "KLU085-01": 2034,  # H2-085
    "KLU086-01": 2032,  # H2-086
    "KLU087-01": 2025,  # H2-087
    "KLU088-01": 2027,  # H2-088
    "KLU089-01": 2030,  # H2-089
    "KLU090-01": 2030,  # H2-090
    "KLU091-01": 2030,  # H2-091
    "KLU092-01": 2028,  # H2-092
    "KLU093-01": 2028,  # H2-093
    "KLU094-01": 2028,  # H2-094
    "KLU095-01": 2028,  # H2-095
    "KLU096-01": 2027,  # H2-096
    "KLU098-01": 2028,  # H2-098
    "KLU099-01": 2029,  # H2-099
    "KLU100-01": 2029,  # H2-100
    "KLU101-01": 2029,  # H2-101
    "KLU102-01": 2029,  # H2-102
    "KLU103-01": 2029,  # H2-103
    "KLU104-01": 2029,  # H2-104
    "KLU105-01": 2029,  # H2-105
    "KLU106-01": 2029,  # H2-106
    "KLU108-01": 2027,  # H2-108
    "KLU110-01": 2028,  # H2-110
    "KLU112-01": 2028,  # H2-112
    "KLU113-01": 2034,  # H2-113
    "KLU114-01": 2030,  # H2-114
    "KLU115-01": 2030,  # H2-115
    "KLU116-01": 2030,  # H2-116
    "KLU117-01": 2030,  # H2-117
    "KLU118-01": 2035,  # H2-118
    "KLU119-01": 2030,  # H2-119
    "KLU120-01": 2031,  # H2-120
    "KLU122-01": 2030,  # H2-122
    "KLU124-01": 2027,  # H2-124
    "KLU125-01": 2032,  # H2-125
    "KLU126-01": 2028,  # H2-126
    "KLU127-01": 2030,  # H2-127
    "KLU128-01": 2030,  # H2-128
    "KLU129-01": 2029,  # H2-129
    "KLU130-01": 2027,  # H2-130
    "KLU131-01": 2028,  # H2-131
    "KLU137-01": 2028,  # H2-137
    "KLU139-01": 2034,  # H2-139
    "KLU140-01": 2030,  # H2-140
    "KLU141-01": 2028,  # H2-141
    "KLU142-01": 2028,  # H2-142
    "KLU143-01": 2030,  # H2-143
    "KVS003-01": 2032,  # H2-2003
    "KVS007-01": 2034,  # H2-2109
}

# Sections of the methane network 2045: (number in Anhang 5, start,
# end, length in km, nominal diameter DN in mm as given in the NEP)
NEP_CH4_NETWORK_2045 = [
    (1, "Haiming", "Finsing", 87, "1.200"),  # Burghausen (Haiming)-Finsing
    (2, "Finsing", "Anwalting", 82, "900"),
    (3, "Wertingen", "Kötz", 41, "700"),
    (4, "Kötz", "Leipheim", 2, "450"),  # Kötz-Abzweig KW Leipheim
    (5, "Rehden", "Lubmin", 441, "1.400"),
    (6, "Lubmin", "Brandov", 480, "1.400"),
    (7, "Rehden", "Drohne", 26, "1.000"),
    (8, "Emsbüren", "Hünxe", 96, "1.000"),
    (9, "Epe", "Heek", 8, "600"),
    (10, "Hünxe", "Hamborn", 32, "600/ 700"),
    (11, "Hamborn", "Lintorf", 20, "600"),
    (12, "Lintorf", "Glehn", 33, "500"),
    (13, "Bocholtz", "Glehn", 78, "300/ 400/ 500"),
    (14, "Elten", "Paffrath", 154, "800-1.000"),
    (15, "Epe", "Ochtrup", 12, "600"),
    (16, "Wallach", "Binsheim", 18, "600"),
    (17, "Binsheim", "Hamborn", 6, "600"),
    (18, "Wallach", "Appeldorn", 20, "400"),
    (19, "Ochtrup", "Datteln", 71, "600"),
    (20, "Datteln", "Herne", 23, "600"),
    (21, "Hoeningen", "Bergheim", 23, "400 und 400/ 600"),
    # Lauchhammer-Lasow (Pl)
    (22, "Lauchhammer", "Lasów", 114, "500/ 600/ 800"),
    (23, "Lauchhammer", "Cörmigk", 143, "600/ 750/ 800/ 900"),
    (24, "Cörmigk", "Bernburg", 9, "750"),  # Cörmigk-UGS Bernburg
    (25, "Cörmigk", "Milzau", 40, "750"),
    # Milzau-Böhlen/ Lippendorf
    (26, "Milzau", "Lippendorf", 55, "500/ 600/ 800/ 900"),
    (27, "Milzau", "Bad Lauchstädt", 8, "600"),  # Milzau-UGS Bad Lauchstädt
    (28, "Sülstorf", "Rostock", 113, "400/ 500"),
    (29, "Überackern", "Haiming", 1, "700"),
    (30, "Dornum", "Wardenburg", 106, "1.200"),
    (31, "Wardenburg", "Werne", 197, "900-1.200"),
    (32, "Werne", "Dorsten", 88, "800-1.000"),
    (33, "Eynatten", "Würselen", 13, "900"),
    (34, "Würselen", "Porz", 83, "800"),
    (35, "Werne", "Paffrath", 106, "800-1.000"),
    (36, "Werne", "Stockum", 10, "600"),
    (37, "Hennen", "Eisborn", 23, "600"),
    (38, "Eisborn", "Hamm", 41, "500"),
    (39, "Paffrath", "Gernsheim", 205, "800-1.200"),
    (40, "Waidhaus", "Medelsheim", 458, "900-1.200"),
    (41, "Rothenstadt", "Finsing", 180, "1.000"),
    (42, "Schwandorf", "Oberkappel", 167, "800"),
    (43, "Brensbach", "Großkrotzenburg", 42, "500"),  # Brensbach-KW Staudinger
    (44, "Mittelbrunn", "Wallbach", 273, "900/ 1.000"),
    (45, "Bocholtz", "Stolberg", 14, "950"),
    (46, "Ellund", "Fockbek", 64, "500"),
    (47, "Klein Offenseth", "Elbe Süd", 30, "750"),  # Klein-Offenseth-Elbe Süd
    (48, "Reichertsheim", "Bierwang", 11, "800"),
    (49, "Herchenrode", "Lampertheim", 34, "1.000"),
    (50, "Lampertheim", "Grenzhof", 25, "700"),
    (51, "Grenzhof", "Blankenloch", 47, "600"),
    (52, "Kirrlach", "Heilbronn", 48, "400"),
    (53, "Heilbronn", "Metterzimmern", 20, "300"),
    (54, "Metterzimmern", "Wiernsheim", 25, "500"),
    (55, "Blankenloch", "Dürrlewang", 70, "600"),
    (56, "Dürrlewang", "Scharenstetten", 70, "500"),
    (57, "Scharenstetten", "Essingen", 41, "500"),
    (58, "Essingen", "Aalen", 9, "200"),
    (59, "Scharenstetten", "Weißensberg", 136, "500"),
    (60, "Weißensberg", "Lindau", 3, "500"),
    (61, "Tunsel", "Basel", 39, "300"),
    (62, "Oude Statenzijl", "Folmhusen", 24, "600"),  # Oude-Folmhusen
    (63, "Emden", "Folmhusen", 65, "750/ 1.000"),
    (64, "Bunde", "Emsbüren", 98, "750"),
    (65, "Lingen", "Uelsen", 27, "600"),
    (66, "Folmhusen", "Ganderkesee", 57, "750"),
    (67, "Ganderkesee", "Achim", 41, "900"),
    (68, "Achim", "Heidenau", 53, "450"),
    (69, "Heidenau", "Elbe Süd", 41, "600"),
    (70, "Klein Offenseth", "Quarnstedt", 16, "500"),
    (71, "Quarnstedt", "Fockbek", 47, "400"),
    (72, "Achim", "Kolshorn", 112, "600"),
    (73, "Kolshorn", "Mehrum", 15, "500"),
    (74, "Anwalting", "Wertingen", 21, "800"),
]

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

# Additional H2 sections 2045 (Anhang 6, p. 187-191, 193 sections, 7,912 km):
# (number, start, end, kind, length [km], DN, DP [bar], NEP scenarios)
NEP_H2_NETWORK_2045 = [
    (1, "Achim", "Elbe Süd", "conversion", 85, 1400, 84, "123"),
    (2, "Achim", "Steinitz", "conversion", 169.5, 1200, 84, "123"),
    (3, "Kolshorn", "Peine", "conversion", 29.2, 1200, 84, "123"),
    (4, "Peine", "Walle", "conversion", 12, 1200, 84, "123"),
    (5, "Wardenburg", "Achim", "conversion", 66.2, 1200, 84, "123"),
    (6, "Werne", "Sannerz", "conversion", 257.4, 1200, 100, "123"),
    (7, "Bernau", "Blumberg", "conversion", 19.3, 1100, 84, "123"),
    (8, "Börnicke", "Kienbaum", "conversion", 40.6, 1100, 84, "123"),
    (9, "Epe", "Legden", "conversion", 14.6, 1100, 100, "123"),
    (10, "Steinitz", "Bernau", "conversion", 181.4, 1100, 84, "123"),
    (11, "Emden", "Etzel", "conversion", 67, 1050, 84, "123"),
    (12, "Bocholtz", "Mittelbrunn", "conversion", 223.6, 1000, 67.5, "123"),
    (13, "Emden", "Werne", "conversion", 240, 1000, 70, "123"),
    (14, "Eynatten", "Legden", "conversion", 216.1, 1000, 100, "123"),
    (15, "Folmhusen", "Wardenburg", "conversion", 47.6, 1000, 84, "123"),
    (16, "Reckrod", "Wirtheim", "conversion", 88.4, 1000, 84, "12"),
    (17, "Sannerz", "Rimpar", "conversion", 67.6, 1000, 100, "123"),
    (18, "Schwarzach", "Eckartsweier", "conversion", 28.6, 1000, 70, "123"),
    (19, "Wilhelmshaven", "Etzel", "conversion", 26, 1000, 100, "123"),
    (20, "Wirtheim", "Herchenrode", "conversion", 83.2, 1000, 100, "12"),
    (21, "Flößberg", "Merzdorf", "conversion", 46.9, 900, 63, "123"),
    (22, "Helmste", "Stade", "conversion", 18, 900, 84, "123"),
    (23, "Hügelheim", "Hüsingen", "conversion", 31.4, 900, 70, "123"),
    (24, "Merzdorf", "Sayda", "conversion", 41.1, 900, 63, "123"),
    (25, "Obermichelbach", "Amerdingen", "conversion", 104.7, 900, 80, "123"),
    (26, "Amerdingen", "Wertingen", "conversion", 29.1, 800, 80, "123"),
    (27, "Arresting", "Bierwang", "conversion", 103.5, 800, 84, "123"),
    (28, "Bad Lauchstädt", "Flößberg", "conversion", 75, 800, 100, "123"),
    (29, "Bierwang", "Breitbrunn", "conversion", 26.3, 800, 80, "123"),
    (30, "Brunsbüttel", "Heist", "conversion", 60, 800, 84, "123"),
    (31, "Burghausen", "Schnaitsee", "conversion", 4.1, 800, 84, "123"),
    (32, "Epe", "Wettringen", "conversion", 25.8, 800, 84, "123"),
    (33, "Niederhohndorf", "Merzdorf", "conversion", 53.6, 800, 84, "123"),
    (34, "Ronneburg", "Vitzeroda", "conversion", 187.4, 800, 84, "123"),
    (35, "Werne", "Duisburg", "conversion", 94.8, 800, 70, "123"),
    (36, "Amerdingen", "Scharenstetten", "conversion", 57.4, 700, 80, "123"),
    # Anschlussltg. Epe
    (37, "Epe", "Epe", "conversion", 4.5, 700, 80, "123"),
    # Bentheim II-Altenlingen
    (38, "Bad Bentheim", "Altenlingen", "conversion", 33.4, 700, 55.7, "123"),
    (39, "Finsing", "Wolfersberg", "conversion", 20.6, 700, 67.5, "123"),
    (40, "Gernsheim", "Rimpar", "conversion", 107.4, 700, 67.5, "123"),
    (41, "Rimpar", "Waidhaus", "conversion", 211.8, 700, 67.5, "123"),
    (42, "Schlüchtern", "Rimpar", "conversion", 68.5, 700, 84, "123"),
    (43, "St. Hubert", "Lintorf", "conversion", 32.8, 700, 67.5, "123"),
    (44, "Steinbrink", "Vinnhorst", "conversion", 81.8, 700, 70, "123"),
    # Verbindungsleitungen Finsing
    (45, "Finsing", "Finsing", "conversion", 0.1, 700, 67.5, "123"),
    (46, "Bockum", "Olfen", "conversion", 4.7, 600, 70, "123"),
    (47, "Buckow", "Lauchhammer", "conversion", 120.3, 600, 67.5, "23"),
    (48, "Drohne", "Steinbrink", "conversion", 28.2, 600, 70, "123"),
    (49, "Egmating", "Oberpframmern", "conversion", 0.2, 600, 70, "123"),
    (50, "Finsing", "Finsing", "conversion", 0.1, 600, 70, "123"),
    (51, "Gernsheim", "Gernsheim", "conversion", 3.6, 600, 67, "123"),
    (52, "Gernsheim", "Lampertheim", "conversion", 19.4, 600, 67, "123"),
    # Gronau1-Ochtrup2
    (53, "Gronau", "Ochtrup", "conversion", 3, 600, 84, "123"),
    (54, "Hamborn", "Lintorf", "conversion", 20.1, 600, 70, "123"),
    # HerneA-HerneD
    (55, "Herne", "Herne", "conversion", 0.5, 600, 70, "12"),
    (56, "Inzenham", "Egmating", "conversion", 32, 600, 70, "123"),
    (
        57,
        "Lauchhammer",
        "Deutschneudorf",
        "conversion",
        115.8,
        600,
        67.5,
        "123",
    ),
    (58, "Leer", "Nüttermoor", "conversion", 4.8, 600, 84, "123"),
    # Mischstation Egenstedt-Station Ahlten
    (59, "Egenstedt", "Ahlten", "conversion", 36.8, 600, 84, "123"),
    # Ochtrup Wester 10-Ennigerloh
    (60, "Ochtrup", "Ennigerloh", "conversion", 80.2, 600, 70, "123"),
    (61, "Olfen", "Dülmen", "conversion", 18.2, 600, 70, "123"),
    (62, "Rapen", "Bockum", "conversion", 3.9, 600, 70, "123"),
    # Steinbrink-Verteilerstation Voigtei
    (63, "Steinbrink", "Voigtei", "conversion", 18.3, 600, 70, "123"),
    (64, "Stockum", "Holzwickede", "conversion", 24.5, 600, 70, "123"),
    (65, "Telgte", "Stockum", "conversion", 34.1, 600, 70, "123"),
    # Verteilerstation Rehden-Station Hallendorf
    (66, "Rehden", "Hallendorf", "conversion", 160.3, 600, 70, "123"),
    (67, "Vinnhorst", "Ahlten", "conversion", 21.3, 600, 84, "123"),
    (68, "Visbek", "Lemförde", "conversion", 48, 600, 70, "123"),
    (69, "Aachen", "Broichweiden", "conversion", 2.7, 500, 70, "12"),
    (70, "Anwalting", "Kissing", "conversion", 20.7, 500, 70, "123"),
    (71, "Bischofsheim", "Frankenthal", "conversion", 57.7, 500, 64, "123"),
    (72, "Dörnigheim", "Walldorf", "conversion", 30.1, 500, 64, "123"),
    (73, "Egmating", "Kissing", "conversion", 82.6, 500, 70, "123"),
    (74, "Essingen", "Scharenstetten", "conversion", 41.3, 500, 67.5, "12"),
    (75, "Finsing", "Bierwang", "conversion", 47.3, 500, 70, "123"),
    (76, "Gröben", "Bierwang", "conversion", 2.5, 500, 80, "123"),
    (77, "Gröben", "Gröben", "conversion", 0.1, 500, 80, "123"),
    (78, "Lampertheim", "Karlsruhe", "conversion", 78.4, 500, 61, "123"),
    (79, "Lintorf", "Uellendahl", "conversion", 28, 500, 40, "123"),
    (80, "Michelbach", "Essingen", "conversion", 54.9, 500, 67.5, "123"),
    (81, "Mittelbrunn", "Remich", "conversion", 113.6, 500, 84, "123"),
    (82, "Niederbonsfeld", "Essen-Süd", "conversion", 10.1, 500, 70, "123"),
    (83, "Niedereimer", "Haarweg", "conversion", 26.6, 500, 70, "123"),
    # Ochtrup Wester 18A -Ochtrup Hermann-LönsA
    (84, "Ochtrup", "Ochtrup", "conversion", 1.6, 500, 70, "123"),
    # Ochtrup Wester 18B -Ochtrup Hermann-LönsB
    (85, "Ochtrup", "Ochtrup", "conversion", 1.6, 500, 70, "123"),
    (
        86,
        "Radevormwald",
        "Niederschelden",
        "conversion",
        65.5,
        500,
        67.5,
        "123",
    ),
    (87, "Reinhardshofen", "Michelbach", "conversion", 62.9, 500, 80, "123"),
    (88, "Scharenstetten", "Ulm", "conversion", 18.1, 500, 58, "123"),
    (89, "Sonsbeck", "Hamborn", "conversion", 30.1, 500, 50, "123"),
    (90, "Ueldener Haar", "Weine", "conversion", 14, 500, 70, "123"),
    (91, "Walldorf", "Bischofsheim", "conversion", 13.3, 500, 64, "123"),
    (92, "Wiernsheim", "Löchgau", "conversion", 28.4, 500, 80, "12"),
    (93, "Wirtheim", "Dörnigheim", "conversion", 39.5, 500, 64, "123"),
    # Kötz-Hittistetten/Senden
    (94, "Kötz", "Hittistetten", "conversion", 13.7, 450, 60, "123"),
    # Mischstation Egenstedt-Station Lenglern
    (95, "Egenstedt", "Lenglern", "conversion", 63.5, 450, 64, "23"),
    # Verteilerstation Kolshorn-Station Clenze
    (96, "Kolshorn", "Clenze", "conversion", 53.1, 450, 70, "123"),
    (97, "Ahrem", "Euskirchen", "conversion", 18.5, 400, 100, "123"),
    # Bonn-Rheinaue-Godesberg
    (98, "Bonn-Rheinaue", "Bad Godesberg", "conversion", 4, 400, 70, "123"),
    (99, "Borken", "Bocholt", "conversion", 15.5, 400, 70, "123"),
    (100, "Breitbrunn", "Bierwang", "conversion", 8.1, 400, 70, "123"),
    (101, "Bunde", "Leer", "conversion", 19, 400, 84, "123"),
    (102, "Büttgen", "Büttgen", "conversion", 0.1, 400, 70, "123"),
    (103, "Castrop-Rauxel", "Witten", "conversion", 18.2, 400, 50, "123"),
    (104, "Cloppenburg", "Steinfeld", "conversion", 26.9, 400, 84, "123"),
    (105, "Dorsten", "Lintorf", "conversion", 41.8, 400, 50, "123"),
    (106, "Dorsten", "Werne", "conversion", 56.8, 400, 50, "123"),
    (107, "Egmating", "Kempten", "conversion", 145.8, 400, 80, "123"),
    (108, "Ennigerloh", "Oelde", "conversion", 16.3, 400, 70, "123"),
    (109, "Fröndenberg", "Holzwickede", "conversion", 2, 400, 70, "123"),
    # Gronau2-Ochtrup3
    (110, "Gronau", "Ochtrup", "conversion", 0.1, 400, 70, "123"),
    # HerneC-Bochum
    (111, "Herne", "Bochum", "conversion", 8.6, 400, 70, "123"),
    (112, "Herne", "Herne Hochlarmark", "conversion", 3.7, 400, 70, "12"),
    (113, "Herne", "Recklinghausen", "conversion", 8, 400, 70, "123"),
    (114, "Hiltrop", "Laer", "conversion", 4.4, 400, 70, "123"),
    (115, "Holzwickede", "Bochum", "conversion", 30, 400, 70, "123"),
    (116, "Huntorf", "Leer", "conversion", 151.8, 400, 84, "123"),
    (117, "Ingolstadt", "Augsburg", "conversion", 69.1, 400, 67.5, "123"),
    (118, "Inzenham", "Kiefersfelden", "conversion", 39.4, 400, 70, "123"),
    (119, "Laer", "Querenburg", "conversion", 2.2, 400, 70, "123"),
    (120, "Leer", "Rastede", "conversion", 33.8, 400, 84, "123"),
    (
        121,
        "Mengede",
        "Dortmund-Scharnhorst",
        "conversion",
        12.3,
        400,
        50,
        "123",
    ),
    (122, "Münchsmünster", "Ingolstadt", "conversion", 23, 400, 67.5, "123"),
    (123, "Neuss", "Neukirchen", "conversion", 11.6, 400, 67.5, "123"),
    # Ochtrup-Ochtrup1
    (124, "Ochtrup", "Ochtrup", "conversion", 0.1, 400, 70, "123"),
    (125, "Oelde", "Uelde", "conversion", 37.1, 400, 70, "123"),
    (126, "Oer Erkenschwick", "Rapen", "conversion", 1.5, 400, 70, "123"),
    (
        127,
        "Oer Erkenschwick",
        "Recklinghausen",
        "conversion",
        6.5,
        400,
        70,
        "123",
    ),
    # Oude-Bunde
    (128, "Oude Statenzijl", "Bunde", "conversion", 1.2, 400, 84, "123"),
    # Oude-Bunde 2
    (129, "Oude Statenzijl", "Bunde", "conversion", 1.2, 400, 84, "123"),
    (
        130,
        "Recklinghausen",
        "Oer Erkenschwick",
        "conversion",
        6.5,
        400,
        70,
        "123",
    ),
    (131, "Stiepel", "Bochum-Westpark", "conversion", 7.8, 400, 70, "123"),
    (132, "Ueldener Haar", "Plackweg", "conversion", 17.2, 400, 70, "123"),
    (133, "Ueldener Haar", "Ueldener Haar", "conversion", 0.3, 400, 70, "123"),
    (134, "Ueldener Haar", "Ueldener Haar", "conversion", 0.1, 400, 70, "123"),
    (135, "Uelde", "Wickede", "conversion", 31.8, 400, 70, "123"),
    (136, "Waldenburg", "Crailsheim", "conversion", 35.5, 400, 67.5, "123"),
    (137, "Walle", "Edesbüttel", "conversion", 16, 400, 70, "123"),
    (138, "Willstätt", "Weier", "conversion", 9.5, 400, 67.5, "123"),
    (139, "Zons", "Selikum", "conversion", 14.3, 400, 40, "123"),
    # Mischstation Gr. Gießen-Verteilerstation Kolshorn
    (140, "Groß Gießen", "Kolshorn", "conversion", 26.7, 350, 70, "123"),
    (141, "Hittistetten", "Lindau", "conversion", 120, 320, 50, "123"),
    # Biemenhorst 1-Bocholt
    (142, "Biemenhorst", "Bocholt", "conversion", 1.6, 300, 70, "123"),
    (143, "Diesenbach", "Regensburg", "conversion", 10.8, 300, 70, "123"),
    # Duisburg-Baerl-Alt-Homberg
    (
        144,
        "Duisburg-Baerl",
        "Homberg (Duisburg)",
        "conversion",
        6.4,
        300,
        70,
        "123",
    ),
    (145, "Euskirchen", "Brüser Berg", "conversion", 27.2, 300, 70, "123"),
    # Friemersheim-DU-Hafen
    (
        146,
        "Friemersheim",
        "Duisburg Hafen",
        "conversion",
        8.9,
        300,
        67.5,
        "123",
    ),
    (147, "Godesberg", "Brüser Berg", "conversion", 12.4, 300, 67.5, "123"),
    # Hövel 1-Gut Melschede
    (148, "Hövel", "Gut Melschede", "conversion", 0.2, 300, 40, "123"),
    # Isarschiene Ost
    (149, None, None, "conversion", 60.7, 300, 67.5, "123"),
    (150, "Katzdorf", "Diesenbach", "conversion", 10.2, 300, 70, "123"),
    # Loop-Dueren-Roelsdorf
    (151, "Düren", "Roelsdorf", "conversion", 2.3, 300, 25, "12"),
    # Olpe-Olpe 1
    (152, "Olpe", "Olpe", "conversion", 0.1, 300, 40, "123"),
    (
        153,
        "Recklinghausen",
        "Oer Erkenschwick",
        "conversion",
        5,
        300,
        70,
        "12",
    ),
    (154, "Regensburg", "Oberisling", "conversion", 15, 300, 70, "123"),
    (155, "Rehden", "Reiningen", "conversion", 22, 300, 64, "12"),
    (156, "Reiningen", "Georgsmarienhütte", "conversion", 49.3, 300, 64, "12"),
    (157, "Ulm", "Hittistetten", "conversion", 12.1, 300, 50, "123"),
    # Biemenhorst-Biemenhorst 1
    (158, "Biemenhorst", "Biemenhorst", "conversion", 0.2, 250, 67.5, "123"),
    # Hövel-Hövel 1
    (159, "Hövel", "Hövel", "conversion", 0.1, 250, 40, "123"),
    (160, "Hövel", "Olpe", "conversion", 18, 250, 40, "123"),
    # Mischstation Gr. Gießen-Station Bolzum
    (161, "Groß Gießen", "Bolzum", "conversion", 13.4, 250, 64, "123"),
    # Olpe 1-Freienohl
    (162, "Olpe", "Freienohl", "conversion", 1.1, 250, 40, "123"),
    (163, "Zopp", "Herzogenrath", "conversion", 5, 250, 70, "123"),
    (164, "Bocholt", "Biemenhorst", "conversion", 3.2, 200, 67.5, "123"),
    (165, "Broichweiden", "Stolberg", "conversion", 0.1, 200, 25, "123"),
    (166, "Freienohl", "Plackweg", "conversion", 7.1, 200, 70, "123"),
    # Loop-Dueren-Langerwehe
    (167, "Düren", "Langerwehe", "conversion", 7.6, 200, 25, "12"),
    # Loop-Langerwehe-Weisweiler
    (168, "Langerwehe", "Weisweiler", "conversion", 4.2, 200, 25, "12"),
    # Voerde-Huenxe
    (169, "Voerde", "Hünxe", "conversion", 3.6, 200, 67.5, "12"),
    (170, "Beckum", "Gut Melschede", "conversion", 0.7, 150, 70, "123"),
    (171, "Hinterschwarzenberg", "Pfronten", "conversion", 21.5, 150, 80, "1"),
    (172, "Achim", "Reckrod", "new", 303, 1400, 84, "12"),
    (173, "Barßel", "Emsbüren", "new", 87.4, 1200, 70, "12"),
    (174, "Elbe Süd", "Elbe Nord", "new", 5, 1200, 84, "12"),
    (175, "Elbe Süd", "Elbe Nord", "new", 5, 1200, 84, "123"),
    # Loop-Fockbek-Ellund
    (176, "Fockbek", "Ellund", "new", 64, 1200, 84, "123"),
    # Loop-Fockbek-Quarnstedt
    (177, "Fockbek", "Quarnstedt", "new", 47, 1200, 84, "123"),
    # Loop-Heist-Elbe Nord
    (178, "Heist", "Elbe Nord", "new", 5, 1200, 84, "12"),
    # Loop-Heist-Klein Offenseth
    (179, "Heist", "Klein Offenseth", "new", 15, 1200, 84, "12"),
    # Loop-Quarnstedt-Klein Offenseth
    (180, "Quarnstedt", "Klein Offenseth", "new", 15.9, 1200, 84, "123"),
    (181, "Au am Rhein", "Schwarzach", "new", 33.4, 1000, 70, "123"),
    (182, "Eckartsweier", "Hügelheim", "new", 85.5, 1000, 70, "123"),
    (183, "Elsterwerda", "Spreetal", "new", 55, 1000, 84, "2"),
    (184, "Herchenrode", "Lampertheim", "new", 34.4, 1000, 100, "12"),
    # Loop-Rothenstadt-Forchheim
    (185, "Rothenstadt", "Forchheim", "new", 108, 1000, 84, "123"),
    # Loop-Wilhelmshaven-Nord-Wilhelmshaven-Süd
    (186, "Wilhelmshaven", "Wilhelmshaven", "new", 11.2, 1000, 100, "12"),
    (187, "Windberg", "Oberkappel", "new", 94, 1000, 84, "123"),
    (188, "Hüsingen", "Wallbach", "new", 14.8, 900, 70, "123"),
    # Loop-Niederhohndorf/Zwickau-Rothenstadt
    (189, "Niederhohndorf", "Rothenstadt", "new", 170, 800, 84, "12"),
    # Loop-Niederhohndorf/Zwickau-Rückersdorf
    (190, "Niederhohndorf", "Rückersdorf", "new", 33.7, 800, 84, "1"),
    # Huntorf-Elsfleth 2
    (191, "Huntorf", "Elsfleth", "new", 4.8, 600, 84, "12"),
    (192, "Marl", "Oer Erkenschwick", "new", 0.7, 300, 70, "12"),
    (193, "Zopp", "Alsdorf", "new", 1.2, 250, 70, "123"),
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

# Criteria of Anlage 3 of the measures used (modelling result 2037 of
# NEP_SCENARIO_2045, section 6.4.2)
NEP_H2_MEASURES_CRITERIA = ("H2(4)", "H2(5)", "H2(6)")

# H2 measures up to 2037 beyond the core network (Anlage 3): (NEP id, start,
# end, kind, length [km], DN, DP [bar], criterion); lengths and DN/DP are
# partly derived, see the documentation of the gas grids
NEP_H2_MEASURES_2037 = [
    (
        "H2-1201",
        "Strohreit",
        "Reitmehring",
        "new",
        4.5,
        400,
        70,
        "H2(4)",
    ),  # default
    (
        "H2-1202",
        "Wertingen",
        "Augsburg/Klärwerk",
        "new",
        26.3,
        600,
        70,
        "H2(4)",
    ),  # default
    (
        "H2-1214",
        "Broichweiden",
        "Aachen",
        "new",
        8.8,
        400,
        70,
        "H2(4)",
    ),  # default
    (
        "H2-1215",
        "Kirchpütz",
        "Weisweiler",
        "new",
        7.1,
        400,
        70,
        "H2(4)",
    ),  # default
    (
        "H2-1216",
        "Breinig",
        "Broichweiden",
        "new",
        12.8,
        400,
        70,
        "H2(4)",
    ),  # default
    (
        "H2-1218",
        "Nievenheim",
        "Neuss",
        "new",
        12.4,
        400,
        70,
        "H2(4)",
    ),  # default
    (
        "H2-1219",
        "Neuss",
        "Neuss Hafen",
        "new",
        1.7,
        400,
        70,
        "H2(4)",
    ),  # default
    (
        "H2-203",
        "Deffingen",
        "Wasserburg",
        "new",
        2.2,
        400,
        70,
        "H2(4)",
    ),  # default
    (
        "H2-205",
        "Münchsmünster",
        "Regensburg",
        "new",
        48.4,
        600,
        70,
        "H2(4)",
    ),  # default
    ("H2-206", "Ulm", "Augsburg", "new", 78.2, 400, 68, "H2(4)"),  # SciGRID
    ("H2-209", "Kötz", "Günzburg", "new", 8.1, 400, 70, "H2(4)"),  # default
    ("H2-231", "Freiburg", "Weier", "new", 60.6, 600, 70, "H2(4)"),  # default
    (
        "H2-233",
        "Uedener Bruch",
        "Wardt",
        "conversion",
        6.8,
        200,
        70,
        "H2(4)",
    ),  # KLU121-01
    ("H2-244", "Aachen", "Soers", "new", 10.7, 400, 70, "H2(4)"),  # default
    (
        "H2-211",
        "Kadeltshofen",
        "Weißenhorn",
        "conversion",
        9.8,
        400,
        70,
        "H2(6)",
    ),  # default
    (
        "H2-213",
        "Weißenhorn",
        "Bellenberg",
        "conversion",
        8.5,
        400,
        70,
        "H2(6)",
    ),  # default
    (
        "H2-218",
        "Birlinghoven",
        "Beuel",
        "conversion",
        8.0,
        400,
        70,
        "H2(6)",
    ),  # default
    (
        "H2-224",
        "Dernbach Elgendorf",
        "Bendorf",
        "conversion",
        18.3,
        600,
        70,
        "H2(6)",
    ),  # default
    (
        "H2-226",
        "Niederkassel",
        "Wesseling",
        "conversion",
        5.1,
        400,
        70,
        "H2(6)",
    ),  # default
    (
        "H2-248",
        "Dormagen",
        "Nievenheim",
        "conversion",
        5.7,
        400,
        70,
        "H2(6)",
    ),  # default
    (
        "H2-249",
        "Neuss Hafen",
        "Düsseldorf Hafen",
        "conversion",
        2.8,
        400,
        70,
        "H2(6)",
    ),  # default
    (
        "H2-1203",
        "Augsburg Nord",
        "Augsburg Mitte",
        "new",
        2.9,
        400,
        70,
        "H2(5)",
    ),  # default
    (
        "H2-1207",
        "Elbe Süd",
        "Achim",
        "new",
        85.7,
        1000,
        70,
        "H2(5)",
    ),  # SciGRID
    (
        "H2-1208",
        "Brunsbüttel",
        "Heist",
        "new",
        51.2,
        600,
        70,
        "H2(5)",
    ),  # default
    ("H2-1209", "Heist", "Elbe Nord", "new", 7.1, 400, 70, "H2(5)"),  # default
    (
        "H2-1212",
        "Borken",
        "Borken-Gemen",
        "new",
        2.1,
        400,
        70,
        "H2(5)",
    ),  # default
    (
        "H2-208",
        "Einmuß",
        "Kelheim",
        "conversion",
        8.2,
        400,
        70,
        "H2(5)",
    ),  # default
    (
        "H2-210",
        "Breitbrunn",
        "Bierwang",
        "conversion",
        18.6,
        600,
        70,
        "H2(5)",
    ),  # default
    (
        "H2-216",
        "Mittelbrunn",
        "Au am Rhein",
        "conversion",
        79.0,
        1000,
        68,
        "H2(5)",
    ),  # SciGRID
    (
        "H2-219",
        "Drohne",
        "Werne",
        "conversion",
        111.8,
        600,
        70,
        "H2(5)",
    ),  # default
    (
        "H2-220",
        "Lauterbach",
        "Scheidt",
        "conversion",
        124.6,
        900,
        70,
        "H2(5)",
    ),  # SciGRID
    (
        "H2-221",
        "Büchelberg",
        "Karlsruhe",
        "conversion",
        10.4,
        400,
        70,
        "H2(5)",
    ),  # default
    (
        "H2-222",
        "Etzel",
        "Wardenburg",
        "conversion",
        57.0,
        1000,
        84,
        "H2(5)",
    ),  # SciGRID
    (
        "H2-223",
        "Wardenburg",
        "Drohne",
        "conversion",
        75.2,
        900,
        70,
        "H2(5)",
    ),  # SciGRID
    (
        "H2-227",
        "Lauterbach",
        "Vitzeroda",
        "conversion",
        67.6,
        800,
        80,
        "H2(5)",
    ),  # SciGRID
    (
        "H2-228",
        "Schwandorf",
        "Windberg",
        "conversion",
        71.9,
        1000,
        68,
        "H2(5)",
    ),  # SciGRID
    (
        "H2-229",
        "Steinitz",
        "Wedringen",
        "conversion",
        73.1,
        900,
        55,
        "H2(5)",
    ),  # SciGRID
    (
        "H2-230",
        "Au am Rhein",
        "Leonberg",
        "conversion",
        73.7,
        350,
        62,
        "H2(5)",
    ),  # SciGRID
    (
        "H2-232",
        "Kirchhausen",
        "Waldenburg",
        "conversion",
        45.9,
        600,
        70,
        "H2(5)",
    ),  # default
    ("H2-234", "Wardt", "Vissel", "new", 4.2, 400, 70, "H2(5)"),  # default
    ("H2-235", "Wardt", "Vissel", "new", 4.2, 400, 70, "H2(5)"),  # default
    (
        "H2-236",
        "Vissel",
        "Hamminkeln",
        "new",
        9.4,
        400,
        70,
        "H2(5)",
    ),  # default
    ("H2-237", "Praest", "Bocholt", "new", 21.7, 600, 70, "H2(5)"),  # default
    (
        "H2-238",
        "Hamminkeln",
        "Coesfeld",
        "new",
        54.2,
        600,
        70,
        "H2(5)",
    ),  # default
    (
        "H2-243",
        "Broichweiden",
        "Stolberg",
        "new",
        6.9,
        400,
        70,
        "H2(5)",
    ),  # default
    (
        "H2-245",
        "Kirchpütz",
        "Alsdorf",
        "new",
        8.7,
        400,
        70,
        "H2(5)",
    ),  # default
    ("H2-250", "Voerde", "Hünxe", "new", 8.3, 400, 70, "H2(5)"),  # default
]

# Commissioning year of NEP_H2_MEASURES_2037 where the BNetzA list
# "Inbetriebnahmedaten" (June 2026) gives one, otherwise NEP_H2_MEASURES_YEAR
NEP_H2_MEASURES_2037_COMMISSIONING = {
    "H2-1201": 2036,
    "H2-1202": 2036,
    "H2-1214": 2036,
    "H2-1215": 2036,
    "H2-1216": 2036,
    "H2-1218": 2036,
    "H2-1219": 2036,
    "H2-203": 2036,
    "H2-205": 2036,
    "H2-206": 2036,
    "H2-209": 2036,
    "H2-231": 2036,
    "H2-233": 2031,
    "H2-244": 2036,
    "H2-211": 2036,
    "H2-213": 2036,
    "H2-218": 2036,
    "H2-224": 2036,
    "H2-226": 2036,
    "H2-248": 2036,
    "H2-249": 2036,
}

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

# Kernnetz measures of the modelling result 2037 not in the network 2045
# of scenario 2 (Tabelle 37, p. 147), NEP id in the comment
NEP_KERNNETZ_NOT_IN_2045 = [
    "KLU038-01",  # H2-038-01 Achim-Heidenau
    "KLU050-01",  # H2-050-01 Heidenau-Elbe Süd
    "KLN017-01",  # H2-1017-02 Huntorf-Elsfleth 2
    "KLN078-01",  # H2-1078-01 Böhlen-Borna
    "KLU139-01",  # H2-139-01 Borna-Thierbach
]

# DN of Kernnetz measures modelled differently by scenario 2 (Anlage 3)
NEP_KERNNETZ_DN_2045 = {"KLN042-01": 1000}

# H2 cross-border points, scenario 2: (GÜP, country, share, border node,
# entry 2037, exit 2037, entry 2045, exit 2045) in GWh/h (Brennwert);
# Tabellen 22, 31 and 36
NEP_H2_BORDER_POINTS = [
    ("Bornholm-Lubmin", "DK", 1, "AWZ", 10.0, 0.0, 10.0, 0.0),
    ("Ellund", "DK", 1, "Ellund", 4.3, 0.0, 24.0, 2.0),
    (
        "AquaDuctus (Offshore)",
        "NO",
        0.5,
        "AQD Offshore SEN 1",
        6.3,
        0.0,
        20.0,
        0.0,
    ),
    (
        "AquaDuctus (Offshore)",
        "GB",
        0.5,
        "AQD Offshore SEN 1",
        6.3,
        0.0,
        20.0,
        0.0,
    ),
    ("Dornum/Emden", "NO", 1, "Emden", 0.0, 0.0, 10.0, 0.0),
    ("Oude Statenzijl/Bunde", "NL", 1, "Oude Statenzijl", 4.0, 0.0, 4.0, 4.0),
    ("Vlieghuis", "NL", 1, "Vlieghuis", 1.3, 0.0, 1.3, 0.0),
    ("Elten", "NL", 1, "Elten", 3.2, 0.0, 3.2, 3.2),
    ("Vreden", "NL", 1, "Vreden", 3.2, 0.0, 3.2, 3.2),
    ("Eynatten", "BE", 1, "Eynatten", 4.5, 0.0, 9.0, 0.0),
    ("Medelsheim", "FR", 1, "Medelsheim", 8.0, 0.0, 9.0, 0.0),
    ("Freiburg", "FR", 1, "Fessenheim", 0.5, 0.0, 0.5, 0.0),
    ("Leidingen", "FR", 1, "Leidingen", 0.2, 0.0, 0.2, 0.0),
    ("Wallbach", "CH", 1, "Wallbach", 0.0, 0.0, 9.5, 0.0),
    ("Oberkappel", "AT", 1, "Oberkappel", 0.0, 0.0, 0.0, 4.0),
    ("Überackern", "AT", 1, "Überackern", 6.3, 0.8, 6.3, 2.3),
    ("Waidhaus", "CZ", 1, "Waidhaus", 6.1, 1.8, 12.0, 6.6),
    ("Deutschneudorf", "CZ", 1, "Deutschneudorf", 0.0, 1.2, 0.0, 0.0),
    ("Oder-Spree", "PL", 1, "Fürstenberg (PL)", 3.7, 2.2, 8.3, 4.2),
    ("Uckermark", "PL", 1, "Greifenhagen", 0.8, 0.6, 8.0, 0.8),
]

# H2 imports via converted LNG terminals (Tabelle 36, p. 143): (terminal,
# border node, entry 2037, entry 2045) in GWh/h
NEP_H2_LNG_TERMINALS = [
    ("Wilhelmshaven", "Wilhelmshaven", 0.0, 8.3),
    ("Stade", "Stade", 0.0, 7.0),
    ("Brunsbüttel", "Brunsbüttel", 0.0, 4.4),
]

# Ratio of the higher to the lower heating value of H2 (approval of the
# Szenariorahmen Gas und Wasserstoff 2025, p. 23)
NEP_H2_HHV_PER_LHV = 1.18

# H2 storage of scenario 2 in GWh/h (Brennwert): (withdrawal, injection);
# Tabellen 20, 21, 34 and 35
NEP_H2_STORAGE = {2037: (36.0, 26.0), 2045: (53.0, 38.0)}

# Additional withdrawal in GWh/h (Brennwert) of scenario 2 in the load case
# "Dunkelflaute" of 2037 (NEP p. 110: 2.4 GW from four storage sites)
NEP_H2_STORAGE_DUNKELFLAUTE = {2037: 2.4}

# H2 storage projects of the market survey per federal state [MW,
# Brennwert] (Szenariorahmen Anlage 2, category "Speicher")
NEP_H2_STORAGE_PROJECTS = {
    2037: {
        "Brandenburg/Berlin": 50.0,
        "Niedersachsen/Bremen": 18887.0,
        "Nordrhein-Westfalen": 1451.0,
        "Sachsen": 78.0,
        "Sachsen-Anhalt": 566.0,
        "Schleswig-Holstein/Hamburg": 7080.0,
    },
    2045: {
        "Bayern": 2130.0,
        "Brandenburg/Berlin": 1150.0,
        "Hessen": 600.0,
        "Niedersachsen/Bremen": 22750.0,
        "Nordrhein-Westfalen": 2092.0,
        "Sachsen": 78.0,
        "Sachsen-Anhalt": 2371.9,
        "Schleswig-Holstein/Hamburg": 7080.0,
        "Thüringen": 354.0,
    },
}

# Federal states of the four "Dunkelflaute" storage sites (p. 110)
NEP_H2_STORAGE_DUNKELFLAUTE_STATES = [
    "Bayern",
    "Brandenburg/Berlin",
    "Niedersachsen/Bremen",
    "Thüringen",
]

# Other H2 imports of scenario 2 (LH2 and derivatives) in GWh/h
# (Brennwert), Tabellen 21 and 35
NEP_H2_OTHER_IMPORTS = {2037: 4.0, 2045: 21.0}

# Other H2 import projects of the market survey per federal state [MW,
# Brennwert] (Szenariorahmen Anlage 2, category "Einspeisung")
NEP_H2_OTHER_IMPORT_PROJECTS = {
    2037: {
        "Brandenburg/Berlin": 33.8,
        "Mecklenburg-Vorpommern": 143.9,
        "Niedersachsen/Bremen": 1646.0,
        "Nordrhein-Westfalen": 411.6,
        "Schleswig-Holstein/Hamburg": 2135.0,
    },
    2045: {
        "Bayern": 29.9,
        "Brandenburg/Berlin": 33.8,
        "Mecklenburg-Vorpommern": 1934.9,
        "Niedersachsen/Bremen": 14974.6,
        "Nordrhein-Westfalen": 637.8,
        "Sachsen": 5.6,
        "Sachsen-Anhalt": 655.0,
        "Schleswig-Holstein/Hamburg": 3135.0,
    },
}

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

# LNG terminals for methane: (name, x, y, capacity [GWh/h]), clusters of
# the approval of the Szenariorahmen (pp. 90-91) shared equally
NEP_CH4_LNG_TERMINALS = [
    ("Wilhelmshaven", 8.114101, 53.52972, 26.0),
    ("Brunsbüttel", 9.137629, 53.900299, 17.5),
    ("Stade", 9.496045, 53.579073, 17.5),
    ("Lubmin", 13.615004, 54.13412, 11.5 / 3),
    ("Mukran", 13.5856, 54.4937, 6.0 + 11.5 / 3),
    ("Rostock", 12.118621, 54.087185, 11.5 / 3),
]

# Share of the LNG capacity used in the peak load case (Tabelle 19)
NEP_CH4_LNG_USE = {2030: 1.0, 2037: 0.5, 2045: 0.0}
