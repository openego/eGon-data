"""The central module containing all code dealing with importing VG250 data.

This module either directly contains the code dealing with importing VG250
data, or it re-exports everything needed to handle it. Please refrain
from importing code from any modules below this one, because it might
lead to unwanted behaviour.

If you have to import code from a module below this one because the code
isn't exported from this module, please file a bug, so we can fix this.
"""

import json
from urllib.request import urlretrieve
import os

from geoalchemy2 import Geometry
import geopandas as gpd

from egon.data import db
import egon.data.config


def download_vg250_files():
    """Download VG250 (Verwaltungsgebiete) shape files."""
    data_config = egon.data.config.datasets()
    vg250_config = data_config["vg250"]["original_data"]

    target_file = os.path.join(
        os.path.dirname(__file__), vg250_config["target"]["path"]
    )

    if not os.path.isfile(target_file):
        urlretrieve(vg250_config["source"]["url"], target_file)


def to_postgres(testmode):

    # Get information from data configuraiton file
    data_config = egon.data.config.datasets()
    vg250_orig = data_config["vg250"]["original_data"]
    vg250_processed = data_config["vg250"]["processed"]

    # Create target schema
    db.execute_sql(f"CREATE SCHEMA IF NOT EXISTS {vg250_processed['schema']};")

    zip_file = os.path.join(
        os.path.dirname(__file__), vg250_orig["target"]["path"]
    )
    engine_local_db = db.engine()

    # Extract shapefiles from zip archive and send it to postgres db
    for filename, table in vg250_processed["file_table_map"].items():
        # Open files and read .shp (within .zip) with geopandas
        data = gpd.read_file(
            f"zip://{zip_file}!vg250_01-01.geo84.shape.ebenen/"
            f"vg250_ebenen_0101/{filename}"
        )

        if testmode:
            bl = "Schleswig-Holstein"
            # read-in borders of federal state Schleswig-Holstein
            data_sta = gpd.read_file(
                    f"zip://{zip_file}!vg250_01-01.geo84.shape.ebenen/"
                    f"vg250_ebenen_0101/VG250_LAN.shp"
                    ).query(f"GEN == '{bl}'")
            data_sta.BEZ = 'Bundesrepublik'
            data_sta.NUTS = 'DE'
            # import borders of Schleswig-Holstein as borders of state
            if table == 'vg250_sta':
                data = data_sta
            # choose only areas in Schleswig-Holstein
            else:
                data = data[data.within(
                    data_sta.dissolve(by='GEN').geometry.values[0])]


        # Set index column and format column headings
        data.index.set_names("gid", inplace=True)
        data.columns = [x.lower() for x in data.columns]

        # Drop table before inserting data
        db.execute_sql(
            f"DROP TABLE IF EXISTS "
            f"{vg250_processed['schema']}.{table} CASCADE;"
        )

        # create database table from geopandas dataframe
        data.to_postgis(
            table,
            engine_local_db,
            schema=vg250_processed["schema"],
            index=True,
            if_exists="replace",
            dtype={"geometry": Geometry()},
        )

        db.execute_sql(
            f"ALTER TABLE {vg250_processed['schema']}.{table} "
            f"ADD PRIMARY KEY (gid);"
        )

        # Add index on geometry column
        db.execute_sql(
            f"CREATE INDEX {table}_geometry_idx ON "
            f"{vg250_processed['schema']}.{table} USING gist (geometry);"
        )



def add_metadata():
    """Writes metadata JSON string into table comment."""
    # Prepare variables
    vg250_config = egon.data.config.datasets()["vg250"]

    title_and_description = {
        "vg250_sta": {
            "title": "BKG - Verwaltungsgebiete 1:250.000 - Staat (STA)",
            "description": "Staatsgrenzen der Bundesrepublik Deutschland",
        },
        "vg250_lan": {
            "title": "BKG - Verwaltungsgebiete 1:250.000 - Länder (LAN)",
            "description": "Landesgrenzen der Bundesländer in der "
            "Bundesrepublik Deutschland",
        },
        "vg250_rbz": {
            "title": "BKG - Verwaltungsgebiete 1:250.000 - Regierungsbezirke "
            "(RBZ)",
            "description": "Grenzen der Regierungsbezirke in der "
            "Bundesrepublik Deutschland",
        },
        "vg250_krs": {
            "title": "BKG - Verwaltungsgebiete 1:250.000 - Kreise (KRS)",
            "description": "Grenzen der Landkreise in der "
            "Bundesrepublik Deutschland",
        },
        "vg250_vwg": {
            "title": "BKG - Verwaltungsgebiete 1:250.000 - "
            "Verwaltungsgemeinschaften (VWG)",
            "description": "Grenzen der Verwaltungsgemeinschaften in der "
            "Bundesrepublik Deutschland",
        },
        "vg250_gem": {
            "title": "BKG - Verwaltungsgebiete 1:250.000 - Gemeinden (GEM)",
            "description": "Grenzen der Gemeinden in der "
            "Bundesrepublik Deutschland",
        },
    }

    url = vg250_config["original_data"]["source"]["url"]

    # Insert metadata for each table
    licenses = [
        {
            "title": "Datenlizenz Deutschland – Namensnennung – Version 2.0",
            "path": "www.govdata.de/dl-de/by-2-0",
            "instruction": (
                "Jede Nutzung ist unter den Bedingungen dieser „Datenlizenz "
                "Deutschland - Namensnennung - Version 2.0 zulässig.\nDie "
                "bereitgestellten Daten und Metadaten dürfen für die "
                "kommerzielle und nicht kommerzielle Nutzung insbesondere:"
                "(1) vervielfältigt, ausgedruckt, präsentiert, verändert, "
                "bearbeitet sowie an Dritte übermittelt werden;\n "
                "(2) mit eigenen Daten und Daten Anderer zusammengeführt und "
                "zu selbständigen neuen Datensätzen verbunden werden;\n "
                "(3) in interne und externe Geschäftsprozesse, Produkte und "
                "Anwendungen in öffentlichen und nicht öffentlichen "
                "elektronischen Netzwerken eingebunden werden."
            ),
            "attribution": "© Bundesamt für Kartographie und Geodäsie",
        }
    ]
    for table in vg250_config["processed"]["file_table_map"].values():
        meta = {
            "title": title_and_description[table]["title"],
            "description": title_and_description[table]["title"],
            "language": ["DE"],
            "spatial": {
                "location": "",
                "extent": "Germany",
                "resolution": "vector",
            },
            "temporal": {
                "referenceDate": "2020-01-01",
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
                    "title": "Dienstleistungszentrum des Bundes für "
                    "Geoinformation und Geodäsie - Open Data",
                    "description": "Dieser Datenbestand steht über "
                    "Geodatendienste gemäß "
                    "Geodatenzugangsgesetz (GeoZG) "
                    "(http://www.geodatenzentrum.de/auftrag/pdf"
                    "/geodatenzugangsgesetz.pdf) für die "
                    "kommerzielle und nicht kommerzielle "
                    "Nutzung geldleistungsfrei zum Download "
                    "und zur Online-Nutzung zur Verfügung. Die "
                    "Nutzung der Geodaten und Geodatendienste "
                    "wird durch die Verordnung zur Festlegung "
                    "der Nutzungsbestimmungen für die "
                    "Bereitstellung von Geodaten des Bundes "
                    "(GeoNutzV) (http://www.geodatenzentrum.de"
                    "/auftrag/pdf/geonutz.pdf) geregelt. "
                    "Insbesondere hat jeder Nutzer den "
                    "Quellenvermerk zu allen Geodaten, "
                    "Metadaten und Geodatendiensten erkennbar "
                    "und in optischem Zusammenhang zu "
                    "platzieren. Veränderungen, Bearbeitungen, "
                    "neue Gestaltungen oder sonstige "
                    "Abwandlungen sind mit einem "
                    "Veränderungshinweis im Quellenvermerk zu "
                    "versehen. Quellenvermerk und "
                    "Veränderungshinweis sind wie folgt zu "
                    "gestalten. Bei der Darstellung auf einer "
                    "Webseite ist der Quellenvermerk mit der "
                    "URL http://www.bkg.bund.de zu verlinken. "
                    "© GeoBasis-DE / BKG <Jahr des letzten "
                    "Datenbezugs> © GeoBasis-DE / BKG "
                    "<Jahr des letzten Datenbezugs> "
                    "(Daten verändert) Beispiel: "
                    "© GeoBasis-DE / BKG 2013",
                    "path": url,
                    "licenses": "Geodatenzugangsgesetz (GeoZG)",
                    "copyright": "© GeoBasis-DE / BKG 2016 (Daten verändert)",
                },
                {
                    "title": "BKG - Verwaltungsgebiete 1:250.000 (vg250)",
                    "description": "Der Datenbestand umfasst sämtliche "
                    "Verwaltungseinheiten aller hierarchischen "
                    "Verwaltungsebenen vom Staat bis zu den "
                    "Gemeinden mit ihren Verwaltungsgrenzen, "
                    "statistischen Schlüsselzahlen und dem "
                    "Namen der Verwaltungseinheit sowie der "
                    "spezifischen Bezeichnung der "
                    "Verwaltungsebene des jeweiligen "
                    "Bundeslandes.",
                    "path": "http://www.bkg.bund.de",
                    "licenses": licenses,
                },
            ],
            "licenses": licenses,
            "contributors": [
                {
                    "title": "Guido Pleßmann",
                    "email": "http://github.com/gplssm",
                    "date": "2020-12-04",
                    "object": "",
                    "comment": "Imported data",
                }
            ],
            "metaMetadata": {
                "metadataVersion": "OEP-1.4.0",
                "metadataLicense": {
                    "name": "CC0-1.0",
                    "title": "Creative Commons Zero v1.0 Universal",
                    "path": (
                        "https://creativecommons.org/publicdomain/zero/1.0/"
                    ),
                },
            },
        }

        meta_json = "'" + json.dumps(meta) + "'"

        db.submit_comment(
            meta_json, vg250_config["processed"]["schema"], table
        )
