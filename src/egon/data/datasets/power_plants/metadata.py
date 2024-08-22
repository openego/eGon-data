"""Metadata for power plants table
"""
import datetime
import json

from egon.data import db
from egon.data.metadata import (
    license_agpl,
    license_ccby,
    licenses_datenlizenz_deutschland,
)


def metadata():
    """Add metdata to power plants table

    Returns
    -------
    None.

    """
    meta = {
        "name": "supply.egon_power_plants",
        "title": "supply.egon_power_plants",
        "id": "",
        "description": "Database of powerplants ",
        "language": "en-GB",
        "keywords": [],
        "publicationDate": datetime.date.today().isoformat(),
        "context": {
            "homepage": "https://ego-n.org/tools_data/",
            "documentation": "https://egon-data.readthedocs.io/en/latest/",
            "sourceCode": "https://github.com/openego/eGon-data",
            "contact": "https://ego-n.org/partners/",
            "grantNo": "03EI1002",
            "fundingAgency": "Bundesministerium für Wirtschaft und Energie",
            "fundingAgencyLogo": "https://www.innovation-beratung-foerderung.de/INNO/Redaktion/DE/Bilder/Titelbilder/titel_foerderlogo_bmwi.jpg?__blob=normal&v=3",
            "publisherLogo": "https://ego-n.org/images/eGon_logo_noborder_transbg.svg",
        },
        "spatial": {"location": "", "extent": "Germany", "resolution": ""},
        "sources": [
            {
                "title": '"open-MaStR power unit registry"',
                "description": "Raw data download Marktstammdatenregister (MaStR) data using the webservice. All data from the Marktstammdatenregister is included. There are duplicates included. For further information read in the documentationg of the original data source: https://www.marktstammdatenregister.de/MaStRHilfe/subpages/statistik.html",
                "path": "https://zenodo.org/record/10480958",
                "licenses": [
                    {
                        "name": licenses_datenlizenz_deutschland(
                            '"© 2021 Bundesnetzagentur für Elektrizität, Gas, Telekommunikation, Post und Eisenbahnen"'
                        ),
                        "title": "",
                        "path": "",
                        "instruction": "",
                        "attribution": "eGon developers",
                    }
                ],
            },
            {
                "title": '"ERA5 global reanalysis"',
                "description": "ERA5 is the fifth generation ECMWF reanalysis for the global climate "
                "and weather for the past 4 to 7 decades. Currently data is available from 1950, "
                "split into Climate Data Store entries for 1950-1978 (preliminary back extension) and f"
                "rom 1979 onwards (final release plus timely updates, this page). ERA5 replaces the ERA-Interim reanalysis. "
                "See the online ERA5 documentation "
                "(https://confluence.ecmwf.int/display/CKB/ERA5%3A+data+documentation#ERA5:datadocumentation-Dataupdatefrequency) "
                "for more information.",
                "path": "https://confluence.ecmwf.int/display/CKB/ERA5%3A+data+documentation#ERA5:datadocumentation-Dataupdatefrequency",
                "licenses": [
                    {
                        "name": "Licence to use Copernicus Products",
                        "title": "Licence to use Copernicus Products",
                        "path": "https://cds.climate.copernicus.eu/api/v2/terms/static/licence-to-use-copernicus-products.pdf",
                        "instruction": "This Licence is free of charge, worldwide, non-exclusive, royalty free and perpetual. "
                        "Access to Copernicus Products is given for any purpose in so far as it is lawful, whereas use "
                        "may include, but is not limited to: reproduction; distribution; communication to the public; "
                        "adaptation, modification and combination with other data and information; or any "
                        "combination of the foregoing",
                        "attribution": "Copernicus Climate Change Service (C3S) Climate Data Store",
                    },
                ],
            },
            {
                "title": '"Verwaltungsgebiete 1:250 000 (Ebenen)"',
                "description": "Der Datenbestand umfasst sämtliche Verwaltungseinheiten der "
                "hierarchischen Verwaltungsebenen vom Staat bis zu den Gemeinden "
                "mit ihren Grenzen, statistischen Schlüsselzahlen, Namen der "
                "Verwaltungseinheit sowie die spezifische Bezeichnung der "
                "Verwaltungsebene des jeweiligen Landes.",
                "path": "https://daten.gdz.bkg.bund.de/produkte/vg/vg250_ebenen_0101/2020/vg250_01-01.geo84.shape.ebenen.zip",
                "licenses": [
                    licenses_datenlizenz_deutschland(
                        "© Bundesamt für Kartographie und Geodäsie "
                        "2020 (Daten verändert)"
                    )
                ],
            },
            {
                "title": '"eGon-data"',
                "description": "Workflow to download, process and generate data sets"
                "suitable for the further research conducted in the project eGon (https://ego-n.org/)",
                "path": "https://github.com/openego/eGon-data",
                "licenses": [
                    license_agpl(
                        "© Jonathan Amme, Clara Büttner, Ilka Cußmann, Julian Endres, Carlos Epia, Stephan Günther, Ulf Müller, Amélia Nadal, Guido Pleßmann, Francesco Witte"
                    )
                ],
            },
            {
                "title": '"Netzentwicklungsplan Strom 2035, Version 2021, erster Entwurf"',
                "description": "Die vier deutschen Übertragungsnetzbetreiber zeigen mit "
                "diesem ersten Entwurf des Netzentwicklungsplans 2035, Version 2021, den "
                "benötigten Netzausbau für die nächsten Jahre auf. Der NEP-Bericht beschreibt "
                "keine konkreten Trassenverläufe von Übertragungsleitungen, sondern er "
                "dokumentiert den notwendigen Übertragungsbedarf zwischen Netzknoten. "
                "Das heißt, es werden Anfangs- und Endpunkte von zukünftigen Leitungsverbindungen "
                "definiert sowie konkrete Empfehlungen für den Aus- und Neubau der Übertragungsnetze "
                "an Land und auf See in Deutschland gemäß den Detailanforderungen im § 12 EnWG gegeben.",
                "path": "https://zenodo.org/record/5743452#.YbCoz7so8go",
                "licenses": [license_ccby("© Übertragungsnetzbetreiber")],
            },
            {
                "title": '"Data bundle for egon-data"',
                "description": "Zenodo repository to provide several different input data sets for eGon-data",
                "path": "https://zenodo.org/record/10226009",
                "licenses": [license_ccby("© eGon development team")],
            },
        ],
        "licenses": [
            {
                "name": "CC-BY-4.0",
                "title": "Creative Commons Attribution 4.0 International",
                "path": "https://creativecommons.org/licenses/by/4.0/legalcode",
                "instruction": "You are free: To Share, To Create, To Adapt; As long as you: Attribute.",
                "attribution": "eGon developers",
            }
        ],
        "contributors": [
            {
                "title": "Ilka Cußmann",
                "email": "",
                "object": "",
                "comment": "Added hydro and biomass plants",
            },
            {
                "title": "Clara Büttner",
                "email": "",
                "object": "",
                "comment": "Added table structure",
            },
            {
                "title": "Katharina Esterl",
                "email": "",
                "object": "",
                "comment": "Add pv ground mounted",
            },
            {
                "title": "Carlos Epia",
                "email": "",
                "object": "",
                "comment": "Added wind power plants",
            },
        ],
        "resources": [
            {
                "profile": "tabular-data-resource",
                "name": "supply.egon_power_plants",
                "path": "",
                "format": "PostgreSQL",
                "encoding": "UTF-8",
                "schema": {
                    "fields": [
                        {
                            "name": "id",
                            "description": "Unique identifier for power plants",
                            "type": "Integer ",
                            "unit": "",
                        },
                        {
                            "name": "sources",
                            "description": "sources of power plants",
                            "type": "Dictionary",
                            "unit": "",
                        },
                        {
                            "name": "source_id",
                            "description": "Unique Identifier of sources ",
                            "type": "Dictionary",
                            "unit": "",
                        },
                        {
                            "name": "carrier",
                            "description": "Energy carrier such as biomass, wind_onshore",
                            "type": "String",
                            "unit": "",
                        },
                        {
                            "name": "el_capacity",
                            "description": "Installed electrical capacity in MW",
                            "type": "",
                            "unit": "MW",
                        },
                        {
                            "name": "bus_id",
                            "description": "bus ID where the power plant is connected",
                            "type": "Integer",
                            "unit": "",
                        },
                        {
                            "name": "voltage_level",
                            "description": "voltage level of power plant",
                            "type": "Integer",
                            "unit": "",
                        },
                        {
                            "name": "weather_cell_id",
                            "description": "Unique identifier of the corresponding weather cell",
                            "type": "Integer",
                            "unit": "",
                        },
                        {
                            "name": "scenario",
                            "description": "eGon scenario, for example eGon235 or eGon100RE",
                            "type": "String",
                            "unit": "",
                        },
                        {
                            "name": "geometry",
                            "description": "geometry of the power plant",
                            "type": "String",
                            "unit": "",
                        },
                        {
                            "name": "",
                            "description": "",
                            "type": "",
                            "unit": "",
                        },
                    ],
                    "primaryKey": "",
                },
                "dialect": {"delimiter": "", "decimalSeparator": ""},
            }
        ],
        "review": {"path": "", "badge": ""},
        "metaMetadata": {
            "metadataVersion": "OEP-1.4.1",
            "metadataLicense": {
                "name": "CC0-1.0",
                "title": "Creative Commons Zero v1.0 Universal",
                "path": "https://creativecommons.org/publicdomain/zero/1.0/",
            },
        },
        "_comment": {
            "metadata": "Metadata documentation and explanation (https://github.com/OpenEnergyPlatform/oemetadata/blob/master/metadata/v141/metadata_key_description.md)",
            "dates": "Dates and time must follow the ISO8601 including time zone (YYYY-MM-DD or YYYY-MM-DDThh:mm:ss±hh)",
            "units": "Use a space between numbers and units (100 m)",
            "languages": "Languages must follow the IETF (BCP47) format (en-GB, en-US, de-DE)",
            "licenses": "License name must follow the SPDX License List (https://spdx.org/licenses/)",
            "review": "Following the OEP Data Review (https://github.com/OpenEnergyPlatform/data-preprocessing/wiki)",
            "none": "If not applicable use (none)",
        },
    }

    db.submit_comment(
        "'" + json.dumps(meta) + "'", "supply", "egon_power_plants"
    )
