from egon.data.db import engine
from geoalchemy2 import Geometry
from sqlalchemy import MetaData, Table
from sqlalchemy.dialects.postgresql.base import ischema_names


def context():
    """
    Project context information for metadata

    Returns
    -------
    dict
        OEP metadata conform data license information
    """

    return {
        "homepage": "https://ego-n.org/",
        "documentation": "https://egon-data.readthedocs.io/en/latest/",
        "sourceCode": "https://github.com/openego/eGon-data",
        "contact": "https://ego-n.org/partners/",
        "grantNo": "03EI1002",
        "fundingAgency": "Bundesministerium für Wirtschaft und Energie",
        "fundingAgencyLogo": "https://www.innovation-beratung-"
        "foerderung.de/INNO/Redaktion/DE/Bilder/"
        "Titelbilder/titel_foerderlogo_bmwi.jpg?"
        "__blob=normal&v=3",
        "publisherLogo": "https://ego-n.org/images/eGon_logo_"
        "noborder_transbg.svg",
    }


def meta_metadata():
    """
    Meta data on metadata

    Returns
    -------
    dict
        OEP metadata conform metadata on metadata
    """

    return {
        "metadataVersion": "OEP-1.4.1",
        "metadataLicense": {
            "name": "CC0-1.0",
            "title": "Creative Commons Zero v1.0 Universal",
            "path": ("https://creativecommons.org/publicdomain/zero/1.0/"),
        },
    }


def licenses_datenlizenz_deutschland(attribution):
    """
    License information for Datenlizenz Deutschland

    Parameters
    ----------
    attribution : str
        Attribution for the dataset incl. © symbol, e.g. '© GeoBasis-DE / BKG'

    Returns
    -------
    dict
        OEP metadata conform data license information
    """

    return {
        "name": "dl-by-de/2.0",
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
            "elektronischen Netzwerken eingebunden werden.\n"
            "Bei der Nutzung ist sicherzustellen, dass folgende Angaben "
            "als Quellenvermerk enthalten sind:\n"
            "(1) Bezeichnung des Bereitstellers nach dessen Maßgabe,\n"
            "(2) der Vermerk Datenlizenz Deutschland – Namensnennung – "
            "Version 2.0 oder dl-de/by-2-0 mit Verweis auf den Lizenztext "
            "unter www.govdata.de/dl-de/by-2-0 sowie\n"
            "(3) einen Verweis auf den Datensatz (URI)."
            "Dies gilt nur soweit die datenhaltende Stelle die Angaben"
            "(1) bis (3) zum Quellenvermerk bereitstellt.\n"
            "Veränderungen, Bearbeitungen, neue Gestaltungen oder "
            "sonstige Abwandlungen sind im Quellenvermerk mit dem Hinweis "
            "zu versehen, dass die Daten geändert wurden."
        ),
        "attribution": attribution,
    }


def license_odbl(attribution):
    """
    License information for Open Data Commons Open Database License (ODbL-1.0)

    Parameters
    ----------
    attribution : str
        Attribution for the dataset incl. © symbol, e.g.
        '© OpenStreetMap contributors'

    Returns
    -------
    dict
        OEP metadata conform data license information
    """
    return {
        "name": "ODbL-1.0",
        "title": "Open Data Commons Open Database License 1.0",
        "path": "https://opendatacommons.org/licenses/odbl/1.0/index.html",
        "instruction": "You are free: To Share, To Create, To Adapt; "
        "As long as you: Attribute, Share-Alike, Keep open!",
        "attribution": attribution,
    }


def license_ccby(attribution):
    """
    License information for Creative Commons Attribution 4.0 International
    (CC-BY-4.0)

    Parameters
    ----------
    attribution : str
        Attribution for the dataset incl. © symbol, e.g. '© GeoBasis-DE / BKG'

    Returns
    -------
    dict
        OEP metadata conform data license information
    """
    return {
        "name": "CC-BY-4.0",
        "title": "Creative Commons Attribution 4.0 International",
        "path": "https://creativecommons.org/licenses/by/4.0/legalcode",
        "instruction": "You are free: To Share, To Create, To Adapt; "
        "As long as you: Attribute.",
        "attribution": attribution,
    }


def license_geonutzv(attribution):
    """
    License information for GeoNutzV

    Parameters
    ----------
    attribution : str
        Attribution for the dataset incl. © symbol, e.g. '© GeoBasis-DE / BKG'

    Returns
    -------
    dict
        OEP metadata conform data license information
    """
    return {
        "name": "geonutzv-de-2013-03-19",
        "title": "Verordnung zur Festlegung der Nutzungsbestimmungen für die "
        "Bereitstellung von Geodaten des Bundes",
        "path": "https://www.gesetze-im-internet.de/geonutzv/",
        "instruction": "Geodaten und Geodatendienste, einschließlich "
        "zugehöriger Metadaten, werden für alle derzeit "
        "bekannten sowie für alle zukünftig bekannten Zwecke "
        "kommerzieller und nicht kommerzieller Nutzung "
        "geldleistungsfrei zur Verfügung gestellt, soweit "
        "durch besondere Rechtsvorschrift nichts anderes "
        "bestimmt ist oder vertragliche oder gesetzliche "
        "Rechte Dritter dem nicht entgegenstehen.",
        "attribution": attribution,
    }


def generate_resource_fields_from_sqla_model(model):
    """Generate a template for the resource fields for metadata from a SQL
    Alchemy model.

    For details on the fields see field 14.6.1 of `Open Energy Metadata
    <https://github.com/OpenEnergyPlatform/ oemetadata/blob/develop/metadata/
    v141/metadata_key_description.md>`_ standard.
    The fields `name` and `type` are automatically filled, the `description`
    and `unit` must be filled manually.

    Examples
    --------
    >>> from egon.data.metadata import generate_resource_fields_from_sqla_model
    >>> from egon.data.datasets.zensus_vg250 import Vg250Sta
    >>> resources = generate_resource_fields_from_sqla_model(Vg250Sta)

    Parameters
    ----------
    model : sqlalchemy.ext.declarative.declarative_base()
        SQLA model

    Returns
    -------
    list of dict
        Resource fields
    """

    return [
        {
            "name": col.name,
            "description": "",
            "type": str(col.type).lower(),
            "unit": "none",
        }
        for col in model.__table__.columns
    ]


def generate_resource_fields_from_db_table(schema, table, geom_columns=None):
    """Generate a template for the resource fields for metadata from a
    database table.

    For details on the fields see field 14.6.1 of `Open Energy Metadata
    <https://github.com/OpenEnergyPlatform/ oemetadata/blob/develop/metadata/
    v141/metadata_key_description.md>`_ standard.
    The fields `name` and `type` are automatically filled, the `description`
    and `unit` must be filled manually.

    Examples
    --------
    >>> from egon.data.metadata import generate_resource_fields_from_db_table
    >>> resources = generate_resource_fields_from_db_table(
    ...     'openstreetmap', 'osm_point', ['geom', 'geom_centroid']
    ... )  # doctest: +SKIP

    Parameters
    ----------
    schema : str
        The target table's database schema
    table : str
        Database table on which to put the given comment
    geom_columns : list of str
        Names of all geometry columns in the table. This is required to return
        Geometry data type for those columns as SQL Alchemy does not recognize
        them correctly. Defaults to ['geom'].

    Returns
    -------
    list of dict
        Resource fields
    """

    # handle geometry columns
    if geom_columns is None:
        geom_columns = ["geom"]
    for col in geom_columns:
        ischema_names[col] = Geometry

    table = Table(
        table, MetaData(), schema=schema, autoload=True, autoload_with=engine()
    )

    return [
        {
            "name": col.name,
            "description": "",
            "type": str(col.type).lower(),
            "unit": "none",
        }
        for col in table.c
    ]


def sources():
    return {
        "bgr_inspee": {
            "title": "Salt structures in Northern Germany",
            "description": 'The application "Information System Salt Structures" provides information about the '
            "areal distribution of salt structures (stocks and pillows) in Northern Germany. With general structural "
            "describing information, such as depth, secondary thickness, types of use or state of exploration, queries "
            "can be conducted. Contours of the salt structures can be displayed at horizontal cross-sections at four "
            "different depths up to a maximum depth of 2000 m below NN. A data sheet with information and further "
            "reading is provided for every single salt structure. Taking into account the fact that this work was "
            "undertaken at a scale for providing an overview and not for investigation of single structures, the scale "
            "of display is limited to a minimum of 1:300.000. This web application is the product of a BMWi-funded "
            'research project "InSpEE" running from the year 2012 to 2015. The acronym stands for "Information system '
            "salt structures: planning basis, selection criteria and estimation of the potential for the construction "
            'of salt caverns for the storage of renewable energies (hydrogen and compressed air)".',
            "path": "https://produktcenter.bgr.de/terraCatalog/DetailResult.do?fileIdentifier=338136ea-261a-4569-a2bf-92999d09bad2",
            "license": license_geonutzv("© BGR, Hannover, 2015"),
        },
        "bgr_inspeeds": {
            "title": "Flat layered salts in Germany",
            "description": "Which salt formations are suitable for storing hydrogen or compressed air? "
            "In the InSpEE-DS research project, scientists developed requirements and criteria for the assessment "
            "of suitable sites even if their exploration is still at an early stage and there is little knowledge of "
            "the salinaries' structures. Scientists at DEEP.KBB GmbH in Hanover, worked together with their project "
            "partners at the Federal Institute for Geosciences and Natural Resources and the Leibniz University "
            "Hanover, Institute for Geotechnics Hanover, to develop the planning basis for the site selection and for "
            "the construction of storage caverns in flat layered salt and multiple or double saliniferous formations. "
            "Such caverns could store renewable energy in the form of hydrogen or compressed air. While the previous "
            "project InSpEE was limited to salt formations of great thickness in Northern Germany, salt horizons of "
            "different ages have now been examined all over Germany. To estimate the potential, depth contour maps of "
            "the top and the base as well as thickness maps of the respective stratigraphic units and reference "
            "profiles were developed. Information on compressed air and hydrogen storage potential were given for the "
            "identified areas and for the individual federal states. The web service "
            '"Information system for flat layered salt" gives access to this data. The scale of display is limited '
            "to a minimum of 1:300.000. This geographic information is product of a BMWi-funded research project "
            '"InSpEE-DS" running from the year 2015 to 2019. The acronym stands for "Information system salt: '
            "planning basis, selection criteria and estimation of the potential for the construction of salt caverns "
            'for the storage of renewable energies (hydrogen and compressed air) - double saline and flat salt layers".',
            "path": "https://produktcenter.bgr.de/terraCatalog/DetailResult.do?fileIdentifier=630430b8-4025-4d6f-9a62-025b53bc8b3d",
            "license": license_geonutzv("© BGR, Hannover, 2021"),
        },
        "bgr_inspeeds_data_bundle": {
            "title": "Informationssystem Salz: Planungsgrundlagen, Auswahlkriterien und Potenzialabschätzung für die "
            "Errichtung von Salzkavernen zur Speicherung von Erneuerbaren Energien (Wasserstoff und Druckluft) – "
            "Doppelsalinare und flach lagernde Salzschichten. Teilprojekt Bewertungskriterien und Potenzialabschätzung",
            "description": "The data-bundle provides the shapefiles corresponding to the data provided in figure 7-1 "
            "(Donadei, S., et al., 2020, p. 7-5). The energy storage potential data are provided per federal state "
            " in table 7-1 (Donadei, S., et al., 2020, p. 7-4). Note: Please include all bgr data sources when using "
            "the data.",
            "path": "https://dx.doi.org/10.5281/zenodo.4896526",
            "license": license_geonutzv("???"),
        },
        "bgr_inspeeds_report": {
            "title": "Informationssystem Salz: Planungsgrundlagen, Auswahlkriterien und Potenzialabschätzung für die "
            "Errichtung von Salzkavernen zur Speicherung von Erneuerbaren Energien (Wasserstoff und Druckluft) – "
            "Doppelsalinare und flach lagernde Salzschichten. Teilprojekt Bewertungskriterien und Potenzialabschätzung",
            "description": "The report includes availability of saltstructures for energy storage and energy "
            "storage potential accumulated per federal state in Germany.",
            "path": "https://www.bgr.bund.de/DE/Themen/Nutzung_tieferer_Untergrund_CO2Speicherung/Downloads/InSpeeDS_TP_Bewertungskriterien.pdf?__blob=publicationFile&v=3",
            "license": license_geonutzv("© BGR, Hannover, 2021"),
        },
        "hotmaps_industrial_sites": {
            "titel": "industrial_sites_Industrial_Database",
            "description": "Georeferenced industrial sites of energy-intensive industry sectors in EU28",
            "path": "https://gitlab.com/hotmaps/industrial_sites/industrial_sites_Industrial_Database",
            "licenses": license_ccby("© 2016-2018: Pia Manz, Tobias Fleiter"),
        },
        "hotmaps_scen_buildings": {
            "titel": "scen_current_building_demand",
            "description": "Energy demand scenarios in buidlings until the year 2050 - current policy scenario",
            "path": "https://gitlab.com/hotmaps/scen_current_building_demand",
            "licenses": license_ccby(
                "© 2016-2018: Michael Hartner, Lukas Kranzl, Sebastian Forthuber, Sara Fritz, Andreas Müller"
            ),
        },
        "mastr": {
            "title": "open-MaStR power unit registry",
            "description": "Raw data download Marktstammdatenregister (MaStR) data "
            "using the webservice. All data from the Marktstammdatenregister is included."
            "There are duplicates included. For further information read in the documentation"
            "of the original data source: https://www.marktstammdatenregister.de/MaStRHilfe/subpages/statistik.html",
            "path": "https://sandbox.zenodo.org/record/808086",
            "licenses": licenses_datenlizenz_deutschland(
                "© 2021 Bundesnetzagentur für Elektrizität, Gas, Telekommunikation, Post und Eisenbahnen"
            ),
        },
        "openffe_gas": {
            "title": "Load Curves of the Industry Sector – eXtremOS solidEU Scenario (Europe NUTS-3)",
            "description": "Load Curves of the Industry Sector for the eXtremOS solidEU Scenario Scenario at NUTS-3-Level. "
            "More information at https://extremos.ffe.de/.",
            "path": "http://opendata.ffe.de/dataset/load-curves-of-the-industry-sector-extremos-solideu-scenario-europe-nuts-3/",
            "license": license_ccby("© FfE, eXtremOS Project"),
        },
        "openstreetmap": {
            "title": "OpenStreetMap Data Extracts (Geofabrik)",
            "description": "Full data extract of OpenStreetMap data for defined "
            "spatial extent at ''referenceDate''",
            "path": "https://download.geofabrik.de/europe/germany-210101.osm.pbf",
            "licenses": license_odbl("© OpenStreetMap contributors"),
        },
        "vg250": {
            "title": "Verwaltungsgebiete 1:250 000 (Ebenen)",
            "description": "Der Datenbestand umfasst sämtliche Verwaltungseinheiten der "
            "hierarchischen Verwaltungsebenen vom Staat bis zu den Gemeinden "
            "mit ihren Grenzen, statistischen Schlüsselzahlen, Namen der "
            "Verwaltungseinheit sowie die spezifische Bezeichnung der "
            "Verwaltungsebene des jeweiligen Landes.",
            "path": "https://daten.gdz.bkg.bund.de/produkte/vg/vg250_ebenen_0101/2020/vg250_01-01.geo84.shape.ebenen.zip",
            "licenses": licenses_datenlizenz_deutschland(
                "© Bundesamt für Kartographie und Geodäsie "
                "2020 (Daten verändert)"
            ),
        },
        "zensus": {
            "title": "Statistisches Bundesamt (Destatis) - Ergebnisse des Zensus 2011 zum Download",
            "description": "Als Download bieten wir Ihnen auf dieser Seite zusätzlich zur "
            "Zensusdatenbank CSV- und teilweise Excel-Tabellen mit umfassenden Personen-, Haushalts- "
            "und Familien- sowie Gebäude- und Wohnungs­merkmalen. Die Ergebnisse liegen auf Bundes-, "
            "Länder-, Kreis- und Gemeinde­ebene vor. Außerdem sind einzelne Ergebnisse für Gitterzellen verfügbar.",
            "path": "https://www.zensus2011.de/SharedDocs/Aktuelles/Ergebnisse/DemografischeGrunddaten.html;jsessionid=E0A2B4F894B258A3B22D20448F2E4A91.2_cid380?nn=3065474",
            "license": licenses_datenlizenz_deutschland(
                "© Statistische Ämter des Bundes und der Länder 2014"
            ),
        },
    }
