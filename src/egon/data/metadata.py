from geoalchemy2 import Geometry
from sqlalchemy import MetaData, Table
from sqlalchemy.dialects.postgresql.base import ischema_names

from egon.data.db import engine

EGON_ATTRIBUTION: str = "© eGon development team"


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


def licenses_datenlizenz_deutschland(attribution=EGON_ATTRIBUTION):
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


def license_odbl(attribution=EGON_ATTRIBUTION):
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


def license_ccby(attribution=EGON_ATTRIBUTION):
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


def license_geonutzv(attribution=EGON_ATTRIBUTION):
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


def license_agpl(attribution=EGON_ATTRIBUTION):
    """
    License information for GNU Affero General Public License v3.0

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
        "name": "AGPL-3.0 License",
        "title": "GNU Affero General Public License v3.0",
        "path": "https://www.gnu.org/licenses/agpl-3.0.de.html",
        "instruction": "Permissions of this strongest copyleft license are"
        "conditioned on making available complete source code of licensed "
        "works and modifications, which include larger works using a licensed"
        "work, under the same license. Copyright and license notices must be"
        "preserved. Contributors provide an express grant of patent rights."
        "When a modified version is used to provide a service over a network,"
        "the complete source code of the modified version must be made "
        "available.",
        "attribution": attribution,
    }


def license_dedl(attribution=EGON_ATTRIBUTION):
    """
    License information for Data licence Germany – attribution – version 2.0

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
        "name": "DL-DE-BY-2.0",
        "title": "Data licence Germany – attribution – version 2.0",
        "path": "https://www.govdata.de/dl-de/by-2-0",
        "instruction": (
            "Any use will be permitted provided it fulfils the requirements of"
            ' this "Data licence Germany – attribution – Version 2.0". The '
            "data and meta-data provided may, for commercial and "
            "non-commercial use, in particular be copied, printed, presented, "
            "altered, processed and transmitted to third parties; be merged "
            "with own data and with the data of others and be combined to form"
            " new and independent datasets; be integrated in internal and "
            "external business processes, products and applications in public "
            "and non-public electronic networks. The user must ensure that the"
            " source note contains the following information: the name of the "
            'provider, the annotation "Data licence Germany – attribution – '
            'Version 2.0" or "dl-de/by-2-0" referring to the licence text '
            "available at www.govdata.de/dl-de/by-2-0, and a reference to the "
            "dataset (URI). This applies only if the entity keeping the data "
            "provides the pieces of information 1-3 for the source note. "
            "Changes, editing, new designs or other amendments must be marked "
            "as such in the source note."
        ),
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
            "licenses": [license_geonutzv("© BGR, Hannover, 2015")],
        },
        "bgr_inspeeds": {
            "title": "Flat layered salts in Germany",
            "description": "Which salt formations are suitable for storing hydrogen or compressed air? "
            "In the InSpEE-DS research project, scientists developed requirements and criteria for the assessment "
            "of suitable sites even if their exploration is still at an early stage and there is little knowledge of "
            "the salinaries structures. Scientists at DEEP.KBB GmbH in Hanover, worked together with their project "
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
            "licenses": [license_geonutzv("© BGR, Hannover, 2021")],
        },
        "bgr_inspeeds_data_bundle": {
            "title": "Informationssystem Salz: Planungsgrundlagen, Auswahlkriterien und Potenzialabschätzung für die "
            "Errichtung von Salzkavernen zur Speicherung von Erneuerbaren Energien (Wasserstoff und Druckluft) – "
            "Doppelsalinare und flach lagernde Salzschichten. Teilprojekt Bewertungskriterien und Potenzialabschätzung",
            "description": "Shapefiles corresponding to the data provided in figure 7-1 "
            "(Donadei, S., et al., 2020, p. 7-5). The energy storage potential data are provided per federal state "
            " in table 7-1 (Donadei, S., et al., 2020, p. 7-4). Note: Please include all bgr data sources when using "
            "the data.",
            "path": "https://dx.doi.org/10.5281/zenodo.4896526",
            "licenses": [license_geonutzv("© BGR, Hannover, 2021")],
        },
        "bgr_inspeeds_report": {
            "title": "Informationssystem Salz: Planungsgrundlagen, Auswahlkriterien und Potenzialabschätzung für die "
            "Errichtung von Salzkavernen zur Speicherung von Erneuerbaren Energien (Wasserstoff und Druckluft) – "
            "Doppelsalinare und flach lagernde Salzschichten. Teilprojekt Bewertungskriterien und Potenzialabschätzung",
            "description": "The report includes availability of saltstructures for energy storage and energy "
            "storage potential accumulated per federal state in Germany.",
            "path": "https://www.bgr.bund.de/DE/Themen/Nutzung_tieferer_Untergrund_CO2Speicherung/Downloads/InSpeeDS_TP_Bewertungskriterien.pdf?__blob=publicationFile&v=3",
            "licenses": [license_geonutzv("© BGR, Hannover, 2021")],
        },
        "demandregio": {
            "title": "DemandRegio",
            "description": "Harmonisierung und Entwicklung von Verfahren zur regionalen und "
            "zeitlichen Auflösung von Energienachfragen",
            "path": "https://doi.org/10.34805/ffe-119-20",
            "licenses": [license_ccby("© FZJ, TUB, FfE")],
        },
        "egon-data": {
            "titel": "eGon-data",
            "description": "Workflow to download, process and generate data sets"
            "suitable for the further research conducted in the project eGon (https://ego-n.org/)",
            "path": "https://github.com/openego/eGon-data",
            "licenses": [license_agpl(EGON_ATTRIBUTION)],
        },
        "Einspeiseatlas": {
            "title": "Einspeiseatlas",
            "description": "Im Einspeiseatlas finden sie sich die Informationen "
            "zu realisierten und geplanten Biomethanaufbereitungsanlagen - mit "
            "und ohne Einspeisung ins Gasnetz - in Deutschland und weltweit.",
            "path": "https://www.biogaspartner.de/einspeiseatlas/",
            "licenses": [
                license_ccby("Deutsche Energie-Agentur (dena, 2021)")
            ],
        },
        "era5": {
            "title": "ERA5 global reanalysis",
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
        "dsm-heitkoetter": {
            "title": "Assessment of the regionalised demand response potential "
            "in Germany using an open source tool and dataset",
            "description": "With the expansion of renewable energies in Germany, "
            "imminent grid congestion events occur more often. One approach for "
            "avoiding curtailment of renewable energies is to cover excess feed-in "
            "by demand response. As curtailment is often a local phenomenon, in "
            "this work we determine the regional demand response potential for "
            "the 401 German administrative districts with a temporal resolution "
            "of 15 min, including technical, socio-technical and economic "
            "restrictions.",
            "path": "https://doi.org/10.1016/j.adapen.2020.100001",
            "licenses": [
                license_ccby(
                    "© 2020 German Aerospace Center (DLR), "
                    "Institute of Networked Energy Systems."
                )
            ],
        },
        "hotmaps_industrial_sites": {
            "titel": "industrial_sites_Industrial_Database",
            "description": "Georeferenced industrial sites of energy-intensive industry sectors in EU28",
            "path": "https://gitlab.com/hotmaps/industrial_sites/industrial_sites_Industrial_Database",
            "licenses": [
                license_ccby("© 2016-2018: Pia Manz, Tobias Fleiter")
            ],
        },
        "hotmaps_scen_buildings": {
            "titel": "scen_current_building_demand",
            "description": "Energy demand scenarios in buidlings until the year 2050 - current policy scenario",
            "path": "https://gitlab.com/hotmaps/scen_current_building_demand",
            "licenses": [
                license_ccby(
                    "© 2016-2018: Michael Hartner, Lukas Kranzl, Sebastian Forthuber, Sara Fritz, Andreas Müller"
                )
            ],
        },
        "mastr": {
            "title": "open-MaStR power unit registry",
            "description": "Raw data download Marktstammdatenregister (MaStR) data "
            "using the webservice. All data from the Marktstammdatenregister is included."
            "There are duplicates included. For further information read in the documentation"
            "of the original data source: https://www.marktstammdatenregister.de/MaStRHilfe/subpages/statistik.html",
            "path": "https://sandbox.zenodo.org/record/808086",
            "licenses": [
                licenses_datenlizenz_deutschland(
                    "© 2021 Bundesnetzagentur für Elektrizität, Gas, Telekommunikation, Post und Eisenbahnen"
                )
            ],
        },
        "nep2021": {
            "title": "Netzentwicklungsplan Strom 2035, Version 2021, erster Entwurf",
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
        "openffe_gas": {
            "title": "Load Curves of the Industry Sector – eXtremOS solidEU Scenario (Europe NUTS-3)",
            "description": "Load Curves of the Industry Sector for the eXtremOS solidEU Scenario Scenario at NUTS-3-Level. "
            "More information at https://extremos.ffe.de/.",
            "path": "http://opendata.ffe.de/dataset/load-curves-of-the-industry-sector-extremos-solideu-scenario-europe-nuts-3/",
            "licenses": [license_ccby("© FfE, eXtremOS Project")],
        },
        "openstreetmap": {
            "title": "OpenStreetMap Data Extracts (Geofabrik)",
            "description": "Full data extract of OpenStreetMap data for defined"
            "spatial extent at ''referenceDate''",
            "path": "https://download.geofabrik.de/europe/germany-210101.osm.pbf",
            "licenses": [license_odbl("© OpenStreetMap contributors")],
        },
        "peta": {
            "title": "Pan-European Thermal Atlas, Peta version 5.0.1",
            "description": "Modelled Heat Demand distribution (in GJ per hectare grid cell) for residential and service "
            "heat demands for space heating and hot water for the year 2015 using HRE4 data and the combined "
            "top-down bottom-up approach of HRE4. "
            "National sector-specific heat demand data, derived by the FORECAST model in HRE4 for residential "
            "(delivered energy, for space heating and hot water) and service-sector (delivered energy, for space heating, hot "
            "water and process heat) buildings for the year 2015, were distributed using modelled, spatial "
            "statistics based floor areas in 100x100m grids and a population grid. "
            "For further information please see the documentation available on the Heat Roadmap Europe website, "
            "in particular D2.3 report: Methodologies and assumptions used in the mapping.",
            "path": "https://s-eenergies-open-data-euf.hub.arcgis.com/search",
            "licenses": [
                license_ccby(
                    "© Europa-Universität Flensburg, Halmstad University and Aalborg University"
                )
            ],
        },
        "pipeline_classification": {
            "title": "Technical pipeline characteristics for high pressure pipelines",
            "description": "Parameters for the classification of gas pipelines, "
            "the whole documentation could is available at: "
            "https://www.econstor.eu/bitstream/10419/173388/1/1011162628.pdf",
            "path": "https://zenodo.org/record/5743452",
            "licenses": [license_ccby("© DIW Berlin, 2017")],
        },
        "schmidt": {
            "title": "Supplementary material to the masters thesis: "
            "NUTS-3 Regionalization of Industrial Load Shifting Potential in Germany using a Time-Resolved Model",
            "description": "Supplementary material to the named masters thesis, containing data on industrial processes"
            "for the estimation of NUTS-3 load shifting potential of suitable electrically powered industrial processes"
            "(cement milling, mechanical pulping, paper production, air separation).",
            "path": "https://zenodo.org/record/3613767",
            "licenses": [license_ccby("© 2019 Danielle Schmidt")],
        },
        "SciGRID_gas": {
            "title": "SciGRID_gas IGGIELGN",
            "description": "The SciGRID_gas dataset represents the European "
            "gas transport network (pressure levels of 20 bars and higher) "
            "including the geo-referenced transport pipelines,  compressor "
            "stations, LNG terminals, storage, production sites, gas power "
            "plants, border points, and demand time series. ",
            "path": "https://dx.doi.org/10.5281/zenodo.4896526",
            "licenses": [
                license_ccby(
                    "Jan Diettrich; Adam Pluta; Wided Medjroubi (DLR-VE)"
                ),
            ],
        },
        "seenergies": {
            "title": "D5 1 Industry Dataset With Demand Data",
            "description": "Georeferenced EU28 industrial sites with quantified annual excess heat volumes and demand data"
            "within main sectors: Chemical industry, Iron and steel, Non-ferrous metals, Non-metallic minerals, Paper and printing, and Refineries.",
            "path": "https://s-eenergies-open-data-euf.hub.arcgis.com/datasets/5e36c0af918040ed936b4e4c101f611d_0/about",
            "licenses": [license_ccby("© Europa-Universität Flensburg")],
        },
        "technology-data": {
            "titel": "Energy System Technology Data v0.3.0",
            "description": "This script compiles assumptions on energy system "
            "technologies (such as costs, efficiencies, lifetimes, etc.) for "
            "chosen years (e.g. [2020, 2030, 2050]) from a variety of sources "
            "into CSV files to be read by energy system modelling software. "
            "The merged outputs have standardized cost years, technology names, "
            "units and source information.",
            "path": "https://github.com/PyPSA/technology-data/tree/v0.3.0",
            "licenses": [
                license_agpl(
                    "© Marta Victoria (Aarhus University), Kun Zhu (Aarhus University), Elisabeth Zeyen (TUB), Tom Brown (TUB)"
                )
            ],
        },
        "tyndp": {
            "title": "Ten-Year Network Development Plan (TYNDP) 2020 Scenarios",
            "description": "ENTSOs’ TYNDP 2020 Scenario Report describes possible European energy futures up to 2050. "
            "Scenarios are not forecasts; they set out a range of possible futures used by the ENTSOs to test future "
            "electricity and gas infrastructure needs and projects. The scenarios are ambitious as they deliver "
            "a low carbon energy system for Europe by 2050. The ENTSOs have developed credible scenarios that are "
            "guided by technically sound pathways, while reflecting country by country specifics, so that a pan-European "
            "low carbon future is achieved.",
            "path": "https://tyndp.entsoe.eu/maps-data",
            "licenses": [license_ccby("© ENTSO-E and ENTSOG")],
        },
        "vg250": {
            "title": "Verwaltungsgebiete 1:250 000 (Ebenen)",
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
        "zensus": {
            "title": "Statistisches Bundesamt (Destatis) - Ergebnisse des Zensus 2011 zum Download",
            "description": "Als Download bieten wir Ihnen auf dieser Seite zusätzlich zur "
            "Zensusdatenbank CSV- und teilweise Excel-Tabellen mit umfassenden Personen-, Haushalts- "
            "und Familien- sowie Gebäude- und Wohnungs­merkmalen. Die Ergebnisse liegen auf Bundes-, "
            "Länder-, Kreis- und Gemeinde­ebene vor. Außerdem sind einzelne Ergebnisse für Gitterzellen verfügbar.",
            "path": "https://www.zensus2011.de/SharedDocs/Aktuelles/Ergebnisse/DemografischeGrunddaten.html;jsessionid=E0A2B4F894B258A3B22D20448F2E4A91.2_cid380?nn=3065474",
            "licenses": [
                licenses_datenlizenz_deutschland(
                    "© Statistische Ämter des Bundes und der Länder 2014"
                )
            ],
        },
    }


def contributors(authorlist):
    contributors_dict = {
        "am": {
            "title": "Aadit Malla",
            "email": "https://github.com/aadit879",
        },
        "an": {
            "title": "Amélia Nadal",
            "email": "https://github.com/AmeliaNadal",
        },
        "cb": {
            "title": "Clara Büttner",
            "email": "https://github.com/ClaraBuettner",
        },
        "ce": {
            "title": "Carlos Epia",
            "email": "https://github.com/CarlosEpia",
        },
        "fw": {
            "title": "Francesco Witte",
            "email": "https://github.com/fwitte",
        },
        "gp": {
            "title": "Guido Pleßmann",
            "email": "https://github.com/gplssm",
        },
        "ic": {
            "title": "Ilka Cußmann",
            "email": "https://github.com/IlkaCu",
        },
        "ja": {
            "title": "Jonathan Amme",
            "email": "https://github.com/nesnoj",
        },
        "je": {
            "title": "Jane Doe",
            "email": "https://github.com/JaneDoe",
        },
        "ke": {
            "title": "Katharina Esterl",
            "email": "https://github.com/KathiEsterl",
        },
        "kh": {
            "title": "Kilian Helfenbein",
            "email": "https://github.com/khelfen",
        },
        "sg": {
            "title": "Stephan Günther",
            "email": "https://github.com/gnn",
        },
        "um": {
            "title": "Ulf Müller",
            "email": "https://github.com/ulfmueller",
        },
    }
    return [
        {key: value for key, value in contributors_dict[author].items()}
        for author in authorlist
    ]
