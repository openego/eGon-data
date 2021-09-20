import datetime


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
        "noborder_transbg.svg"
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
        "attribution": attribution
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
        "instruction": "You are free: To Share, To Create, To Adapt; As long as you: Attribute, Share-Alike, Keep open!",
        "attribution": attribution
    }
