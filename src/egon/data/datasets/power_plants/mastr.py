"""Import MaStR dataset and write to DB tables"""
import pandas as pd

from egon.data.datasets.power_plants import (
    EgonPowerPlantsBiomass,
    EgonPowerPlantsHydro,
    EgonPowerPlantsPv,
    EgonPowerPlantsWind,
)
import egon.data.config


def import_mastr():
    cfg = egon.data.config.datasets()["power_plants"]

    cols_mapping = {
        "all": {
            "EinheitMastrNummer": "gens_id",
            "EinheitBetriebsstatus": "status",
            "Inbetriebnahmedatum": "commissioning_date",
            "Postleitzahl": "postcode",
            "Ort": "city",
            "Bundesland": "federal_state",
            "Nettonennleistung": "capacity",
            "Einspeisungsart": "feedin_type",
        },
        "pv": {
            "Lage": "site_type",
            "Nutzungsbereich": "usage_sector",
            "Hauptausrichtung": "orientation_primary",
            "HauptausrichtungNeigungswinkel": "orientation_primary_angle",
            "Nebenausrichtung": "orientation_secondary",
            "NebenausrichtungNeigungswinkel": "orientation_secondary_angle",
            "EinheitlicheAusrichtungUndNeigungswinkel": "orientation_uniform",
            "AnzahlModule": "module_count",
            "ZugeordneteWirkleistungWechselrichter": "capacity_inverter",
        },
        "wind": {
            "Lage": "site_type",
            "Hersteller": "manufacturer_name",
            "Typenbezeichnung": "type_name",
            "Nabenhoehe": "hub_height",
            "Rotordurchmesser": "rotor_diameter",
        },
        "biomass": {
            "Technologie": "technology",
            "Hauptbrennstoff": "fuel_name",
            "Biomasseart": "fuel_type",
            "ThermischeNutzleistung": "th_capacity",
        },
        "hydro": {
            "ArtDerWasserkraftanlage": "type",
            "ArtDesZuflusses": "water_origin",
        },
    }

    source_files = {
        "pv": cfg["sources"]["mastr_pv"],
        "wind": cfg["sources"]["mastr_wind"],
        "biomass": cfg["sources"]["mastr_biomass"],
        "hydro": cfg["sources"]["mastr_hydro"],
    }
    target_tables = {
        "pv": EgonPowerPlantsPv,
        "wind": EgonPowerPlantsWind,
        "biomass": EgonPowerPlantsBiomass,
        "hydro": EgonPowerPlantsHydro,
    }

    # import locations
    locations = pd.read_csv(cfg["sources"]["mastr_location"], index_col=None)

    # import units
    technologies = ["pv", "wind", "biomass", "hydro"]
    for tech in technologies:
        units = pd.read_csv(
            source_files[tech],
            usecols=(
                list(cols_mapping["all"].keys())
                + list(cols_mapping[tech].keys())
            ),
            index_col=None,
        ).rename(columns=cols_mapping)
