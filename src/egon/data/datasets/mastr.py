"""
Download Marktstammdatenregister (MaStR) from Zenodo.

"""

from pathlib import Path
from urllib.request import urlretrieve
import pandas as pd
import os
import zipfile

from egon.data.datasets import Dataset
import egon.data.config

WORKING_DIR_MASTR_NEW = Path(".", "bnetza_mastr", "dump_2025-02-09")


def download_mastr_data():
    """Download MaStR data from Zenodo."""

    def download(dataset_name, download_dir):
        print(f"Downloading dataset {dataset_name} to {download_dir} ...")
        # Get parameters from config and set download URL
        data_config = egon.data.config.datasets()[dataset_name]
        zenodo_files_url = (
            f"https://zenodo.org/record/" f"{data_config['deposit_id']}/files/"
        )

        dump_file_name = data_config["dump_name"] + ".zip"

        if not os.path.isfile(dump_file_name):
            urlretrieve(
                zenodo_files_url + dump_file_name,
                download_dir / dump_file_name,
            )

    if not os.path.exists(WORKING_DIR_MASTR_NEW):
        WORKING_DIR_MASTR_NEW.mkdir(exist_ok=True, parents=True)

    download(dataset_name="mastr_new", download_dir=WORKING_DIR_MASTR_NEW)


def extract_and_preprocess_mastr():

    # Extract mastr
    data_config = egon.data.config.datasets()["mastr_new"]
    dump_file_name = data_config["dump_name"]
    raw_data_path = WORKING_DIR_MASTR_NEW / dump_file_name

    with zipfile.ZipFile(
        WORKING_DIR_MASTR_NEW / (dump_file_name + ".zip"), "r"
    ) as zip_ref:
        zip_ref.extractall(WORKING_DIR_MASTR_NEW)

    # prepocess mastr data
    wind = pd.read_csv(raw_data_path / "bnetza_mastr_wind_raw.csv")
    solar = pd.read_csv(raw_data_path / "bnetza_mastr_solar_raw.csv")
    bio_with_th_power = pd.read_csv(
        raw_data_path / "bnetza_mastr_biomass_raw.csv"
    )
    hydro = pd.read_csv(raw_data_path / "bnetza_mastr_hydro_raw.csv")
    gsgk = pd.read_csv(raw_data_path / "bnetza_mastr_gsgk_raw.csv")
    storage = pd.read_csv(raw_data_path / "bnetza_mastr_storage_raw.csv")
    combustion_with_th_power = pd.read_csv(
        raw_data_path / "bnetza_mastr_combustion_raw.csv"
    )
    nuclear = pd.read_csv(raw_data_path / "bnetza_mastr_nuclear_raw.csv")

    loc = pd.read_csv(
        raw_data_path / "bnetza_mastr_locations_extended_raw.csv"
    )
    gcp = pd.read_csv(raw_data_path / "bnetza_mastr_grid_connections_raw.csv")

    loc_vlevel = loc.merge(
        gcp,
        left_on="Netzanschlusspunkte",
        right_on="NetzanschlusspunktMastrNummer",
        how="left",
    )

    loc_vlevel.replace(
        {
            "Spannungsebene": {
                "Niederspannung (= Hausanschluss/Haushaltsstrom)": "Niederspannung",
                "Umspannebene Mittelspannung/Niederspannung": "UmspannungZurNiederspannung",
                "Umspannebene Hochspannung/Mittelspannung": "UmspannungZurMittelspannung",
                "Umspannebene Höchstspannung/Hochspannung": "UmspannungZurHochspannung",
            }
        },
        inplace=True,
    )

    # Locations and grid conn. points
    cols_mapping = {"MastrNummer": "MaStRNummer"}
    loc_vlevel.rename(columns=cols_mapping).to_csv(
        WORKING_DIR_MASTR_NEW / "location_elec_generation_raw.csv",
        index=None,
        encoding="UTF-8",
    )

    # Fix solar
    solar["Standort"] = solar.Postleitzahl.apply(str) + " " + solar.Ort
    solar["Bruttoleistung_extended"] = solar.Bruttoleistung
    solar["InstallierteLeistung"] = solar.Bruttoleistung

    cols_mapping = {
        "ZugeordneteWirkleistungWechselrichter": "zugeordneteWirkleistungWechselrichter"
    }

    solar.rename(columns=cols_mapping, inplace=True)

    cols_mapping = {"MastrNummer": "MaStRNummer"}

    states_renaming = {
        "Thüringen": "Thueringen",
        "Schleswig-Holstein": "SchleswigHolstein",
        "Nordrhein-Westfalen": "NordrheinWestfalen",
        "Rheinland-Pfalz": "RheinlandPfalz",
        "Baden-Württemberg": "BadenWuerttemberg",
        "Sachsen-Anhalt": "SachsenAnhalt",
        "Mecklenburg-Vorpommern": "MecklenburgVorpommern",
        "Ausschließliche Wirtschaftszone": "AusschliesslicheWirtschaftszone",
    }
    status_renaming = {
        "In Betrieb": "InBetrieb",
        "Vorübergehend stillgelegt": "VoruebergehendStillgelegt",
        "Endgültig stillgelegt": "DauerhaftStillgelegt",
        "In Planung": "InPlanung",
    }
    values_renaming = {
        "Bundesland": states_renaming,
        "EinheitBetriebsstatus": status_renaming,
    }

    # Export data
    wind.rename(columns=cols_mapping).replace(values_renaming).to_csv(
        WORKING_DIR_MASTR_NEW / "bnetza_mastr_wind_cleaned.csv",
        index=None,
        encoding="UTF-8",
    )

    solar.rename(columns=cols_mapping).replace(values_renaming).to_csv(
        WORKING_DIR_MASTR_NEW / "bnetza_mastr_solar_cleaned.csv",
        index=None,
        encoding="UTF-8",
    )

    bio_with_th_power.rename(columns=cols_mapping).replace(
        values_renaming
    ).to_csv(
        WORKING_DIR_MASTR_NEW / "bnetza_mastr_biomass_cleaned.csv",
        index=None,
        encoding="UTF-8",
    )

    hydro.rename(columns=cols_mapping).replace(values_renaming).to_csv(
        WORKING_DIR_MASTR_NEW / "bnetza_mastr_hydro_cleaned.csv",
        index=None,
        encoding="UTF-8",
    )

    gsgk.rename(columns=cols_mapping).replace(values_renaming).to_csv(
        WORKING_DIR_MASTR_NEW / "bnetza_mastr_gsgk_cleaned.csv",
        index=None,
        encoding="UTF-8",
    )

    storage.rename(columns=cols_mapping).replace(values_renaming).to_csv(
        WORKING_DIR_MASTR_NEW / "bnetza_mastr_storage_cleaned.csv",
        index=None,
        encoding="UTF-8",
    )

    combustion_with_th_power.rename(columns=cols_mapping).replace(
        values_renaming
    ).to_csv(
        WORKING_DIR_MASTR_NEW / "bnetza_mastr_combustion_cleaned.csv",
        index=None,
        encoding="UTF-8",
    )

    nuclear.rename(columns=cols_mapping).replace(values_renaming).to_csv(
        WORKING_DIR_MASTR_NEW / "bnetza_mastr_nuclear_cleaned.csv",
        index=None,
        encoding="UTF-8",
    )


class mastr_data_setup(Dataset):
    """
    Download Marktstammdatenregister (MaStR) from Zenodo.

    *Dependencies*
      * :py:func:`Setup <egon.data.datasets.database.setup>`

    The downloaded data incorporates two different datasets:

    Dump 2021-04-30
      * Source: https://zenodo.org/records/10480930
      * Used technologies: PV plants, wind turbines, biomass, hydro plants,
        combustion, nuclear, gsgk, storage
      * Data is further processed in the :py:class:`PowerPlants
        <egon.data.datasets.power_plants.PowerPlants>` dataset

    Dump 2022-11-17
      * Source: https://zenodo.org/records/10480958
      * Used technologies: PV plants, wind turbines, biomass, hydro plants
      * Data is further processed in module :py:mod:`mastr
        <egon.data.datasets.power_plants.mastr>` and :py:class:`PowerPlants
        <egon.data.datasets.power_plants.PowerPlants>`

    See documentation section :ref:`mastr-ref` for more information.

    """

    #:
    name: str = "MastrData"
    #:
    version: str = "0.0.3"
    #:
    tasks = (download_mastr_data, extract_and_preprocess_mastr)

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=self.tasks,
        )
