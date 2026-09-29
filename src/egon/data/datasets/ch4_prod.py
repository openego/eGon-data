# -*- coding: utf-8 -*-
"""
The central module containing code dealing with importing CH4 production data

For the scenarios, the gas produced in Germany can be natural gas, biogas
or LNG. The source productions are geolocalised potentials described as
PyPSA generators. These generators are not extendable and their overall
production over the year is limited directly in eTraGo by values stored in
the table
:py:class:`scenario.egon_scenario_parameters <egon.data.datasets.scenario_parameters.EgonScenario>`.

"""

from pathlib import Path
from urllib.request import urlretrieve
import ast
import logging
import re

import geopandas as gpd
import numpy as np
import pandas as pd

from egon.data import config, db
from egon.data.config import settings
from egon.data.datasets import Dataset, DatasetSources, DatasetTargets
from egon.data.datasets.hydrogen_etrago.nep2025 import (
    NEP_CH4_LNG_TERMINALS,
    NEP_CH4_LNG_USE,
)
from egon.data.datasets.scenario_parameters import (
    get_scenario_year,
    get_sector_parameters,
)

logger = logging.getLogger(__name__)

# Capacity of the CH4 slack generator of the status quo scenarios [MW]
STATUS_SLACK_P_NOM = 100000


class CH4Production(Dataset):
    """
    Insert the CH4 productions into the database

    Insert the CH4 productions into the database by using the function
    :py:func:`insert_ch4_generators` for the scenarios with a CH4 grid
    and :py:func:`insert_ch4_generators_status` for the status quo
    scenarios, once per scenario.

    *Dependencies*
      * :py:class:`GasAreas <egon.data.datasets.gas_areas.GasAreas>`
      * :py:class:`GasNodesAndPipes <egon.data.datasets.gas_grid.GasNodesAndPipes>`

    *Resulting tables*
      * :py:class:`grid.egon_etrago_generator <egon.data.datasets.etrago_setup.EgonPfHvGenerator>` is extended

    """

    #:
    name: str = "CH4Production"
    #:

    version: str = "0.0.12"

    sources = DatasetSources(
        files={
            "scigrid_productions": (
                "datasets/gas_data/data/IGGIELGN_Productions.csv"
            ),
            # Einspeiseatlas of the dena (Biogaspartner), version of
            # 9 October 2025, see download_biogas_data
            "biogas_einspeiseatlas": (
                "datasets/gas_data/20251009_Einspeiseatlas_biogaspartner.xlsx"
            ),
        },
        urls={
            "biogas_einspeiseatlas": (
                "https://www.dena.de/fileadmin/biogaspartner/Dokumente/"
                "20251009_Einspeiseatlas_biogaspartner.xlsx"
            ),
        },
        tables={
            "buses": "grid.egon_etrago_bus",
            "gas_voronoi": "grid.egon_gas_voronoi",
            "vg250_sta_union": "boundaries.vg250_sta_union",
        },
    )

    targets = DatasetTargets(
        tables={
            "generators": "grid.egon_etrago_generator",
        }
    )

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(download_biogas_data, import_gas_generators),
            # Gas validation rules (#1526)
            validation=self.validation_rules(),
            proceed_on_validation_failure=True,
        )

    @staticmethod
    def validation_rules():
        """
        Return the validation rules of the dataset

        Returns
        -------
        dict
            Validation rules per validation task

        """
        from egon.data.validation.rules.custom.sanity.gas import (
            GasBusReferences,
            GasComponentTotal,
            GasParameterMatch,
            for_each_scenario,
        )

        return {
            "sanity": [
                # status2024: one slack generator at the single CH4 bus
                *for_each_scenario(
                    GasComponentTotal,
                    "SANITY_GAS_CH4_GENERATORS_DE",
                    table="grid.egon_etrago_generator",
                    carriers=["CH4"],
                    expected={
                        "status2024": 1,
                        "eGon2035": 5,
                        "reGon2037": 5,
                        "reGon2045": 4,
                    },
                ),
                *for_each_scenario(
                    GasComponentTotal,
                    "SANITY_GAS_CH4_GENERATORS_DE_P_NOM",
                    table="grid.egon_etrago_generator",
                    carriers=["CH4"],
                    measure=("sum", "p_nom"),
                    expected={
                        "status2024": STATUS_SLACK_P_NOM,
                        "eGon2035": 11295.486,
                        "reGon2037": 8795.486,
                        "reGon2045": 45.486,
                    },
                ),
                # Natural gas, biogas and LNG differ in the marginal cost only
                *for_each_scenario(
                    GasParameterMatch,
                    "SANITY_GAS_CH4_GENERATORS_DE_MARGINAL_COST",
                    table="grid.egon_etrago_generator",
                    carriers=["CH4"],
                    column="marginal_cost",
                    parameter=[
                        ("gas", "marginal_cost", "CH4"),
                        ("gas", "marginal_cost", "biogas"),
                        ("gas", "marginal_cost", "LNG"),
                    ],
                ),
                *for_each_scenario(
                    GasBusReferences,
                    "SANITY_GAS_CH4_GENERATORS_BUSES",
                    table="grid.egon_etrago_generator",
                    carriers=["CH4"],
                ),
            ],
        }


def download_biogas_data():
    """
    Download the Einspeiseatlas of the dena (version of 9 October 2025)

    The file is downloaded into ./datasets/gas_data, only if it does not
    exist yet. For more information on this data refer to the
    `Einspeiseatlas <https://www.dena.de/en/biogaspartner/translate-to-english-biomethan/translate-to-english-einspeiseatlas/>`_.

    Returns
    -------
    None

    """
    target_file = Path(CH4Production.sources.files["biogas_einspeiseatlas"])
    if target_file.is_file():
        return

    target_file.parent.mkdir(parents=True, exist_ok=True)
    url = CH4Production.sources.urls["biogas_einspeiseatlas"]
    try:
        urlretrieve(url, target_file)
    except Exception as e:
        raise RuntimeError(
            f"The Einspeiseatlas of the dena could not be downloaded from "
            f"{url} ({type(e).__name__}: {e}). The dena may have moved or "
            "renamed the file; check "
            "https://www.dena.de/en/biogaspartner/translate-to-english-"
            "biomethan/translate-to-english-einspeiseatlas/ for a current "
            "link and update biogas_einspeiseatlas in CH4Production.sources."
        ) from e


def load_NG_generators(scn_name):
    """
    Define the fossil CH4 production units in Germany

    This function reads from the SciGRID_gas dataset the fossil CH4
    production units in Germany, adjusts and returns them.
    Natural gas production reference: SciGRID_gas dataset (datasets/gas_data/data/IGGIELGN_Production.csv
    downloaded in :func:`download_SciGRID_gas_data <egon.data.datasets.gas_grid.download_SciGRID_gas_data>`).
    For more information on this data, refer to the
    `SciGRID_gas IGGIELGN documentation <https://zenodo.org/record/4767098>`_.

    Parameters
    ----------
    scn_name : str
        Name of the scenario.

    Returns
    -------
    CH4_generators_list : pandas.DataFrame
        Dataframe containing the natural gas production units in Germany

    """
    # read carrier information from scnario parameter data
    scn_params = get_sector_parameters("gas", scn_name)

    target_file = Path(CH4Production.sources.files["scigrid_productions"])

    NG_generators_list = pd.read_csv(
        target_file,
        delimiter=";",
        decimal=".",
        usecols=["lat", "long", "country_code", "param"],
    )

    NG_generators_list = NG_generators_list[
        NG_generators_list["country_code"].str.match("DE")
    ]

    # Cut data to federal state if in testmode
    NUTS1 = []
    for index, row in NG_generators_list.iterrows():
        param = ast.literal_eval(row["param"])
        NUTS1.append(param["nuts_id_1"])
    NG_generators_list = NG_generators_list.assign(NUTS1=NUTS1)

    boundary = settings()["egon-data"]["--dataset-boundary"]
    if boundary != "Everything":
        map_states = {
            "Baden-Württemberg": "DE1",
            "Nordrhein-Westfalen": "DEA",
            "Hessen": "DE7",
            "Brandenburg": "DE4",
            "Bremen": "DE5",
            "Rheinland-Pfalz": "DEB",
            "Sachsen-Anhalt": "DEE",
            "Schleswig-Holstein": "DEF",
            "Mecklenburg-Vorpommern": "DE8",
            "Thüringen": "DEG",
            "Niedersachsen": "DE9",
            "Sachsen": "DED",
            "Hamburg": "DE6",
            "Saarland": "DEC",
            "Berlin": "DE3",
            "Bayern": "DE2",
        }

        NG_generators_list = NG_generators_list[
            NG_generators_list["NUTS1"].isin([map_states[boundary], np.nan])
        ]

    NG_generators_list = NG_generators_list.rename(
        columns={"lat": "y", "long": "x"}
    )
    NG_generators_list = gpd.GeoDataFrame(
        NG_generators_list,
        geometry=gpd.points_from_xy(
            NG_generators_list["x"], NG_generators_list["y"]
        ),
    )
    NG_generators_list = NG_generators_list.rename(
        columns={"geometry": "geom"}
    ).set_geometry("geom", crs=4326)

    # Insert p_nom
    p_nom = []
    for index, row in NG_generators_list.iterrows():
        param = ast.literal_eval(row["param"])
        p_nom.append(param["max_supply_M_m3_per_d"])

    conversion_factor = 437.5  # MCM/day to MWh/h
    NG_generators_list["p_nom"] = [i * conversion_factor for i in p_nom]

    # Add missing columns
    NG_generators_list["marginal_cost"] = scn_params["marginal_cost"]["CH4"]

    # Remove useless columns
    NG_generators_list = NG_generators_list.drop(
        columns=["x", "y", "param", "country_code", "NUTS1"]
    )

    return NG_generators_list


def parse_coordinates(text):
    """
    Read latitude and longitude from the text of the Einspeiseatlas.

    Parameters
    ----------
    text : str
        Coordinates as in the column "Koordinaten" of the Einspeiseatlas

    Returns
    -------
    tuple of float
        Latitude and longitude

    """
    numbers = re.findall(r"-?\d+(?:[.,]\d+)?", str(text))

    if len(numbers) != 2:
        raise ValueError(f"The coordinates '{text}' can not be read.")

    values = []
    for number in numbers:
        value = float(number.replace(",", "."))
        # Entries without decimal separator have six decimals
        if abs(value) > 180 and not any(sep in number for sep in ".,"):
            value = value / 1e6
        values.append(value)
    latitude, longitude = values

    return latitude, longitude


def load_biogas_generators(scn_name):
    """
    Define the biogas production units in Germany

    This function reads the biogas production units in Germany from the
    Einspeiseatlas of the dena (see :py:func:`download_biogas_data`),
    adjusts and returns them.

    Parameters
    ----------
    scn_name : str
        Name of the scenario

    Returns
    -------
    CH4_generators_list : pandas.DataFrame
        Dataframe containing the biogas production units in Germany

    """
    # read carrier information from scnario parameter data
    scn_params = get_sector_parameters("gas", scn_name)

    target_file = Path(CH4Production.sources.files["biogas_einspeiseatlas"])

    # Read-in data from csv-file
    biogas_generators_list = pd.read_excel(
        target_file,
        usecols=[
            "Koordinaten",
            "Status",
            "Einspeisung Biomethan [(N*m^3)/h)]",
        ],
    )
    biogas_generators_list = biogas_generators_list[
        ~biogas_generators_list["Status"]
        .astype(str)
        .str.strip()
        .str.lower()
        .eq("außer betrieb")
    ].dropna(subset=["Koordinaten", "Einspeisung Biomethan [(N*m^3)/h)]"])
    biogas_generators_list = biogas_generators_list.drop(columns="Status")

    coordinates = biogas_generators_list["Koordinaten"].apply(
        parse_coordinates
    )
    biogas_generators_list["y"] = [c[0] for c in coordinates]
    biogas_generators_list["x"] = [c[1] for c in coordinates]

    # Plants outside of Germany are not assigned to a CH4 bus later
    outside = ~(
        biogas_generators_list["y"].between(47, 56)
        & biogas_generators_list["x"].between(5, 16)
    )
    if outside.any():
        print(
            f"Warning: {int(outside.sum())} biogas plants have coordinates "
            "outside of Germany and are not assigned to a CH4 bus."
        )

    biogas_generators_list = gpd.GeoDataFrame(
        biogas_generators_list,
        geometry=gpd.points_from_xy(
            biogas_generators_list["x"], biogas_generators_list["y"]
        ),
    )
    biogas_generators_list = biogas_generators_list.rename(
        columns={"geometry": "geom"}
    ).set_geometry("geom", crs=4326)

    biogas_generators_list = within_boundary(biogas_generators_list)

    # Insert p_nom
    conversion_factor = 0.01083  # m^3/h to MWh/h
    biogas_generators_list["p_nom"] = [
        i * conversion_factor
        for i in biogas_generators_list["Einspeisung Biomethan [(N*m^3)/h)]"]
    ]

    # Add missing columns
    biogas_generators_list["marginal_cost"] = scn_params["marginal_cost"][
        "biogas"
    ]

    # Remove useless columns
    biogas_generators_list = biogas_generators_list.drop(
        columns=["x", "y", "Koordinaten", "Einspeisung Biomethan [(N*m^3)/h)]"]
    )
    return biogas_generators_list


def within_boundary(generators):
    """
    Drop the generators outside of the area of the test mode

    Parameters
    ----------
    generators : geopandas.GeoDataFrame
        Generators with point geometries in EPSG:4326

    Returns
    -------
    geopandas.GeoDataFrame
        The generators inside the boundary (all of them without a boundary)

    """
    boundary = settings()["egon-data"]["--dataset-boundary"]
    if boundary == "Everything":
        return generators
    boundary_geom = db.select_geodataframe(
        f"""
        SELECT geometry AS geom
        FROM {CH4Production.sources.tables['vg250_sta_union']}
        """,
        geom_col="geom",
        epsg=4326,
    ).union_all()
    return generators[generators.within(boundary_geom)]


def load_LNG_generators(scn_name):
    """
    Define the LNG terminals of one scenario (planned capacity times the
    share used by the NEP in its peak load case)

    Parameters
    ----------
    scn_name : str
        Name of the scenario

    Returns
    -------
    geopandas.GeoDataFrame
        LNG terminals with the columns geom, p_nom and marginal_cost

    """
    use = pd.Series(NEP_CH4_LNG_USE)
    share = np.interp(get_scenario_year(scn_name), use.index, use.values)

    terminals = pd.DataFrame(
        NEP_CH4_LNG_TERMINALS, columns=["name", "x", "y", "capacity"]
    )
    terminals = gpd.GeoDataFrame(
        terminals,
        geometry=gpd.points_from_xy(terminals.x, terminals.y),
        crs=4326,
    ).rename_geometry("geom")
    # GWh/h -> MW
    terminals["p_nom"] = terminals.capacity * 1000 * share
    terminals = within_boundary(terminals[terminals.p_nom > 0])

    marginal_cost = (
        get_sector_parameters("gas", scn_name)["marginal_cost"]["LNG"]
        if not terminals.empty
        else np.nan
    )
    terminals["marginal_cost"] = marginal_cost
    print(
        f"{scn_name}: LNG terminals {terminals.p_nom.sum() / 1000:.1f} GW "
        f"({share:.0%} of the planned capacity"
        + (
            f", {', '.join(terminals.name)}, {marginal_cost:.1f} EUR/MWh)."
            if not terminals.empty
            else ")."
        )
    )
    return terminals[["geom", "p_nom", "marginal_cost"]]


def import_gas_generators():
    """
    Insert the CH4 generators of all configured scenarios

    The status quo scenarios get one slack generator per CH4 bus
    (:py:func:`insert_ch4_generators_status`), the other scenarios the
    geolocalised production units (:py:func:`insert_ch4_generators`).

    Returns
    -------
    None

    """
    for scn_name in config.settings()["egon-data"]["--scenarios"]:
        if "status" in scn_name:
            insert_ch4_generators_status(scn_name)
        else:
            insert_ch4_generators(scn_name)


def clean_ch4_generators(scn_name):
    """
    Delete the German CH4 generators of one scenario from the database

    Parameters
    ----------
    scn_name : str
        Name of the scenario

    Returns
    -------
    None

    """
    sources = CH4Production.sources
    targets = CH4Production.targets

    db.execute_sql(
        f"""
        DELETE FROM {targets.tables['generators']}
        WHERE "carrier" = 'CH4'
        AND scn_name = '{scn_name}'
        AND bus NOT IN (
            SELECT bus_id
            FROM {sources.tables['buses']}
            WHERE scn_name = '{scn_name}' AND country != 'DE'
        );
        """
    )


def insert_ch4_generators_db(CH4_generators_list):
    """
    Insert the CH4 generators of one scenario into the database

    Parameters
    ----------
    CH4_generators_list : pandas.DataFrame
        Dataframe containing the CH4 generators to insert

    Returns
    -------
    None

    """
    targets = CH4Production.targets

    CH4_generators_list["generator_id"] = db.next_etrago_id(
        "generator", len(CH4_generators_list)
    )

    CH4_generators_list.to_sql(
        targets.get_table_name("generators"),
        db.engine(),
        schema=targets.get_table_schema("generators"),
        index=False,
        if_exists="append",
    )


def insert_ch4_generators(scn_name):
    """
    Inserts the gas production units of one scenario into the database

    This function is used for the scenarios with a CH4 grid. The
    following steps are followed:

    * cleaning of the database table grid.egon_etrago_generator of the
      German CH4 generators of the scenario,
    * call of the functions :py:func:`load_NG_generators`,
      :py:func:`load_biogas_generators` and :py:func:`load_LNG_generators`
      that respectively return dataframes containing the natural- an
      bio-gas production units and the LNG terminals in Germany. The
      natural gas units are only inserted in the scenarios that still
      have a domestic fossil methane production, the LNG terminals in the
      scenarios before 2045,
    * attribution of the bus_id to which each generator is connected
      (call the function :func:`assign_gas_bus_id <egon.data.db.assign_gas_bus_id>`
      from :py:mod:`egon.data.db <egon.data.db>`),
    * aggregation of the CH4 productions with same properties at the
      same bus. The properties that should be the same in order that
      different generators are aggregated are:

      * scenario
      * carrier
      * marginal cost: this parameter differentiates the natural gas
        generators from the biogas generators,
    * addition of the missing columns: scn_name, carrier and
      generator_id,
    * insertion of the generators into the database.

    Parameters
    ----------
    scn_name : str
        Name of the scenario.

    Returns
    -------
    None

    """
    clean_ch4_generators(scn_name)

    # Domestic natural gas production is a scenario parameter
    fossil_ch4_production = (
        get_sector_parameters("gas", scn_name)[
            "max_gas_generation_overtheyear"
        ]["CH4"]
        > 0
    )

    generators = []
    if fossil_ch4_production:
        generators.append(load_NG_generators(scn_name))
    generators.append(load_biogas_generators(scn_name))
    lng = load_LNG_generators(scn_name)
    if not lng.empty:
        generators.append(lng)

    CH4_generators_list = pd.concat(generators)

    # Add missing columns
    c = {"scn_name": scn_name, "carrier": "CH4"}
    CH4_generators_list = CH4_generators_list.assign(**c)

    # Match to associated CH4 bus
    CH4_generators_list = db.assign_gas_bus_id(
        CH4_generators_list, scn_name, "CH4"
    )

    # Remove useless columns
    CH4_generators_list = CH4_generators_list.drop(columns=["geom", "bus_id"])

    # Aggregate ch4 productions with same properties at the same bus
    CH4_generators_list = (
        CH4_generators_list.groupby(
            ["bus", "carrier", "scn_name", "marginal_cost"]
        )
        .agg({"p_nom": "sum"})
        .reset_index(drop=False)
    )

    insert_ch4_generators_db(CH4_generators_list)


def insert_ch4_generators_status(scn_name):
    """
    Inserts the gas production of one status quo scenario into the database

    One slack generator per CH4 bus, limited over the year in eTraGo.

    Parameters
    ----------
    scn_name : str
        Name of the scenario.

    Returns
    -------
    None

    """
    sources = CH4Production.sources

    clean_ch4_generators(scn_name)

    # Add one large CH4 generator at each CH4 bus
    CH4_generators_list = db.select_dataframe(
        f"""
        SELECT bus_id as bus, scn_name, carrier
        FROM {sources.tables['gas_voronoi']}
        WHERE scn_name = '{scn_name}'
        AND carrier = 'CH4'
        """
    )

    CH4_generators_list["marginal_cost"] = get_sector_parameters(
        "gas", scn_name
    )["marginal_cost"]["CH4"]
    CH4_generators_list["p_nom"] = STATUS_SLACK_P_NOM

    insert_ch4_generators_db(CH4_generators_list)
