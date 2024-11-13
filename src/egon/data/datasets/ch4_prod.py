# -*- coding: utf-8 -*-
"""
The central module containing code dealing with importing CH4 production data for eGon2035.

For eGon2035, the gas produced in Germany can be natural gas or biogas.
The source productions are geolocalised potentials described as PyPSA
generators. These generators are not extendable and their overall
production over the year is limited directly in eTraGo by values from
the Netzentwicklungsplan Gas 2020–2030 (36 TWh natural gas and 10 TWh
biogas), also stored in the table
:py:class:`scenario.egon_scenario_parameters <egon.data.datasets.scenario_parameters.EgonScenario>`.

"""
from pathlib import Path
from urllib.request import urlretrieve
import ast

import geopandas as gpd
import numpy as np
import pandas as pd

from egon.data import config, db
from egon.data.config import settings
from egon.data.datasets import Dataset
from egon.data.datasets.scenario_parameters import get_sector_parameters


class CH4Production(Dataset):
    """
    Insert the CH4 productions into the database for eGon2035

    Insert the CH4 productions into the database for eGon2035 by using
    the function :py:func:`import_gas_generators`.

    *Dependencies*
      * :py:class:`GasAreaseGon2035 <egon.data.datasets.gas_areas.GasAreaseGon2035>`
      * :py:class:`GasNodesAndPipes <egon.data.datasets.gas_grid.GasNodesAndPipes>`

    *Resulting tables*
      * :py:class:`grid.egon_etrago_generator <egon.data.datasets.etrago_setup.EgonPfHvGenerator>` is extended

    """

    #:
    name: str = "CH4Production"
    #:

    version: str = "0.0.9"

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(import_gas_generators),
        )


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

    target_file = (
        Path(".")
        / "datasets"
        / "gas_data"
        / "data"
        / "IGGIELGN_Productions.csv"
    )

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


def load_biogas_generators(scn_name):
    """
    Define the biogas production units in Germany

    This function downloads the Biogaspartner Einspeiseatlas into
    (datasets/gas_data/Biogaspartner_Einspeiseatlas_Deutschland_2021.xlsx),
    reads the biogas production units in Germany data, adjusts and
    returns them.
    For more information on this data refer to the
    `Einspeiseatlas website <https://www.biogaspartner.de/einspeiseatlas/>`_.

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

    # Download file
    basename = "Biogaspartner_Einspeiseatlas_Deutschland_2021.xlsx"
    url = (
        "https://www.biogaspartner.de/fileadmin/Biogaspartner/Dokumente/Einspeiseatlas/"
        + basename
    )
    target_file = Path(".") / "datasets" / "gas_data" / basename

    urlretrieve(url, target_file)

    # Read-in data from csv-file
    biogas_generators_list = pd.read_excel(
        target_file,
        usecols=["Koordinaten", "Einspeisung Biomethan [(N*m^3)/h)]"],
    )

    x = []
    y = []
    for index, row in biogas_generators_list.iterrows():
        coordinates = row["Koordinaten"].split(",")
        y.append(coordinates[0])
        x.append(coordinates[1])
    biogas_generators_list["x"] = x
    biogas_generators_list["y"] = y

    biogas_generators_list = gpd.GeoDataFrame(
        biogas_generators_list,
        geometry=gpd.points_from_xy(
            biogas_generators_list["x"], biogas_generators_list["y"]
        ),
    )
    biogas_generators_list = biogas_generators_list.rename(
        columns={"geometry": "geom"}
    ).set_geometry("geom", crs=4326)

    # Connect to local database
    engine = db.engine()

    # Cut data to federal state if in testmode
    boundary = settings()["egon-data"]["--dataset-boundary"]
    if boundary != "Everything":
        db.execute_sql(
            """
              DROP TABLE IF EXISTS grid.egon_biogas_generator CASCADE;
            """
        )
        biogas_generators_list.to_postgis(
            "egon_biogas_generator",
            engine,
            schema="grid",
            index=False,
            if_exists="replace",
        )

        sql = """SELECT *
            FROM grid.egon_biogas_generator, boundaries.vg250_sta_union  as vg
            WHERE ST_Transform(vg.geometry,4326) && egon_biogas_generator.geom
            AND ST_Contains(ST_Transform(vg.geometry,4326), egon_biogas_generator.geom)"""

        biogas_generators_list = gpd.GeoDataFrame.from_postgis(
            sql, con=engine, geom_col="geom", crs=4326
        )
        biogas_generators_list = biogas_generators_list.drop(
            columns=["id", "bez", "area_ha", "geometry"]
        )
        db.execute_sql(
            """
              DROP TABLE IF EXISTS grid.egon_biogas_generator CASCADE;
            """
        )

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


def import_gas_generators():
    """
    Inserts list of gas production units into the database

    To insert the gas production units into the database, the following
    steps are followed:

      * cleaning of the database table grid.egon_etrago_generator of the
        CH4 generators of the specific scenario (eGon2035),
      * call of the functions :py:func:`load_NG_generators` and
        :py:func:`load_biogas_generators` that respectively return
        dataframes containing the natural- an bio-gas production units
        in Germany,
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
    # Connect to local database
    engine = db.engine()

    # Select source and target from dataset configuration
    source = config.datasets()["gas_prod"]["source"]
    target = config.datasets()["gas_prod"]["target"]

    for scn_name in config.settings()["egon-data"]["--scenarios"]:
        # Clean table
        db.execute_sql(
            f"""
            DELETE FROM {target['stores']['schema']}.{target['stores']['table']}
            WHERE "carrier" = 'CH4' AND
            scn_name = '{scn_name}' AND bus not IN (
                SELECT bus_id FROM {source['buses']['schema']}.{source['buses']['table']}
                WHERE scn_name = '{scn_name}' AND country != 'DE'
            );
            """
        )

        if scn_name == "eGon2035":
            CH4_generators_list = pd.concat(
                [
                    load_NG_generators(scn_name),
                    load_biogas_generators(scn_name),
                ]
            )

            # Add missing columns
            c = {"scn_name": scn_name, "carrier": "CH4"}
            CH4_generators_list = CH4_generators_list.assign(**c)

            # Match to associated CH4 bus
            CH4_generators_list = db.assign_gas_bus_id(
                CH4_generators_list, scn_name, "CH4"
            )

            # Remove useless columns
            CH4_generators_list = CH4_generators_list.drop(
                columns=["geom", "bus_id"]
            )

            # Aggregate ch4 productions with same properties at the same bus
            CH4_generators_list = (
                CH4_generators_list.groupby(
                    ["bus", "carrier", "scn_name", "marginal_cost"]
                )
                .agg({"p_nom": "sum"})
                .reset_index(drop=False)
            )

        elif "status" in scn_name:
            # Add one large CH4 generator at each CH4 bus
            CH4_generators_list = db.select_dataframe(
                f"""
                SELECT bus_id as bus, scn_name, carrier
                FROM grid.egon_gas_voronoi
                WHERE scn_name = '{scn_name}'
                AND carrier = 'CH4'
                """
            )

            CH4_generators_list["marginal_cost"] = get_sector_parameters(
                "gas", scn_name
            )["marginal_cost"]["CH4"]
            CH4_generators_list["p_nom"] = 100000

        elif scn_name == "eGon100RE":
            CH4_generators_list = pd.concat(
                [
                    load_biogas_generators(scn_name),
                ]
            )

            # Add missing columns
            c = {"scn_name": scn_name, "carrier": "CH4"}
            CH4_generators_list = CH4_generators_list.assign(**c)

            # Match to associated CH4 bus
            CH4_generators_list = db.assign_gas_bus_id(
                CH4_generators_list, scn_name, "CH4"
            )

            # Remove useless columns
            CH4_generators_list = CH4_generators_list.drop(
                columns=["geom", "bus_id"]
            )

            # Aggregate ch4 productions with same properties at the same bus
            CH4_generators_list = (
                CH4_generators_list.groupby(
                    ["bus", "carrier", "scn_name", "marginal_cost"]
                )
                .agg({"p_nom": "sum"})
                .reset_index(drop=False)
            )

        else:
            raise ValueError(f"{scn_name} is not a valid scenario name")

        new_id = db.next_etrago_id("generator")
        CH4_generators_list["generator_id"] = range(
            new_id, new_id + len(CH4_generators_list)
        )

        # Insert data to db
        CH4_generators_list.to_sql(
            target["stores"]["table"],
            engine,
            schema=target["stores"]["schema"],
            index=False,
            if_exists="append",
        )
