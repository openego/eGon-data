"""
Home Battery allocation to buildings

Main module for allocation of home batteries onto buildings and sizing them
depending on pv rooftop system size.

**Contents of this module**

* Creation of DB tables
* Allocate given home battery capacity per mv grid to buildings with pv rooftop
  systems. The sizing of the home battery system depends on the size of the
  pv rooftop system and can be set within the *datasets.yml*. Default sizing is
  1:1 between the pv rooftop capacity (kWp) and the battery capacity (kWh).
* Write results to DB

**Configuration**

The config of this dataset can be found in *datasets.yml* in section
*home_batteries*.

**Scenarios and variations**

Assumptions can be changed within the *datasets.yml*.

Only buildings with a pv rooftop systems are considered within the allocation
process. The default sizing of home batteries is 1:1 between the pv rooftop
capacity (kWp) and the battery capacity (kWh). Reaching the exact value of the
allocation of battery capacities per grid area leads to slight deviations from
this specification.

## Methodology

The selection of buildings is done randomly until a result is reached which is
close to achieving the sizing specification.
"""
import datetime
import json

from loguru import logger
from numpy.random import RandomState
from omi.dialects import get_dialect
from sqlalchemy import Column, Float, Integer, String
from sqlalchemy.ext.declarative import declarative_base
import numpy as np
import pandas as pd

from egon.data import config, db
from egon.data.datasets.scenario_parameters import get_sector_parameters
from egon.data.metadata import (
    context,
    contributors,
    generate_resource_fields_from_db_table,
    license_dedl,
    license_odbl,
    meta_metadata,
    sources,
)

Base = declarative_base()


def get_cbat_pbat_ratio():
    """
    Mean ratio between the storage capacity and the power of the pv rooftop
    system

    Returns
    -------
    int
        Mean ratio between the storage capacity and the power of the pv
        rooftop system
    """
    sources = config.datasets()["home_batteries"]["sources"]

    sql = f"""
    SELECT max_hours
    FROM {sources["etrago_storage"]["schema"]}
    .{sources["etrago_storage"]["table"]}
    WHERE carrier = 'home_battery'
    """

    return int(db.select_dataframe(sql).iat[0, 0])


def allocate_home_batteries_to_buildings():
    """
    Allocate home battery storage systems to buildings with pv rooftop systems
    """
    # get constants
    constants = config.datasets()["home_batteries"]["constants"]
    scenarios = config.settings()["egon-data"]["--scenarios"]
    if "status2019" in scenarios:
        scenarios.remove("status2019")
    cbat_ppv_ratio = constants["cbat_ppv_ratio"]
    rtol = constants["rtol"]
    max_it = constants["max_it"]

    sources = config.datasets()["home_batteries"]["sources"]

    df_list = []

    for scenario in scenarios:
        # get home battery capacity per mv grid id
        sql = f"""
        SELECT el_capacity as p_nom_min, bus_id as bus FROM
        {sources["storage"]["schema"]}
        .{sources["storage"]["table"]}
        WHERE carrier = 'home_battery'
        AND scenario = '{scenario}';
        """
        cbat_pbat_ratio = get_sector_parameters(
            "electricity", scenario
        )["efficiency"]["battery"]["max_hours"]
        
        home_batteries_df = db.select_dataframe(sql)

        home_batteries_df = home_batteries_df.assign(
            bat_cap=home_batteries_df.p_nom_min * cbat_pbat_ratio
        )

        sql = """
        SELECT building_id, capacity
        FROM supply.egon_power_plants_pv_roof_building
        WHERE scenario = '{}'
        AND bus_id = {}
        """

        for bus_id, bat_cap in home_batteries_df[
            ["bus", "bat_cap"]
        ].itertuples(index=False):
            pv_df = db.select_dataframe(sql.format(scenario, bus_id))

            grid_ratio = bat_cap / pv_df.capacity.sum()

            if grid_ratio > cbat_ppv_ratio:
                logger.warning(
                    f"In Grid {bus_id} and scenario {scenario}, the ratio of "
                    f"home storage capacity to pv rooftop capacity is above 1"
                    f" ({grid_ratio: g}). The storage capacity of pv rooftop "
                    f"systems will be high."
                )

            if grid_ratio < cbat_ppv_ratio:
                random_state = RandomState(seed=bus_id)

                n = max(int(len(pv_df) * grid_ratio), 1)

                best_df = pv_df.sample(n=n, random_state=random_state)

                i = 0

                while (
                    not np.isclose(best_df.capacity.sum(), bat_cap, rtol=rtol)
                    and i < max_it
                ):
                    sample_df = pv_df.sample(n=n, random_state=random_state)

                    if abs(best_df.capacity.sum() - bat_cap) > abs(
                        sample_df.capacity.sum() - bat_cap
                    ):
                        best_df = sample_df.copy()

                    i += 1

                    if sample_df.capacity.sum() < bat_cap:
                        n = min(n + 1, len(pv_df))
                    else:
                        n = max(n - 1, 1)

                if not np.isclose(best_df.capacity.sum(), bat_cap, rtol=rtol):
                    logger.warning(
                        f"No suitable generators could be found in Grid "
                        f"{bus_id} and scenario {scenario} to achieve the "
                        f"desired ratio between battery capacity and pv "
                        f"rooftop capacity. The ratio will be "
                        f"{bat_cap / best_df.capacity.sum()}."
                    )

                pv_df = best_df.copy()

            bat_df = pv_df.drop(columns=["capacity"]).assign(
                capacity=pv_df.capacity / pv_df.capacity.sum() * bat_cap,
                p_nom=pv_df.capacity
                / pv_df.capacity.sum()
                * bat_cap
                / cbat_pbat_ratio,
                scenario=scenario,
                bus_id=bus_id,
            )

            df_list.append(bat_df)

    create_table(pd.concat(df_list, ignore_index=True))

    add_metadata()


class EgonHomeBatteries(Base):
    targets = config.datasets()["home_batteries"]["targets"]

    __tablename__ = targets["home_batteries"]["table"]
    __table_args__ = {"schema": targets["home_batteries"]["schema"]}

    index = Column(Integer, primary_key=True, index=True)
    scenario = Column(String)
    bus_id = Column(Integer)
    building_id = Column(Integer)
    p_nom = Column(Float)
    capacity = Column(Float)


def add_metadata():
    """
    Add metadata to table supply.egon_home_batteries
    """
    targets = config.datasets()["home_batteries"]["targets"]
    deposit_id_mastr = config.datasets()["mastr_new"]["deposit_id"]
    deposit_id_data_bundle = config.datasets()["data-bundle"]["sources"][
        "zenodo"
    ]["deposit_id"]

    contris = contributors(["kh", "kh"])

    contris[0]["date"] = "2023-03-15"

    contris[0]["object"] = "metadata"
    contris[1]["object"] = "dataset"

    contris[0]["comment"] = "Add metadata to dataset."
    contris[1]["comment"] = "Add workflow to generate dataset."

    meta = {
        "name": (
            f"{targets['home_batteries']['schema']}."
            f"{targets['home_batteries']['table']}"
        ),
        "title": "eGon Home Batteries",
        "id": "WILL_BE_SET_AT_PUBLICATION",
        "description": "Home storage systems allocated to buildings",
        "language": "en-US",
        "keywords": ["battery", "batteries", "home", "storage", "building"],
        "publicationDate": datetime.date.today().isoformat(),
        "context": context(),
        "spatial": {
            "location": "none",
            "extent": "Germany",
            "resolution": "building",
        },
        "temporal": {
            "referenceDate": "2021-12-31",
            "timeseries": {},
        },
        "sources": [
            {
                "title": "Data bundle for egon-data",
                "description": (
                    "Data bundle for egon-data: A transparent and "
                    "reproducible data processing pipeline for energy "
                    "system modeling"
                ),
                "path": (
                    "https://zenodo.org/record/"
                    f"{deposit_id_data_bundle}#.Y_dWM4CZMVM"
                ),
                "licenses": [license_dedl(attribution="© Cußmann, Ilka")],
            },
            {
                "title": ("open-MaStR power unit registry for eGo^n project"),
                "description": (
                    "Data from Marktstammdatenregister (MaStR) data using "
                    "the data dump from 2022-11-17 for eGon-data."
                ),
                "path": (
                    f"https://zenodo.org/record/{deposit_id_mastr}"
                ),
                "licenses": [license_dedl(attribution="© Amme, Jonathan")],
            },
            sources()["openstreetmap"],
            sources()["era5"],
            sources()["vg250"],
            sources()["egon-data"],
            sources()["nep2021"],
            sources()["mastr"],
            sources()["technology-data"],
        ],
        "licenses": [license_odbl("© eGon development team")],
        "contributors": contris,
        "resources": [
            {
                "profile": "tabular-data-resource",
                "name": (
                    f"{targets['home_batteries']['schema']}."
                    f"{targets['home_batteries']['table']}"
                ),
                "path": "None",
                "format": "PostgreSQL",
                "encoding": "UTF-8",
                "schema": {
                    "fields": generate_resource_fields_from_db_table(
                        targets["home_batteries"]["schema"],
                        targets["home_batteries"]["table"],
                    ),
                    "primaryKey": "index",
                },
                "dialect": {"delimiter": "", "decimalSeparator": ""},
            }
        ],
        "review": {"path": "", "badge": ""},
        "metaMetadata": meta_metadata(),
        "_comment": {
            "metadata": (
                "Metadata documentation and explanation (https://github.com/"
                "OpenEnergyPlatform/oemetadata/blob/master/metadata/v141/"
                "metadata_key_description.md)"
            ),
            "dates": (
                "Dates and time must follow the ISO8601 including time zone "
                "(YYYY-MM-DD or YYYY-MM-DDThh:mm:ss±hh)"
            ),
            "units": "Use a space between numbers and units (100 m)",
            "languages": (
                "Languages must follow the IETF (BCP47) format (en-GB, en-US, "
                "de-DE)"
            ),
            "licenses": (
                "License name must follow the SPDX License List "
                "(https://spdx.org/licenses/)"
            ),
            "review": (
                "Following the OEP Data Review (https://github.com/"
                "OpenEnergyPlatform/data-preprocessing/wiki)"
            ),
            "none": "If not applicable use (none)",
        },
    }

    dialect = get_dialect(meta_metadata())()

    meta = dialect.compile_and_render(dialect.parse(json.dumps(meta)))

    db.submit_comment(
        f"'{json.dumps(meta)}'",
        targets["home_batteries"]["schema"],
        targets["home_batteries"]["table"],
    )


def create_table(df):
    """Create mapping table home battery <-> building id"""
    engine = db.engine()

    EgonHomeBatteries.__table__.drop(bind=engine, checkfirst=True)
    EgonHomeBatteries.__table__.create(bind=engine, checkfirst=True)

    df.reset_index().to_sql(
        name=EgonHomeBatteries.__table__.name,
        schema=EgonHomeBatteries.__table__.schema,
        con=engine,
        if_exists="append",
        index=False,
    )
