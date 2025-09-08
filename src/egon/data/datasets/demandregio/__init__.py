"""The central module containing all code dealing with importing and
adjusting data from demandRegio
"""
from pathlib import Path
import os
import zipfile

from sqlalchemy import ARRAY, Column, Float, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base
import numpy as np
import pandas as pd

from egon.data import db, logger
from egon.data.datasets import Dataset, DatasetSources, DatasetTargets, wrapped_partial
from egon.data.datasets.demandregio.install_disaggregator import (
    clone_and_install,
)
from egon.data.datasets.scenario_parameters import (
    EgonScenario,
    get_sector_parameters,
)
from egon.data.datasets.zensus import download_and_check
import egon.data.config
import egon.data.datasets.scenario_parameters.parameters as scenario_parameters

try:
    from disaggregator import config, data, spatial, temporal
except ImportError as e:
    pass

Base = declarative_base()

class DemandRegio(Dataset):
    """Docstring for the class..."""
    sources = DatasetSources(
        files={
            "wz_cts": "WZ_definition/WZ_def_GHD.csv",
            "wz_industry": "WZ_definition/WZ_def_IND.csv",
            "pes_demand_today": "pypsa_eur/resources/industrial_demand_oblasts_today_elec.csv",
            "pes_production_tomorrow": "pypsa_eur/resources/industrial_production_per_country_tomorrow.csv",
            "pes_sector_ratios": "pypsa_eur/resources/sector_ratios_elec.csv",
            "new_consumers_2035": "nep2035_version2021/NEP2035_neue_verbraucher.csv",
            "cache_zip": "demand_regio_backup/demandregio_cache.zip",
            "dbdump_zip": "demand_regio_backup/demandregio_dbdump.zip",
        },
        tables={"vg250_krs": "boundaries.vg250_krs"}
    )
    targets = DatasetTargets(
        files={
            "cache_dir": "demandregio/cache",
            "dbdump_dir": "demandregio/dbdump",
        },
        tables={
            "hh_demand": "demand.egon_demandregio_hh",
            "cts_ind_demand": "demand.egon_demandregio_cts_ind",
            "population": "society.egon_demandregio_population",
            "households": "society.egon_demandregio_household",
            "wz_definitions": "demand.egon_demandregio_wz",
            "timeseries_cts_ind": "demand.egon_demandregio_timeseries_cts_ind",
        }
    )
    name: str = "DemandRegio"
    version: str = "0.0.11"

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(
                get_cached_tables,
                create_tables,
                {
                    insert_household_demand,
                    insert_society_data,
                    insert_cts_ind_demands,
                },
            ),
        )

# ... (SQLAlchemy Base classes are unchanged) ...

def create_tables():
    # ... (This function is already correct)

def data_in_boundaries(df):
    engine = db.engine()
    df = df.reset_index()
    nuts_names = {"DEB16": "DEB1C", "DEB19": "DEB1D"}
    df.loc[df.nuts3.isin(nuts_names), "nuts3"] = df.loc[df.nuts3.isin(nuts_names), "nuts3"].map(nuts_names)
    df = df.set_index("nuts3")
    return df[df.index.isin(pd.read_sql(f"SELECT DISTINCT ON (nuts) nuts FROM {DemandRegio.sources.tables['vg250_krs']}", engine).nuts)]

def insert_cts_ind_wz_definitions():
    engine = db.engine()
    wz_files = {"CTS": "wz_cts", "industry": "wz_industry"}
    for sector, file_key in wz_files.items():
        file_path = Path(".") / "data_bundle_egon_data" / "WZ_definition" / DemandRegio.sources.files[file_key]
        delimiter = ";" if sector == "CTS" else ","
        df = pd.read_csv(file_path, delimiter=delimiter, header=None).rename({0: "wz", 1: "definition"}, axis="columns").set_index("wz")
        df["sector"] = sector
        df.to_sql(
            DemandRegio.targets.get_table_name("wz_definitions"),
            engine,
            schema=DemandRegio.targets.get_table_schema("wz_definitions"),
            if_exists="append",
        )

def adjust_ind_pes(ec_cts_ind):
    pes_path = Path(".") / "data_bundle_powerd_data" / "pypsa_eur" / "resources"
    demand_today = pd.read_csv(pes_path / DemandRegio.sources.files["pes_demand_today"], header=None).transpose()
    # ... (rest of function logic)
    prod_tomorrow = pd.read_csv(pes_path / DemandRegio.sources.files["pes_production_tomorrow"])
    # ... (rest of function logic)
    sector_ratio = pd.read_csv(pes_path / DemandRegio.sources.files["pes_sector_ratios"]).set_index("MWh/tMaterial").loc["elec"]
    # ... (rest of function logic is unchanged)
    return ec_cts_ind

def adjust_cts_ind_nep(ec_cts_ind, sector):
    file_path = Path(".") / "data_bundle_egon_data" / "nep2035_version2021" / DemandRegio.sources.files["new_consumers_2035"]
    new_con = pd.read_csv(file_path, delimiter=";", decimal=",", index_col=0)
    groups = ec_cts_ind.groupby(match_nuts3_bl().gen)
    for group in groups.indices.keys():
        g = groups.get_group(group)
        data_new = g.mul(1 + new_con[sector][group] * 1e6 / g.sum().sum())
        ec_cts_ind[ec_cts_ind.index.isin(g.index)] = data_new
    return ec_cts_ind

# ... (The other functions like `insert_hh_demand`, `insert_cts_ind`, `insert_society_data`, etc. need to be
# fully refactored as shown in the previous detailed messages, removing all `config.datasets()` calls.)

def get_cached_tables():
    source_path_cache = DemandRegio.sources.files["cache_zip"]
    target_path_cache = Path(DemandRegio.targets.files["cache_dir"])
    os.makedirs(target_path_cache, exist_ok=True)
    with zipfile.ZipFile(source_path_cache, "r") as zip_ref:
        zip_ref.extractall(path=target_path_cache)

    source_path_dbdump = DemandRegio.sources.files["dbdump_zip"]
    target_path_dbdump = Path(DemandRegio.targets.files["dbdump_dir"])
    os.makedirs(target_path_dbdump, exist_ok=True)
    with zipfile.ZipFile(source_path_dbdump, "r") as zip_ref:
        zip_ref.extractall(path=target_path_dbdump)