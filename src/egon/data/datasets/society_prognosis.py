"""The central module containing all code dealing with processing and
forecast Zensus data.
"""
import numpy as np
import egon.data.config
import pandas as pd
from egon.data import db
from egon.data.datasets import Dataset, DatasetSources, DatasetTargets
from sqlalchemy import Column, Float, Integer
from sqlalchemy.ext.declarative import declarative_base

# will be later imported from another file ###
Base = declarative_base()

# ############################################################
class SocietyPrognosis(Dataset):
    name: str = "SocietyPrognosis"
    version: str = "0.0.4"

    sources = DatasetSources(
        tables={
            "map_zensus_vg250": "boundaries.egon_map_zensus_vg250",
            "zensus_population": "society.destatis_zensus_population_per_ha",
            "zensus_households": "society.egon_destatis_zensus_household_per_ha",
            "demandregio_population": "society.egon_demandregio_population",
            "demandregio_households": "society.egon_demandregio_household",
        }
    )

    targets = DatasetTargets(
        tables={
            "population_prognosis": {
                "schema": "society",
                "table": "egon_population_prognosis"
            },
            "household_prognosis": {
                "schema": "society",
                "table": "egon_household_prognosis"
            }
        }
    )

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(create_tables, {zensus_population, zensus_household}),
        )


class EgonPopulationPrognosis(Base):
    __tablename__ = "egon_population_prognosis"
    __table_args__ = {"schema": "society"}
    zensus_population_id = Column(Integer, primary_key=True)
    year = Column(Integer, primary_key=True)
    population = Column(Float)


class EgonHouseholdPrognosis(Base):
    __tablename__ = "egon_household_prognosis"
    __table_args__ = {"schema": "society"}
    zensus_population_id = Column(Integer, primary_key=True)
    year = Column(Integer, primary_key=True)
    households = Column(Float)


def create_tables():
    """Create table to map zensus grid and administrative districts (nuts3)"""
    engine = db.engine()
    db.execute_sql("CREATE SCHEMA IF NOT EXISTS society;")
    EgonPopulationPrognosis.__table__.create(bind=engine, checkfirst=True)
    EgonHouseholdPrognosis.__table__.create(bind=engine, checkfirst=True)


def zensus_population():
    """Bring population prognosis from DemandRegio to Zensus grid"""

    #cfg = egon.data.config.datasets()["society_prognosis"]

    local_engine = db.engine()

    # Input: Zensus2011 population data including the NUTS3-Code
    zensus_district = db.select_dataframe(
        f"""SELECT zensus_population_id, vg250_nuts3
        FROM {SocietyPrognosis.sources.tables['map_zensus_vg250']}
        WHERE zensus_population_id IN (
            SELECT id
            FROM {SocietyPrognosis.sources.tables['zensus_population']})""",
        index_col="zensus_population_id",
   )

    zensus = db.select_dataframe(
        f"""SELECT id, population
            FROM {SocietyPrognosis.sources.tables['zensus_population']}
            WHERE population > 0""",
        index_col="id",
    )

    zensus["nuts3"] = zensus_district.vg250_nuts3

    # Rename index
    zensus.index = zensus.index.rename("zensus_population_id")

    # Replace population value of uninhabited cells for calculation
    zensus.population = zensus.population.replace(-1, 0)

    # Calculate share of population per cell in nuts3-region
    zensus["share"] = (
        zensus.groupby(zensus.nuts3)
        .population.apply(lambda grp: grp / grp.sum())
        .fillna(0)
    ).values

    db.execute_sql(
        f"DELETE FROM {SocietyPrognosis.targets.tables['population_prognosis']['schema']}."
        f"{SocietyPrognosis.targets.tables['population_prognosis']['table']}"
    )
    # Scale to pogosis values from demandregio
    for year in [2035, 2050]:
        # Input: dataset on population prognosis on district-level (NUTS3)
        prognosis = db.select_dataframe(
            f"""SELECT nuts3, population
            FROM {SocietyPrognosis.sources.tables['demandregio_population']}
            WHERE year={year}""",
            index_col="nuts3",
        )

        df = pd.DataFrame(
            zensus["share"]
            .mul(prognosis.population[zensus["nuts3"]].values)
            .replace(0, -1)
        ).rename({"share": "population"}, axis=1)

        df["year"] = year

        # Insert to database
        df.to_sql(
             SocietyPrognosis.targets.tables["population_prognosis"]["table"],
             schema=SocietyPrognosis.targets.tables["population_prognosis"]["schema"],
             con=local_engine,
             if_exists="append",
        )


def household_prognosis_per_year(prognosis_nuts3, zensus, year):
    """Calculate household prognosis for a specitic year"""

    prognosis_total = prognosis_nuts3.groupby(
        prognosis_nuts3.index
    ).households.sum()

    prognosis = pd.DataFrame(index=zensus.index)
    prognosis["nuts3"] = zensus.nuts3
    prognosis["quantity"] = zensus["share"].mul(
        prognosis_total[zensus["nuts3"]].values
    )
    prognosis["rounded"] = prognosis["quantity"].astype(int)
    prognosis["rest"] = prognosis["quantity"] - prognosis["rounded"]

    # Set seed for reproducibility
    np.random.seed(
        seed=egon.data.config.settings()["egon-data"]["--random-seed"]
    )

    # Rounding process to meet exact values from demandregio on nuts3-level
    for name, group in prognosis.groupby(prognosis.nuts3):
        print(f"start prognosis nuts3 {name}")
        while prognosis_total[name] > group["rounded"].sum():
            index = np.random.choice(
                group["rest"].index.values[group["rest"] == max(group["rest"])]
            )
            group.at[index, "rounded"] += 1
            group.at[index, "rest"] = 0
        print(f"finished prognosis nuts3 {name}")
        prognosis[prognosis.index.isin(group.index)] = group

    prognosis = prognosis.drop(["nuts3", "quantity", "rest"], axis=1).rename(
        {"rounded": "households"}, axis=1
    )
    prognosis["year"] = year

    return prognosis


def zensus_household():
    """Bring household prognosis from DemandRegio to Zensus grid"""
    #cfg = egon.data.config.datasets()["society_prognosis"]

    local_engine = db.engine()

    # Input: Zensus2011 household data including the NUTS3-Code
    district = db.select_dataframe(
        f"""SELECT zensus_population_id, vg250_nuts3
        FROM {SocietyPrognosis.sources.tables['map_zensus_vg250']}""",
        index_col="zensus_population_id",
    )

    zensus = db.select_dataframe(
        f"""SELECT zensus_population_id, quantity
        FROM {SocietyPrognosis.sources.tables['zensus_households']}""",
        index_col="zensus_population_id",
    )

    # Group all household types
    zensus = zensus.groupby(zensus.index).sum()

    zensus["nuts3"] = district.loc[zensus.index, "vg250_nuts3"]

    # Calculate share of households per nuts3 region in each zensus cell
    zensus["share"] = (
        zensus.groupby(zensus.nuts3)
        .quantity.apply(lambda grp: grp / grp.sum())
        .fillna(0)
        .values
    )

    db.execute_sql(
        f"DELETE FROM {SocietyPrognosis.targets.tables['household_prognosis']['schema']}."
        f"{SocietyPrognosis.targets.tables['household_prognosis']['table']}"
    )

    # Apply prognosis function
    for year in [2035, 2050]:
        print(f"start prognosis for year {year}")
        # Input: dataset on household prognosis on district-level (NUTS3)
        prognosis_nuts3 = db.select_dataframe(
            f"""SELECT nuts3, hh_size, households
            FROM {SocietyPrognosis.sources.tables['demandregio_households']}
            WHERE year={year}""",
            index_col="nuts3",
        )

        # Insert into database
        household_prognosis_per_year(prognosis_nuts3, zensus, year).to_sql(
            SocietyPrognosis.targets.tables["household_prognosis"]["table"],
            schema=SocietyPrognosis.targets.tables["household_prognosis"]["schema"],
            con=local_engine,
            if_exists="append",
        )
        print(f"finished prognosis for year {year}")
