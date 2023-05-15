"""The central module containing all code dealing with heat supply data

"""

from egon.data import db, config

from egon.data.datasets.heat_supply.district_heating import (
    cascade_heat_supply,
    backup_gas_boilers,
    backup_resistive_heaters,
)
from egon.data.datasets.heat_supply.individual_heating import (
    cascade_heat_supply_indiv,
)
from egon.data.datasets.heat_supply.geothermal import potential_germany
from egon.data.datasets.district_heating_areas import EgonDistrictHeatingAreas
from sqlalchemy import Column, String, Float, Integer, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2.types import Geometry
from egon.data.datasets import Dataset

# Will later be imported from another file.
Base = declarative_base()


class EgonDistrictHeatingSupply(Base):
    __tablename__ = "egon_district_heating"
    __table_args__ = {"schema": "supply"}
    index = Column(Integer, primary_key=True, autoincrement=True)
    district_heating_id = Column(
        Integer, ForeignKey(EgonDistrictHeatingAreas.id)
    )
    carrier = Column(String(25))
    category = Column(String(25))
    capacity = Column(Float)
    geometry = Column(Geometry("POINT", 3035))
    scenario = Column(String(50))


class EgonIndividualHeatingSupply(Base):
    __tablename__ = "egon_individual_heating"
    __table_args__ = {"schema": "supply"}
    index = Column(Integer, primary_key=True, autoincrement=True)
    mv_grid_id = Column(Integer)
    carrier = Column(String(25))
    category = Column(String(25))
    capacity = Column(Float)
    geometry = Column(Geometry("POINT", 3035))
    scenario = Column(String(50))


def create_tables():
    """Create tables for district heating areas

    Returns
    -------
        None
    """

    engine = db.engine()
    EgonDistrictHeatingSupply.__table__.drop(bind=engine, checkfirst=True)
    EgonDistrictHeatingSupply.__table__.create(bind=engine, checkfirst=True)
    EgonIndividualHeatingSupply.__table__.drop(bind=engine, checkfirst=True)
    EgonIndividualHeatingSupply.__table__.create(bind=engine, checkfirst=True)


def district_heating():
    """Insert supply for district heating areas

    Returns
    -------
    None.

    """
    sources = config.datasets()["heat_supply"]["sources"]
    targets = config.datasets()["heat_supply"]["targets"]

    db.execute_sql(
        f"""
        DELETE FROM {targets['district_heating_supply']['schema']}.
        {targets['district_heating_supply']['table']}
        """
    )

    for scenario in config.settings()["egon-data"]["--scenarios"]:
        supply = cascade_heat_supply(scenario, plotting=False)

        supply["scenario"] = scenario

        supply.to_postgis(
            targets["district_heating_supply"]["table"],
            schema=targets["district_heating_supply"]["schema"],
            con=db.engine(),
            if_exists="append",
        )

        # Do not check data for status2019 as is it not listed in the table
        if scenario != "status2019":
            # Compare target value with sum of distributed heat supply
            df_check = db.select_dataframe(
                f"""
                SELECT a.carrier,
                (SUM(a.capacity) - b.capacity) / SUM(a.capacity) as deviation
                FROM {targets['district_heating_supply']['schema']}.
                {targets['district_heating_supply']['table']} a,
                {sources['scenario_capacities']['schema']}.
                {sources['scenario_capacities']['table']} b
                WHERE a.scenario = '{scenario}'
                AND b.scenario_name = '{scenario}'
                AND b.carrier = CONCAT('urban_central_', a.carrier)
                GROUP BY (a.carrier,  b.capacity);
                """
            )
            # If the deviation is > 1%, throw an error
            assert (
                df_check.deviation.abs().max() < 1
            ), f"""Unexpected deviation between target value and distributed
                heat supply: {df_check}
            """

        # Add gas boilers as conventional backup capacities
        backup = backup_gas_boilers(scenario)

        backup.to_postgis(
            targets["district_heating_supply"]["table"],
            schema=targets["district_heating_supply"]["schema"],
            con=db.engine(),
            if_exists="append",
        )

        # Insert resistive heaters which are not available in status2019
        if scenario != "status2019":
            backup_rh = backup_resistive_heaters(scenario)

            if not backup_rh.empty:
                backup_rh.to_postgis(
                    targets["district_heating_supply"]["table"],
                    schema=targets["district_heating_supply"]["schema"],
                    con=db.engine(),
                    if_exists="append",
                )


def individual_heating():
    """Insert supply for individual heating

    Returns
    -------
    None.

    """
    targets = config.datasets()["heat_supply"]["targets"]

    for scenario in config.settings()["egon-data"]["--scenarios"]:
        db.execute_sql(
            f"""
            DELETE FROM {targets['individual_heating_supply']['schema']}.
            {targets['individual_heating_supply']['table']}
            WHERE scenario = '{scenario}'
            """
        )
        if scenario == "eGon2035":
            distribution_level = "federal_states"
        else:
            distribution_level = "national"

        supply = cascade_heat_supply_indiv(
            scenario, distribution_level=distribution_level, plotting=False
        )

        supply["scenario"] = scenario

        supply.to_postgis(
            targets["individual_heating_supply"]["table"],
            schema=targets["individual_heating_supply"]["schema"],
            con=db.engine(),
            if_exists="append",
        )


class HeatSupply(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="HeatSupply",
            version="0.0.9",
            dependencies=dependencies,
            tasks=(
                create_tables,
                {
                    district_heating,
                    individual_heating,
                    potential_germany,
                },
            ),
        )
