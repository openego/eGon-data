"""The central module containing all code dealing with heat supply data"""

from geoalchemy2.types import Geometry
from sqlalchemy import Column, Float, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base

from egon.data import config, db
from egon.data.datasets import Dataset, DatasetSources, DatasetTargets
from egon.data.datasets.district_heating_areas import EgonDistrictHeatingAreas
from egon.data.datasets.heat_supply.district_heating import (
    backup_gas_boilers,
    backup_resistive_heaters,
    cascade_heat_supply,
)
from egon.data.datasets.heat_supply.geothermal import potential_germany
from egon.data.datasets.heat_supply.individual_heating import (
    cascade_heat_supply_indiv,
)

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
    sources = HeatSupply.sources
    targets = HeatSupply.targets

    db.execute_sql(
        f"""
        DELETE FROM {HeatSupply.targets.tables["district_heating_supply"]}
        """
    )

    for scenario in config.settings()["egon-data"]["--scenarios"]:
        supply = cascade_heat_supply(scenario, plotting=False)

        supply["scenario"] = scenario

        supply.to_postgis(
            HeatSupply.targets.get_table_name("district_heating_supply"),
            schema=HeatSupply.targets.get_table_schema(
                "district_heating_supply"
            ),
            con=db.engine(),
            if_exists="append",
        )

        # Do not check data for status quo as is it not listed in the table
        if "status" not in scenario:
            # Compare target value with sum of distributed heat supply
            df_check = db.select_dataframe(
                f"""
                SELECT a.carrier,
                (SUM(a.capacity) - b.capacity) / SUM(a.capacity) as deviation
                FROM {targets.tables['district_heating_supply']} a,
                {sources.tables['scenario_capacities']} b
                WHERE a.scenario = '{scenario}'
                AND b.scenario_name = '{scenario}'
                AND b.carrier = CONCAT('urban_central_', a.carrier)
                GROUP BY (a.carrier,  b.capacity);
                """
            )
            # If the deviation is > 1%, throw an error
            assert df_check.deviation.abs().max() < 1, (
                "Unexpected deviation between target value and distributed "
                f"heat supply: {df_check}"
            )

        # Add gas boilers as conventional backup capacities
        backup = backup_gas_boilers(scenario)

        backup.to_postgis(
            targets.get_table_name("district_heating_supply"),
            schema=targets.get_table_schema("district_heating_supply"),
            con=db.engine(),
            if_exists="append",
        )

        # Insert resistive heaters which are not available in status quo
        if "status" not in scenario:
            backup_rh = backup_resistive_heaters(scenario)

            if not backup_rh.empty:
                backup_rh.to_postgis(
                    targets.get_table_name("district_heating_supply"),
                    schema=targets.get_table_schema("district_heating_supply"),
                    con=db.engine(),
                    if_exists="append",
                )


def individual_heating():
    """Insert supply for individual heating

    Returns
    -------
    None.
    """
    targets = HeatSupply.targets

    for scenario in config.settings()["egon-data"]["--scenarios"]:
        db.execute_sql(
            f"""
            DELETE FROM {targets.tables['individual_heating_supply']}
            WHERE scenario = '{scenario}'
            """
        )
        if scenario == "eGon2035":
            distribution_level = "federal_states"
        else:
            distribution_level = "national"

        supply = cascade_heat_supply_indiv(
            scenario,
            distribution_level=distribution_level,
            plotting=False,
        )

        supply["scenario"] = scenario

        supply.to_postgis(
            targets.get_table_name("individual_heating_supply"),
            schema=targets.get_table_schema("individual_heating_supply"),
            con=db.engine(),
            if_exists="append",
        )


class HeatSupply(Dataset):
    """
    Select and store heat supply technologies for inidvidual and
    district heating

    This dataset distributes heat supply technologies to each district
    heating grid and individual supplies buildings per medium voltage
    grid district. National installed capacities are predefined from
    external sources within :py:class:`ScenarioCapacities
    <egon.data.datasets.scenario_capacities.ScenarioCapacities>`.
    The further distribution is done using a cascade that follows a
    specific order of supply technologies and the heat demand.

    *Dependencies*
      * :py:class:`DataBundle
        <egon.data.datasets.data_bundle.DataBundle>`
      * :py:class:`DistrictHeatingAreas
        <egon.data.datasets.district_heating_areas.DistrictHeatingAreas>`
      * :py:class:`ZensusMvGridDistricts
        <egon.data.datasets.zensus_mv_grid_districts.ZensusMvGridDistricts>`
      * :py:class:`Chp <egon.data.datasets.chp.Chp>`

    *Resulting tables*
      * :py:class:`demand.egon_district_heating
        <egon.data.datasets.heat_supply.EgonDistrictHeatingSupply>` is
        created and filled
      * :py:class:`demand.egon_individual_heating
        <egon.data.datasets.heat_supply.EgonIndividualHeatingSupply>` is
        created and filled
    """

    #:
    name: str = "HeatSupply"
    #:
    version: str = "0.0.18"

    sources = DatasetSources(
        tables={
            "scenario_capacities": "supply.egon_scenario_capacities",
            "district_heating_areas": "demand.egon_district_heating_areas",
            "chp": "supply.egon_chp_plants",
            "federal_states": "boundaries.vg250_lan",
            "heat_demand": "demand.egon_peta_heat",
            "map_zensus_grid": "boundaries.egon_map_zensus_grid_districts",
            "map_vg250_grid": "boundaries.egon_map_mvgriddistrict_vg250",
            "mv_grids": "grid.egon_mv_grid_district",
            "map_dh": "demand.egon_map_zensus_district_heating_areas",
            "etrago_buses": "grid.egon_etrago_bus",
        }
    )

    targets = DatasetTargets(
        tables={
            "district_heating_supply": "supply.egon_district_heating",
            "individual_heating_supply": "supply.egon_individual_heating",
        }
    )

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(
                create_tables,
                {
                    district_heating,
                    individual_heating,
                },
            ),
        )


class GeothermalPotentialGermany(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="GeothermalPotentialGermany",
            version="0.0.2",
            dependencies=dependencies,
            tasks=(potential_germany,),
        )
