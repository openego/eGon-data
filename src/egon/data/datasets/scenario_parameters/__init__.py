"""The central module containing all code dealing with scenario table."""

from pathlib import Path
from urllib.request import urlretrieve
import shutil
import zipfile

from sqlalchemy import VARCHAR, Column, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pandas as pd

from egon.data import db
from egon.data.datasets import Dataset, DatasetSources, DatasetTargets
import egon.data.config
import egon.data.datasets.scenario_parameters.parameters as parameters

Base = declarative_base()


class EgonScenario(Base):
    __tablename__ = "egon_scenario_parameters"
    __table_args__ = {"schema": "scenario"}
    name = Column(String, primary_key=True)
    global_parameters = Column(JSONB)
    electricity_parameters = Column(JSONB)
    gas_parameters = Column(JSONB)
    heat_parameters = Column(JSONB)
    mobility_parameters = Column(JSONB)
    description = Column(VARCHAR)


def create_table():
    """Create table for scenarios
    Returns
    -------
    None.
    """
    engine = db.engine()
    db.execute_sql(
        f"CREATE SCHEMA IF NOT EXISTS {ScenarioParameters.targets.get_table_schema('egon_scenario_parameters')};"
    )
    db.execute_sql(
        f"DROP TABLE IF EXISTS {ScenarioParameters.targets.tables['egon_scenario_parameters']} CASCADE;"
    )
    EgonScenario.__table__.create(bind=engine, checkfirst=True)


def get_scenario_year(scenario_name):
    """Derives scenarios year from scenario name."""
    year = int(scenario_name[-4:])
    return year


def insert_scenarios():
    """Insert scenarios and their parameters to scenario table

    Returns
    -------
    None.

    """

    db.execute_sql(
        f"DELETE FROM {ScenarioParameters.targets.tables['egon_scenario_parameters']} CASCADE;"
    )

    session = sessionmaker(bind=db.engine())()

    # Scenario eGon2035
    egon2035 = EgonScenario(name="eGon2035")

    egon2035.description = """
        The mid-term scenario eGon2035 is based on scenario C 2035 of the
        Netzentwicklungsplan Strom 2035, Version 2021.
        Scenario C 2035 is characretized by an ambitious expansion of
        renewable energies and a higher share of sector coupling.
        Analogous to the Netzentwicklungsplan, the countries bordering germany
        are modeled based on Ten-Year Network Development Plan, Version 2020.
        """
    egon2035.global_parameters = parameters.global_settings(egon2035.name)

    egon2035.electricity_parameters = parameters.electricity(egon2035.name)

    egon2035.gas_parameters = parameters.gas(egon2035.name)

    egon2035.heat_parameters = parameters.heat(egon2035.name)

    egon2035.mobility_parameters = parameters.mobility(egon2035.name)

    session.add(egon2035)

    session.commit()

    # Scenario eGon2021
    eGon2021 = EgonScenario(name="eGon2021")

    eGon2021.description = """
        Status quo scenario for 2021. Note: This is NOT A COMPLETE SCENARIO
        and covers only some sector data required by ding0, such as demand
        on NUTS 3 level and generation units .
        """
    eGon2021.global_parameters = parameters.global_settings(eGon2021.name)

    eGon2021.electricity_parameters = parameters.electricity(eGon2021.name)

    eGon2021.gas_parameters = parameters.gas(eGon2021.name)

    eGon2021.heat_parameters = parameters.heat(eGon2021.name)

    eGon2021.mobility_parameters = parameters.mobility(eGon2021.name)

    session.add(eGon2021)

    session.commit()

    # Scenario status2024
    status2024 = EgonScenario(name="status2024")

    status2024.description = """
        Status quo ante scenario for 2024.
        """
    status2024.global_parameters = parameters.global_settings(status2024.name)

    status2024.electricity_parameters = parameters.electricity(status2024.name)

    status2024.gas_parameters = parameters.gas(status2024.name)

    status2024.heat_parameters = parameters.heat(status2024.name)

    status2024.mobility_parameters = parameters.mobility(status2024.name)

    session.add(status2024)

    session.commit()

    # Scenario reGon2037
    reGon2037 = EgonScenario(name="reGon2037")

    reGon2037.description = """
        The scenario reGon2037 is based on scenario C 2037 of the
        Netzentwicklungsplan Strom, Version 2025.
        Scenario C 2037 is characterized by an ambitious expansion of
        renewable energies and a higher share of sector coupling.
        """
    reGon2037.global_parameters = parameters.global_settings(reGon2037.name)

    reGon2037.electricity_parameters = parameters.electricity(reGon2037.name)

    reGon2037.gas_parameters = parameters.gas(reGon2037.name)

    reGon2037.heat_parameters = parameters.heat(reGon2037.name)

    reGon2037.mobility_parameters = parameters.mobility(reGon2037.name)

    session.add(reGon2037)

    session.commit()

    # Scenario reGon2045
    reGon2045 = EgonScenario(name="reGon2045")

    reGon2045.description = """
        The scenario reGon2045 is based on scenario C 2045 of the
        Netzentwicklungsplan Strom, Version 2025.
        Scenario C 2045 is characterized by an ambitious expansion of
        renewable energies and a higher share of sector coupling.
        """
    reGon2045.global_parameters = parameters.global_settings(reGon2045.name)

    reGon2045.electricity_parameters = parameters.electricity(reGon2045.name)

    reGon2045.gas_parameters = parameters.gas(reGon2045.name)

    reGon2045.heat_parameters = parameters.heat(reGon2045.name)

    reGon2045.mobility_parameters = parameters.mobility(reGon2045.name)

    session.add(reGon2045)

    session.commit()


def get_sector_parameters(sector, scenario=None):
    """Returns parameters for each sector as dictionary.

    If scenario=None data for all scenarios is returned as pandas.DataFrame.
    Otherwise the parameters of the specific scenario are returned as a dict.

    Parameters
    ----------
    sector : str
        Name of the sector.
        Options are: ['global', 'electricity', 'heat', 'gas', 'mobility']
    scenario : str, optional
        Name of the scenario. The default is None.

    Returns
    -------
    values : dict or pandas.DataFrane
        List or table of parameters for the selected sector

    """

    if scenario:
        if (
            scenario
            in db.select_dataframe(
                f"SELECT name FROM {ScenarioParameters.targets.tables['egon_scenario_parameters']}"
            ).name.values
        ):
            values = db.select_dataframe(
                f"""
                    SELECT {sector}_parameters as val
                    FROM {ScenarioParameters.targets.tables['egon_scenario_parameters']}
                    WHERE name = '{scenario}';"""
            ).val[0]
        else:
            print(f"Scenario name {scenario} is not valid.")
    else:
        values = pd.concat(
            [
                pd.DataFrame(
                    db.select_dataframe(
                        f"""
                        SELECT {sector}_parameters as val
                        FROM {ScenarioParameters.targets.tables['egon_scenario_parameters']}
                        WHERE name='eGon2035'"""
                    ).val[0],
                    index=["eGon2035"],
                ),
                pd.DataFrame(
                    db.select_dataframe(
                        f"""
                        SELECT {sector}_parameters as val
                        FROM {ScenarioParameters.targets.tables['egon_scenario_parameters']}
                        WHERE name='reGon2037'"""
                    ).val[0],
                    index=["reGon2037"],
                ),
                pd.DataFrame(
                    db.select_dataframe(
                        f"""
                        SELECT {sector}_parameters as val
                        FROM {ScenarioParameters.targets.tables['egon_scenario_parameters']}
                        WHERE name='reGon2045'"""
                    ).val[0],
                    index=["reGon2045"],
                ),
                pd.DataFrame(
                    db.select_dataframe(
                        f"""
                        SELECT {sector}_parameters as val
                        FROM {ScenarioParameters.targets.tables['egon_scenario_parameters']}
                        WHERE name='eGon2021'"""
                    ).val[0],
                    index=["eGon2021"],
                ),
            ],
            ignore_index=True,
        )

    return values


def download_pypsa_technology_data():
    """Downlad PyPSA technology data results."""
    data_path = Path(
        ScenarioParameters.targets.files["technology_data"]
    ).parent
    # Delete folder if it already exists
    if data_path.exists() and data_path.is_dir():
        shutil.rmtree(data_path)
    # Retrieve files
    urlretrieve(
        ScenarioParameters.sources.urls["pypsa_technology_data"]["url"],
        ScenarioParameters.targets.files["pypsa_zip"],
    )

    with zipfile.ZipFile(
        ScenarioParameters.targets.files["pypsa_zip"], "r"
    ) as zip_ref:
        zip_ref.extractall(".")


class ScenarioParameters(Dataset):
    """
    Create and fill table with central parameters for each scenario

    This dataset creates and fills a table in the database that includes central parameters
    for each scenarios. These parameters are mostly from extrernal sources, they are defined
    and referenced within this dataset.
    The table is acced by various datasets to access the parameters for all sectors.


    *Dependencies*
      * :py:func:`Setup <egon.data.datasets.database.setup>`


    *Resulting tables*
      * :py:class:`scenario.egon_scenario_parameters <egon.data.datasets.scenario_parameters.EgonScenario>` is created and filled


    """

    #:
    name: str = "ScenarioParameters"
    #:
    version: str = "0.0.22"

    sources = DatasetSources(
        urls={
            "pypsa_technology_data": {
                "url": "https://zenodo.org/record/5544025/files/PyPSA/technology-data-v0.3.0.zip",
            }
        }
    )

    targets = DatasetTargets(
        tables={
            "egon_scenario_parameters": "scenario.egon_scenario_parameters",
        },
        files={
            "pypsa_zip": "pypsa_technology_data_egon_data.zip",
            "data_dir": "PyPSA-technology-data-94085a8/outputs/",
            "technology_data": "pypsa_technology_data/technology_data.xlsx",
        },
    )

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(
                create_table,
                download_pypsa_technology_data,
                insert_scenarios,
            ),
        )
