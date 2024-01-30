"""
Household electricity demand time series for scenarios in 2035 and 2050 at
census cell level.

Electricity demand data for households in Germany in 1-hourly resolution for
an entire year. Spatially, the data is resolved to 100 x 100 m cells and
provides individual and distinct time series for each household in a cell.
The cells are defined by the dataset Zensus 2011.

The resulting data is stored in two separate tables

* `demand.household_electricity_profiles_in_census_cells`:
  Lists references and scaling parameters to time series data for each
  household in a cell by identifiers. This table is fundamental for creating
  subsequent data like demand profiles on MV grid level or for determining
  the peak load at load area level.
  The table is created by:func:`houseprofiles_in_census_cells`.
* `demand.household_electricity_profiles_hvmv_substation`:
  Household electricity demand profiles aggregated at MV grid district level
  in MWh. Primarily used to create the eTraGo data model.
  The table is created with :func:`mv_grid_district_HH_electricity_load`.

The following datasets are used for creating the data:

* Electricity demand time series for household categories
  produced by demand profile generator (DPG) from Fraunhofer IEE
  (see :func:`get_iee_hh_demand_profiles_raw`)
* Spatial information about people living in households by Zensus 2011 at
  federal state level
    * Type of household (family status)
    * Age
    * Number of people
* Spatial information about number of households per ha, categorized by type
  of household (family status) with 5 categories (also from Zensus 2011)
* Demand-Regio annual household demand at NUTS3 level

**What is the goal?**

To use the electricity demand time series from the `demand profile generator`
to created spatially reference household demand time series for Germany at a
resolution of 100 x 100 m cells.

**What is the challenge?**

The electricity demand time series produced by demand profile generator offer
12 different household profile categories.
To use most of them, the spatial information about the number of households
per cell (5 categories) needs to be enriched by supplementary data to match
the household demand profile categories specifications. Hence, 10 out of 12
different household profile categories can be distinguished by increasing
the number of categories of cell-level household data.

**How are these datasets combined?**

* Spatial information about people living in households by zensus (2011) at
  federal state NUTS1 level :var:`df_zensus` is aggregated to be compatible
  to IEE household profile specifications.
    * exclude kids and reduce to adults and seniors
    * group as defined in :var:`HH_TYPES`
    * convert data from people living in households to number of households
      by :var:`mapping_people_in_households`
    * calculate fraction of fine household types (10) within subgroup of rough
      household types (5) :var:`df_dist_households`
* Spatial information about number of households per ha
  :var:`df_census_households_nuts3` is mapped to NUTS1 and NUTS3 level.
  Data is refined with household subgroups via
  :var:`df_dist_households` to :var:`df_census_households_grid_refined`.
* Enriched 100 x 100 m household dataset is used to sample and aggregate
  household profiles. A table including individual profile id's for each cell
  and scaling factor to match Demand-Regio annual sum projections for 2035
  and 2050 at NUTS3 level is created in the database as
  `demand.household_electricity_profiles_in_census_cells`.

**What are central assumptions during the data processing?**

* Mapping zensus data to IEE household categories is not trivial. In
  conversion from persons in household to number of
  households, number of inhabitants for multi-person households is estimated
  as weighted average in :var:`OO_factor`
* The distribution to refine household types at cell level are the same for
  each federal state
* Refining of household types lead to float number of profiles drew at cell
  level and need to be rounded to nearest int by np.rint().
* 100 x 100 m cells are matched to NUTS via cells centroid location
* Cells with households in unpopulated areas are removed

**Drawbacks and limitations of the data**

* The distribution to refine household types at cell level are the same for
  each federal state
* Household profiles aggregated annual demand matches Demand Regio demand at
  NUTS-3 level, but it is not matching the demand regio time series profile
* Due to secrecy, some census data are highly modified under certain attributes
 (quantity_q = 2). This cell data is not corrected, but excluded.
* There is deviation in the Census data from table to table. The statistical
 methods are not stringent. Hence, there are cases in which data contradicts.
* Census data with attribute 'HHTYP_FAM' is missing for some cells with small
 amount of households. This data is generated using the average share of
 household types for cells with similar household number. For some cells the
 summed amount of households per type deviates from the total number with
 attribute 'INSGESAMT'. As the profiles are scaled with demand-regio data at
 nuts3-level the impact at a higher aggregation level is negligible.
 For sake of simplicity, the data is not corrected.
* There are cells without household data but a population. A randomly chosen
 household distribution is taken from a subgroup of cells with same population
 value and applied to all cells with missing household distribution and the
 specific population value.

Helper functions
----
* To access the DB, select specific profiles at various aggregation levels
use:func:`get_hh_profiles_from_db'
* To access the DB, select specific profiles at various aggregation levels
and scale profiles use :func:`get_scaled_profiles_from_db`


Notes
-----

This module docstring is rather a dataset documentation. Once, a decision
is made in ... the content of this module docstring needs to be moved to
docs attribute of the respective dataset class.
"""
from itertools import cycle, product
from pathlib import Path
import os
import random

from airflow.operators.python_operator import PythonOperator
from sqlalchemy import ARRAY, Column, Float, Integer, String
from sqlalchemy.dialects.postgresql import CHAR, INTEGER, REAL
from sqlalchemy.ext.declarative import declarative_base
import numpy as np
import pandas as pd

from egon.data import db
from egon.data.datasets import Dataset
from egon.data.datasets.zensus_mv_grid_districts import MapZensusGridDistricts
import egon.data.config

Base = declarative_base()
engine = db.engine()


# Get random seed from config
RANDOM_SEED = egon.data.config.settings()["egon-data"]["--random-seed"]


class IeeHouseholdLoadProfiles(Base):
    __tablename__ = "iee_household_load_profiles"
    __table_args__ = {"schema": "demand"}

    id = Column(INTEGER, primary_key=True)
    type = Column(CHAR(8), index=True)
    load_in_wh = Column(ARRAY(REAL))


class HouseholdElectricityProfilesInCensusCells(Base):
    __tablename__ = "egon_household_electricity_profile_in_census_cell"
    __table_args__ = {"schema": "demand"}

    cell_id = Column(Integer, primary_key=True)
    grid_id = Column(String)
    cell_profile_ids = Column(ARRAY(String, dimensions=1))
    nuts3 = Column(String)
    nuts1 = Column(String)
    factor_2019 = Column(Float)
    factor_2023 = Column(Float)
    factor_2035 = Column(Float)
    factor_2050 = Column(Float)


class EgonDestatisZensusHouseholdPerHaRefined(Base):
    __tablename__ = "egon_destatis_zensus_household_per_ha_refined"
    __table_args__ = {"schema": "society"}

    id = Column(INTEGER, primary_key=True)
    cell_id = Column(Integer, index=True)
    grid_id = Column(String, index=True)
    nuts3 = Column(String)
    nuts1 = Column(String)
    characteristics_code = Column(Integer)
    hh_5types = Column(Integer)
    hh_type = Column(CHAR(2))
    hh_10types = Column(Integer)


class EgonEtragoElectricityHouseholds(Base):
    __tablename__ = "egon_etrago_electricity_households"
    __table_args__ = {"schema": "demand"}

    bus_id = Column(Integer, primary_key=True)
    scn_name = Column(String, primary_key=True)
    p_set = Column(ARRAY(Float))
    q_set = Column(ARRAY(Float))


class HouseholdDemands(Dataset):
    def __init__(self, dependencies):
        tasks = (houseprofiles_in_census_cells,)

        if (
            "status2019"
            in egon.data.config.settings()["egon-data"]["--scenarios"]
        ):
            mv_hh_electricity_load_2035 = PythonOperator(
                task_id="MV-hh-electricity-load-2019",
                python_callable=mv_grid_district_HH_electricity_load,
                op_args=["status2019", 2019],
                op_kwargs={"drop_table": True},
            )

            tasks = tasks + (mv_hh_electricity_load_2035,)

        if (
            "status2023"
            in egon.data.config.settings()["egon-data"]["--scenarios"]
        ):
            mv_hh_electricity_load_2035 = PythonOperator(
                task_id="MV-hh-electricity-load-2023",
                python_callable=mv_grid_district_HH_electricity_load,
                op_args=["status2023", 2023],
                op_kwargs={"drop_table": True},
            )

            tasks = tasks + (mv_hh_electricity_load_2035,)

        if (
            "eGon2035"
            in egon.data.config.settings()["egon-data"]["--scenarios"]
        ):
            mv_hh_electricity_load_2035 = PythonOperator(
                task_id="MV-hh-electricity-load-2035",
                python_callable=mv_grid_district_HH_electricity_load,
                op_args=["eGon2035", 2035],
            )

            tasks = tasks + (mv_hh_electricity_load_2035,)

        if (
            "eGon100RE"
            in egon.data.config.settings()["egon-data"]["--scenarios"]
        ):
            mv_hh_electricity_load_2050 = PythonOperator(
                task_id="MV-hh-electricity-load-2050",
                python_callable=mv_grid_district_HH_electricity_load,
                op_args=["eGon100RE", 2050],
            )

            tasks = tasks + (mv_hh_electricity_load_2050,)

        super().__init__(
            name="Household Demands",
            version="0.0.12",
            dependencies=dependencies,
            tasks=tasks,
        )


def clean(x):
    """Clean zensus household data row-wise

    Clean dataset by

    * converting '.' and '-' to str(0)
    * removing brackets

    Table can be converted to int/floats afterwards

    Parameters
    ----------
    x: pd.Series
        It is meant to be used with :code:`df.applymap()`

    Returns
    -------
    pd.Series
        Re-formatted data row
    """
    x = str(x).replace("-", str(0))
    x = str(x).replace(".", str(0))
    x = x.strip("()")
    return x


def write_hh_profiles_to_db(hh_profiles):
    """Write HH demand profiles of IEE into db. One row per profile type.
    The annual load profile timeseries is an array.

    schema: demand
    tablename: iee_household_load_profiles



    Parameters
    ----------
    hh_profiles: pd.DataFrame
        It is meant to be used with :code:`df.applymap()`

    Returns
    -------
    """
    hh_profiles = hh_profiles.rename_axis("type", axis=1)
    hh_profiles = hh_profiles.rename_axis("timestep", axis=0)
    hh_profiles = hh_profiles.stack().rename("load_in_wh")
    hh_profiles = hh_profiles.to_frame().reset_index()
    hh_profiles = hh_profiles.groupby("type").load_in_wh.apply(tuple)
    hh_profiles = hh_profiles.reset_index()

    IeeHouseholdLoadProfiles.__table__.drop(bind=engine, checkfirst=True)
    IeeHouseholdLoadProfiles.__table__.create(bind=engine)

    hh_profiles.to_sql(
        name=IeeHouseholdLoadProfiles.__table__.name,
        schema=IeeHouseholdLoadProfiles.__table__.schema,
        con=engine,
        if_exists="append",
        method="multi",
        chunksize=100,
        index=False,
        dtype={
            "load_in_wh": IeeHouseholdLoadProfiles.load_in_wh.type,
            "type": IeeHouseholdLoadProfiles.type.type,
            "id": IeeHouseholdLoadProfiles.id.type,
        },
    )


def get_iee_hh_demand_profiles_raw():
    """Gets and returns household electricity demand profiles from the
    egon-data-bundle.

    Household electricity demand profiles generated by Fraunhofer IEE.
    Methodology is described in
    :ref:`Erzeugung zeitlich hochaufgelöster Stromlastprofile für verschiedene
    Haushaltstypen
    <https://www.researchgate.net/publication/273775902_Erzeugung_zeitlich_hochaufgeloster_Stromlastprofile_fur_verschiedene_Haushaltstypen>`_.
    It is used and further described in the following theses by:

    * Jonas Haack:
      "Auswirkungen verschiedener Haushaltslastprofile auf PV-Batterie-Systeme"
      (confidential)
    * Simon Ruben Drauz
      "Synthesis of a heat and electrical load profile for single and
      multi-family houses used for subsequent performance tests of a
      multi-component energy system",
      http://dx.doi.org/10.13140/RG.2.2.13959.14248

    Notes
    -----
    The household electricity demand profiles have been generated for 2016
    which is a leap year (8784 hours) starting on a Friday. The weather year
    is 2011 and the heat timeseries 2011 are generated for 2011 too (cf.
    dataset :mod:`egon.data.datasets.heat_demand_timeseries.HTS`), having
    8760h and starting on a Saturday. To align the profiles, the first day of
    the IEE profiles are deleted, resulting in 8760h starting on Saturday.

    Returns
    -------
    pd.DataFrame
        Table with profiles in columns and time as index. A pd.MultiIndex is
        used to distinguish load profiles from different EUROSTAT household
        types.
    """
    data_config = egon.data.config.datasets()
    pa_config = data_config["hh_demand_profiles"]

    def ve(s):
        raise (ValueError(s))

    dataset = egon.data.config.settings()["egon-data"]["--dataset-boundary"]

    file_section = (
        "path"
        if dataset == "Everything"
        else "path_testmode"
        if dataset == "Schleswig-Holstein"
        else ve(f"'{dataset}' is not a valid dataset boundary.")
    )

    file_path = pa_config["sources"]["household_electricity_demand_profiles"][
        file_section
    ]

    download_directory = os.path.join(
        "data_bundle_egon_data", "household_electricity_demand_profiles"
    )

    hh_profiles_file = (
        Path(".") / Path(download_directory) / Path(file_path).name
    )

    df_hh_profiles = pd.read_hdf(hh_profiles_file)

    # aggregate profile types O2, O1 and O0 as there is no differentiation
    # possible at cell level see :func:`regroup_nuts1_census_data`.
    merge_profiles = [i for i in df_hh_profiles.columns if "O1" in i[:3]]
    merge_profiles += [i for i in df_hh_profiles.columns if "O2" in i[:3]]
    merge_profiles += [i for i in df_hh_profiles.columns if "O0" in i[:3]]
    mapping = {f"{old}": f"O0a{i:05d}" for i, old in enumerate(merge_profiles)}
    df_hh_profiles.rename(columns=mapping, inplace=True)

    return df_hh_profiles


def set_multiindex_to_profiles(hh_profiles):
    """The profile id is split into type and number and set as multiindex.

    Parameters
    ----------
    hh_profiles: pd.DataFrame
        Profiles
    Returns
    -------
    hh_profiles: pd.DataFrame
        Profiles with Multiindex
    """

    # set multiindex to HH_types
    hh_profiles.columns = pd.MultiIndex.from_arrays(
        [hh_profiles.columns.str[:2], hh_profiles.columns.str[3:]]
    )

    # Cast profile ids into tuple of type and int
    hh_profiles.columns = pd.MultiIndex.from_tuples(
        [(a, int(b)) for a, b in hh_profiles.columns]
    )

    return hh_profiles


def get_census_households_nuts1_raw():
    """Get zensus age x household type data from egon-data-bundle

    Dataset about household size with information about the categories:

    * family type
    * age class
    * household size

    for Germany in spatial resolution of federal states NUTS-1.

    Data manually selected and retrieved from:
    https://ergebnisse2011.zensus2022.de/datenbank/online
    For reproducing data selection, please do:

    * Search for: "1000A-3016"
    * or choose topic: "Bevölkerung kompakt"
    * Choose table code: "1000A-3016" with title "Personen: Alter
      (11 Altersklassen) - Größe des privaten Haushalts - Typ des privaten
      Haushalts (nach Familien/Lebensform)"
    - Change setting "GEOLK1" to "Bundesländer (16)"

    Data would be available in higher resolution
    ("Landkreise und kreisfreie Städte (412)"), but only after registration.

    The downloaded file is called 'Zensus2011_Personen.csv'.


    Returns
    -------
    pd.DataFrame
        Pre-processed zensus household data
    """
    data_config = egon.data.config.datasets()
    pa_config = data_config["hh_demand_profiles"]
    file_path = pa_config["sources"]["zensus_household_types"]["path"]

    download_directory = os.path.join(
        "data_bundle_egon_data", "zensus_households"
    )

    households_file = (
        Path(".") / Path(download_directory) / Path(file_path).name
    )

    households_raw = pd.read_csv(
        households_file,
        sep=";",
        decimal=".",
        skiprows=5,
        skipfooter=7,
        index_col=[0, 1],
        header=[0, 1],
        encoding="latin1",
        engine="python",
    )

    return households_raw


def create_missing_zensus_data(
    df_households_typ, df_missing_data, missing_cells
):
    """
    There is missing data for specific attributes in the zensus dataset because
    of secrecy reasons. Some cells with only small amount of households are
    missing with attribute HHTYP_FAM. However the total amount of households
    is known with attribute INSGESAMT. The missing data is generated as average
    share of the household types for cell groups with the same amount of
    households.

    Parameters
    ----------
    df_households_typ: pd.DataFrame
        Zensus households data
    df_missing_data: pd.DataFrame
        number of missing cells of group of amount of households
    missing_cells: dict
        dictionary with list of grids of the missing cells grouped by amount of
        households in cell

    Returns
    ----------
    df_average_split: pd.DataFrame
        generated dataset of missing cells

    """
    # grid_ids of missing cells grouped by amount of households
    missing_grid_ids = {
        group: list(df.grid_id)
        for group, df in missing_cells.groupby("quantity")
    }

    # Grid ids for cells with low household numbers
    df_households_typ = df_households_typ.set_index("grid_id", drop=True)
    hh_in_cells = df_households_typ.groupby("grid_id")["quantity"].sum()
    hh_index = {
        i: hh_in_cells.loc[hh_in_cells == i].index
        for i in df_missing_data.households.values
    }

    df_average_split = pd.DataFrame()
    for hh_size, index in hh_index.items():
        # average split of household types in cells with low household numbers
        split = (
            df_households_typ.loc[index].groupby("characteristics_code").sum()
            / df_households_typ.loc[index].quantity.sum()
        )
        split = split.quantity * hh_size

        # correct rounding
        difference = int(split.sum() - split.round().sum())
        if difference > 0:
            # add to any row
            split = split.round()
            random_row = split.sample()
            split[random_row.index] = random_row + difference
        elif difference < 0:
            # subtract only from rows > 0
            split = split.round()
            random_row = split[split > 0].sample()
            split[random_row.index] = random_row + difference
        else:
            split = split.round()

        # Dataframe with average split for each cell
        temp = pd.DataFrame(
            product(zip(split, range(1, 6)), missing_grid_ids[hh_size]),
            columns=["tuple", "grid_id"],
        )
        temp = pd.DataFrame(temp.tuple.tolist()).join(temp.grid_id)
        temp = temp.rename(columns={0: "hh_5types", 1: "characteristics_code"})
        temp = temp.dropna()
        temp = temp[(temp["hh_5types"] != 0)]
        # append for each cell group of households
        df_average_split = pd.concat(
            [df_average_split, temp], ignore_index=True
        )
    df_average_split["hh_5types"] = df_average_split["hh_5types"].astype(int)

    return df_average_split


def process_nuts1_census_data(df_census_households_raw):
    """Make data compatible with household demand profile categories

    Removes and reorders categories which are not needed to fit data to
    household types of IEE electricity demand time series generated by
    demand-profile-generator (DPG).

    * Kids (<15) are excluded as they are also excluded in DPG origin dataset
    * Adults (15<65)
    * Seniors (<65)

    Parameters
    ----------
    df_census_households_raw: pd.DataFrame
        cleaned zensus household type x age category data

    Returns
    -------
    pd.DataFrame
        Aggregated zensus household data on NUTS-1 level
    """

    # Clean data to int only
    df_census_households = df_census_households_raw.applymap(clean).applymap(
        int
    )

    # Group data to fit Load Profile Generator categories
    # define kids/adults/seniors
    kids = ["Unter 3", "3 - 5", "6 - 14"]  # < 15
    adults = [
        "15 - 17",
        "18 - 24",
        "25 - 29",
        "30 - 39",
        "40 - 49",
        "50 - 64",
    ]  # 15 < x <65
    seniors = ["65 - 74", "75 und älter"]  # >65

    # sum groups of kids, adults and seniors and concat
    df_kids = (
        df_census_households.loc[:, (slice(None), kids)]
        .groupby(level=0, axis=1)
        .sum()
    )
    df_adults = (
        df_census_households.loc[:, (slice(None), adults)]
        .groupby(level=0, axis=1)
        .sum()
    )
    df_seniors = (
        df_census_households.loc[:, (slice(None), seniors)]
        .groupby(level=0, axis=1)
        .sum()
    )
    df_census_households = pd.concat(
        [df_kids, df_adults, df_seniors],
        axis=1,
        keys=["Kids", "Adults", "Seniors"],
        names=["age", "persons"],
    )

    # reduce column names to state only
    mapping_state = {
        i: i.split()[1]
        for i in df_census_households.index.get_level_values(level=0)
    }

    # rename index
    df_census_households = df_census_households.rename(
        index=mapping_state, level=0
    )
    # rename axis
    df_census_households = df_census_households.rename_axis(["state", "type"])
    # unstack
    df_census_households = df_census_households.unstack()
    # reorder levels
    df_census_households = df_census_households.reorder_levels(
        order=["type", "persons", "age"], axis=1
    )

    return df_census_households


def regroup_nuts1_census_data(df_census_households_nuts1):
    """Regroup census data and map according to demand-profile types.
    For more information look at the respective publication:
    https://www.researchgate.net/publication/273775902_Erzeugung_zeitlich_hochaufgeloster_Stromlastprofile_fur_verschiedene_Haushaltstypen


    Parameters
    ----------
    df_census_households_nuts1: pd.DataFrame
        census household data on NUTS-1 level in absolute values

    Returns
    ----------
    df_dist_households: pd.DataFrame
        Distribution of households type
    """

    # Mapping of census household family types to Eurostat household types
    # - Adults living in households type
    # - kids are  not included even if mentioned in household type name
    # **! The Eurostat data only counts adults/seniors, excluding kids <15**
    # Eurostat household types are used for demand-profile-generator
    # @iee-fraunhofer
    hh_types_eurostat = {
        "SR": [
            ("Einpersonenhaushalte (Singlehaushalte)", "Insgesamt", "Seniors"),
            ("Alleinerziehende Elternteile", "Insgesamt", "Seniors"),
        ],
        # Single Seniors Single Parents Seniors
        "SO": [
            ("Einpersonenhaushalte (Singlehaushalte)", "Insgesamt", "Adults")
        ],  # Single Adults
        "SK": [("Alleinerziehende Elternteile", "Insgesamt", "Adults")],
        # Single Parents Adult
        "PR": [
            ("Paare ohne Kind(er)", "2 Personen", "Seniors"),
            (
                "Mehrpersonenhaushalte ohne Kernfamilie",
                "2 Personen",
                "Seniors",
            ),
        ],
        # Couples without Kids Senior & same sex couples & shared flat seniors
        "PO": [
            ("Paare ohne Kind(er)", "2 Personen", "Adults"),
            ("Mehrpersonenhaushalte ohne Kernfamilie", "2 Personen", "Adults"),
        ],
        # Couples without Kids adults & same sex couples & shared flat adults
        "P1": [("Paare mit Kind(ern)", "3 Personen", "Adults")],
        "P2": [("Paare mit Kind(ern)", "4 Personen", "Adults")],
        "P3": [
            ("Paare mit Kind(ern)", "5 Personen", "Adults"),
            ("Paare mit Kind(ern)", "6 und mehr Personen", "Adults"),
        ],
        "OR": [
            (
                "Mehrpersonenhaushalte ohne Kernfamilie",
                "3 Personen",
                "Seniors",
            ),
            (
                "Mehrpersonenhaushalte ohne Kernfamilie",
                "4 Personen",
                "Seniors",
            ),
            (
                "Mehrpersonenhaushalte ohne Kernfamilie",
                "5 Personen",
                "Seniors",
            ),
            (
                "Mehrpersonenhaushalte ohne Kernfamilie",
                "6 und mehr Personen",
                "Seniors",
            ),
            ("Paare mit Kind(ern)", "3 Personen", "Seniors"),
            ("Paare ohne Kind(er)", "3 Personen", "Seniors"),
            ("Paare mit Kind(ern)", "4 Personen", "Seniors"),
            ("Paare ohne Kind(er)", "4 Personen", "Seniors"),
            ("Paare mit Kind(ern)", "5 Personen", "Seniors"),
            ("Paare ohne Kind(er)", "5 Personen", "Seniors"),
            ("Paare mit Kind(ern)", "6 und mehr Personen", "Seniors"),
            ("Paare ohne Kind(er)", "6 und mehr Personen", "Seniors"),
        ],
        # no info about share of kids
        # OO, O1, O2 have the same amount, as no information about the share of
        # kids within census data set.
        "OO": [
            ("Mehrpersonenhaushalte ohne Kernfamilie", "3 Personen", "Adults"),
            ("Mehrpersonenhaushalte ohne Kernfamilie", "4 Personen", "Adults"),
            ("Mehrpersonenhaushalte ohne Kernfamilie", "5 Personen", "Adults"),
            (
                "Mehrpersonenhaushalte ohne Kernfamilie",
                "6 und mehr Personen",
                "Adults",
            ),
            ("Paare ohne Kind(er)", "3 Personen", "Adults"),
            ("Paare ohne Kind(er)", "4 Personen", "Adults"),
            ("Paare ohne Kind(er)", "5 Personen", "Adults"),
            ("Paare ohne Kind(er)", "6 und mehr Personen", "Adults"),
        ],
        # no info about share of kids
    }

    # absolute values
    df_hh_distribution_abs = pd.DataFrame(
        (
            {
                hhtype: df_census_households_nuts1.loc[countries, codes].sum()
                for hhtype, codes in hh_types_eurostat.items()
            }
            for countries in df_census_households_nuts1.index
        ),
        index=df_census_households_nuts1.index,
    )
    # drop zero columns
    df_hh_distribution_abs = df_hh_distribution_abs.loc[
        :, (df_hh_distribution_abs != 0).any(axis=0)
    ].T

    return df_hh_distribution_abs


def inhabitants_to_households(df_hh_people_distribution_abs):
    """
    Convert number of inhabitant to number of household types

    Takes the distribution of peoples living in types of households to
    calculate a distribution of household types by using a people-in-household
    mapping. Results are not rounded to int as it will be used to calculate
    a relative distribution anyways.
    The data of category 'HHGROESS_KLASS' in census households
    at grid level is used to determine an average wherever the amount
    of people is not trivial (OR, OO). Kids are not counted.

    Parameters
    ----------
    df_hh_people_distribution_abs: pd.DataFrame
        Grouped census household data on NUTS-1 level in absolute values

    Returns
    ----------
    df_dist_households: pd.DataFrame
        Distribution of households type

    """

    # Get household size for each census cell grouped by
    # As this is only used to estimate size of households for OR, OO
    # The hh types 1 P and 2 P households are dropped
    df_hh_size = db.select_dataframe(
        sql="""
                SELECT characteristics_text, SUM(quantity) as summe
                FROM society.egon_destatis_zensus_household_per_ha as egon_d
                WHERE attribute = 'HHGROESS_KLASS' AND quantity_q < 2
                GROUP BY characteristics_text """,
        index_col="characteristics_text",
    )
    df_hh_size = df_hh_size.drop(index=["1 Person", "2 Personen"])

    # Define/ estimate number of persons (w/o kids) for each household category
    # For categories S* and P* it's clear; for multi-person households (OO,OR)
    # the number is estimated as average by taking remaining persons
    OO_factor = (
        sum(df_hh_size["summe"] * [3, 4, 5, 6]) / df_hh_size["summe"].sum()
    )
    mapping_people_in_households = {
        "SR": 1,
        "SO": 1,
        "SK": 1,  # kids are excluded
        "PR": 2,
        "PO": 2,
        "P1": 2,  # kids are excluded
        "P2": 2,  # ""
        "P3": 2,  # ""
        "OR": OO_factor,
        "OO": OO_factor,
    }

    # compare categories and remove form mapping if to many
    diff = set(df_hh_people_distribution_abs.index) ^ set(
        mapping_people_in_households.keys()
    )

    if bool(diff):
        for key in diff:
            mapping_people_in_households = dict(mapping_people_in_households)
            del mapping_people_in_households[key]
        print(f"Removed {diff} from mapping!")

    # divide amount of people by people in household types
    df_dist_households = df_hh_people_distribution_abs.div(
        mapping_people_in_households, axis=0
    )

    return df_dist_households


def impute_missing_hh_in_populated_cells(df_census_households_grid):
    """There are cells without household data but a population. A randomly
    chosen household distribution is taken from a subgroup of cells with same
    population value and applied to all cells with missing household
    distribution and the specific population value. In the case, in which there
    is no subgroup with household data of the respective population value, the
    fallback is the subgroup with the last last smaller population value.

    Parameters
    ----------
    df_census_households_grid: pd.DataFrame
        census household data at 100x100m grid level

    Returns
    -------
    pd.DataFrame
        substituted census household data at 100x100m grid level"""

    df_w_hh = df_census_households_grid.dropna().reset_index(drop=True)
    df_wo_hh = df_census_households_grid.loc[
        df_census_households_grid.isna().any(axis=1)
    ].reset_index(drop=True)

    # iterate over unique population values
    for population in df_wo_hh["population"].sort_values().unique():
        # create fallback if no cell with specific population available
        if population in df_w_hh["population"].unique():
            fallback_value = population
            population_value = population
        # use fallback of last possible household distribution
        else:
            population_value = fallback_value

        # get cells with specific population value from cells with
        # household distribution
        df_w_hh_population_i = df_w_hh.loc[
            df_w_hh["population"] == population_value
        ]
        # choose random cell within this group
        rnd_cell_id_population_i = np.random.choice(
            df_w_hh_population_i["cell_id"].unique()
        )
        # get household distribution of this cell
        df_rand_hh_distribution = df_w_hh_population_i.loc[
            df_w_hh_population_i["cell_id"] == rnd_cell_id_population_i
        ]
        # get cells with specific population value from cells without
        # household distribution
        df_wo_hh_population_i = df_wo_hh.loc[
            df_wo_hh["population"] == population
        ]

        # all cells will get the same random household distribution

        # prepare size of dataframe by number of household types
        df_repeated = pd.concat(
            [df_wo_hh_population_i] * df_rand_hh_distribution.shape[0],
            ignore_index=True,
        )
        df_repeated = df_repeated.sort_values("cell_id").reset_index(drop=True)

        # insert random household distribution
        columns = ["characteristics_code", "hh_5types"]
        df_repeated.loc[:, columns] = pd.concat(
            [df_rand_hh_distribution.loc[:, columns]]
            * df_wo_hh_population_i.shape[0]
        ).values
        # append new cells
        df_w_hh = pd.concat([df_w_hh, df_repeated], ignore_index=True)

    return df_w_hh


def get_census_households_grid():
    """Query census household data at 100x100m grid level from database. As
    there is a divergence in the census household data depending which
    attribute is used. There also exist cells without household but with
    population data. The missing data in these cases are substituted. First
    census household data with attribute 'HHTYP_FAM' is missing for some
    cells with small amount of households. This data is generated using the
    average share of household types for cells with similar household number.
    For some cells the summed amount of households per type deviates from the
    total number with attribute 'INSGESAMT'. As the profiles are scaled with
    demand-regio data at nuts3-level the impact at a higher aggregation level
    is negligible. For sake of simplicity, the data is not corrected.

    Returns
    -------
    pd.DataFrame
        census household data at 100x100m grid level"""

    # Retrieve information about households for each census cell
    # Only use cell-data which quality (quantity_q<2) is acceptable
    df_census_households_grid = db.select_dataframe(
        sql="""
                SELECT grid_id, attribute, characteristics_code,
                 characteristics_text, quantity
                FROM society.egon_destatis_zensus_household_per_ha
                WHERE attribute = 'HHTYP_FAM' AND quantity_q <2"""
    )
    df_census_households_grid = df_census_households_grid.drop(
        columns=["attribute", "characteristics_text"]
    )

    # Missing data is detected
    df_missing_data = db.select_dataframe(
        sql="""
                    SELECT count(joined.quantity_gesamt) as amount,
                     joined.quantity_gesamt as households
                    FROM(
                        SELECT t2.grid_id, quantity_gesamt, quantity_sum_fam,
                         (quantity_gesamt-(case when quantity_sum_fam isnull
                         then 0 else quantity_sum_fam end))
                         as insgesamt_minus_fam
                    FROM (
                        SELECT  grid_id, SUM(quantity) as quantity_sum_fam
                        FROM society.egon_destatis_zensus_household_per_ha
                        WHERE attribute = 'HHTYP_FAM'
                        GROUP BY grid_id) as t1
                    Full JOIN (
                        SELECT grid_id, sum(quantity) as quantity_gesamt
                        FROM society.egon_destatis_zensus_household_per_ha
                        WHERE attribute = 'INSGESAMT'
                        GROUP BY grid_id) as t2 ON t1.grid_id = t2.grid_id
                        ) as joined
                    WHERE quantity_sum_fam isnull
                    Group by quantity_gesamt """
    )
    missing_cells = db.select_dataframe(
        sql="""
                    SELECT t12.grid_id, t12.quantity
                    FROM (
                    SELECT t2.grid_id, (case when quantity_sum_fam isnull
                    then quantity_gesamt end) as quantity
                    FROM (
                        SELECT  grid_id, SUM(quantity) as quantity_sum_fam
                        FROM society.egon_destatis_zensus_household_per_ha
                        WHERE attribute = 'HHTYP_FAM'
                        GROUP BY grid_id) as t1
                    Full JOIN (
                        SELECT grid_id, sum(quantity) as quantity_gesamt
                        FROM society.egon_destatis_zensus_household_per_ha
                        WHERE attribute = 'INSGESAMT'
                        GROUP BY grid_id) as t2 ON t1.grid_id = t2.grid_id
                        ) as t12
                    WHERE quantity is not null"""
    )

    # Missing cells are substituted by average share of cells with same amount
    # of households.
    df_average_split = create_missing_zensus_data(
        df_census_households_grid, df_missing_data, missing_cells
    )

    df_census_households_grid = df_census_households_grid.rename(
        columns={"quantity": "hh_5types"}
    )

    df_census_households_grid = pd.concat(
        [df_census_households_grid, df_average_split], ignore_index=True
    )

    # Census cells with nuts3 and nuts1 information
    df_grid_id = db.select_dataframe(
        sql="""
                SELECT pop.grid_id, pop.id as cell_id, pop.population,
                 vg250.vg250_nuts3 as nuts3, lan.nuts as nuts1, lan.gen
                FROM
                society.destatis_zensus_population_per_ha_inside_germany as pop
                LEFT JOIN boundaries.egon_map_zensus_vg250 as vg250
                ON (pop.id=vg250.zensus_population_id)
                LEFT JOIN boundaries.vg250_lan as lan
                ON (LEFT(vg250.vg250_nuts3, 3) = lan.nuts)
                WHERE lan.gf = 4 """
    )
    df_grid_id = df_grid_id.drop_duplicates()
    df_grid_id = df_grid_id.reset_index(drop=True)

    # Merge household type and size data with considered (populated) census
    # cells how='right' is used as ids of unpopulated areas are removed
    # by df_grid_id or ancestors. See here:
    # https://github.com/openego/eGon-data/blob/add4944456f22b8873504c5f579b61dca286e357/src/egon/data/datasets/zensus_vg250.py#L269
    df_census_households_grid = pd.merge(
        df_census_households_grid,
        df_grid_id,
        left_on="grid_id",
        right_on="grid_id",
        how="right",
    )
    df_census_households_grid = df_census_households_grid.sort_values(
        ["cell_id", "characteristics_code"]
    )

    return df_census_households_grid


def proportionate_allocation(
    df_group, dist_households_nuts1, hh_10types_cluster
):
    """Household distribution at nuts1 are applied at census cell within group

    To refine the hh_5types and keep the distribution at nuts1 level,
    the household types are clustered and drawn with proportionate weighting.
    The resulting pool is splitted into subgroups with sizes according to
    the number of households of clusters in cells.

    Parameters
    ----------
    df_group: pd.DataFrame
        Census household data at grid level for specific hh_5type cluster in
        a federal state
    dist_households_nuts1: pd.Series
        Household distribution of of hh_10types in a federal state
    hh_10types_cluster: list of str
        Cluster of household types to be refined to

    Returns
    -------
    pd.DataFrame
        Refined household data with hh_10types of cluster at nuts1 level
    """

    # get probability of households within hh_5types group
    probability = dist_households_nuts1[hh_10types_cluster].values
    # get total number of households within hh_5types group in federal state
    size = df_group["hh_5types"].sum().astype(int)

    # random sample within hh_5types group with probability for whole federal
    # state
    choices = np.random.choice(
        a=hh_10types_cluster, size=size, replace=True, p=probability
    )
    # get section sizes to split the sample pool from federal state to grid
    # cells
    split_sections = df_group["hh_5types"].cumsum().astype(int)[:-1]
    # split into grid cell groups
    samples = np.split(choices, split_sections)
    # count number of hh_10types for each cell
    sample_count = [np.unique(x, return_counts=True) for x in samples]

    df_distribution = pd.DataFrame(
        sample_count, columns=["hh_type", "hh_10types"]
    )
    # add cell_ids
    df_distribution["cell_id"] = df_group["cell_id"].unique()

    # unnest
    df_distribution = (
        df_distribution.apply(pd.Series.explode)
        .reset_index(drop=True)
        .dropna()
    )

    return df_distribution


def refine_census_data_at_cell_level(
    df_census_households_grid,
    df_census_households_nuts1,
):
    """The census data is processed to define the number and type of households
    per zensus cell. Two subsets of the census data are merged to fit the
    IEE profiles specifications. To do this, proportionate allocation is
    applied at nuts1 level and within household type clusters.

    .. csv-table:: Mapping table
    :header: "characteristics_code", "characteristics_text", "mapping"

    "1", "Einpersonenhaushalte (Singlehaushalte)", "SR; SO"
    "2", "Paare ohne Kind(er)", "PR; PO"
    "3", "Paare mit Kind(ern)", "P1; P2; P3"
    "4", "Alleinerziehende Elternteile", "SK"
    "5", "Mehrpersonenhaushalte ohne Kernfamilie", "OR; OO"


    Parameters
    ----------
    df_census_households_grid: pd.DataFrame
        Aggregated zensus household data on 100x100m grid level
    df_census_households_nuts1: pd.DataFrame
        Aggregated zensus household data on NUTS-1 level

    Returns
    -------
    pd.DataFrame
        Number of hh types per census cell
    """
    mapping_zensus_hh_subgroups = {
        1: ["SR", "SO"],
        2: ["PR", "PO"],
        3: ["P1", "P2", "P3"],
        4: ["SK"],
        5: ["OR", "OO"],
    }

    # Calculate fraction of fine household types within subgroup of
    # rough household types
    df_dist_households = df_census_households_nuts1.copy()
    for value in mapping_zensus_hh_subgroups.values():
        df_dist_households.loc[value] = df_census_households_nuts1.loc[
            value
        ].div(df_census_households_nuts1.loc[value].sum())

    # Refine from hh_5types to hh_10types
    df_distribution_nuts0 = pd.DataFrame()
    # Loop over federal states
    for gen, df_nuts1 in df_census_households_grid.groupby("gen"):
        # take subgroup distribution from federal state
        dist_households_nuts1 = df_dist_households[gen]

        df_distribution_nuts1 = pd.DataFrame()
        # loop over hh_5types as cluster
        for (
            hh_5type_cluster,
            hh_10types_cluster,
        ) in mapping_zensus_hh_subgroups.items():
            # get census household of hh_5type and federal state
            df_group = df_nuts1.loc[
                df_nuts1["characteristics_code"] == hh_5type_cluster
            ]

            # apply proportionate allocation function within cluster
            df_distribution_group = proportionate_allocation(
                df_group, dist_households_nuts1, hh_10types_cluster
            )
            df_distribution_group["characteristics_code"] = hh_5type_cluster
            df_distribution_nuts1 = pd.concat(
                [df_distribution_nuts1, df_distribution_group],
                ignore_index=True,
            )

        df_distribution_nuts0 = pd.concat(
            [df_distribution_nuts0, df_distribution_nuts1], ignore_index=True
        )

    df_census_households_grid_refined = df_census_households_grid.merge(
        df_distribution_nuts0,
        how="inner",
        left_on=["cell_id", "characteristics_code"],
        right_on=["cell_id", "characteristics_code"],
    )

    df_census_households_grid_refined[
        "characteristics_code"
    ] = df_census_households_grid_refined["characteristics_code"].astype(int)
    df_census_households_grid_refined[
        "hh_5types"
    ] = df_census_households_grid_refined["hh_5types"].astype(int)
    df_census_households_grid_refined[
        "hh_10types"
    ] = df_census_households_grid_refined["hh_10types"].astype(int)

    return df_census_households_grid_refined


def get_cell_demand_profile_ids(df_cell, pool_size):
    """
    Generates tuple of hh_type and zensus cell ids

    Takes a random sample of profile ids for given cell:
      * if pool size >= sample size: without replacement
      * if pool size < sample size: with replacement


    Parameters
    ----------
    df_cell: pd.DataFrame
        Household type information for a single zensus cell
    pool_size: int
        Number of available profiles to select from

    Returns
    -------
    list of tuple
        List of (`hh_type`, `cell_id`)

    """
    # maybe use instead
    # np.random.default_rng().integers(low=0, high=pool_size[hh_type], size=sq)
    # instead of random.sample use random.choices() if with replacement
    # list of sample ids per hh_type in cell
    cell_profile_ids = [
        (hh_type, random.sample(range(pool_size[hh_type]), k=sq))
        if pool_size[hh_type] >= sq
        else (hh_type, random.choices(range(pool_size[hh_type]), k=sq))
        for hh_type, sq in zip(
            df_cell["hh_type"],
            df_cell["hh_10types"],
        )
    ]

    # format to lists of tuples (hh_type, id)
    cell_profile_ids = [
        list(zip(cycle([hh_type]), ids)) for hh_type, ids in cell_profile_ids
    ]
    # reduce to list
    cell_profile_ids = [a for b in cell_profile_ids for a in b]

    return cell_profile_ids


# can be parallelized with grouping df_zensus_cells by grid_id/nuts3/nuts1
def assign_hh_demand_profiles_to_cells(df_zensus_cells, df_iee_profiles):
    """
    Assign household demand profiles to each census cell.

    A table including the demand profile ids for each cell is created by using
    :func:`get_cell_demand_profile_ids`. Household profiles are randomly
    sampled for each cell. The profiles are not replaced to the pool within
    a cell but after.

    Parameters
    ----------
    df_zensus_cells: pd.DataFrame
        Household type parameters. Each row representing one household. Hence,
        multiple rows per zensus cell.
    df_iee_profiles: pd.DataFrame
        Household load profile data

        * Index: Times steps as serial integers
        * Columns: pd.MultiIndex with (`HH_TYPE`, `id`)

    Returns
    -------
    pd.DataFrame
        Tabular data with one row represents one zensus cell.
        The column `cell_profile_ids` contains
        a list of tuples (see :func:`get_cell_demand_profile_ids`) providing a
        reference to the actual load profiles that are associated with this
        cell.
    """

    df_hh_profiles_in_census_cells = pd.DataFrame(
        index=df_zensus_cells.grid_id.unique(),
        columns=[
            "cell_profile_ids",
            "cell_id",
            "nuts3",
            "nuts1",
            "factor_2035",
            "factor_2050",
        ],
    )

    df_hh_profiles_in_census_cells = (
        df_hh_profiles_in_census_cells.rename_axis("grid_id")
    )

    pool_size = df_iee_profiles.groupby(level=0, axis=1).size()

    # only use non zero entries
    df_zensus_cells = df_zensus_cells.loc[df_zensus_cells["hh_10types"] != 0]
    for grid_id, df_cell in df_zensus_cells.groupby(by="grid_id"):
        # random sampling of household profiles for each cell
        # with or without replacement (see :func:`get_cell_demand_profile_ids`)
        # within cell but after number of households are rounded to the nearest
        # integer if float this results in a small deviation for the course of
        # the aggregated profiles.
        cell_profile_ids = get_cell_demand_profile_ids(df_cell, pool_size)

        df_hh_profiles_in_census_cells.at[grid_id, "cell_id"] = df_cell.loc[
            :, "cell_id"
        ].unique()[0]
        df_hh_profiles_in_census_cells.at[
            grid_id, "cell_profile_ids"
        ] = cell_profile_ids
        df_hh_profiles_in_census_cells.at[grid_id, "nuts3"] = df_cell.loc[
            :, "nuts3"
        ].unique()[0]
        df_hh_profiles_in_census_cells.at[grid_id, "nuts1"] = df_cell.loc[
            :, "nuts1"
        ].unique()[0]

    return df_hh_profiles_in_census_cells


# can be parallelized with grouping df_zensus_cells by grid_id/nuts3/nuts1
def adjust_to_demand_regio_nuts3_annual(
    df_hh_profiles_in_census_cells, df_iee_profiles, df_demand_regio
):
    """
    Computes the profile scaling factor for alignment to demand regio data

    The scaling factor can be used to re-scale each load profile such that the
    sum of all load profiles within one NUTS-3 area equals the annual demand
    of demand regio data.

    Parameters
    ----------
    df_hh_profiles_in_census_cells: pd.DataFrame
        Result of :func:`assign_hh_demand_profiles_to_cells`.
    df_iee_profiles: pd.DataFrame
        Household load profile data

        * Index: Times steps as serial integers
        * Columns: pd.MultiIndex with (`HH_TYPE`, `id`)

    df_demand_regio: pd.DataFrame
        Annual demand by demand regio for each NUTS-3 region and scenario year.
        Index is pd.MultiIndex with :code:`tuple(scenario_year, nuts3_code)`.

    Returns
    -------
    pd.DataFrame
        Returns the same data as :func:`assign_hh_demand_profiles_to_cells`,
        but with filled columns `factor_2035` and `factor_2050`.
    """
    for nuts3_id, df_nuts3 in df_hh_profiles_in_census_cells.groupby(
        by="nuts3"
    ):
        nuts3_cell_ids = df_nuts3.index
        nuts3_profile_ids = df_nuts3.loc[:, "cell_profile_ids"].sum()

        # take all profiles of one nuts3, aggregate and sum
        # profiles in Wh
        nuts3_profiles_sum_annual = (
            df_iee_profiles.loc[:, nuts3_profile_ids].sum().sum()
        )

        # Scaling Factor
        # ##############
        # demand regio in MWh
        # profiles in Wh

        if (
            "status2019"
            in egon.data.config.settings()["egon-data"]["--scenarios"]
        ):
            df_hh_profiles_in_census_cells.loc[
                nuts3_cell_ids, "factor_2019"
            ] = (
                df_demand_regio.loc[(2019, nuts3_id), "demand_mwha"]
                * 1e3
                / (nuts3_profiles_sum_annual / 1e3)
            )

        if (
            "status2023"
            in egon.data.config.settings()["egon-data"]["--scenarios"]
        ):
            df_hh_profiles_in_census_cells.loc[
                nuts3_cell_ids, "factor_2023"
            ] = (
                df_demand_regio.loc[(2023, nuts3_id), "demand_mwha"]
                * 1e3
                / (nuts3_profiles_sum_annual / 1e3)
            )

        if (
            "eGon2035"
            in egon.data.config.settings()["egon-data"]["--scenarios"]
        ):
            df_hh_profiles_in_census_cells.loc[
                nuts3_cell_ids, "factor_2035"
            ] = (
                df_demand_regio.loc[(2035, nuts3_id), "demand_mwha"]
                * 1e3
                / (nuts3_profiles_sum_annual / 1e3)
            )

        if (
            "eGon100RE"
            in egon.data.config.settings()["egon-data"]["--scenarios"]
        ):
            df_hh_profiles_in_census_cells.loc[
                nuts3_cell_ids, "factor_2050"
            ] = (
                df_demand_regio.loc[(2050, nuts3_id), "demand_mwha"]
                * 1e3
                / (nuts3_profiles_sum_annual / 1e3)
            )

    return df_hh_profiles_in_census_cells


def get_load_timeseries(
    df_iee_profiles,
    df_hh_profiles_in_census_cells,
    cell_ids,
    year,
    aggregate=True,
    peak_load_only=False,
):
    """
    Get peak load for one load area in MWh

    The peak load is calculated in aggregated manner for a group of zensus
    cells that belong to one load area (defined by `cell_ids`).

    Parameters
    ----------
    df_iee_profiles: pd.DataFrame
        Household load profile data in Wh

        * Index: Times steps as serial integers
        * Columns: pd.MultiIndex with (`HH_TYPE`, `id`)

        Used to calculate the peak load from.
    df_hh_profiles_in_census_cells: pd.DataFrame
        Return value of :func:`adjust_to_demand_regio_nuts3_annual`.
    cell_ids: list
        Zensus cell ids that define one group of zensus cells that belong to
        the same load area.
    year: int
        Scenario year. Is used to consider the scaling factor for aligning
        annual demand to NUTS-3 data.
    aggregate: bool
        If true, all profiles are aggregated
    peak_load_only: bool
        If true, only the peak load value is returned (the type of the return
        value is `float`). Defaults to False which returns the entire time
        series as pd.Series.

    Returns
    -------
    pd.Series or float
        Aggregated time series for given `cell_ids` or peak load of this time
        series in MWh.
    """
    timesteps = len(df_iee_profiles)
    if aggregate:
        full_load = pd.Series(
            data=np.zeros(timesteps), dtype=np.float64, index=range(timesteps)
        )
    else:
        full_load = pd.DataFrame(index=range(timesteps))
    load_area_meta = df_hh_profiles_in_census_cells.loc[
        cell_ids, ["cell_profile_ids", "nuts3", f"factor_{year}"]
    ]
    # loop over nuts3 (part_load) and sum (full_load) as the scaling factor
    # applies at nuts3 level
    for (nuts3, factor), df in load_area_meta.groupby(
        by=["nuts3", f"factor_{year}"]
    ):
        if aggregate:
            part_load = (
                df_iee_profiles.loc[:, df["cell_profile_ids"].sum()].sum(
                    axis=1
                )
                * factor
                / 1e6
            )  # from Wh to MWh
            full_load = full_load.add(part_load)
        elif not aggregate:
            part_load = (
                df_iee_profiles.loc[:, df["cell_profile_ids"].sum()]
                * factor
                / 1e6
            )  # from Wh to MWh
            full_load = pd.concat([full_load, part_load], axis=1).dropna(
                axis=1
            )
        else:
            raise KeyError("Parameter 'aggregate' needs to be bool value!")
    if peak_load_only:
        full_load = full_load.max()
    return full_load


def write_refinded_households_to_db(df_census_households_grid_refined):
    # Write allocation table into database
    EgonDestatisZensusHouseholdPerHaRefined.__table__.drop(
        bind=engine, checkfirst=True
    )
    EgonDestatisZensusHouseholdPerHaRefined.__table__.create(
        bind=engine, checkfirst=True
    )

    with db.session_scope() as session:
        session.bulk_insert_mappings(
            EgonDestatisZensusHouseholdPerHaRefined,
            df_census_households_grid_refined.to_dict(orient="records"),
        )


def houseprofiles_in_census_cells():
    """
    Allocate household electricity demand profiles for each census cell.

    Creates a table that maps household electricity demand profiles to census
    cells. Each row represents one cell and contains a list of profile IDs.

    Use :func:`get_houseprofiles_in_census_cells` to retrieve the data from
    the database as pandas

    """

    def gen_profile_names(n):
        """Join from Format (str),(int) to (str)a000(int)"""
        a = f"{n[0]}a{int(n[1]):05d}"
        return a

    # Init random generators using global seed
    random.seed(RANDOM_SEED)
    np.random.seed(RANDOM_SEED)

    # Read demand profiles from egon-data-bundle
    df_iee_profiles = get_iee_hh_demand_profiles_raw()

    # Write raw profiles into db
    write_hh_profiles_to_db(df_iee_profiles)

    # Process profiles for further use
    df_iee_profiles = set_multiindex_to_profiles(df_iee_profiles)

    # Download zensus household NUTS-1 data with family type and age categories
    df_census_households_nuts1_raw = get_census_households_nuts1_raw()

    # Reduce age intervals and remove kids
    df_census_households_nuts1 = process_nuts1_census_data(
        df_census_households_nuts1_raw
    )

    # Regroup data to be compatible with categories from demand profile
    # generator.
    df_census_households_nuts1 = regroup_nuts1_census_data(
        df_census_households_nuts1
    )

    # Convert data from people living in households to households
    # Using a specified amount of inhabitants per household type
    df_census_households_nuts1 = inhabitants_to_households(
        df_census_households_nuts1
    )

    # Query census household grid data with family type
    df_census_households_grid = get_census_households_grid()

    # fill cells with missing household distribution values but population
    # by hh distribution value of random cell with same population value
    df_census_households_grid = impute_missing_hh_in_populated_cells(
        df_census_households_grid
    )

    # Refine census household grid data with additional NUTS-1 level attributes
    df_census_households_grid_refined = refine_census_data_at_cell_level(
        df_census_households_grid, df_census_households_nuts1
    )

    write_refinded_households_to_db(df_census_households_grid_refined)

    # Allocate profile ids to each cell by census data
    df_hh_profiles_in_census_cells = assign_hh_demand_profiles_to_cells(
        df_census_households_grid_refined, df_iee_profiles
    )

    # Annual household electricity demand on NUTS-3 level (demand regio)
    df_demand_regio = db.select_dataframe(
        sql="""
                SELECT year, nuts3, SUM (demand) as demand_mWha
                FROM demand.egon_demandregio_hh as egon_d
                GROUP BY nuts3, year
                ORDER BY year""",
        index_col=["year", "nuts3"],
    )

    # Scale profiles to meet demand regio annual demand projections
    df_hh_profiles_in_census_cells = adjust_to_demand_regio_nuts3_annual(
        df_hh_profiles_in_census_cells, df_iee_profiles, df_demand_regio
    )

    df_hh_profiles_in_census_cells = (
        df_hh_profiles_in_census_cells.reset_index(drop=False)
    )
    df_hh_profiles_in_census_cells["cell_id"] = df_hh_profiles_in_census_cells[
        "cell_id"
    ].astype(int)

    # Cast profile ids back to initial str format
    df_hh_profiles_in_census_cells[
        "cell_profile_ids"
    ] = df_hh_profiles_in_census_cells["cell_profile_ids"].apply(
        lambda x: list(map(gen_profile_names, x))
    )

    # Write allocation table into database
    HouseholdElectricityProfilesInCensusCells.__table__.drop(
        bind=engine, checkfirst=True
    )
    HouseholdElectricityProfilesInCensusCells.__table__.create(
        bind=engine, checkfirst=True
    )

    with db.session_scope() as session:
        session.bulk_insert_mappings(
            HouseholdElectricityProfilesInCensusCells,
            df_hh_profiles_in_census_cells.to_dict(orient="records"),
        )


def get_houseprofiles_in_census_cells():
    """
    Retrieve household electricity demand profile mapping from database

    See Also
    --------
    :func:`houseprofiles_in_census_cells`

    Returns
    -------
    pd.DataFrame
        Mapping of household demand profiles to zensus cells
    """
    with db.session_scope() as session:
        q = session.query(HouseholdElectricityProfilesInCensusCells)

        census_profile_mapping = pd.read_sql(
            q.statement, q.session.bind, index_col="cell_id"
        )

    return census_profile_mapping


def get_cell_demand_metadata_from_db(attribute, list_of_identifiers):
    """
    Retrieve selection of household electricity demand profile mapping

    Parameters
    ----------
    attribute: str
        attribute to filter the table

        * nuts3
        * nuts1
        * cell_id

    list_of_identifiers: list of str/int
        nuts3/nuts1 need to be str
        cell_id need to be int

    See Also
    --------
    :func:`houseprofiles_in_census_cells`

    Returns
    -------
    pd.DataFrame
        Selection of mapping of household demand profiles to zensus cells
    """
    attribute_options = ["nuts3", "nuts1", "cell_id"]
    if attribute not in attribute_options:
        raise ValueError(f"attribute has to be one of: {attribute_options}")

    if not isinstance(list_of_identifiers, list):
        raise KeyError("'list_of_identifiers' is not a list!")

    # Query profile ids and scaling factors for specific attributes
    with db.session_scope() as session:
        if attribute == "nuts3":
            cells_query = session.query(
                HouseholdElectricityProfilesInCensusCells.cell_id,
                HouseholdElectricityProfilesInCensusCells.cell_profile_ids,
                HouseholdElectricityProfilesInCensusCells.nuts3,
                HouseholdElectricityProfilesInCensusCells.nuts1,
                HouseholdElectricityProfilesInCensusCells.factor_2035,
                HouseholdElectricityProfilesInCensusCells.factor_2050,
            ).filter(
                HouseholdElectricityProfilesInCensusCells.nuts3.in_(
                    list_of_identifiers
                )
            )
        elif attribute == "nuts1":
            cells_query = session.query(
                HouseholdElectricityProfilesInCensusCells.cell_id,
                HouseholdElectricityProfilesInCensusCells.cell_profile_ids,
                HouseholdElectricityProfilesInCensusCells.nuts3,
                HouseholdElectricityProfilesInCensusCells.nuts1,
                HouseholdElectricityProfilesInCensusCells.factor_2035,
                HouseholdElectricityProfilesInCensusCells.factor_2050,
            ).filter(
                HouseholdElectricityProfilesInCensusCells.nuts1.in_(
                    list_of_identifiers
                )
            )
        elif attribute == "cell_id":
            cells_query = session.query(
                HouseholdElectricityProfilesInCensusCells.cell_id,
                HouseholdElectricityProfilesInCensusCells.cell_profile_ids,
                HouseholdElectricityProfilesInCensusCells.nuts3,
                HouseholdElectricityProfilesInCensusCells.nuts1,
                HouseholdElectricityProfilesInCensusCells.factor_2035,
                HouseholdElectricityProfilesInCensusCells.factor_2050,
            ).filter(
                HouseholdElectricityProfilesInCensusCells.cell_id.in_(
                    list_of_identifiers
                )
            )

    cell_demand_metadata = pd.read_sql(
        cells_query.statement, cells_query.session.bind, index_col="cell_id"
    )
    return cell_demand_metadata


def get_hh_profiles_from_db(profile_ids):
    """
    Retrieve selection of household electricity demand profiles

    Parameters
    ----------
    profile_ids: list of str (str, int)
        (type)a00..(profile number) with number having exactly 4 digits


    See Also
    --------
    :func:`houseprofiles_in_census_cells`

    Returns
    -------
    pd.DataFrame
         Selection of household demand profiles
    """
    # Query load profiles
    with db.session_scope() as session:
        cells_query = session.query(
            IeeHouseholdLoadProfiles.load_in_wh, IeeHouseholdLoadProfiles.type
        ).filter(IeeHouseholdLoadProfiles.type.in_(profile_ids))

    df_profile_loads = pd.read_sql(
        cells_query.statement, cells_query.session.bind, index_col="type"
    )

    # convert array to Dataframe
    df_profile_loads = pd.DataFrame.from_records(
        df_profile_loads["load_in_wh"], index=df_profile_loads.index
    ).T

    return df_profile_loads


def mv_grid_district_HH_electricity_load(
    scenario_name, scenario_year, drop_table=False
):
    """
    Aggregated household demand time series at HV/MV substation level

    Calculate the aggregated demand time series based on the demand profiles
    of each zensus cell inside each MV grid district. Profiles are read from
    local hdf5-file.

    Parameters
    ----------
    scenario_name: str
        Scenario name identifier, i.e. "eGon2035"
    scenario_year: int
        Scenario year according to `scenario_name`
    drop_table: bool
        Toggle to True for dropping table at beginning of this function.
        Be careful, delete any data.

    Returns
    -------
    pd.DataFrame
        Multiindexed dataframe with `timestep` and `bus_id` as indexers.
        Demand is given in kWh.
    """

    def tuple_format(x):
        """Convert Profile ids from string to tuple (type, id)
        Convert from (str)a000(int) to (str), (int)
        """
        return x[:2], int(x[3:])

    with db.session_scope() as session:
        cells_query = session.query(
            HouseholdElectricityProfilesInCensusCells,
            MapZensusGridDistricts.bus_id,
        ).join(
            MapZensusGridDistricts,
            HouseholdElectricityProfilesInCensusCells.cell_id
            == MapZensusGridDistricts.zensus_population_id,
        )

    cells = pd.read_sql(
        cells_query.statement, cells_query.session.bind, index_col="cell_id"
    )

    # convert profile ids to tuple (type, id) format
    cells["cell_profile_ids"] = cells["cell_profile_ids"].apply(
        lambda x: list(map(tuple_format, x))
    )

    # Read demand profiles from egon-data-bundle
    df_iee_profiles = get_iee_hh_demand_profiles_raw()

    # Process profiles for further use
    df_iee_profiles = set_multiindex_to_profiles(df_iee_profiles)

    # Create aggregated load profile for each MV grid district
    mvgd_profiles_dict = {}
    for grid_district, data in cells.groupby("bus_id"):
        mvgd_profile = get_load_timeseries(
            df_iee_profiles=df_iee_profiles,
            df_hh_profiles_in_census_cells=data,
            cell_ids=data.index,
            year=scenario_year,
            peak_load_only=False,
        )
        mvgd_profiles_dict[grid_district] = [mvgd_profile.round(3).to_list()]
    mvgd_profiles = pd.DataFrame.from_dict(mvgd_profiles_dict, orient="index")

    # Reshape data: put MV grid ids in columns to a single index column
    mvgd_profiles = mvgd_profiles.reset_index()
    mvgd_profiles.columns = ["bus_id", "p_set"]

    # Add remaining columns
    mvgd_profiles["scn_name"] = scenario_name

    if drop_table:
        EgonEtragoElectricityHouseholds.__table__.drop(
            bind=engine, checkfirst=True
        )
    EgonEtragoElectricityHouseholds.__table__.create(
        bind=engine, checkfirst=True
    )
    # Insert data into respective database table
    mvgd_profiles.to_sql(
        name=EgonEtragoElectricityHouseholds.__table__.name,
        schema=EgonEtragoElectricityHouseholds.__table__.schema,
        con=engine,
        if_exists="append",
        method="multi",
        chunksize=10000,
        index=False,
    )


def get_scaled_profiles_from_db(
    attribute, list_of_identifiers, year, aggregate=True, peak_load_only=False
):
    """Retrieve selection of scaled household electricity demand profiles

    Parameters
    ----------
    attribute: str
        attribute to filter the table

        * nuts3
        * nuts1
        * cell_id

    list_of_identifiers: list of str/int
        nuts3/nuts1 need to be str
        cell_id need to be int

    year: int
         * 2035
         * 2050

    aggregate: bool
        If True, all profiles are summed. This uses a lot of RAM if a high
        attribute level is chosen

    peak_load_only: bool
        If True, only peak load value is returned

    Notes
    -----
    Aggregate == False option can use a lot of RAM if many profiles are selected


    Returns
    -------
    pd.Series or float
     Aggregated time series for given `cell_ids` or peak load of this time
     series in MWh.
    """
    cell_demand_metadata = get_cell_demand_metadata_from_db(
        attribute=attribute, list_of_identifiers=list_of_identifiers
    )
    profile_ids = cell_demand_metadata.cell_profile_ids.sum()

    df_iee_profiles = get_hh_profiles_from_db(profile_ids)

    scaled_profiles = get_load_timeseries(
        df_iee_profiles=df_iee_profiles,
        df_hh_profiles_in_census_cells=cell_demand_metadata,
        cell_ids=cell_demand_metadata.index.to_list(),
        year=year,
        aggregate=aggregate,
        peak_load_only=peak_load_only,
    )
    return scaled_profiles
