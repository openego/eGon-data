"""
Household electricity demand time series for scenarios in 2035 and 2050

Electricity demand data for households in Germany in 1-hourly resolution for
an entire year. Spatially, the data is resolved to 100 x 100 m cells and
provides individual and distinct time series for each household in a cell.
The cells are defined by the dataset Zensus 2011.

The resulting data is stored in two separate tables

* `demand.household_electricity_profiles_in_census_cells`:
  Lists references and scaling parameters to time series data for each household in a cell by
  identifiers. This table is fundamental for creating subsequent data like
  demand profiles on MV grid level or for determining the peak load at load
  area level. The table is created by :func:`houseprofiles_in_census_cells`.
* `demand.household_electricity_profiles_hvmv_substation`:
  Household electricity demand profiles aggregated at MV grid district level in MWh.
  Primarily used to create the eTraGo data model.
  The table is created with :func:`mv_grid_district_HH_electricity_load`.

The following datasets are used for creating the data:

* Electricity demand time series for household categories
  produced by demand profile generator (DPG) from Fraunhofer IEE
  (see :func:`download_process_household_demand_profiles_raw`)
* Spatial information about people living in households by Zensus 2011 at
  federal state level
    * Type of household (family status)
    * Age
    * Size
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

* Spatial information about people living in households by zensus (2011) at federal state NUTS1 level
 :var:`df_zensus` is aggregated to be compatible to IEE household profile specifications.
    * exclude kids and reduce to adults and seniors
    * group as defined in :var:`HH_TYPES`
    * convert data from people living in households to number of households by :var:`mapping_people_in_households`
    * calculate fraction of fine household types (10) within subgroup of rough household types (5) :var:`df_dist_households`
* Spatial information about number of households per ha :var:`df_households_typ` is mapped to NUTS1 and NUTS3 level.
  Data is enriched with refined household subgroups via :var:`df_dist_households` in :var:`df_zensus_cells`.
* Enriched 100 x 100 m household dataset is used to sample and aggregate household profiles. A table including
  individual profile id's for each cell and scaling factor to match Demand-Regio annual sum projections for 2035 and 2050
  at NUTS3 level is created in the database as `demand.household_electricity_profiles_in_census_cells`.

**What are central assumptions during the data processing?**

* Mapping zensus data to IEE household categories is not trivial. In
  conversion from persons in household to number of
  households, number of inhabitants for multi-person households is estimated
  as weighted average in :var:`OO_factor`
* The distribution to refine household types at cell level are the same for each federal state
* Refining of household types lead to float number of profiles drew at cell level and need to be rounded to nearest int.
* 100 x 100 m cells are matched to NUTS via centroid location
* Cells with households in unpopulated areas are removed

**Drawbacks and limitations of the data**

* The distribution to refine household types at cell level are the same for
  each federal state
* Household profiles aggregated annual demand matches Demand Regio demand at
  NUTS-3 level, but it is not matching the demand regio time series profile
* Due to secrecy, some census data are highly modified under certain attributes
 (quantity_q = 2). This cell data is not corrected, but excluded.
* Census data with attribute 'HHTYP_FAM' is missing for some cells with small
 amount of households. This data is generated using the average share of household types
  for cells with similar household number


Notes
-----

This module docstring is rather a dataset documentation. Once, a decision
is made in ... the content of this module docstring needs to be moved to
docs attribute of the respective dataset class.
"""
from functools import partial
from itertools import cycle, product
from pathlib import Path
import os
import random

from sqlalchemy import ARRAY, Column, Float, Integer, String
from sqlalchemy.dialects.postgresql import CHAR, INTEGER, REAL
from sqlalchemy.ext.declarative import declarative_base
import numpy as np
import pandas as pd

import egon.data.config
from egon.data import db
from egon.data.datasets import Dataset
from egon.data.datasets.zensus_mv_grid_districts import MapZensusGridDistricts
Base = declarative_base()


# Set random_seed as long as no global_seed exists (see #351).
RANDOM_SEED = 42

# Define mapping of zensus household categories to eurostat categories
# - Adults living in househould type
# - number of kids not included even if in housholdtype name
# **! The Eurostat data only gives the amount of adults/seniors, excluding the amount of kids <15**
# eurostat is used for demand-profile-generator @fraunhofer
HH_TYPES = {
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
        ("Mehrpersonenhaushalte ohne Kernfamilie", "2 Personen", "Seniors"),
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
        ("Mehrpersonenhaushalte ohne Kernfamilie", "3 Personen", "Seniors"),
        ("Mehrpersonenhaushalte ohne Kernfamilie", "4 Personen", "Seniors"),
        ("Mehrpersonenhaushalte ohne Kernfamilie", "5 Personen", "Seniors"),
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
    # OO, O1, O2 have the same amount, as no information about the share of kids within zensus data set.
    # if needed the total amount can be corrected in the hh_tools.get_hh_dist function
    # using multi_adjust=True option
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

MAPPING_ZENSUS_HH_SUBGROUPS = {
    1: ["SR", "SO"],
    2: ["PR", "PO"],
    3: ["SK"],
    4: ["P1", "P2", "P3"],
    5: ["OR", "OO"],
}


class IeeHouseholdLoadProfiles(Base):
    __tablename__ = "iee_household_load_profiles"
    __table_args__ = {"schema": "demand"}

    id = Column(INTEGER, primary_key=True)
    type = Column(CHAR(7))
    load_in_wh = Column(ARRAY(REAL))  # , dimensions=2))


class HouseholdElectricityProfilesInCensusCells(Base):
    __tablename__ = "egon_household_electricity_profile_in_census_cell"
    __table_args__ = {"schema": "demand"}

    cell_id = Column(Integer, primary_key=True)
    grid_id = Column(String)
    cell_profile_ids = Column(ARRAY(String, dimensions=2))
    nuts3 = Column(String)
    nuts1 = Column(String)
    factor_2035 = Column(Float)
    factor_2050 = Column(Float)


class EgonEtragoElectricityHouseholds(Base):
    __tablename__ = "egon_etrago_electricity_households"
    __table_args__ = {"schema": "demand"}

    version = Column(String, primary_key=True)
    subst_id = Column(Integer, primary_key=True)
    scn_name = Column(String, primary_key=True)
    p_set = Column(ARRAY(Float))
    q_set = Column(ARRAY(Float))


hh_demand_setup = partial(
    Dataset,
    name="HH Demand",
    version="0.0.1",
    dependencies=[],
    # Tasks are declared in pipeline as function is used multiple times with different args
    # To differentiate these tasks PythonOperator with specific id-names are used
    # PythonOperator needs to be declared in pipeline to be mapped to DAG
    # tasks=[],
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

    engine = db.engine()

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


def get_household_demand_profiles_raw():
    """Gets and returns household electricity demand profiles from the egon-data-bundle.

        Household electricity demand profiles generated by Fraunhofer IEE.
        Methodology is described in
        :ref:`Erzeugung zeitlich hochaufgelöster Stromlastprofile für verschiedene
        Haushaltstypen
        <https://www.researchgate.net/publication/273775902_Erzeugung_zeitlich_hochaufgeloster_Stromlastprofile_fur_verschiedene_Haushaltstypen>`_.
        It is used and further described in the following theses by:

        * Jonas Haack:
          "Auswirkungen verschiedener Haushaltslastprofile auf PV-Batterie-Systeme" (confidential)
        * Simon Ruben Drauz
          "Synthesis of a heat and electrical load profile for single and multi-family houses used for subsequent
          performance tests of a multi-component energy system",
          http://dx.doi.org/10.13140/RG.2.2.13959.14248


        Returns
        -------
        pd.DataFrame
            Table with profiles in columns and time as index. A pd.MultiIndex is
            used to distinguish load profiles from different EUROSTAT household
            types.
        """
    data_config = egon.data.config.datasets()
    pa_config = data_config['hh_demand_profiles']

    def ve(s):
        raise (ValueError(s))

    dataset = egon.data.config.settings()["egon-data"]["--dataset-boundary"]

    file_section = (
        "path" if dataset == "Everything" else "path_testmode"
        if dataset == "Schleswig-Holstein"
        else ve(f"'{dataset}' is not a valid dataset boundary.")
    )

    file_path = pa_config["sources"]["household_electricity_demand_profiles"][file_section]

    download_directory = os.path.join("data_bundle_egon_data", "household_electricity_demand_profiles")

    hh_profiles_file = (
        Path(".") / Path(download_directory) / Path(file_path).name
    )

    df_hh_profiles = pd.read_hdf(hh_profiles_file)

    return df_hh_profiles


def process_household_demand_profiles(hh_profiles):
    """Process household demand profiles in a more easy to use format.
    The profile type is splitted into type and number and set as multiindex.

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

    # Cast profile ids into int
    hh_profiles.columns = pd.MultiIndex.from_tuples(
        [(a, int(b)) for a, b in hh_profiles.columns]
    )

    return hh_profiles


def get_zensus_households_raw():
    """Get zensus age x household type data from egon-data-bundle

    Dataset about household size with information about the categories:

    * family type
    * age class
    * household size

    for Germany in spatial resolution of federal states.

    Data manually selected and retrieved from:
    https://ergebnisse2011.zensus2022.de/datenbank/online
    For reproducing data selection, please do:

    * Search for: "1000A-3016"
    * or choose topic: "Bevölkerung kompakt"
    * Choose table code: "1000A-3016" with title "Personen: Alter (11 Altersklassen) - Größe des
    privaten Haushalts - Typ des privaten Haushalts (nach Familien/Lebensform)"
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
    pa_config = data_config['hh_demand_profiles']
    file_path = pa_config["sources"]["zensus_household_types"]["path"]

    download_directory = os.path.join("data_bundle_egon_data", "zensus_households")

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
    There is missing data for specific attributes in the zensus dataset because of secrecy reasons.
    Some cells with only small amount of households are missing with the attribute HHTYP_FAM.
    However the total amount of households is known with attribute INSGESAMT.
    The missing data is generated as average share of the household types for cell groups
    with the same amount of households.

    Parameters
    ----------
    df_households_typ: pd.DataFrame
        Zensus households data
    df_missing_data: pd.DataFrame
        number of missing cells of group of amount of households
    missing_cells: dict
        dictionary with lists of grids of the missing cells grouped by amount of
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
            random_row = split.sample(random_state=RANDOM_SEED)
            split[random_row.index] = random_row + difference
        elif difference < 0:
            # subtract only from rows > 0
            split = split.round()
            random_row = split[split > 0].sample(random_state=RANDOM_SEED)
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


def get_hh_dist(df_zensus, hh_types, multi_adjust=True, relative=True):
    """
    Group zensus data to fit Demand-Profile-Generator (DPG) format.

    Parameters
    ----------
    df_zensus: pd.DataFrame
        Zensus households data
    hh_types: dict
        Mapping of zensus groups to DPG groups
    multi-adjust: bool
        If True (default), splits DPG-group 'OO' into 3 subgroups and uses
        distribution factor derived by table II in
        https://www.researchgate.net/publication/273775902_Erzeugung_zeitlich_hochaufgeloster_Stromlastprofile_fur_verschiedene_Haushaltstypen
    relative: bool
        if True produces relative values

    Returns
    ----------
    df_hh_types: pd.DataFrame
        distribution of people by household type and regional-resolution

        .. warning::

            Data still needs to be converted from amount of people to amount
            of households
    """
    # adjust multi with/without kids via eurostat share as not clearly derivable without infos about share of kids
    if multi_adjust:
        adjust = {
            "SR": 1,
            "SO": 1,
            "SK": 1,
            "PR": 1,
            "PO": 1,
            "P1": 1,
            "P2": 1,
            "P3": 1,
            "OR": 1,
            "OO": 0.703,
            "O1": 0.216,
            "O2": 0.081,
        }
    else:
        adjust = {
            "SR": 1,
            "SO": 1,
            "SK": 1,
            "PR": 1,
            "PO": 1,
            "P1": 1,
            "P2": 1,
            "P3": 1,
            "OR": 1,
            "OO": 1,
            "O1": 0,
            "O2": 0,
        }

    df_hh_types = pd.DataFrame(
        (
            {
                hhtype: adjust[hhtype] * df_zensus.loc[countries, codes].sum()
                for hhtype, codes in hh_types.items()
            }
            for countries in df_zensus.index
        ),
        index=df_zensus.index,
    )
    # drop zero columns
    df_hh_types = df_hh_types.loc[:, (df_hh_types != 0).any(axis=0)]
    if relative:
        # normalize
        df_hh_types = df_hh_types.div(df_hh_types.sum(axis=1), axis=0)
    return df_hh_types.T


def inhabitants_to_households(
    df_people_by_householdtypes_abs, mapping_people_in_households
):
    """
    Convert number of inhabitant to number of household types

    Takes the distribution of peoples living in types of households to
    calculate a distribution of household types by using a people-in-household
    mapping.

    Results are rounded to int (ceiled) to full households.

    Parameters
    ----------
    df_people_by_householdtypes_abs: pd.DataFrame
        Distribution of people living in households
    mapping_people_in_households: dict
        Mapping of people living in certain types of households

    Returns
    ----------
    df_households_by_type: pd.DataFrame
        Distribution of households type

    """
    # compare categories and remove form mapping if to many
    diff = set(df_people_by_householdtypes_abs.index) ^ set(
        mapping_people_in_households.keys()
    )

    if bool(diff):
        for key in diff:
            mapping_people_in_households = dict(mapping_people_in_households)
            del mapping_people_in_households[key]
        print(f"Removed {diff} from mapping!")

    # divide amount of people by people in household types
    df_households_by_type = df_people_by_householdtypes_abs.div(
        mapping_people_in_households, axis=0
    )
    # Number of people gets adjusted to integer values by ceiling
    # This introduces a small deviation
    df_households_by_type = df_households_by_type.apply(np.ceil)

    return df_households_by_type


def process_nuts1_zensus_data(df_zensus):
    """Make data compatible with household demand profile categories

    Groups, removes and reorders categories which are not needed to fit data to household types of
    IEE electricity demand time series generated by demand-profile-generator (DPG).

    * Kids (<15) are excluded as they are also excluded in DPG origin dataset
    * Adults (15<65)
    * Seniors (<65)

    Parameters
    ----------
    df_zensus: pd.DataFrame
        cleaned zensus household type x age category data

    Returns
    -------
    pd.DataFrame
        Aggregated zensus household data on NUTS-1 level
    """
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
        df_zensus.loc[:, (slice(None), kids)].groupby(level=0, axis=1).sum()
    )
    df_adults = (
        df_zensus.loc[:, (slice(None), adults)].groupby(level=0, axis=1).sum()
    )
    df_seniors = (
        df_zensus.loc[:, (slice(None), seniors)].groupby(level=0, axis=1).sum()
    )
    df_zensus = pd.concat(
        [df_kids, df_adults, df_seniors],
        axis=1,
        keys=["Kids", "Adults", "Seniors"],
        names=["age", "persons"],
    )

    # reduce column names to state only
    mapping_state = {
        i: i.split()[1] for i in df_zensus.index.get_level_values(level=0)
    }

    # rename index
    df_zensus = df_zensus.rename(index=mapping_state, level=0)
    # rename axis
    df_zensus = df_zensus.rename_axis(["state", "type"])
    # unstack
    df_zensus = df_zensus.unstack()
    # reorder levels
    df_zensus = df_zensus.reorder_levels(
        order=["type", "persons", "age"], axis=1
    )

    return df_zensus


def enrich_zensus_data_at_cell_level(df_zensus):
    """The zensus data is processed to define the number and type of households per zensus cell.
    Two subsets of the zensus data are merged to fit the IEE profiles specifications.
    For this, the dataset 'HHGROESS_KLASS' is converted from people living in households to number of households
    of specific size. Missing data in 'HHTYP_FAM' is substituted in :func:`create_missing_zensus_data`.

    Parameters
    ----------
    df_zensus: pd.DataFrame
        Aggregated zensus household data on NUTS-1 level

    Returns
    -------
    pd.DataFrame
        Number of hh types per census cell and scaling factors
    """

    # hh_tools.get_hh_dist without eurostat adjustment for O1-03 Groups in absolute values
    df_hh_types_nad_abs = get_hh_dist(
        df_zensus, HH_TYPES, multi_adjust=False, relative=False
    )

    # Get household size for each census cell grouped by
    # As this is only used to estimate size of households for OR, OO, 1 P and 2 P households are dropped
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
    # Determine number of persons for each household category and per federal state
    df_dist_households = inhabitants_to_households(
        df_hh_types_nad_abs, mapping_people_in_households
    )

    # Calculate fraction of fine household types within subgroup of rough household types
    for value in MAPPING_ZENSUS_HH_SUBGROUPS.values():
        df_dist_households.loc[value] = df_dist_households.loc[value].div(
            df_dist_households.loc[value].sum()
        )

    # Retrieve information about households for each census cell
    # Only use cell-data which quality (quantity_q<2) is acceptable
    df_households_typ = db.select_dataframe(
        sql="""
                    SELECT grid_id, attribute, characteristics_code, characteristics_text, quantity
                    FROM society.egon_destatis_zensus_household_per_ha
                    WHERE attribute = 'HHTYP_FAM' AND quantity_q <2"""
    )
    df_households_typ = df_households_typ.drop(
        columns=["attribute", "characteristics_text"]
    )
    df_missing_data = db.select_dataframe(
        sql="""
                SELECT count(joined.quantity_gesamt) as amount, joined.quantity_gesamt as households
                FROM(
                    SELECT t2.grid_id, quantity_gesamt, quantity_sum_fam,
                     (quantity_gesamt-(case when quantity_sum_fam isnull then 0 else quantity_sum_fam end))
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
                    SELECT t2.grid_id, (case when quantity_sum_fam isnull then quantity_gesamt end) as quantity
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

    df_average_split = create_missing_zensus_data(
        df_households_typ, df_missing_data, missing_cells
    )

    df_households_typ = df_households_typ.rename(
        columns={"quantity": "hh_5types"}
    )

    df_households_typ = pd.concat(
        [df_households_typ, df_average_split], ignore_index=True
    )

    # Census cells with nuts3 and nuts1 information
    df_grid_id = db.select_dataframe(
        sql="""
                            SELECT pop.grid_id, pop.gid as cell_id, vg250.vg250_nuts3 as nuts3, lan.nuts as nuts1, lan.gen
                            FROM society.destatis_zensus_population_per_ha_inside_germany as pop
                            LEFT JOIN boundaries.egon_map_zensus_vg250 as vg250
                            ON (pop.gid=vg250.zensus_population_id)
                            LEFT JOIN boundaries.vg250_lan as lan
                            ON (LEFT(vg250.vg250_nuts3, 3)=lan.nuts)
                            WHERE lan.gf = 4 """
    )
    df_grid_id = df_grid_id.drop_duplicates()
    df_grid_id = df_grid_id.reset_index(drop=True)

    # Merge household type and size data with considered (populated) census cells
    # how='inner' is used as ids of unpopulated areas are removed df_grid_id or earliers tables. see here:
    # https://github.com/openego/eGon-data/blob/59195926e41c8bd6d1ca8426957b97f33ef27bcc/src/egon/data/importing/zensus/__init__.py#L418-L449
    df_households_typ = pd.merge(
        df_households_typ,
        df_grid_id,
        left_on="grid_id",
        right_on="grid_id",
        how="inner",
    )

    # Merge Zensus nuts1 level household data with zensus cell level 100 x 100 m
    # by refining hh-groups with MAPPING_ZENSUS_HH_SUBGROUPS
    df_zensus_cells = pd.DataFrame()
    for (country, code), df_country_type in df_households_typ.groupby(
        ["gen", "characteristics_code"]
    ):

        # iterate over zenus_country subgroups
        for typ in MAPPING_ZENSUS_HH_SUBGROUPS[code]:
            df_country_type["hh_type"] = typ
            df_country_type["factor"] = df_dist_households.loc[typ, country]
            df_country_type["hh_10types"] = (
                df_country_type["hh_5types"]
                * df_dist_households.loc[typ, country]
            )
            df_zensus_cells = df_zensus_cells.append(
                df_country_type, ignore_index=True
            )

    df_zensus_cells = df_zensus_cells.sort_values(
        by=["grid_id", "characteristics_code"]
    ).reset_index(drop=True)

    return df_zensus_cells


def get_cell_demand_profile_ids(df_cell, pool_size):
    """
    Generates tuple of hh_type and zensus cell ids

    Takes a random sample of profile ids for given cell:
      * if pool size >= sample size: without replacement
      * if pool size < sample size: with replacement
    The number of households are rounded to the nearest integer if float.

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
    # np.random.default_rng().integers(low=0, high=pool_size[hh_type], size=sq) instead of random.sample
    # use random.choices() if with replacement
    # list of sample ids per hh_type in cell
    random.seed(RANDOM_SEED)
    cell_profile_ids = [
        (hh_type, random.sample(range(pool_size[hh_type]), k=sq))
        if pool_size[hh_type] >= sq
        else (hh_type, random.choices(range(pool_size[hh_type]), k=sq))
        for hh_type, sq in zip(
            df_cell["hh_type"],
            np.rint(df_cell["hh_10types"].values).astype(int),
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
def get_cell_demand_metadata(df_zensus_cells, df_profiles):
    """
    Defines information about profiles for each zensus cell

    A table including the demand profile ids for each cell is created by using
    :func:`get_cell_demand_profile_ids`. Household profiles are randomly sampled for each cell. The profiles
    are not replaced to the pool within a cell but after. The number of households are rounded to the nearest integer
    if float. This results in a small deviation for the course of the aggregated profiles.

    Parameters
    ----------
    df_zensus_cells: pd.DataFrame
        Household type parameters. Each row representing one household. Hence,
        multiple rows per zensus cell.
    df_profiles: pd.DataFrame
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

    df_cell_demand_metadata = pd.DataFrame(
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
    # 'peak_loads_hh', 'peak_load_cell',
    df_cell_demand_metadata = df_cell_demand_metadata.rename_axis("grid_id")

    pool_size = df_profiles.groupby(level=0, axis=1).size()

    for grid_id, df_cell in df_zensus_cells.groupby(by="grid_id"):

        # random sampling of household profiles for each cell
        # with or without replacement (see :func:`get_cell_demand_profile_ids`)
        # within cell but after number of households are rounded to the nearest
        # integer if float this results in a small deviation for the course of
        # the aggregated profiles.
        cell_profile_ids = get_cell_demand_profile_ids(df_cell, pool_size)

        df_cell_demand_metadata.at[grid_id, "cell_id"] = df_cell.loc[
            :, "cell_id"
        ].unique()[0]
        df_cell_demand_metadata.at[
            grid_id, "cell_profile_ids"
        ] = cell_profile_ids
        df_cell_demand_metadata.at[grid_id, "nuts3"] = df_cell.loc[
            :, "nuts3"
        ].unique()[0]
        df_cell_demand_metadata.at[grid_id, "nuts1"] = df_cell.loc[
            :, "nuts1"
        ].unique()[0]

    return df_cell_demand_metadata


# can be parallelized with grouping df_zensus_cells by grid_id/nuts3/nuts1
def adjust_to_demand_regio_nuts3_annual(
    df_cell_demand_metadata, df_profiles, df_demand_regio
):
    """
    Computes the profile scaling factor for alignment to demand regio data

    The scaling factor can be used to re-scale each load profile such that the
    sum of all load profiles within one NUTS-3 area equals the annual demand
    of demand regio data.

    Parameters
    ----------
    df_cell_demand_metadata: pd.DataFrame
        Result of :func:`get_cell_demand_metadata`.
    df_profiles: pd.DataFrame
        Household load profile data

        * Index: Times steps as serial integers
        * Columns: pd.MultiIndex with (`HH_TYPE`, `id`)

    df_demand_regio: pd.DataFrame
        Annual demand by demand regio for each NUTS-3 region and scenario year.
        Index is pd.MultiIndex with :code:`tuple(scenario_year, nuts3_code)`.

    Returns
    -------
    pd.DataFrame
        Returns the same data as :func:`get_cell_demand_metadata`, but with
        filled columns `factor_2035` and `factor_2050`.
    """
    for nuts3_id, df_nuts3 in df_cell_demand_metadata.groupby(by="nuts3"):
        nuts3_cell_ids = df_nuts3.index
        nuts3_profile_ids = df_nuts3.loc[:, "cell_profile_ids"].sum()

        # take all profiles of one nuts3, aggregate and sum
        # profiles in Wh
        nuts3_profiles_sum_annual = (
            df_profiles.loc[:, nuts3_profile_ids].sum().sum()
        )

        # Scaling Factor
        # ##############
        # demand regio in MWh
        # profiles in Wh
        df_cell_demand_metadata.loc[nuts3_cell_ids, "factor_2035"] = (
            df_demand_regio.loc[(2035, nuts3_id), "demand_mwha"]
            * 1e3
            / (nuts3_profiles_sum_annual / 1e3)
        )
        df_cell_demand_metadata.loc[nuts3_cell_ids, "factor_2050"] = (
            df_demand_regio.loc[(2050, nuts3_id), "demand_mwha"]
            * 1e3
            / (nuts3_profiles_sum_annual / 1e3)
        )

    return df_cell_demand_metadata


def get_load_timeseries(
    df_profiles, df_cell_demand_metadata, cell_ids, year, peak_load_only=False
):
    """
    Get peak load for one load area in MWh

    The peak load is calculated in aggregated manner for a group of zensus
    cells that belong to one load area (defined by `cell_ids`).

    Parameters
    ----------
    df_profiles: pd.DataFrame
        Household load profile data in Wh

        * Index: Times steps as serial integers
        * Columns: pd.MultiIndex with (`HH_TYPE`, `id`)

        Used to calculate the peak load from.
    df_cell_demand_metadata: pd.DataFrame
        Return value of :func:`adjust_to_demand_regio_nuts3_annual`.
    cell_ids: list
        Zensus cell ids that define one group of zensus cells that belong to
        the same load area.
    year: int
        Scenario year. Is used to consider the scaling factor for aligning
        annual demand to NUTS-3 data.
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
    timesteps = len(df_profiles)
    full_load = pd.Series(
        data=np.zeros(timesteps), dtype=np.float64, index=range(timesteps)
    )
    load_area_meta = df_cell_demand_metadata.loc[
        cell_ids, ["cell_profile_ids", "nuts3", f"factor_{year}"]
    ]
    # loop over nuts3 (part_load) and sum (full_load) as the scaling factor applies at nuts3 level
    for (nuts3, factor), df in load_area_meta.groupby(
        by=["nuts3", f"factor_{year}"]
    ):
        part_load = (
            df_profiles.loc[:, df["cell_profile_ids"].sum()].sum(axis=1)
            * factor
            / 1e6
        )  # from Wh to MWh
        full_load = full_load.add(part_load)
    if peak_load_only:
        full_load = full_load.max()
    return full_load


def houseprofiles_in_census_cells():
    """
    Identify household electricity profiles for each census cell

    Creates a table that maps household electricity demand profiles to zensus
    cells. Each row represents one cell and contains a list of profile IDs.

    Use :func:`get_houseprofiles_in_census_cells` to retrieve the data from
    the database as pandas

    """
    # Read demand profiles from egon-data-bundle
    df_profiles = get_household_demand_profiles_raw()

    # Write raw profiles into db
    write_hh_profiles_to_db(df_profiles)

    # Process profiles for further use
    df_profiles = process_household_demand_profiles(df_profiles)

    # Download zensus household type x age category data
    df_households_raw = get_zensus_households_raw()

    # Clean data
    df_households = df_households_raw.applymap(clean).applymap(int)

    # Make data compatible with household demand profile categories
    # Use less age interval and aggregate data to NUTS-1 level
    df_zensus_nuts1 = process_nuts1_zensus_data(df_households)

    # Enrich census cell data with nuts1 level attributes
    df_zensus_cells = enrich_zensus_data_at_cell_level(df_zensus_nuts1)

    # Annual household electricity demand on NUTS-3 level (demand regio)
    df_demand_regio = db.select_dataframe(
        sql="""
                                SELECT year, nuts3, SUM (demand) as demand_mWha
                                FROM demand.egon_demandregio_hh as egon_d
                                GROUP BY nuts3, year
                                ORDER BY year""",
        index_col=["year", "nuts3"],
    )

    # Finally create table that stores profile ids for each cell
    df_cell_demand_metadata = get_cell_demand_metadata(
        df_zensus_cells, df_profiles
    )
    df_cell_demand_metadata = adjust_to_demand_regio_nuts3_annual(
        df_cell_demand_metadata, df_profiles, df_demand_regio
    )
    df_cell_demand_metadata = df_cell_demand_metadata.reset_index(drop=False)

    # Insert Zensus-cell-profile metadata-table into respective database table
    engine = db.engine()
    HouseholdElectricityProfilesInCensusCells.__table__.drop(
        bind=engine, checkfirst=True
    )
    HouseholdElectricityProfilesInCensusCells.__table__.create(
        bind=engine, checkfirst=True
    )
    df_cell_demand_metadata["cell_id"] = df_cell_demand_metadata[
        "cell_id"
    ].astype(int)
    with db.session_scope() as session:
        session.bulk_insert_mappings(
            HouseholdElectricityProfilesInCensusCells,
            df_cell_demand_metadata.to_dict(orient="records"),
        )


def get_houseprofiles_in_census_cells():
    """
    Retrieve household electricity demand profile mapping

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

    census_profile_mapping["cell_profile_ids"] = census_profile_mapping[
        "cell_profile_ids"
    ].apply(lambda x: [(cat, int(profile_id)) for cat, profile_id in x])

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
    attribute_options = ['nuts3', 'nuts1', 'cell_id']
    if attribute not in attribute_options:
        raise ValueError(f'attribute has to be one of: {attribute_options}')

    # Query profile ids and scaling factors for specific attributes
    with db.session_scope() as session:
        if attribute == 'nuts3':
            cells_query = session.query(
                HouseholdElectricityProfilesInCensusCells.cell_id,
                HouseholdElectricityProfilesInCensusCells.cell_profile_ids,
                HouseholdElectricityProfilesInCensusCells.nuts3,
                HouseholdElectricityProfilesInCensusCells.nuts1,
                HouseholdElectricityProfilesInCensusCells.factor_2035,
                HouseholdElectricityProfilesInCensusCells.factor_2050). \
                filter(HouseholdElectricityProfilesInCensusCells.nuts3.in_(list_of_identifiers))
        elif attribute == 'nuts1':
            cells_query = session.query(
                HouseholdElectricityProfilesInCensusCells.cell_id,
                HouseholdElectricityProfilesInCensusCells.cell_profile_ids,
                HouseholdElectricityProfilesInCensusCells.nuts3,
                HouseholdElectricityProfilesInCensusCells.nuts1,
                HouseholdElectricityProfilesInCensusCells.factor_2035,
                HouseholdElectricityProfilesInCensusCells.factor_2050). \
                filter(HouseholdElectricityProfilesInCensusCells.nuts1.in_(list_of_identifiers))
        elif attribute == 'cell_id':
            cells_query = session.query(
                HouseholdElectricityProfilesInCensusCells.cell_id,
                HouseholdElectricityProfilesInCensusCells.cell_profile_ids,
                HouseholdElectricityProfilesInCensusCells.nuts3,
                HouseholdElectricityProfilesInCensusCells.nuts1,
                HouseholdElectricityProfilesInCensusCells.factor_2035,
                HouseholdElectricityProfilesInCensusCells.factor_2050). \
                filter(HouseholdElectricityProfilesInCensusCells.cell_id.in_(list_of_identifiers))

    cell_demand_metadata = pd.read_sql(
        cells_query.statement, cells_query.session.bind, index_col='cell_id')
    cell_demand_metadata["cell_profile_ids"] = cell_demand_metadata["cell_profile_ids"].apply(
        lambda x: [(cat, int(profile_id)) for cat, profile_id in x]
    )
    return cell_demand_metadata


def get_hh_profiles_from_db(profile_ids):
    """
    Retrieve selection of household electricity demand profiles

    Parameters
    ----------
    profile_ids: list of tuple (str, int)
        tuple consists of (category, profile number)

    See Also
    --------
    :func:`houseprofiles_in_census_cells`

    Returns
    -------
    pd.DataFrame
         Selection of household demand profiles
    """
    def gen_profile_names(n):
        """Join from Format (str),(int) to (str)a000(int)"""
        a = f"{n[0]}a{int(n[1]):04d}"
        return a

    # Format profile ids to query
    profile_ids = list(map(gen_profile_names, profile_ids))

    # Query load profiles
    with db.session_scope() as session:
        cells_query = session.query(
            IeeHouseholdLoadProfiles.load_in_wh,
            IeeHouseholdLoadProfiles.type). \
            filter(IeeHouseholdLoadProfiles.type.in_(profile_ids))

    df_profile_loads = pd.read_sql(
        cells_query.statement, cells_query.session.bind, index_col="type")

    df_profile_loads = pd.DataFrame.from_records(df_profile_loads['load_in_wh'],
                                                 index=df_profile_loads.index).T

    return df_profile_loads


def get_scaled_profiles_from_db(attribute, list_of_identifiers, year):
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

       See Also
       --------
       :func:`houseprofiles_in_census_cells`

       Returns
       -------
       pd.DataFrame
           Selection of scaled household electricity demand profiles
       """
    cell_demand_metadata = get_cell_demand_metadata_from_db(attribute=attribute,
                                                            list_of_identifiers=list_of_identifiers)
    profile_ids = cell_demand_metadata.cell_profile_ids.sum()

    df_profiles = get_hh_profiles_from_db(profile_ids)
    df_profiles = process_household_demand_profiles(df_profiles)

    df_scaled_profiles = get_load_timeseries(df_profiles=df_profiles,
                                             df_cell_demand_metadata=cell_demand_metadata,
                                             cell_ids=cell_demand_metadata.index.to_list(),
                                             year=year)
    return df_scaled_profiles


def mv_grid_district_HH_electricity_load(
    scenario_name, scenario_year, version, drop_table=False
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
    version: str
        Version identifier
    drop_table: bool
        Toggle to True for dropping table at beginning of this function.
        Be careful, delete any data.

    Returns
    -------
    pd.DataFrame
        Multiindexed dataframe with `timestep` and `subst_id` as indexers.
        Demand is given in kWh.
    """
    engine = db.engine()

    with db.session_scope() as session:
        cells_query = session.query(
            HouseholdElectricityProfilesInCensusCells,
            MapZensusGridDistricts.subst_id,
        ).join(
            MapZensusGridDistricts,
            HouseholdElectricityProfilesInCensusCells.cell_id
            == MapZensusGridDistricts.zensus_population_id,
        )

    cells = pd.read_sql(
        cells_query.statement, cells_query.session.bind, index_col="cell_id"
    )
    cells["cell_profile_ids"] = cells["cell_profile_ids"].apply(
        lambda x: [(cat, int(profile_id)) for cat, profile_id in x]
    )

    # Read demand profiles from egon-data-bundle
    df_profiles = get_household_demand_profiles_raw()

    # Process profiles for further use
    df_profiles = process_household_demand_profiles(df_profiles)

    # Create aggregated load profile for each MV grid district
    mvgd_profiles_dict = {}
    for grid_district, data in cells.groupby("subst_id"):
        mvgd_profile = get_load_timeseries(
            df_profiles=df_profiles,
            df_cell_demand_metadata=data,
            cell_ids=data.index,
            year=scenario_year,
            peak_load_only=False,
        )
        mvgd_profiles_dict[grid_district] = [
            mvgd_profile.round(3).to_list()
        ]
    mvgd_profiles = pd.DataFrame.from_dict(mvgd_profiles_dict, orient="index")

    # Reshape data: put MV grid ids in columns to a single index column
    mvgd_profiles = mvgd_profiles.reset_index()
    mvgd_profiles.columns = ["subst_id", "p_set"]

    # Add remaining columns
    mvgd_profiles["version"] = version
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
