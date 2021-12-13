# -*- coding: utf-8 -*-

# This script is part of eGon-data.

# license text - to be added.

"""
Central module containing all code creating with district heating areas.

This module obtains the information from the census tables and the heat demand
densities, demarcates so the current and future district heating areas. In the
end it saves them in the database.
"""
import os
from egon.data import db
from egon.data.datasets.scenario_parameters import (
    get_sector_parameters,
    EgonScenario,
)

import pandas as pd
import geopandas as gpd
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.polygon import Polygon
from matplotlib import pyplot as plt
from egon.data.datasets.district_heating_areas.plot import (
    plot_heat_density_sorted,
)

# for metadata creation
import time
import datetime
from egon.data.metadata import (
    context,
    meta_metadata,
    license_ccby,
    sources,
    generate_resource_fields_from_sqla_model,
)
import json

# import time

# packages for ORM class definition
from sqlalchemy import Column, String, Integer, Sequence, Float, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2.types import Geometry

from egon.data.datasets import Dataset

# class for airflow task management (and version control)
class DistrictHeatingAreas(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="district-heating-areas",
            # version=self.target_files + "_0.0",
            version="0.0.2",  # maybe rethink the naming
            dependencies=dependencies,
            tasks=(create_tables, demarcation),
        )


Base = declarative_base()
# definition of classes for saving data in the database
class MapZensusDistrictHeatingAreas(Base):
    __tablename__ = "egon_map_zensus_district_heating_areas"
    __table_args__ = {"schema": "demand"}
    id = Column(
        Integer,
        Sequence("map_zensus_district_heating_areas_seq", schema="demand"),
        server_default=Sequence(
            "map_zensus_district_heating_areas_seq", schema="demand"
        ).next_value(),
        primary_key=True,
    )
    area_id = Column(Integer)
    scenario = Column(String, ForeignKey(EgonScenario.name))
    zensus_population_id = Column(Integer)


class EgonDistrictHeatingAreas(Base):
    __tablename__ = "egon_district_heating_areas"
    __table_args__ = {"schema": "demand"}
    id = Column(
        Integer,
        Sequence("district_heating_areas_seq", schema="demand"),
        server_default=Sequence(
            "district_heating_areas_seq", schema="demand"
        ).next_value(),
        primary_key=True,
    )
    area_id = Column(Integer)
    scenario = Column(String, ForeignKey(EgonScenario.name))
    geom_polygon = Column(Geometry("MULTIPOLYGON", 3035))
    residential_and_service_demand = Column(Float)


def create_tables():
    """Create tables for district heating areas

    Returns
    -------
        None
    """

    # Create schema
    db.execute_sql("CREATE SCHEMA IF NOT EXISTS demand;")

    # Drop tables
    db.execute_sql(
        """DROP TABLE IF EXISTS
            demand.egon_district_heating_areas CASCADE;"""
    )

    db.execute_sql(
        """DROP TABLE IF EXISTS
            demand.egon_map_zensus_district_heating_areas CASCADE;"""
    )

    db.execute_sql(
        """DROP TABLE IF EXISTS
            demand.district_heating_areas CASCADE;"""
    )

    db.execute_sql(
        """DROP TABLE IF EXISTS
            demand.map_zensus_district_heating_areas CASCADE;"""
    )

    # Drop sequences
    db.execute_sql(
        """DROP SEQUENCE IF EXISTS
            demand.district_heating_areas_seq CASCADE;"""
    )

    db.execute_sql(
        """DROP SEQUENCE IF EXISTS
            demand.egon_map_zensus_district_heating_areas_seq CASCADE;"""
    )

    engine = db.engine()
    EgonDistrictHeatingAreas.__table__.create(bind=engine, checkfirst=True)
    MapZensusDistrictHeatingAreas.__table__.create(
        bind=engine, checkfirst=True
    )


# Methods used are explained here:
# https://geopandas.org/docs/user_guide/geometric_manipulations.html


def load_census_data():
    """
    Load the heating type information from the census database table.

    The census apartment and the census building table contains information
    about the heating type. The information are loaded from the apartment
    table, because they might be more useful when it comes to the estimation of
    the connection rates. Only cells with a connection rate equal to or larger
    than 30% (based on the census apartment data) are included in the returned
    district_heat GeoDataFrame.

    Parameters
    ----------
        None

    Returns
    -------
    district_heat: geopandas.geodataframe.GeoDataFrame
        polygons (hectare cells) with district heat information

    heating_type: geopandas.geodataframe.GeoDataFrame
        polygons (hectare cells) with the number of flats having heating
        type information

    Notes
    -----
        The census contains only information on residential buildings.
        Therefore, also connection rate of the residential buildings can be
        estimated.

    TODO
    ----
        - ONLY load cells with flats.quantity_q <2
        - remove heating_type return, if not needed
        - store less columns in the district_heat (pop.geom_point),
            drop characteristics_text after use

    """

    # only census cells where egon-data has a heat demand are considered

    district_heat = db.select_geodataframe(
        """SELECT flats.zensus_population_id, flats.characteristics_text,
        flats.quantity, flats.quantity_q, pop.geom_point,
        pop.geom AS geom_polygon
        FROM society.egon_destatis_zensus_apartment_per_ha AS flats
        JOIN society.destatis_zensus_population_per_ha AS pop
        ON flats.zensus_population_id = pop.id
        AND flats.characteristics_text = 'Fernheizung (Fernwärme)'
        AND flats.zensus_population_id IN
        (SELECT zensus_population_id FROM demand.egon_peta_heat);""",
        index_col="zensus_population_id",
        geom_col="geom_polygon",
    )

    heating_type = db.select_geodataframe(
        """SELECT flats.zensus_population_id,
        SUM(flats.quantity) AS quantity, pop.geom AS geom_polygon
        FROM society.egon_destatis_zensus_apartment_per_ha AS flats
        JOIN society.destatis_zensus_population_per_ha AS pop
        ON flats.zensus_population_id = pop.id
        AND flats.attribute = 'HEIZTYP'
        AND flats.zensus_population_id IN
        (SELECT zensus_population_id FROM demand.egon_peta_heat)
        GROUP BY flats.zensus_population_id, pop.geom;""",
        index_col="zensus_population_id",
        geom_col="geom_polygon",
    )

    # district_heat.to_file(results_path+"dh.shp")
    # heating_type.to_file(results_path+"heating.shp")

    # calculate the connection rate for all census cells with DH
    # adding it to the district_heat geodataframe
    district_heat["connection_rate"] = district_heat["quantity"].div(
        heating_type["quantity"]
    )[district_heat.index]
    # district_heat.head
    # district_heat['connection_rate'].describe()

    district_heat = district_heat[district_heat["connection_rate"] >= 0.3]
    # district_heat.columns

    return district_heat, heating_type


def load_heat_demands(scenario_name):
    """
    Load scenario specific heat demand data from the local database.

    Parameters
    ----------
    scenario_name: str
        name of the scenario studied

    Returns
    -------
    heat_demand: geopandas.geodataframe.GeoDataFrame
        polygons (hectare cells) with heat demand data

    """

    # load the total heat demand (residential plus service sector)
    heat_demand = db.select_geodataframe(
        f"""SELECT demand.zensus_population_id,
        SUM(demand.demand) AS residential_and_service_demand,
        pop.geom AS geom_polygon
        FROM demand.egon_peta_heat AS demand
        JOIN society.destatis_zensus_population_per_ha AS pop
        ON demand.zensus_population_id = pop.id
        AND demand.scenario = '{scenario_name}'
        GROUP BY demand.zensus_population_id, pop.geom;""",
        index_col="zensus_population_id",
        geom_col="geom_polygon",
    )

    return heat_demand


def select_high_heat_demands(heat_demand):
    """
    Take heat demand cells and select cells with higher heat demand.

    Those can be used to identify prospective district heating supply areas.

    Parameters
    ----------
    heat_demand: geopandas.geodataframe.GeoDataFrame
        dataset of heat demand cells.

    Returns
    -------
    high_heat_demand: geopandas.geodataframe.GeoDataFrame
             polygons (hectare cells) with heat demands high enough to be
             potentially high enough to be in a district heating area
    """

    # starting point are 100 or 200 GJ/ (ha a), converted into MWh
    minimum_demand = 100 / 3.6

    high_heat_demand = heat_demand[
        heat_demand["residential_and_service_demand"] > minimum_demand
    ]
    # high_heat_demand.head()
    # print(high_heat_demand.area) # all cells are 10,000 m²

    return high_heat_demand


def area_grouping(
    raw_polygons,
    distance=200,
    minimum_total_demand=None,
    maximum_total_demand=None,
):
    """
    Group polygons which are close to each other.

    This function creates buffers around the given cell polygons (called
    "raw_polygons") and unions the intersecting buffer polygons. Afterwards, it
    unions the cell polygons which are within one unified buffer polygon.
    If requested, the cells being in areas fulfilling the minimum heat demand
    criterium are selected.

    Parameters
    ----------
    raw_polygons: geopandas.geodataframe.GeoDataFrame
        polygons to be grouped.

    distance: integer
        distance for buffering

    minimum_total_demand: integer
        optional minimum total heat demand to achieve a minimum size of areas

    maximal_total_demand: integer
        optional maximal total heat demand per area, if demand is higher the
        area is cut at nuts3 borders


    Returns
    -------
    join: geopandas.geodataframe.GeoDataFrame
        cell polygons with area id

    Notes
    -----
        None

    TODO
    ----


    """

    buffer_distance = distance + 1
    cell_buffers = raw_polygons.copy()
    cell_buffers["geom_polygon"] = cell_buffers["geom_polygon"].buffer(
        buffer_distance
    )
    # print(cell_buffers.area)

    # create a shapely Multipolygon which is split into a list
    buffer_polygons = list(cell_buffers["geom_polygon"].unary_union)

    # change the data type into geopandas geodataframe
    buffer_polygons_gdf = gpd.GeoDataFrame(geometry=buffer_polygons, crs=3035)
    # buffer_polygons_gdf.plot()

    # Join studied cells with buffer polygons
    columnname = "area_id"
    join = gpd.sjoin(
        raw_polygons, buffer_polygons_gdf, how="inner", op="intersects"
    )

    join = join.rename({"index_right": columnname}, axis=1)
    # join.plot(column=columnname)

    # minimum total heat demand for the areas with minimum criterium
    if (
        minimum_total_demand is not None
        and "residential_and_service_demand" in raw_polygons.columns
    ):
        # total_heat_demand = join.dissolve('area_id', aggfunc='sum')
        # type(large_areas)
        # filtered = join.groupby(['area_id'])[
        #     'residential_and_service_demand'].agg('sum') > 0.7
        large_areas = gpd.GeoDataFrame(
            join.groupby(["area_id"])["residential_and_service_demand"].agg(
                "sum"
            )
        )
        # large_areas = large_areas[large_areas[
        #     'residential_and_service_demand'] > minimum_total_demand]
        large_areas = (
            large_areas["residential_and_service_demand"]
            > minimum_total_demand
        )
        join = join[join.area_id.isin(large_areas[large_areas].index)]

    elif (
        minimum_total_demand is not None
        and "residential_and_service_demand" not in raw_polygons.columns
    ):
        print(
            """The minimum total heat demand criterium can only be applied
              on geodataframe having a column named
              'residential_and_service_demand' """
        )

    if (
        maximum_total_demand
        and "residential_and_service_demand" in join.columns
    ):

        huge_areas_index = (
            join.groupby("area_id").residential_and_service_demand.sum()
            > maximum_total_demand
        )

        cells_in_huge_areas = join[
            join.area_id.isin(huge_areas_index[huge_areas_index].index)
        ]

        nuts3_boundaries = db.select_geodataframe(
            """
            SELECT gen, geometry as geom FROM boundaries.vg250_krs
            """
        )
        join_2 = gpd.sjoin(
            cells_in_huge_areas, nuts3_boundaries, how="inner", op="intersects"
        )

        join = join.drop(cells_in_huge_areas.index)

        max_area_id = join.area_id.max()

        join_2["area_id"] = join_2.index_right + max_area_id + 1

        join = join.append(
            join_2[
                ["residential_and_service_demand", "geom_polygon", "area_id"]
            ]
        )

    return join


def district_heating_areas(scenario_name, plotting=False):
    """
    Create scenario specific district heating areas considering on census data.

    This function loads the district heating share from the scenario table and
    demarcate the scenario specific district heating areas. To do so it
    uses the census data on flats currently supplied with district heat, which
    are supplied selected first, if the estimated connection rate >= 30%.

    All scenarios use the Prospective Supply Districts (PSDs) made for the
    eGon2035 scenario to identify the areas where additional district heating
    supply is feasible. One PSD dataset is to defined which is constant over
    the years to allow comparisons. Moreover, it is
    assumed that the eGon2035 PSD dataset is suitable, even though the heat
    demands will continue to decrease from 2035 to 2050, because district
    heating systems will be to planned and built before 2050, to exist in 2050.

    It is assumed that the connection rate in cells with district heating will
    be a 100%. That is because later in project the number of buildings per
    cell will be used and connection rates not being 0 or 100% will create
    buildings which are not fully supplied by one technology.

    The cell polygons which carry information (like heat demand etc.) are
    grouped into areas which are close to each other.
    Only cells with a minimum heat demand density (e.g. >100 GJ/(ha a)) are
    considered when creating PSDs. Therefore, the select_high_heat_demands()
    function is used. There is minimum heat demand per PSDs to achieve a
    certain size.
    While the grouping buffer for the creation of Prospective Supply Districts
    (PSDs) is 200m as in the sEEnergies project, the buffer for grouping census
    data cell with an estimated connection rate >= 30% is 500m.
    The 500m buffer is also used when the resulting district heating areas are
    grouped, because they are built upon the existing district heating systems.

    To reduce the final number of district heating areas having the size of
    only one hectare, the minimum heat demand critrium is also applied when
    grouping the cells with census data on district heat.

    To avoid huge district heating areas, as they appear in the Ruhr area,
    district heating areas with an annual demand > 4,000,000 MWh are split
    by nuts3 boundaries. This as set as maximum_total_demand of the
    area_grouping function.


    Parameters
    ----------
    scenario_name: str
        name of scenario to be studies

    plotting: boolean
        if True, figure showing the heat demand density curve will be created


    Returns
    -------
        None

    Notes
    -----
        None

    TODO
    ----
        Do "area_grouping(load_census_data()[0])" only once, not for all
        scenarios.

        Check the applied buffer distances, find a justification for the
        documentation

    """

    # Load district heating shares from the scenario table
    if scenario_name == "eGon2015":
        district_heating_share = 0.08
    else:
        heat_parameters = get_sector_parameters("heat", scenario=scenario_name)

        district_heating_share = heat_parameters["DE_district_heating_share"]

    # heat_demand is scenario specific
    heat_demand_cells = load_heat_demands(scenario_name)

    # Firstly, supply the cells which already have district heating according
    # to 2011 Census data and which are within likely dh areas (created
    # by the area grouping function), load only the first returned result: [0]
    min_hd_census = 10000 / 3.6  # in MWh

    census_plus_heat_demand = load_census_data()[0].copy()
    census_plus_heat_demand[
        "residential_and_service_demand"
    ] = heat_demand_cells.loc[
        census_plus_heat_demand.index.values, "residential_and_service_demand"
    ]

    cells = area_grouping(
        census_plus_heat_demand,
        distance=500,
        minimum_total_demand=min_hd_census,
    )
    # cells.groupby("area_id").size().sort_values()

    total_district_heat = (
        heat_demand_cells["residential_and_service_demand"].sum()
        * district_heating_share
    )

    diff = total_district_heat - cells["residential_and_service_demand"].sum()

    assert (
        diff > 0
    ), """The chosen district heating share in combination with the heat
        demand reduction leads to an amount of district heat which is
        lower than the current one. This case is not implemented yet."""

    # Secondly, supply the cells with the highest heat demand not having
    # district heating yet
    # ASSUMPTION HERE: 2035 HD defined the PSDs
    min_hd = 10000 / 3.6
    PSDs = area_grouping(
        select_high_heat_demands(load_heat_demands("eGon2035")),
        distance=200,
        minimum_total_demand=min_hd,
    )

    # PSDs.groupby("area_id").size().sort_values()

    # select all cells not already suppied with district heat
    new_areas = heat_demand_cells[~heat_demand_cells.index.isin(cells.index)]
    # sort by heat demand density
    new_areas = new_areas[new_areas.index.isin(PSDs.index)].sort_values(
        "residential_and_service_demand", ascending=False
    )
    new_areas[
        "Cumulative_Sum"
    ] = new_areas.residential_and_service_demand.cumsum()
    # select cells to be supplied with district heating until district
    # heating share is reached
    new_areas = new_areas[new_areas["Cumulative_Sum"] <= diff]

    print(
        f"""Minimum heat demand density for cells with new district heat
          supply in scenario {scenario_name} is
          {new_areas.residential_and_service_demand.tail(1).values[0]}
          MWh / (ha a)."""
    )
    print(
        f"""Number of cells with new district heat supply in scenario
          {scenario_name} is {len(new_areas)}."""
    )

    # check = gpd.GeoDataFrame(
    #     cells[['residential_and_service_demand', 'geom_polygon']].append(
    #         new_areas[['residential_and_service_demand', 'geom_polygon']]),
    #     geometry='geom_polygon')

    # group the resulting scenario specific district heating areas
    scenario_dh_area = area_grouping(
        gpd.GeoDataFrame(
            cells[["residential_and_service_demand", "geom_polygon"]].append(
                new_areas[["residential_and_service_demand", "geom_polygon"]]
            ),
            geometry="geom_polygon",
        ),
        distance=500,
        maximum_total_demand=4e6,
    )
    # scenario_dh_area.plot(column = "area_id")

    scenario_dh_area.groupby("area_id").size().sort_values()
    scenario_dh_area.residential_and_service_demand.sum()
    # scenario_dh_area.sort_index()
    # cells[cells.index==1416974]

    # store the results in the database
    scenario_dh_area["scenario"] = scenario_name

    db.execute_sql(
        f"""DELETE FROM demand.egon_map_zensus_district_heating_areas
                   WHERE scenario = '{scenario_name}'"""
    )
    scenario_dh_area[["scenario", "area_id"]].to_sql(
        "egon_map_zensus_district_heating_areas",
        schema="demand",
        con=db.engine(),
        if_exists="append",
    )

    # Create polygons around the grouped cells and store them in the database
    # join.dissolve(columnname).convex_hull.plot() # without holes, too big
    areas_dissolved = scenario_dh_area.dissolve("area_id", aggfunc="sum")
    areas_dissolved["scenario"] = scenario_name

    areas_dissolved["geom_polygon"] = [
        MultiPolygon([feature]) if type(feature) == Polygon else feature
        for feature in areas_dissolved["geom_polygon"]
    ]
    # type(areas_dissolved["geom"][0])
    # print(type(areas_dissolved))
    # print(areas_dissolved.head())

    if len(areas_dissolved[areas_dissolved.area == 100 * 100]) > 0:
        print(
            f"""Number of district heating areas of single zensus cells:
              {len(areas_dissolved[areas_dissolved.area == 100*100])
               }"""
        )
        # print(f"""District heating areas ids of single zensus cells in
        #       district heating areas:
        #       {areas_dissolved[areas_dissolved.area == 100*100].index.values
        #        }""")
        # print(f"""Zensus_population_ids of single zensus cells
        #       in district heating areas:
        #       {scenario_dh_area[scenario_dh_area.area_id.isin(
        #           areas_dissolved[areas_dissolved.area == 100*100].index.values
        #           )].index.values}""")

    db.execute_sql(
        f"""DELETE FROM demand.egon_district_heating_areas
                   WHERE scenario = '{scenario_name}'"""
    )
    areas_dissolved.reset_index().to_postgis(
        "egon_district_heating_areas",
        schema="demand",
        con=db.engine(),
        if_exists="append",
    )
    # Alternative:
    # join.groupby("columnname").demand.sum()
    # add the sorted heat demand density curve
    no_district_heating = heat_demand_cells[
        ~heat_demand_cells.index.isin(scenario_dh_area.index)
    ]
    collection = pd.concat(
        [
            cells.sort_values(
                "residential_and_service_demand", ascending=False
            ),
            new_areas.sort_values(
                "residential_and_service_demand", ascending=False
            ),
            no_district_heating.sort_values(
                "residential_and_service_demand", ascending=False
            ),
        ],
        ignore_index=True,
    )
    collection["Cumulative_Sum"] = (
        collection.residential_and_service_demand.cumsum()
    ) / 1000000
    if plotting:
        plot_heat_density_sorted({scenario_name: collection}, scenario_name)

    return collection


def add_metadata():
    """
    Writes metadata JSON string into table comment.


    """

    # Prepare variables
    license_district_heating_areas = [
        license_ccby("© Europa-Universität Flensburg")
    ]

    # Metadata creation for district heating areas (polygons)
    meta = {
        "name": "district_heating_areas_metadata",
        "title": "eGo^n scenario-specific future district heating areas",
        "description": "Modelled future district heating areas for "
        "the supply of residential and service-sector heat demands",
        "language": ["EN"],
        "publicationDate": datetime.date.today().isoformat(),
        "context": context(),
        "spatial": {"location": "", "extent": "Germany", "resolution": ""},
        "sources": [
            sources()["peta"],
            sources()["egon-data"],
            sources()["zensus"],
            sources()["vg250"],
        ],
        "resources": [
            {
                "profile": "tabular-data-resource",
                "name": "egon_district_heating_areas",
                "path": "",
                "format": "PostgreSQL",
                "encoding": "UTF-8",
                "schema": {
                    "fields": [
                        {
                            "name": "id",
                            "description": "Unique identifier",
                            "type": "serial",
                            "unit": "none",
                        },
                        {
                            "name": "area_id",
                            "description": "District heating area id",
                            "type": "integer",
                            "unit": "none",
                        },
                        {
                            "name": "scenario",
                            "description": "scenario name",
                            "type": "text",
                            "unit": "none",
                        },
                        {
                            "name": "residential_and_service_demand",
                            "description": "annual heat demand",
                            "type": "double precision",
                            "unit": "MWh",
                        },
                        {
                            "name": "geom_polygon",
                            "description": "geo information of multipolygons",
                            "type": "geometry(MULTIPOLYGON, 3035)",
                            "unit": "none",
                        },
                    ],
                    "primaryKey": ["id"],
                    "foreignKeys": [
                        {
                            "fields": ["scenario"],
                            "reference": {
                                "resource": "scenario.egon_scenario_parameters",
                                "fields": ["name"],
                            },
                        }
                    ],
                },
                "dialect": {"delimiter": "none", "decimalSeparator": "."},
            }
        ],
        "licenses": license_district_heating_areas,
        "contributors": [
            {
                "title": "EvaWie",
                "email": "http://github.com/EvaWie",
                "date": time.strftime("%Y-%m-%d"),
                "object": None,
                "comment": "Imported data",
            },
            {
                "title": "Clara Büttner",
                "email": "http://github.com/ClaraBuettner",
                "date": time.strftime("%Y-%m-%d"),
                "object": None,
                "comment": "Updated metadata",
            },
        ],
        "metaMetadata": meta_metadata(),
    }
    meta_json = "'" + json.dumps(meta) + "'"

    db.submit_comment(meta_json, "demand", "egon_district_heating_areas")

    # Metadata creation for "id mapping" table
    meta = {
        "name": "map_zensus_district_heating_areas_metadata",
        "title": "district heating area ids assigned to zensus_population_ids",
        "description": "Ids of scenario specific future district heating areas"
        " for supply of residential and service-sector heat demands"
        " assigned to zensus_population_ids",
        "language": ["EN"],
        "publicationDate": datetime.date.today().isoformat(),
        "context": context(),
        "spatial": {"location": "", "extent": "Germany", "resolution": ""},
        "sources": [
            sources()["peta"],
            sources()["egon-data"],
            sources()["zensus"],
            sources()["vg250"],
        ],
        # Add the license for the map table
        "resources": [
            {
                "profile": "tabular-data-resource",
                "name": "egon_map_zensus_district_heating_areas",
                "path": "",
                "format": "PostgreSQL",
                "encoding": "UTF-8",
                "schema": {
                    "fields": [
                        {
                            "name": "id",
                            "description": "Unique identifier",
                            "type": "serial",
                            "unit": "none",
                        },
                        {
                            "name": "area_id",
                            "description": "district heating area id",
                            "type": "integer",
                            "unit": "none",
                        },
                        {
                            "name": "scenario",
                            "description": "scenario name",
                            "type": "text",
                            "unit": "none",
                        },
                    ],
                    "primaryKey": ["id"],
                    "foreignKeys": [
                        {
                            "fields": ["zensus_population_id"],
                            "reference": {
                                "resource": "society.destatis_zensus_population_per_ha",
                                "fields": ["id"],
                            },
                        },
                        {
                            "fields": ["scenario"],
                            "reference": {
                                "resource": "scenario.egon_scenario_parameters",
                                "fields": ["name"],
                            },
                        },
                    ],
                },
                "dialect": {"delimiter": "none", "decimalSeparator": "."},
            }
        ],
        "licenses": license_district_heating_areas,
        "contributors": [
            {
                "title": "EvaWie",
                "email": "http://github.com/EvaWie",
                "date": time.strftime("%Y-%m-%d"),
                "object": None,
                "comment": "Imported data",
            },
            {
                "title": "Clara Büttner",
                "email": "http://github.com/ClaraBuettner",
                "date": time.strftime("%Y-%m-%d"),
                "object": None,
                "comment": "Updated metadata",
            },
        ],
        "metaMetadata": meta_metadata(),
    }
    meta_json = "'" + json.dumps(meta) + "'"

    db.submit_comment(
        meta_json, "demand", "egon_map_zensus_district_heating_areas"
    )

    return None


def study_prospective_district_heating_areas():
    """
    Get information about Prospective Supply Districts for district heating.

    This optional function executes the functions so that you can study the
    heat demand density data of different scenarios and compare them and the
    resulting Prospective Supply Districts (PSDs) for district heating. This
    functions saves local shapefiles, because these data are not written into
    database. Moreover, heat density curves are drawn.
    This function is tailor-made and includes the scenarios eGon2035 and
    eGon100RE.

    Parameters
    ----------
        None

    Returns
    -------
        None

    Notes
    -----
        None

    TODO
    ----
        PSD statistics (average PSD connection rate, total HD per PSD) could
        be studied
    """

    # create directory to store files
    results_path = "district_heating_areas/"

    if not os.path.exists(results_path):
        os.mkdir(results_path)

    # load the total heat demand by census cell (residential plus service)
    # HD_2015 = load_heat_demands('eGon2015')
    # status quo heat demand data are part of the regluar database content
    # to get them, line 463 ("if not '2015' in source.stem:") has to be
    # deleted from
    # importing/heat_demand_data/__init__.py
    # and an empty row has to be added to scenario table:
    # INSERT INTO scenario.egon_scenario_parameters (name)
    # VALUES ('eGon2015');
    # because egon2015 is not part of the regular EgonScenario table!
    HD_2035 = load_heat_demands("eGon2035")
    HD_2050 = load_heat_demands("eGon100RE")

    # select only cells with heat demands > 100 GJ / (ha a)
    # HD_2015_above_100GJ = select_high_heat_demands(HD_2015)
    HD_2035_above_100GJ = select_high_heat_demands(HD_2035)
    HD_2050_above_100GJ = select_high_heat_demands(HD_2050)

    # PSDs
    # grouping cells applying the 201m distance buffer, including heat demand
    # aggregation
    # after decision for one year/scenario (here 2035), in the pipeline PSDs
    # are only calculeated for the one selected year/scenario;
    # here you can see all years/scenarios:
    # PSD_2015_201m = area_grouping(HD_2015_above_100GJ, distance=200,
    #                               minimum_total_demand=(10000/3.6)
    #                                ).dissolve('area_id', aggfunc='sum')
    # PSD_2015_201m.to_file(results_path+"PSDs_2015based.shp")
    PSD_2035_201m = area_grouping(
        HD_2035_above_100GJ, distance=200, minimum_total_demand=(10000 / 3.6)
    ).dissolve("area_id", aggfunc="sum")
    # HD_2035.to_file(results_path+"HD_2035.shp")
    # HD_2035_above_100GJ.to_file(results_path+"HD_2035_above_100GJ.shp")
    PSD_2035_201m.to_file(results_path + "PSDs_2035based.shp")
    PSD_2050_201m = area_grouping(
        HD_2050_above_100GJ, distance=200, minimum_total_demand=(10000 / 3.6)
    ).dissolve("area_id", aggfunc="sum")
    PSD_2050_201m.to_file(results_path + "PSDs_2050based.shp")

    # plotting all cells - not considering census data
    # https://matplotlib.org/stable/api/_as_gen/matplotlib.pyplot.plot.html
    # https://www.earthdatascience.org/courses/scientists-guide-to-plotting-data-in-python/plot-with-matplotlib/introduction-to-matplotlib-plots/customize-plot-colors-labels-matplotlib/
    fig, ax = plt.subplots(1, 1)
    # add the sorted heat demand densities
    # HD_2015 = HD_2015.sort_values('residential_and_service_demand',
    #                               ascending=False).reset_index()
    # HD_2015["Cumulative_Sum"] = (HD_2015.residential_and_service_demand.
    #                              cumsum()) / 1000000
    # ax.plot(HD_2015.Cumulative_Sum,
    #         HD_2015.residential_and_service_demand, label='eGon2015')

    HD_2035 = HD_2035.sort_values(
        "residential_and_service_demand", ascending=False
    ).reset_index()
    HD_2035["Cumulative_Sum"] = (
        HD_2035.residential_and_service_demand.cumsum()
    ) / 1000000
    ax.plot(
        HD_2035.Cumulative_Sum,
        HD_2035.residential_and_service_demand,
        label="eGon2035",
    )

    HD_2050 = HD_2050.sort_values(
        "residential_and_service_demand", ascending=False
    ).reset_index()
    HD_2050["Cumulative_Sum"] = (
        HD_2050.residential_and_service_demand.cumsum()
    ) / 1000000
    ax.plot(
        HD_2050.Cumulative_Sum,
        HD_2050.residential_and_service_demand,
        label="eGon100RE",
    )

    # add the district heating shares

    heat_parameters = get_sector_parameters("heat", "eGon2035")
    district_heating_share_2035 = heat_parameters["DE_district_heating_share"]
    plt.axvline(
        x=HD_2035.residential_and_service_demand.sum()
        / 1000000
        * district_heating_share_2035,
        ls=":",
        lw=0.5,
        label="72TWh DH in 2035 in Germany => 14% DH",
        color="black",
    )
    heat_parameters = get_sector_parameters("heat", "eGon100RE")
    district_heating_share_100RE = heat_parameters["DE_district_heating_share"]
    plt.axvline(
        x=HD_2050.residential_and_service_demand.sum()
        / 1000000
        * district_heating_share_100RE,
        ls="-.",
        lw=0.5,
        label="75TWh DH in 100RE in Germany => 19% DH",
        color="black",
    )

    # axes meet in (0/0)
    ax.margins(x=0, y=0)  # default is 0.05
    # axis style
    # https://matplotlib.org/stable/gallery/ticks_and_spines/centered_spines_with_arrows.html
    # Hide the right and top spines
    ax.spines["right"].set_visible(False)
    ax.spines["top"].set_visible(False)
    ax.plot(1, 0, ">k", transform=ax.get_yaxis_transform(), clip_on=False)
    ax.plot(0, 1, "^k", transform=ax.get_xaxis_transform(), clip_on=False)

    ax.set(title="Heat Demand in eGo^n")
    ax.set_xlabel("Cumulative Heat Demand [TWh / a]")
    ax.set_ylabel("Heat Demand Densities [MWh / (ha a)]")

    plt.legend()
    plt.savefig(results_path + "Complete_HeatDemandDensities_Curves.png")

    return None


def demarcation(plotting=True):
    """
    Load scenario specific district heating areas with metadata into database.

    This function executes the functions that identifies the areas which will
    be supplied with district heat in the two eGo^n scenarios. The creation of
    heat demand density curve figures is optional. So is also the export of
    scenario specific Prospective Supply Districts for district heating (PSDs)
    as shapefiles including the creation of a figure showing the comparison
    of sorted heat demand densities.

    The method was executed for 2015, 2035 and 2050 to find out which
    scenario year defines the PSDs. The year 2035 was selected and
    the function was adjusted accordingly.
    If you need the 2015 scenario heat demand data, please have a look at
    the heat demand script commit 270bea50332016447e869f69d51e96113073b8a0,
    where the 2015 scenario was deactivated. You can study the 2015 PSDs in
    the study_prospective_district_heating_areas function after
    un-commenting some lines.

    Parameters
    ----------
    plotting: boolean
        if True, figure showing the heat demand density curve will be created

    Returns
    -------
        None

    Notes
    -----
        None

    TODO
    ----
        Create diagrams/curves, make better curves with matplotlib

        Make PSD and DH system statistics
        Check if you need the current / future number of DH
        supplied flats and the total number of flats to calculate the
        connection rate

        Add datasets to datasets configuration

    """

    # load the census district heat data on apartments, and group them
    # This is currently done in the grouping function:
    # district_heat_zensus, heating_type_zensus = load_census_data()
    # Zenus_DH_areas_201m = area_grouping(district_heat_zensus)

    heat_density_per_scenario = {}
    # scenario specific district heating areas
    heat_density_per_scenario["eGon2035"] = district_heating_areas(
        "eGon2035", plotting
    )
    heat_density_per_scenario["eGon100RE"] = district_heating_areas(
        "eGon100RE", plotting
    )

    if plotting:
        plot_heat_density_sorted(heat_density_per_scenario)
    # if you want to study/export the Prospective Supply Districts (PSDs)
    # for all scenarios
    # study_prospective_district_heating_areas()

    add_metadata()

    return None
