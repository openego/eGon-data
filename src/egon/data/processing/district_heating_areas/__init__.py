# -*- coding: utf-8 -*-

# This script is part of eGon-data.

# license text - to be added.

"""
Central module containing all code creating with district heating areas.

This module obtains the information from the census tables and the heat demand
densities, demarcates so the current and future district heating areas. In the
end it saves them in the database.
"""

from egon.data import db
from egon.data.importing.scenarios import get_sector_parameters, EgonScenario

import pandas as pd
import geopandas as gpd
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.polygon import Polygon
from matplotlib import pyplot as plt

# for metadata creation
import json
# import time

# packages for ORM class definition
from sqlalchemy import Column, String, Integer, Sequence, Float, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2.types import Geometry


Base = declarative_base()


class MapZensusDistrictHeatingAreas(Base):
    __tablename__ = "map_zensus_district_heating_areas"
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
    version = Column(String)
    zensus_population_id = Column(Integer)


class DistrictHeatingAreas(Base):
    __tablename__ = "district_heating_areas"
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
    version = Column(String)
    geom_polygon = Column(Geometry('MULTIPOLYGON', 3035))
    residential_and_service_demand = Column(Float)



def create_tables():
    """Create tables for district heating areas

    Returns
    -------
        None
    """

    # Drop tables
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
            demand.map_zensus_district_heating_areas_seq CASCADE;"""
    )

    engine = db.engine()
    DistrictHeatingAreas.__table__.create(bind=engine, checkfirst=True)
    MapZensusDistrictHeatingAreas.__table__.create(bind=engine,
                                                   checkfirst=True)


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
        f"""SELECT flats.zensus_population_id, flats.characteristics_text,
        flats.quantity, flats.quantity_q, pop.geom_point,
        pop.geom AS geom_polygon
        FROM society.destatis_zensus_apartment_per_ha AS flats
        JOIN society.destatis_zensus_population_per_ha AS pop
        ON flats.zensus_population_id = pop.id
        AND flats.characteristics_text = 'Fernheizung (Fernwärme)'
        AND flats.zensus_population_id IN
        (SELECT zensus_population_id FROM demand.egon_peta_heat);""",
        index_col="zensus_population_id",
        geom_col="geom_polygon"
    )

    heating_type = db.select_geodataframe(
        f"""SELECT flats.zensus_population_id,
        SUM(flats.quantity) AS quantity, pop.geom AS geom_polygon
        FROM society.destatis_zensus_apartment_per_ha AS flats
        JOIN society.destatis_zensus_population_per_ha AS pop
        ON flats.zensus_population_id = pop.id
        AND flats.attribute = 'HEIZTYP'
        AND flats.zensus_population_id IN
        (SELECT zensus_population_id FROM demand.egon_peta_heat)
        GROUP BY flats.zensus_population_id, pop.geom;""",
        index_col="zensus_population_id",
        geom_col="geom_polygon"
    )

    # district_heat.to_file("dh.shp")
    # heating_type.to_file("heating.shp")

    # calculate the connection rate for all census cells with DH
    # adding it to the district_heat geodataframe
    district_heat['connection_rate'] = district_heat['quantity'].div(
        heating_type['quantity'])[district_heat.index]
    # district_heat.head
    # district_heat['connection_rate'].describe()

    district_heat = district_heat[district_heat['connection_rate'] >= 0.3]
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
        AND demand.version = '0.0.0'
        AND demand.scenario = '{scenario_name}'
        GROUP BY demand.zensus_population_id, pop.geom;""",
        index_col="zensus_population_id",
        geom_col="geom_polygon"
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

    high_heat_demand = heat_demand[heat_demand[
        'residential_and_service_demand'] > minimum_demand]
    # high_heat_demand.head()
    # print(high_heat_demand.area) # all cells are 10,000 m²

    return high_heat_demand


def area_grouping(raw_polygons, distance = 200, minimum_total_demand = None):
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
    cell_buffers['geom_polygon'] = cell_buffers['geom_polygon'
                                                ].buffer(buffer_distance)
    # print(cell_buffers.area)

    # create a shapely Multipolygon which is split into a list
    buffer_polygons = list(cell_buffers['geom_polygon'].unary_union)

    # change the data type into geopandas geodataframe
    buffer_polygons_gdf = gpd.GeoDataFrame(geometry=buffer_polygons,
                                           crs=3035)
    # buffer_polygons_gdf.plot()

    # Join studied cells with buffer polygons
    columnname = "area_id"
    join = gpd.sjoin(raw_polygons, buffer_polygons_gdf, how="inner",
                     op="intersects")

    join = join.rename({'index_right': columnname}, axis=1)
    # join.plot(column=columnname)

    # minimum total heat demand for the areas with minimum criterium
    if (minimum_total_demand is not None and
        'residential_and_service_demand' in raw_polygons.columns):
         # total_heat_demand = join.dissolve('area_id', aggfunc='sum')
         # type(large_areas)
         # filtered = join.groupby(['area_id'])[
         #     'residential_and_service_demand'].agg('sum') > 0.7
         large_areas = gpd.GeoDataFrame(join.groupby(['area_id'])
                                        ['residential_and_service_demand'].
                                        agg('sum'))
         # large_areas = large_areas[large_areas[
         #     'residential_and_service_demand'] > minimum_total_demand]
         large_areas = (large_areas['residential_and_service_demand'] >
                        minimum_total_demand)
         join = join[join.area_id.isin(large_areas[large_areas].index)]

    elif (minimum_total_demand is not None and
          'residential_and_service_demand' not in raw_polygons.columns):
        print("""The minimum total heat demand criterium can only be applied
              on geodataframe having a column named
              'residential_and_service_demand' """)

    return join


def district_heating_areas(scenario_name, plotting = False):
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
        heat_parameters = get_sector_parameters('heat', scenario=scenario_name)

        district_heating_share = heat_parameters['DE_district_heating_share']

    # heat_demand is scenario specific
    heat_demand_cells = load_heat_demands(scenario_name)

    # Firstly, supply the cells which already have district heating according
    # to 2011 Census data and which are within likely dh areas (created
    # by the area grouping function), load only the first returned result: [0]
    min_hd_census = 10000 / 3.6 # in MWh

    census_plus_heat_demand = load_census_data()[0].copy()
    census_plus_heat_demand['residential_and_service_demand'] = (
        heat_demand_cells.loc[census_plus_heat_demand.index.values,
                              'residential_and_service_demand'])

    cells = area_grouping(census_plus_heat_demand, distance = 500,
                          minimum_total_demand = min_hd_census)
    # cells.groupby("area_id").size().sort_values()

    total_district_heat = (heat_demand_cells['residential_and_service_demand'
                                             ].sum() * district_heating_share)

    diff = total_district_heat - cells['residential_and_service_demand'].sum()


    assert diff > 0, (
        """The chosen district heating share in combination with the heat
        demand reduction leads to an amount of district heat which is
        lower than the current one. This case is not implemented yet.""")

    # Secondly, supply the cells with the highest heat demand not having
    # district heating yet
    # ASSUMPTION HERE: 2035 HD defined the PSDs
    min_hd = 10000 / 3.6
    PSDs = area_grouping(select_high_heat_demands(
        load_heat_demands("eGon2035")), distance = 200,
        minimum_total_demand = min_hd)

    # PSDs.groupby("area_id").size().sort_values()

    # select all cells not already suppied with district heat
    new_areas = heat_demand_cells[~heat_demand_cells.index.isin(cells.index)]
    # sort by heat demand density
    new_areas = new_areas[new_areas.index.isin(PSDs.index)].sort_values(
        'residential_and_service_demand', ascending=False)
    new_areas["Cumulative_Sum"] = new_areas.residential_and_service_demand.cumsum()
    # select cells to be supplied with district heating until district
    # heating share is reached
    new_areas = new_areas[new_areas["Cumulative_Sum"] <= diff]

    print(f"""Minimum heat demand density for cells with new district heat
          supply in scenario {scenario_name} is
          {new_areas.residential_and_service_demand.tail(1).values[0]}
          MWh / (ha a).""")
    print(f"""Number of cells with new district heat supply in scenario
          {scenario_name} is {len(new_areas)}.""")

    # check = gpd.GeoDataFrame(
    #     cells[['residential_and_service_demand', 'geom_polygon']].append(
    #         new_areas[['residential_and_service_demand', 'geom_polygon']]),
    #     geometry='geom_polygon')

    # group the resulting scenario specific district heating areas
    scenario_dh_area = area_grouping(gpd.GeoDataFrame(
        cells[['residential_and_service_demand', 'geom_polygon']].append(
            new_areas[['residential_and_service_demand', 'geom_polygon']]),
        geometry='geom_polygon'), distance=500)
    # scenario_dh_area.plot(column = "area_id")

    scenario_dh_area.groupby("area_id").size().sort_values()
    scenario_dh_area.residential_and_service_demand.sum()
    # scenario_dh_area.sort_index()
    # cells[cells.index==1416974]

    # store the results in the database
    scenario_dh_area["scenario"] = scenario_name
    scenario_dh_area["version"] = '0.0.0'

    db.execute_sql(f"""DELETE FROM demand.map_zensus_district_heating_areas
                   WHERE scenario = '{scenario_name}'""")
    scenario_dh_area[['version', 'scenario', 'area_id']].to_sql(
        'map_zensus_district_heating_areas',
                                       schema='demand', con=db.engine(),
                                       if_exists="append")

    # Create polygons around the grouped cells and store them in the database
    # join.dissolve(columnname).convex_hull.plot() # without holes, too big
    areas_dissolved = scenario_dh_area.dissolve('area_id', aggfunc='sum')
    areas_dissolved["scenario"] = scenario_name
    areas_dissolved["version"] = '0.0.0'

    areas_dissolved["geom_polygon"] = [MultiPolygon([feature]) \
                                       if type(feature) == Polygon \
                                           else feature for feature in \
                                               areas_dissolved["geom_polygon"]]
    # type(areas_dissolved["geom"][0])
    # print(type(areas_dissolved))
    # print(areas_dissolved.head())

    if len(areas_dissolved[areas_dissolved.area == 100*100]) > 0:
        print(f"""District heating areas ids of single zensus cells in
              district heating areas:
              {areas_dissolved[areas_dissolved.area == 100*100].index.values
               }""")
        print(f"""Zensus_population_ids of single zensus cells
              in district heating areas:
              {scenario_dh_area[scenario_dh_area.area_id.isin(
                  areas_dissolved[areas_dissolved.area == 100*100].index.values
                  )].index.values}""")

    db.execute_sql(f"""DELETE FROM demand.district_heating_areas
                   WHERE scenario = '{scenario_name}'""")
    areas_dissolved.reset_index().to_postgis('district_heating_areas',
                                             schema='demand',
                                             con=db.engine(),
                                             if_exists="append")
    # Alternative:
    # join.groupby("columnname").demand.sum()

    if plotting:

        # create diagrams for visualisation:
        # fristly, census district heating cell sorted by heat demand density,
        # secondly, sorted new area cells sorted by heat demand density
        # remaining cells sorted by heat demand density:
        # create one dataframe with all data: first the cells with existing,
        # then the cells with new district heating systems and in the end the
        # ones without;
        # DH share as a vertical line

        fig, ax = plt.subplots(1, 1)
        # add the district heating share as a line
        procent = round(district_heating_share * 100, 0)
        plt.axvline(x=total_district_heat / 1000000, ls = "--", lw = 0.5,
                    label = (f'District Heating Share of {procent} %'),
                    color = 'red')
        # add the sorted heat demand density curve
        no_district_heating = heat_demand_cells[~heat_demand_cells.index.isin(
            scenario_dh_area.index)]
        collection = pd.concat([cells.sort_values(
                                    'residential_and_service_demand',
                                    ascending=False),
                                new_areas.sort_values(
                                    'residential_and_service_demand',
                                    ascending=False),
                                no_district_heating.sort_values(
                                    'residential_and_service_demand',
                                    ascending=False)],
                               ignore_index=True)
        collection["Cumulative_Sum"] = (collection.
                                        residential_and_service_demand.
                                        cumsum()) / 1000000

        ax.plot(collection.Cumulative_Sum,
                collection.residential_and_service_demand, label =
                " Heat demand densities, sorted")

        # annotations
        x1 = total_district_heat / 1000000 / 2
        x2 = x1 * 4
        # x2 = (total_district_heat + ((heat_demand_cells[
        #     'residential_and_service_demand'].sum() -
        #     total_district_heat) / 2)) / 1000000
        y = heat_demand_cells['residential_and_service_demand'].max() * 0.7

        ax.text(x1, y, "District\nheat", ha="center", va="center", size=8,
                bbox=dict(boxstyle="round, pad=0.5", fc="none",
                          ec="red", # lw=2
                          ))
        ax.text(x2, y, "Individual\nheat supply", ha='center', va="center",
                size=8,
                bbox=dict(boxstyle="round, pad=0.5", fc="none",
                          ec="red", # lw=2
                          ))

        ax.set(title = ("Heat Sector in " + scenario_name))
        ax.set_xlabel("Cumulative Heat Demand [TWh / a]")
        ax.set_ylabel("Heat Demand Densities [MWh / (ha a)]")

        # remove empty space between axis and graph
        ax.margins(x=0, y=0)
        # axes style
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.plot(1, 0, ">k", transform=ax.get_yaxis_transform(), clip_on=False)
        ax.plot(0, 1, "^k", transform=ax.get_xaxis_transform(), clip_on=False)
        ax.legend()  # or: plt.legend()
        plt.savefig(f'HeatDemandDensities_Curve_{scenario_name}.png')

    return None


def add_metadata():
    """
    Writes metadata JSON string into table comment.

    TODO
    ----

        Meta data must be check and adjusted to the egon_data standard:
            - Add context
            - authors and institutions

    """

    # Prepare variables
    license_district_heating_areas = [
        {
            # this could be the license of the "district_heating_areas"
            "name": "Creative Commons Attribution 4.0 International",
            "title": "CC BY 4.0",
            "path": "https://creativecommons.org/licenses/by/4.0/",
            "instruction": (
                "You are free: To Share, To Adapt;"
                " As long as you: Attribute!"
            ),
            "attribution": "© Europa-Universität Flensburg",  # if all agree
            # "attribution": "© ZNES Flensburg",  # alternative
        }
    ]

    # Metadata creation for district heating areas (polygons)
    meta = {
        "name": "district_heating_areas_metadata",
        "title": "eGo^n scenario-specific future district heating areas",
        "description": "Modelled future district heating areas for "
        "the supply of residential and service-sector heat demands",
        "language": ["EN"],
        "spatial": {
            "location": "",
            "extent": "Germany",
            "resolution": "",
        },
        "temporal": {
            "referenceDate": "scenario-specific",
            "timeseries": {
                "start": "",
                "end": "",
                "resolution": "",
                "alignment": "",
                "aggregationType": "",
            },
        },
        "sources": [
            {
                # eGon scenario specific heat demand distribution based
                # on Peta5_0_1, using vg250 boundaries
                },
            {
                # Census gridded apartment data
            },
        ],

        "resources": [
            {
                "profile": "tabular-data-resource",
                "name": "district_heating_areas",
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
                            "name": "version",
                            "description": "data version number",
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
                "title": "Eva, Clara",
                "email": "",
                "date": "2021-05-07",
                "object": "",
                "comment": "Processed data",
            }
        ],
        "metaMetadata": {  # https://github.com/OpenEnergyPlatform/oemetadata
            "metadataVersion": "OEP-1.4.0",
            "metadataLicense": {
                "name": "CC0-1.0",
                "title": "Creative Commons Zero v1.0 Universal",
                "path": ("https://creativecommons.org/publicdomain/zero/1.0/"),
            },
        },
    }
    meta_json = "'" + json.dumps(meta) + "'"

    db.submit_comment(meta_json, "demand", "district_heating_areas")


    # Metadata creation for "id mapping" table
    meta = {
        "name": "map_zensus_district_heating_areas_metadata",
        "title": "district heating area ids assigned to zensus_population_ids",
        "description": "Ids of scenario specific future district heating areas"
        " for supply of residential and service-sector heat demands"
        " assigned to zensus_population_ids",
        "language": ["EN"],
        "spatial": {
            "location": "",
            "extent": "Germany",
            "resolution": "",
        },
        "temporal": {
            "referenceDate": "scenario-specific",
            "timeseries": {
                "start": "",
                "end": "",
                "resolution": "",
                "alignment": "",
                "aggregationType": "",
            },
        },
        "sources": [
            {
                # eGon scenario specific heat demand distribution based
                # on Peta5_0_1, using vg250 boundaries
                },
            {
                # Census gridded apartment data
            },
        ],


    # Add the license for the map table

        "resources": [
            {
                "profile": "tabular-data-resource",
                "name": "map_zensus_district_heating_areas",
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
                        {
                            "name": "version",
                            "description": "data version number",
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
                        }
                    ],
                },
                "dialect": {"delimiter": "none", "decimalSeparator": "."},
            }
        ],
        "licenses": license_district_heating_areas,
        "contributors": [
            {
                "title": "Eva, Clara",
                "email": "",
                "date": "2021-05-07",
                "object": "",
                "comment": "Processed data",
            }
        ],
        "metaMetadata": {  # https://github.com/OpenEnergyPlatform/oemetadata
            "metadataVersion": "OEP-1.4.0",
            "metadataLicense": {
                "name": "CC0-1.0",
                "title": "Creative Commons Zero v1.0 Universal",
                "path": ("https://creativecommons.org/publicdomain/zero/1.0/"),
            },
        },
    }
    meta_json = "'" + json.dumps(meta) + "'"

    db.submit_comment(meta_json, "demand", "map_zensus_district_heating_areas")

    return None


def district_heating_areas_demarcation():
    """
    Call all functions related to the creation of district heating areas.

    This function executes the functions that....

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
        Run the model for 20150, 2035 and 2050 to find out which scenario year
        defines the PSDs -> 2035; implement it accordingly and remove the 2015
        data, if they are not needed anymore.

        Run the model for Germany and see if there are created large district
        heating systems in the Ruhr area which need to be split by the
        municiplality boundaries for example

        Create diagrams/curves, make better curves with matplotlib

        Make PSD and DH system statistics

        Add datasets to datasets configuration

        Check which tasks need to run (according to version number)
    """

    # load the census district heat data on apartments, and group them
    # This is currently done in the grouping function:
    # district_heat_zensus, heating_type_zensus = load_census_data()
    # Zenus_DH_areas_201m = area_grouping(district_heat_zensus)

    # load the total heat demand by census cell (residential plus service)
    HD_2015 = load_heat_demands('eGon2015')
    # status quo heat demand data are part of the regluar database content
    # to get them, line 463 has to be deleted from
    # importing/heat_demand_data/__init__.py
    # and an empty row has to be added to scenario table:
    # INSERT INTO scenario.egon_scenario_parameters (name)
    # VALUES ('eGon2015');
    # because egon2015 is not part of the regular EgonScenario table!
    HD_2035 = load_heat_demands('eGon2035')
    HD_2050 = load_heat_demands('eGon100RE')

    # select only cells with heat demands > 100 GJ / (ha a)
    HD_2015_above_100GJ = select_high_heat_demands(HD_2015)
    HD_2035_above_100GJ = select_high_heat_demands(HD_2035)
    HD_2050_above_100GJ = select_high_heat_demands(HD_2050)

    # PSDs
    # grouping cells applying the 201m distance buffer, including heat demand
    # aggregation
    # KEEP ONLY ONE after decision
    PSD_2015_201m = area_grouping(HD_2015_above_100GJ, distance=200,
                                  minimum_total_demand=(10000/3.6)
                                   ).dissolve('area_id', aggfunc='sum')
    PSD_2015_201m.to_file("PSDs_2015based.shp")
    PSD_2035_201m = area_grouping(HD_2035_above_100GJ, distance=200,
                                  minimum_total_demand=(10000/3.6)
                                  ).dissolve('area_id', aggfunc='sum')
    HD_2035.to_file("HD_2035.shp")
    HD_2035_above_100GJ.to_file("HD_2035_above_100GJ.shp")

    PSD_2035_201m.to_file("PSDs_2035based.shp")
    PSD_2050_201m = area_grouping(HD_2050_above_100GJ, distance=200,
                                  minimum_total_demand=(10000/3.6)
                                  ).dissolve('area_id', aggfunc='sum')
    PSD_2050_201m.to_file("PSDs_2050based.shp")

    # PSD Statistics: average PSD connection rate, total HD

    # scenario specific district heating areas
    district_heating_areas('eGon2035', plotting = True)
    district_heating_areas('eGon100RE', plotting = True)

    # plotting all cells
    # https://matplotlib.org/stable/api/_as_gen/matplotlib.pyplot.plot.html
    # https://www.earthdatascience.org/courses/scientists-guide-to-plotting-data-in-python/plot-with-matplotlib/introduction-to-matplotlib-plots/customize-plot-colors-labels-matplotlib/
    fig, ax = plt.subplots(1, 1)
    # add the sorted heat demand densities
    HD_2015 = HD_2015.sort_values('residential_and_service_demand',
                                  ascending=False).reset_index()
    HD_2015["Cumulative_Sum"] = (HD_2015.residential_and_service_demand.
                                 cumsum()) / 1000000
    ax.plot(HD_2015.Cumulative_Sum,
            HD_2015.residential_and_service_demand, label='eGon2015')

    HD_2035 = HD_2035.sort_values('residential_and_service_demand',
                                  ascending=False).reset_index()
    HD_2035["Cumulative_Sum"] = (HD_2035.residential_and_service_demand.
                                 cumsum()) / 1000000
    ax.plot(HD_2035.Cumulative_Sum,
            HD_2035.residential_and_service_demand, label='eGon2035')

    HD_2050 = HD_2050.sort_values('residential_and_service_demand',
                                  ascending=False).reset_index()
    HD_2050["Cumulative_Sum"] = (HD_2050.residential_and_service_demand.
                                 cumsum()) / 1000000
    ax.plot(HD_2050.Cumulative_Sum,
            HD_2050.residential_and_service_demand, label='eGon100RE')

    # add the district heating shares
    plt.axvline(x=HD_2035.residential_and_service_demand.sum()/1000000*0.14,
                ls = ":", lw = 0.5,
                label = '72TWh DH in 2035 in Germany => 14% DH',
                color = 'black')
    plt.axvline(x=HD_2050.residential_and_service_demand.sum()/1000000*0.19,
                ls = "-.", lw = 0.5,
                label = '75TWh DH in 100RE in Germany => 19% DH',
                color = 'black')

    # axes meet in (0/0)
    ax.margins(x=0, y=0) # default is 0.05
    # axis style
    # https://matplotlib.org/stable/gallery/ticks_and_spines/centered_spines_with_arrows.html
    # Hide the right and top spines
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    ax.plot(1, 0, ">k", transform=ax.get_yaxis_transform(), clip_on=False)
    ax.plot(0, 1, "^k", transform=ax.get_xaxis_transform(), clip_on=False)

    ax.set(title = "Heat Demand in eGo^n")
    ax.set_xlabel("Cumulative Heat Demand [TWh / a]")
    ax.set_ylabel("Heat Demand Densities [MWh / (ha a)]")

    plt.legend()
    plt.savefig('Complete_HeatDemandDensities_Curves.png')

    add_metadata()

    return None
