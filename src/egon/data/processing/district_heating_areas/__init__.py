# -*- coding: utf-8 -*-

# This script is part of eGon-data.

# license text - to be added.

"""
Central module containing all code creating with district heating areas.

This module obtains the information from the census tables and the heat demand
densities, demarcates so the current and future district heating areas. In the
end it saves them in the database.
"""

from egon.data import db  # , subprocess
# import egon.data.config
from egon.data.importing.scenarios import get_sector_parameters, EgonScenario

# import pandas as pd
import geopandas as gpd
from matplotlib import pyplot as plt

# for metadata creation
import json
# import time

# packages for ORM class definition
from sqlalchemy import Column, String, Float, Integer, Sequence, ForeignKey
from sqlalchemy.ext.declarative import declarative_base

# TO DO: Finish the class definition
# egon2015 is not part of the EgonScenario table!

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
    geom_polygon = Column(Geometry)


# Methods used are explained here:
# https://geopandas.org/docs/user_guide/geometric_manipulations.html


def load_census_data():
    """
    Load the heating type information from the census database table.

    The census apartment and the census building table contains information
    about the heating type. The information are loaded from the apartment
    table, because they might be more useful when it comes to the estimation of
    the connection rates.

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

    district_heat['connection_rate'] = district_heat['quantity'].div(heating_type['quantity'])[district_heat.index]
    # district_heat.head
    # district_heat['connection_rate'].describe()

    district_heat = district_heat[district_heat['connection_rate'] >= 0.3]
    # district_heat.columns

    """
    Alternative
    Return a geodataframe with the number of DH supplied flats and all flats
    to calculate the connection rate in a PSD from the number of flats instead
    of the calculation of an average
    """

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


def area_grouping(raw_polygons):
    """
    This function groups polygons which are close to each other.

    Heat demand density polygons can be grouped into prospective district
    heating supply districts (PSDs). Only cells with a minimum heat demand
    density (e.g. >100 GJ/(ha a)) are considered. Therefore, the
    select_high_heat_demands() function is used.

    Census cells with district heating supply can be grouped into
    existing district heating system areas.

    Method
    - buffer around the cell polygons
    - union the intersecting buffer polygons
    - union the cell polygons which are within one unioned buffer polygon

    Parameters
    ----------
    raw_polygons: geopandas.geodataframe.GeoDataFrame
        polygons to be grouped.

    Returns
    -------
    join: geopandas.geodataframe.GeoDataFrame
        cell polygons with area id

    Notes
    -----
        None

    TODO
    ----
        Make the buffer distance a parameter, 501 m for Census DH areas?

        Implement the total minimum demand for PSDs:
        There is a minimum total heat demand of 10,000 GJ / a for PSDs.
        It could be an optional parameter.

    """

    # WARNING:
    # A value is trying to be set on a copy of a slice from a DataFrame.
    # Try using .loc[row_indexer,col_indexer] = value instead
    cell_buffers = raw_polygons
    cell_buffers['geom_polygon'] = cell_buffers['geom_polygon'].buffer(201)
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

    return join


def district_heating_areas(scenario_name):
    """
    Load district heating share from a sceanto table.

    ...

    Parameters
    ----------
    scenario_name: str
        name of scenario to be studies


    Returns
    -------
        None

    Notes
    -----
        None

    TODO
    ----
        Error messages when the amount of DH is lower than today

        Do "area_grouping(load_census_data()[0])" only once, not for all
        scenarios.

        Make sure that puting data into the area_grouping, does not lead
        totally wrong data e.g. aggregated connection rates.

        Create the diagram with the curve, maybe in the final function or here

    """

    # Load district heating shares from the scenario table
    if scenario_name == "eGon2015":
        district_heating_share = 0.08
    else:
        heat_parameters = get_sector_parameters('heat', scenario=scenario_name)

        district_heating_share = heat_parameters['DE_district_heating_share']

    # Firstly, supply the cells which already have district heating according
    # to 2011 Census data and which are within likely dh areas (created
    # by the area grouping function), load only the first returned result: [0]
    cells = area_grouping(load_census_data()[0])
    # heat_demand is scenario specific
    heat_demand_cells = load_heat_demands(scenario_name)
    cells['residential_and_service_demand'] = heat_demand_cells.loc[
        cells.index.values, 'residential_and_service_demand']

    total_district_heat = (heat_demand_cells['residential_and_service_demand'
                                             ].sum() * district_heating_share)

    diff = total_district_heat - cells['residential_and_service_demand'].sum()

    # Secondly, supply the cells with the highest heat demand not having
    # district heating yet
    # ASSUMPTION HERE: 2035 HD defined the PSDs
    PSDs = area_grouping(select_high_heat_demands(
        load_heat_demands("eGon2035")))

    # select all cells not already suppied with district heat
    new_areas = heat_demand_cells[~heat_demand_cells.index.isin(cells.index)]
    # sort by heat demand density
    new_areas = new_areas[new_areas.index.isin(PSDs.index)].sort_values(
        'residential_and_service_demand', ascending=False)
    new_areas["Cumulative_Sum"] = new_areas.residential_and_service_demand.cumsum()
    # select cells to be supplied with district heating until district
    # heating share is reached
    new_areas = new_areas[new_areas["Cumulative_Sum"] <= diff]
    # group the resulting scenario specify district heating areas
    scenario_dh_area = area_grouping(gpd.GeoDataFrame(
        cells[['residential_and_service_demand', 'geom_polygon']].append(
            new_areas[['residential_and_service_demand', 'geom_polygon']]),
        geometry='geom_polygon'))
    # scenario_dh_area.plot(column = "area_id")

    # store the results in the database
    scenario_dh_area["scenario"] = scenario_name
    scenario_dh_area["version"] = '0.0.0'

    db.execute_sql(f"""DELETE FROM demand.map_zensus_district_heating_areas
                   WHERE scenario = '{scenario_name}'""")
    # LATER HERE THE GEO INFORMATION COULD BE DELETED WHEN SAVING IN DB:
    # .drop('geom_polygon', axis=1) - how to drop two?
    scenario_dh_area.drop('residential_and_service_demand', axis=1
                          ).to_postgis('map_zensus_district_heating_areas',
                                       schema='demand', con=db.engine(),
                                       if_exists="append")
    # scenario_dh_area.columns

    # Create polygons around the grouped cells and store them in the database
    # CHECK ALTERNATIVE METHODS
    # join.dissolve(columnname).convex_hull.plot() # without holes, too big
    areas_dissolved = scenario_dh_area.dissolve('area_id', aggfunc='sum')
    areas_dissolved["scenario"] = scenario_name
    areas_dissolved["version"] = '0.0.0'

    db.execute_sql(f"""DELETE FROM demand.district_heating_areas
                   WHERE scenario = '{scenario_name}'""")
    areas_dissolved.to_postgis('district_heating_areas', schema='demand',
                               con=db.engine(), if_exists="append")
    # Alternative:
    # join.groupby("columnname").demand.sum()

    # create diagrams for visualisation, sorted by HDD
    # sorted census dh first, sorted new areas, left overs, DH share
    fig, ax = plt.subplots(1, 1)
    new_areas.sort_values('residential_and_service_demand', ascending=False
                          ).reset_index().residential_and_service_demand.plot(
                              ax=ax)
    plt.savefig(f'HeatDemandDensities_Curve_{scenario_name}.png')

    return None


def add_metadata():
    """
    Writes metadata JSON string into table comment.

    TODO
    ----
        Do it!
    """


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
        Find out which scenario year defines the PSDs

        Create diagrams/curves, make better curves with matplotlib
        PSD statistics

        Check which tasks need to run (according to version number)
    """
    # load the census district heat data on apartments, and group them
    # This is currently done in the grouping function:
    # district_heat_zensus, heating_type_zensus = load_census_data()
    # Zenus_DH_areas_201m = area_grouping(district_heat_zensus)

    # load the total heat demand by census cell (residential plus service)
    HD_2015 = load_heat_demands('eGon2015')
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
    PSD_2015_201m = area_grouping(HD_2015_above_100GJ
                                  ).dissolve('area_id', aggfunc='sum')
    PSD_2015_201m.to_file("PSDs_2015based.shp")
    PSD_2035_201m = area_grouping(HD_2035_above_100GJ
                                  ).dissolve('area_id', aggfunc='sum')
    PSD_2035_201m.to_file("PSDs_2035based.shp")
    PSD_2050_201m = area_grouping(HD_2050_above_100GJ
                                  ).dissolve('area_id', aggfunc='sum')
    PSD_2050_201m.to_file("PSDs_2050based.shp")

    # PSD Statistics: average PSD connection rate, total HD

    # scenario specific district heating areas
    district_heating_areas('eGon2035')
    district_heating_areas('eGon2050')

    # plotting all cells
    fig, ax = plt.subplots(1, 1)
    HD_2015.sort_values('demand', ascending=False
                        ).reset_index().demand.plot(ax=ax)
    HD_2035.sort_values('demand', ascending=False
                        ).reset_index().demand.plot(ax=ax)
    HD_2050.sort_values('demand', ascending=False
                        ).reset_index().demand.plot(ax=ax)
    plt.savefig('complete_HeatDemandDensities_Curves.png')

    add_metadata()

    return None
