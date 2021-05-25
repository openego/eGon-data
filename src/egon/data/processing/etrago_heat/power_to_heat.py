"""The central module containing all code dealing with power to heat
"""
import pandas as pd
import geopandas as gpd
from egon.data import db
from shapely.geometry import LineString

def next_id(component):
    """ Select next id value for components in pf-tables

    Parameters
    ----------
    component : str
        Name of componenet

    Returns
    -------
    next_id : int
        Next index value

    """
    max_id = db.select_dataframe(
        f"""
        SELECT MAX({component}_id) FROM grid.egon_pf_hv_{component}
        """)['max'][0]

    if max_id:
        next_id = max_id + 1
    else:
        next_id = 1

    return next_id



def insert_central_power_to_heat(version = '0.0.0', scenario='eGon2035'):
    """ Insert power to heat in district heating areas into database

    Parameters
    ----------
    version : str, optional
        Version number. The default is '0.0.0'.
    scenario : str, optional
        Name of the scenario The default is 'eGon2035'.

    Returns
    -------
    None.

    """
    # Delete existing entries
    db.execute_sql(
        """
        DELETE FROM grid.egon_pf_hv_link
        WHERE carrier = 'central_heat_pump'
        """)
    # Select heat pumps in district heating
    central_heat_pumps = db.select_geodataframe(
        f"""
        SELECT * FROM supply.egon_district_heating
        WHERE scenario = '{scenario}'
        AND carrier = 'heat_pump'
        """,
        geom_col='geometry')

    # Assign voltage level
    central_heat_pumps = assign_voltage_level(central_heat_pumps)

    # Insert heatpumps in mv and below
    # (one hvmv substation per district heating grid)
    insert_power_to_heat_per_level(
        central_heat_pumps[central_heat_pumps.voltage_level>3],
        multiple_per_mv_grid=False,
        version = '0.0.0', scenario='eGon2035')
    # Insert heat pumps in hv grid
    # (as many hvmv substations as intersect with district heating grid)
    insert_power_to_heat_per_level(
        central_heat_pumps[central_heat_pumps.voltage_level<3],
        multiple_per_mv_grid=True,
        version = '0.0.0', scenario='eGon2035')


def insert_power_to_heat_per_level(heat_pumps, multiple_per_mv_grid,
                                   version = '0.0.0', scenario='eGon2035'):
    """ Insert power to heat plants per grid level

    Parameters
    ----------
    heat_pumps : pandas.DataFrame
        Heat pumps in selected grid level
    multiple_per_mv_grid : boolean
        Choose if one district heating areas is supplied by one hvmv substation
    version : str, optional
        Version number. The default is '0.0.0'.
    scenario : str, optional
        Name of the scenario The default is 'eGon2035'.

    Returns
    -------
    None.

    """

    # Calculate heat pumps per electrical bus
    gdf = assign_electrical_bus(heat_pumps, multiple_per_mv_grid)

    # Select geometry of buses
    geom_buses = db.select_geodataframe(
        """
        SELECT bus_id, geom FROM grid.egon_pf_hv_bus
        """,
        index_col='bus_id',
        epsg=4326)

    # Create topology of heat pumps
    gdf['geom_power'] = geom_buses.geom[gdf.power_bus].values
    gdf['geom_heat'] = geom_buses.loc[gdf.heat_bus, 'geom'].reset_index().geom
    gdf['geometry']=gdf.apply(
            lambda x: LineString([x['geom_power'], x['geom_heat']]),axis=1)

    # Choose next unused link id
    next_link_id = next_id('link')

    # Initilize dataframe of links
    links = gpd.GeoDataFrame(
        index = range(len(gdf)),
        columns = [
            'version', 'scn_name', 'bus0', 'bus1',
            'carrier', 'link_id', 'p_nom', 'topo'],
        data = {'version': version, 'scn_name': scenario,
                'carrier': 'central_heat_pump'}
        ).set_geometry('topo').set_crs(epsg=4326)

    # Insert values into dataframe
    links.bus0 = gdf.power_bus.values
    links.bus1 = gdf.heat_bus.values
    links.p_nom = gdf.capacity.values
    links.topo = gdf.geometry.values
    links.link_id = range(next_link_id, next_link_id+len(links))

    # Insert data into database
    links.to_postgis('egon_pf_hv_link',
                     schema='grid',
                     if_exists = 'append',
                     con=db.engine())

def assign_voltage_level(heat_pumps):
    """ Assign voltage level to heat pumps

    Parameters
    ----------
    heat_pumps : pandas.DataFrame
        Heat pumps without voltage level

    Returns
    -------
    heat_pumps : pandas.DataFrame
        Heat pumps including voltage level

    """
    # set voltage level for heat pumps according to category
    heat_pumps['voltage_level'] = 0

    heat_pumps.loc[
        heat_pumps[(heat_pumps.carrier=='heat_pump')
                             & (heat_pumps.category=='small')].index
        , 'voltage_level'] = 7

    heat_pumps.loc[
        heat_pumps[(heat_pumps.carrier=='heat_pump')
                             & (heat_pumps.category=='medium')].index
        , 'voltage_level'] = 5

    heat_pumps.loc[
        heat_pumps[(heat_pumps.carrier=='heat_pump')
                             & (heat_pumps.category=='large')].index
        , 'voltage_level'] = 1

    # if capacity > 5.5 MW, heatpump is installed in HV
    heat_pumps.loc[
        heat_pumps[(heat_pumps.carrier=='heat_pump')
                             & (heat_pumps.capacity>5.5)].index
        , 'voltage_level'] = 1

    return heat_pumps

def assign_electrical_bus(heat_pumps, multiple_per_mv_grid=False):
    """ Calculates heat pumps per electrical bus

    Parameters
    ----------
    heat_pumps : pandas.DataFrame
        Heat pumps including voltage level
    multiple_per_mv_grid : boolean, optional
        Choose if a district heating area can by supplied by multiple
        hvmv substaions/mv grids. The default is False.

    Returns
    -------
    gdf : pandas.DataFrame
        Heat pumps per electrical bus

    """

    # Map heat buses to district heating id and area_id
    heat_buses = db.select_dataframe(
        """
        SELECT bus_id, area_id, id FROM grid.egon_pf_hv_bus
        JOIN demand.district_heating_areas
        ON ST_Transform(ST_Centroid(geom_polygon), 4326) = geom
        WHERE carrier = 'central_heat'
        AND scenario='eGon2035'
        """,
        index_col='id')

    heat_pumps['power_bus'] = ''

    # Select mv grid distrcits
    mv_grid_district = db.select_geodataframe(
        """
        SELECT subst_id, geom FROM grid.mv_grid_districts
        """)

    # Map zensus cells to district heating areas
    map_zensus_dh = db.select_geodataframe(
        """
        SELECT area_id, a.zensus_population_id,
        geom_point as geom, sum(a.demand) as demand
        FROM demand.map_zensus_district_heating_areas
        JOIN demand.egon_peta_heat a
        ON demand.map_zensus_district_heating_areas.zensus_population_id =
        a.zensus_population_id
        JOIN society.destatis_zensus_population_per_ha
        ON society.destatis_zensus_population_per_ha.id =
        a.zensus_population_id
        WHERE a.scenario = 'eGon2035'
        AND demand.map_zensus_district_heating_areas.scenario = 'eGon2035'
        GROUP BY (area_id, a.zensus_population_id, geom_point)
        """)

    # Select area_id per heat pump
    heat_pumps['area_id'] = heat_buses.area_id[
        heat_pumps.district_heating_id.values].values

    heat_buses.set_index('area_id', inplace=True)

    # Select only cells in choosen district heating areas
    cells = map_zensus_dh[map_zensus_dh.area_id.isin(heat_pumps.area_id)]

    # Assign power bus per zensus cell
    cells['power_bus'] = gpd.sjoin(cells, mv_grid_district,
                     how='inner', op='intersects').subst_id

    # Calclate district heating demand per substaion
    demand_per_substation = pd.DataFrame(
        cells.groupby(['area_id', 'power_bus']).demand.sum())

    heat_pumps.set_index('area_id', inplace=True)

    # If district heating areas are supplied by multiple hvmv-substations,
    # create one heatpup per electrical bus.
    # The installed capacity is assigned regarding the share of heat demand.
    if multiple_per_mv_grid:

        power_to_heat = demand_per_substation.reset_index()

        power_to_heat.loc[:, 'carrier'] = 'urban_central_heat_pump'

        power_to_heat.loc[:, 'voltage_level'] = heat_pumps.voltage_level[
            power_to_heat.area_id].values

        power_to_heat['share_demand'] = power_to_heat.groupby(
            'area_id').demand.apply(lambda grp: grp/grp.sum())

        power_to_heat['capacity'] = power_to_heat['share_demand'].mul(
            heat_pumps.capacity[power_to_heat.area_id].values)

        power_to_heat = power_to_heat[power_to_heat.voltage_level.notnull()]


        gdf = gpd.GeoDataFrame(power_to_heat, index = power_to_heat.index,
                               geometry = heat_pumps.geometry[
                                       power_to_heat.area_id].values)

    # If district heating areas are supplied by one hvmv-substations,
    # the hvmv substation which has the most heat demand is choosen.
    else:

        substation_max_demand = demand_per_substation.reset_index(
            ).set_index('power_bus').groupby('area_id').demand.max()

        selected_substations = demand_per_substation[
            demand_per_substation.demand.isin(
                substation_max_demand)].reset_index().set_index('area_id')

        selected_substations.rename({'demand': 'demand_selected_substation'},
                                    axis=1, inplace=True)

        selected_substations['share_demand'] = cells.groupby(
            ['area_id', 'power_bus']).demand.sum().reset_index().groupby(
                'area_id').demand.max()/cells.groupby(
                    ['area_id', 'power_bus']).demand.sum(
                        ).reset_index().groupby('area_id').demand.sum()

        power_to_heat = selected_substations

        power_to_heat.loc[:, 'carrier'] = 'urban_central_heat_pump'

        power_to_heat.loc[:, 'voltage_level'] = heat_pumps.voltage_level

        power_to_heat['capacity'] = heat_pumps.capacity[
            power_to_heat.index].values

        power_to_heat = power_to_heat[power_to_heat.voltage_level.notnull()]

        gdf = gpd.GeoDataFrame(power_to_heat, index = power_to_heat.index,
                               geometry = heat_pumps.geometry)

    gdf.reset_index(inplace=True)

    gdf['heat_bus'] = heat_buses.loc[
            gdf.area_id, 'bus_id'].reset_index().bus_id

    return gdf

