"""The central module containing all code dealing with power to heat
"""
import pandas as pd
import geopandas as gpd
from egon.data import db, config
from shapely.geometry import LineString

def insert_individual_power_to_heat(scenario='eGon2035'):
    """ Insert power to heat into database

    Parameters
    ----------
    scenario : str, optional
        Name of the scenario The default is 'eGon2035'.

    Returns
    -------
    None.

    """

    sources = config.datasets()['etrago_heat']['sources']
    targets = config.datasets()['etrago_heat']['targets']

    # Delete existing entries
    db.execute_sql(
        f"""
        DELETE FROM {targets['heat_links']['schema']}.
        {targets['heat_links']['table']}
        WHERE carrier = 'individual_heat_pump'
        """)

    # Select heat pumps for individual heating
    heat_pumps = db.select_dataframe(
        f"""
        SELECT mv_grid_id as power_bus,
        a.carrier, capacity, b.bus_id as heat_bus
        FROM {sources['individual_heating_supply']['schema']}.
            {sources['individual_heating_supply']['table']} a
        JOIN {targets['heat_buses']['schema']}.
        {targets['heat_buses']['table']} b
        ON ST_Intersects(
            ST_Buffer(ST_Transform(ST_Centroid(a.geometry), 4326), 0.00000001),
            geom)
        WHERE scenario = '{scenario}'
        AND scn_name  = '{scenario}'
        AND a.carrier = 'heat_pump'
        AND b.carrier = 'rural_heat'
        """)

    # Assign voltage level
    heat_pumps['voltage_level'] = 7

    # Insert heatpumps
    insert_power_to_heat_per_level(
        heat_pumps,
        carrier = 'individual_heat_pump',
        multiple_per_mv_grid=False,
        scenario='eGon2035')


def insert_central_power_to_heat(scenario='eGon2035'):
    """ Insert power to heat in district heating areas into database

    Parameters
    ----------
    scenario : str, optional
        Name of the scenario The default is 'eGon2035'.

    Returns
    -------
    None.

    """

    sources = config.datasets()['etrago_heat']['sources']
    targets = config.datasets()['etrago_heat']['targets']

    # Delete existing entries
    db.execute_sql(
        f"""
        DELETE FROM {targets['heat_links']['schema']}.
        {targets['heat_links']['table']}
        WHERE carrier = 'central_heat_pump'
        """)
    # Select heat pumps in district heating
    central_heat_pumps = db.select_geodataframe(
        f"""
        SELECT * FROM {sources['district_heating_supply']['schema']}.
            {sources['district_heating_supply']['table']}
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
        scenario='eGon2035')
    # Insert heat pumps in hv grid
    # (as many hvmv substations as intersect with district heating grid)
    insert_power_to_heat_per_level(
        central_heat_pumps[central_heat_pumps.voltage_level<3],
        multiple_per_mv_grid=True,
        scenario='eGon2035')


def insert_power_to_heat_per_level(heat_pumps, multiple_per_mv_grid,
                                   carrier = 'central_heat_pump',
                                   scenario='eGon2035'):
    """ Insert power to heat plants per grid level

    Parameters
    ----------
    heat_pumps : pandas.DataFrame
        Heat pumps in selected grid level
    multiple_per_mv_grid : boolean
        Choose if one district heating areas is supplied by one hvmv substation
    scenario : str, optional
        Name of the scenario The default is 'eGon2035'.

    Returns
    -------
    None.

    """
    sources = config.datasets()['etrago_heat']['sources']
    targets = config.datasets()['etrago_heat']['targets']

    if 'central' in carrier:
        # Calculate heat pumps per electrical bus
        gdf = assign_electrical_bus(heat_pumps, multiple_per_mv_grid)

    else:
        gdf = heat_pumps.copy()

    # Select geometry of buses
    geom_buses = db.select_geodataframe(
        f"""
        SELECT bus_id, geom FROM {targets['heat_buses']['schema']}.
        {targets['heat_buses']['table']}
        WHERE scn_name = '{scenario}'
        """,
        index_col='bus_id',
        epsg=4326)

    # Create topology of heat pumps
    gdf['geom_power'] = geom_buses.geom[gdf.power_bus].values
    gdf['geom_heat'] = geom_buses.loc[gdf.heat_bus, 'geom'].reset_index().geom
    gdf['geometry']=gdf.apply(
            lambda x: LineString([x['geom_power'], x['geom_heat']]),axis=1)

    # Choose next unused link id
    next_link_id = db.next_etrago_id('link')

    # Initilize dataframe of links
    links = gpd.GeoDataFrame(
        index = range(len(gdf)),
        columns = [
            'scn_name', 'bus0', 'bus1',
            'carrier', 'link_id', 'p_nom', 'topo'],
        data = {'scn_name': scenario,
                'carrier': carrier}
        ).set_geometry('topo').set_crs(epsg=4326)

    # Insert values into dataframe
    links.bus0 = gdf.power_bus.values
    links.bus1 = gdf.heat_bus.values
    links.p_nom = gdf.capacity.values
    links.topo = gdf.geometry.values
    links.link_id = range(next_link_id, next_link_id+len(links))

    # Insert data into database
    links.to_postgis(targets['heat_links']['table'],
                     schema=targets['heat_links']['schema'],
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

    sources = config.datasets()['etrago_heat']['sources']
    targets = config.datasets()['etrago_heat']['targets']

    # Map heat buses to district heating id and area_id
    heat_buses = db.select_dataframe(
        f"""
        SELECT bus_id, area_id, id FROM
        {targets['heat_buses']['schema']}.
        {targets['heat_buses']['table']}
        JOIN {sources['district_heating_areas']['schema']}.
            {sources['district_heating_areas']['table']}
        ON ST_Transform(ST_Centroid(geom_polygon), 4326) = geom
        WHERE carrier = 'central_heat'
        AND scenario='eGon2035'
        """,
        index_col='id')

    heat_pumps['power_bus'] = ''

    # Select mv grid distrcits
    mv_grid_district = db.select_geodataframe(
        f"""
        SELECT subst_id, geom FROM
        {sources['mv_grid_districts']['schema']}.
        {sources['mv_grid_districts']['table']}
        """)

    # Map zensus cells to district heating areas
    map_zensus_dh = db.select_geodataframe(
        f"""
        SELECT area_id, a.zensus_population_id,
        geom_point as geom, sum(a.demand) as demand
        FROM {sources['map_district_heating_areas']['schema']}.
            {sources['map_district_heating_areas']['table']} b
        JOIN {sources['heat_demand']['schema']}.
            {sources['heat_demand']['table']} a
        ON b.zensus_population_id = a.zensus_population_id
        JOIN society.destatis_zensus_population_per_ha
        ON society.destatis_zensus_population_per_ha.id =
        a.zensus_population_id
        WHERE a.scenario = 'eGon2035'
        AND b.scenario = 'eGon2035'
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

