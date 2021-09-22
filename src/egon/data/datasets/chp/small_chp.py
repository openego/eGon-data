"""
The module containing all code dealing with chp < 10MW.
"""
from egon.data import db, config
from egon.data.datasets.power_plants import (
    assign_bus_id, assign_gas_bus_id, filter_mastr_geometry, select_target)
from sqlalchemy.orm import sessionmaker
import geopandas as gpd
import numpy as np

def insert_mastr_chp(mastr_chp, EgonChp):
    """Insert MaStR data from exising CHPs into database table

    Parameters
    ----------
    mastr_chp : pandas.DataFrame
        List of existing CHPs in MaStR.
    EgonChp : class
        Class definition of daabase table for CHPs

    Returns
    -------
    None.

    """

    session = sessionmaker(bind=db.engine())()
    for i, row in mastr_chp.iterrows():
        entry = EgonChp(
                sources={
                    "chp": "MaStR",
                    "el_capacity": "MaStR",
                    "th_capacity": "MaStR",
                },
                source_id={"MastrNummer": row.EinheitMastrNummer},
                carrier='gas',
                el_capacity=row.el_capacity,
                th_capacity= row.th_capacity,
                electrical_bus_id = row.bus_id,
                ch4_bus_id = row.gas_bus_id,
                district_heating=row.district_heating,
                voltage_level=row.voltage_level,
                scenario='eGon2035',
                geom=f"SRID=4326;POINT({row.geometry.x} {row.geometry.y})",
            )
        session.add(entry)
    session.commit()

def existing_chp_smaller_10mw(sources, MaStR_konv, EgonChp):
    """ Insert existing small CHPs based on MaStR and target values

    Parameters
    ----------
    MaStR_konv : pandas.DataFrame
        List of conevntional CHPs in MaStR whoes locateion is not used
    EgonChp : class
        Class definition of daabase table for CHPs

    Returns
    -------
    additional_capacitiy : pandas.Series
        Capacity of new locations for small chp per federal state

    """


    existsting_chp_smaller_10mw = MaStR_konv[
        #(MaStR_konv.Nettonennleistung>0.1)
        (MaStR_konv.el_capacity<=10)
        &(MaStR_konv.th_capacity>0)]

    targets = select_target('small_chp', 'eGon2035')

    for federal_state in targets.index:
        mastr_chp = gpd.GeoDataFrame(
            filter_mastr_geometry(existsting_chp_smaller_10mw, federal_state))

        mastr_chp.crs = "EPSG:4326"

        # Assign gas bus_id
        mastr_chp_c = mastr_chp.copy()
        mastr_chp['gas_bus_id'] = assign_gas_bus_id(mastr_chp_c).gas_bus_id

        # Assign bus_id
        mastr_chp['bus_id'] = assign_bus_id(
            mastr_chp, config.datasets()["chp_location"]).bus_id

        mastr_chp = assign_use_case(mastr_chp, sources)

        insert_mastr_chp(mastr_chp, EgonChp)

def extension_to_areas(areas, additional_capacity, existing_chp, flh, EgonChp,
                       district_heating = True):
    """ Builds new CHPs on potential industry or district heating areas.

    This method can be used to distrectly extend and spatial allocate CHP
    for industry or district heating areas.
    The following steps are running in a loop until the additional
    capacity is reached:

        1. Randomly select an existing CHP < 10MW and its parameters.

        2. Select possible areas where the CHP can be located.
        It is assumed that CHPs are only build if the demand of the industry
        or district heating grid exceeds the annual energy output of the CHP.
        The energy output is calculated using the installed capacity and
        estimated full load hours.
        The thermal output is used for district heating areas. Since there are
        no explicit heat demands for industry, the electricity output and
        demands are used.

        3. Randomly select one of the possible areas.
        The areas are weighted by the annal demand, assuming that the
        possibility of building a CHP plant is higher when for large consumers.

        4. Insert allocated CHP plant into the database

        5. Substract capacity of new build CHP from the additional capacity.
        The energy demands of the areas are reduced by the estimated energy
        output of the CHP plant.

    Parameters
    ----------
    areas : geopandas.GeoDataFrame
        Possible areas for a new CHP plant, including their energy demand
    additional_capacity : float
        Overall eletcrical capacity of CHPs that should be build in MW.
    existing_chp : pandas.DataFrame
        List of existing CHP plants including electrical and thermal capacity
    flh : int
        Assumed electrical or thermal full load hours.
    EgonChp : class
        ORM-class definition of CHP database-table.
    district_heating : boolean, optional
        State if the areas are district heating areas. The default is True.

    Returns
    -------
    None.

    """
    session = sessionmaker(bind=db.engine())()

    np.random.seed(seed=config.settings()['egon-data']['--random-seed'])

    # n = 0
    # Add new CHP as long as the additional capacity is not reached
    while additional_capacity > existing_chp.el_capacity.min():

        # # Break loop after 500 iterations without a fitting CHP
        # if n > 500:
        #     print(
        #         f'{additional_capacity} MW are not matched to an area.')
        #     break
        if district_heating:
            possible_areas = areas[
                    areas.demand
                    > existing_chp.th_capacity.min()*flh].to_crs(4326)
        else:
            possible_areas = areas[
                    areas.demand
                    > existing_chp.el_capacity.min()*flh].to_crs(4326)


        if len(possible_areas) > 0:
            # Assign gas bus_id
            possible_areas['gas_bus_id'] = assign_gas_bus_id(
                possible_areas.copy()).gas_bus_id

            # Select randomly one area from the list of possible areas
            # weighted by the share of demand
            id_area = np.random.choice(
                possible_areas.index,
                p = possible_areas.demand/possible_areas.demand.sum())
            selected_area = possible_areas[possible_areas.index==id_area]

            if district_heating:
                possible_chp = existing_chp[
                    (existing_chp.th_capacity*flh<selected_area.demand.values[0])
                    &(existing_chp.el_capacity<= additional_capacity)]
            else:
                possible_chp = existing_chp[
                    (existing_chp.el_capacity*flh<selected_area.demand.values[0])
                    &(existing_chp.el_capacity<= additional_capacity)]

            # Select random new build CHP from list of existing CHP
            # which is smaller than the remaining capacity to distribute
            id_chp = np.random.choice(range(len(possible_chp)))
            selected_chp = possible_chp.iloc[id_chp]

            # Assign bus_id
            selected_area['voltage_level'] = selected_chp.voltage_level

            selected_area['bus_id'] = assign_bus_id(
                selected_area,
                config.datasets()["chp_location"]).bus_id

        # # Select random new build CHP from list of existing CHP
        # # which is smaller than the remaining capacity to distribute
        # id_chp = np.random.choice(range(len(existing_chp[
        #     existing_chp.el_capacity <= additional_capacity])))
        # selected_chp = existing_chp[
        #     existing_chp.el_capacity <= additional_capacity].iloc[id_chp]

        # # Select areas whoes remaining demand, which is not
        # # covered by another CHP, fits to the selected CHP
        # if district_heating:
        #     possible_areas = areas[
        #             areas.demand
        #             > selected_chp.th_capacity*flh].to_crs(4326)
        # else:
        #     possible_areas = areas[
        #             areas.demand
        #             > selected_chp.el_capacity*flh].to_crs(4326)


        # # If there is no district heating area whoes demand (not covered by
        # # another CHP) fit to the CHP, quit and select another CHP
        # if len(possible_areas) > 0:

        #     # Assign gas bus_id
        #     possible_areas['gas_bus_id'] = assign_gas_bus_id(
        #         possible_areas.copy()).gas_bus_id

        #     # Assign bus_id
        #     possible_areas['voltage_level'] = selected_chp.voltage_level
        #     possible_areas['bus_id'] = assign_bus_id(
        #         possible_areas, config.datasets()["chp_location"]).bus_id

        #     # Select randomly one area from the list of possible areas
        #     # weighted by the share of demand
        #     id_area = np.random.choice(
        #         range(len(possible_areas)),
        #         p = possible_areas.demand/possible_areas.demand.sum())
        #     selected_area = possible_areas.iloc[id_area]

            entry = EgonChp(
                        sources={
                            "chp": "MaStR",
                            "el_capacity": "MaStR",
                            "th_capacity": "MaStR",
                            "CHP extension algorithm" : ""
                        },
                        carrier='gas extended',
                        el_capacity=selected_chp.el_capacity,
                        th_capacity= selected_chp.th_capacity,
                        district_heating=district_heating,
                        voltage_level=selected_chp.voltage_level,
                        electrical_bus_id = int(selected_area.bus_id),
                        gas_bus_id = int(selected_area.gas_bus_id),
                        scenario='eGon2035',
                        geom=f"""
                        SRID=4326;
                        POINT({selected_area.geom.values[0].x} {selected_area.geom.values[0].y})
                        """,
                    )
            if district_heating:
                entry.district_heating_area_id = int(selected_area.area_id)

            session.add(entry)
            session.commit()

            # Reduce additional capacity by newly build CHP
            additional_capacity -= selected_chp.el_capacity

            # Reduce the demand of the selected area by the estimated
            # enrgy output of the CHP
            if district_heating:
                areas.loc[
                    areas.index[areas.area_id == selected_area.area_id.values[0]],
                    'demand'] -= selected_chp.th_capacity*flh
            else:
                areas.loc[
                areas.index[areas.osm_id == selected_area.osm_id.values[0]],
                'demand'] -= selected_chp.th_capacity*flh
            areas = areas[areas.demand > 0]

        else:
            print(
                f'{additional_capacity} MW are not matched to an area.')
            break

def extension_district_heating(
        federal_state, additional_capacity, flh_chp, EgonChp,
        areas_without_chp_only=True):
    """ Build new CHP < 10 MW for district areas considering existing CHP
    and the heat demand.

    For more details on the placement alogrithm have a look at the description
    of extension_to_areas().

    Parameters
    ----------
    federal_state : str
        Name of the federal state.
    additional_capacity : float
        Additional electrical capacity of new CHP plants in district heating
    flh_chp : int
        Assumed number of full load hours of heat output.
    EgonChp : class
        ORM-class definition of CHP database-table.
    areas_without_chp_only : boolean, optional
        Set if CHPs are only assigned to district heating areas which don't
        have an existing CHP. The default is True.

    Returns
    -------
    None.

    """

    sources = config.datasets()["chp_location"]["sources"]
    targets = config.datasets()["chp_location"]["targets"]

    existing_chp = db.select_dataframe(
        f"""
        SELECT el_capacity, th_capacity, voltage_level, b.area_id
        FROM
        supply.egon_chp a,
        {sources['district_heating_areas']['schema']}.
        {sources['district_heating_areas']['table']} b
        WHERE a.scenario = 'eGon2035'
        AND b.scenario = 'eGon2035'
        AND district_heating = True
        AND ST_Intersects(
            ST_Transform(
                ST_Centroid(geom_polygon), 4326),
            (SELECT ST_Union(geometry)
             FROM {sources['vg250_lan']['schema']}.
             {sources['vg250_lan']['table']}
            WHERE REPLACE(REPLACE(gen, '-', ''), 'ü', 'ue') ='{federal_state}'))
        AND el_capacity < 10
        ORDER BY el_capacity, residential_and_service_demand

        """)

    # Select all district heating areas without CHP
    dh_areas = db.select_geodataframe(
        f"""
        SELECT
        residential_and_service_demand as demand, area_id,
        ST_Transform(ST_PointOnSurface(geom_polygon), 4326)  as geom
        FROM
        {sources['district_heating_areas']['schema']}.
        {sources['district_heating_areas']['table']}
        WHERE scenario = 'eGon2035'
        AND ST_Intersects(ST_Transform(ST_Centroid(geom_polygon), 4326), (
            SELECT ST_Union(d.geometry)
            FROM
            {sources['vg250_lan']['schema']}.{sources['vg250_lan']['table']} d
            WHERE REPLACE(REPLACE(gen, '-', ''), 'ü', 'ue') ='{federal_state}'))
        AND area_id NOT IN (
            SELECT district_heating_area_id
            FROM {targets['chp_table']['schema']}.
            {targets['chp_table']['table']}
            WHERE scenario = 'eGon2035'
            AND district_heating = TRUE)
        """)

    extension_to_areas(dh_areas, additional_capacity, existing_chp, flh_chp,
                       EgonChp, district_heating = True)

    if not areas_without_chp_only:
        # Append district heating areas with CHP
        # assumed dispatch of existing CHP is substracted from remaining demand
        dh_areas = dh_areas.append(
            db.select_geodataframe(
                f"""
                SELECT
                b.residential_and_service_demand - sum(a.el_capacity)*{flh_chp}
                as demand, b.area_id,
                ST_Transform(ST_Centroid(geom_polygon), 4326) as geom
                FROM
                {targets['chp_table']['schema']}.
                {targets['chp_table']['table']} a,
                {sources['district_heating_areas']['schema']}.
                {sources['district_heating_areas']['table']} b
                WHERE b.scenario = 'eGon2035'
                AND a.scenario = 'eGon2035'
                AND ST_Intersects(
                    ST_Transform(ST_Centroid(geom_polygon), 4326),
                    (SELECT ST_Union(d.geometry)
                     FROM {sources['vg250_lan']['schema']}.
                      {sources['vg250_lan']['table']} d
                    WHERE REPLACE(REPLACE(gen, '-', ''), 'ü', 'ue') ='{federal_state}'))
                AND a.district_heating_area_id = b.area_id
                GROUP BY (
                    b.residential_and_service_demand,
                    b.area_id, geom_polygon)
                """),ignore_index=True
                )


def extension_industrial(federal_state, additional_capacity, flh_chp, EgonChp):
    """ Build new CHP < 10 MW for industry considering existing CHP,
    osm landuse areas and electricity demands.

    For more details on the placement alogrithm have a look at the description
    of extension_to_areas().

    Parameters
    ----------
    federal_state : str
        Name of the federal state.
    additional_capacity : float
        Additional electrical capacity of new CHP plants in indsutry.
    flh_chp : int
        Assumed number of full load hours of electricity output.
    EgonChp : class
        ORM-class definition of CHP database-table.

    Returns
    -------
    None.

    """

    sources = config.datasets()["chp_location"]["sources"]
    targets = config.datasets()["chp_location"]["targets"]


    existing_chp = db.select_dataframe(
        f"""
        SELECT el_capacity, th_capacity, voltage_level
        FROM
        {targets['chp_table']['schema']}.
        {targets['chp_table']['table']} a
        WHERE a.scenario = 'eGon2035'
        AND district_heating = False
        AND el_capacity < 10
        ORDER BY el_capacity

        """)


    # Select all industrial areas without CHP
    industry_areas = db.select_geodataframe(
        f"""
        SELECT
        SUM(demand) as demand, a.osm_id, ST_Centroid(b.geom) as geom, b.name
        FROM
        {sources['industrial_demand_osm']['schema']}.
        {sources['industrial_demand_osm']['table']} a,
        {sources['osm_landuse']['schema']}.
        {sources['osm_landuse']['table']} b
        WHERE a.scenario = 'eGon2035'
        AND b.gid = a.osm_id
        AND NOT ST_Intersects(
            ST_Transform(b.geom, 4326),
            (SELECT ST_Union(geom) FROM
              {targets['chp_table']['schema']}.
              {targets['chp_table']['table']}
              ))
        AND b.tags::json->>'landuse' = 'industrial'
        AND b.name NOT LIKE '%%kraftwerk%%'
        AND b.name NOT LIKE '%%Stadtwerke%%'
        AND b.name NOT LIKE '%%Müllverbrennung%%'
        AND b.name NOT LIKE '%%Müllverwertung%%'
        AND b.name NOT LIKE '%%Abfall%%'
        AND b.name NOT LIKE '%%Kraftwerk%%'
        AND b.name NOT LIKE '%%Wertstoff%%'
        AND b.name NOT LIKE '%%olarpark%%'
        AND b.name NOT LIKE '%%Gewerbegebiet%%'
        AND b.name NOT LIKE '%%Gewerbepark%%'
        AND ST_Intersects(
            ST_Transform(ST_Centroid(b.geom), 4326),
            (SELECT ST_Union(d.geometry)
             FROM {sources['vg250_lan']['schema']}.
             {sources['vg250_lan']['table']} d
             WHERE REPLACE(REPLACE(gen, '-', ''), 'ü', 'ue') ='{federal_state}'))

        GROUP BY (a.osm_id, b.geom, b.name)
        ORDER BY SUM(demand)

        """)

    extension_to_areas(industry_areas, additional_capacity, existing_chp,
                       flh_chp, EgonChp, district_heating = False)


def extension_per_federal_state(additional_capacity, federal_state, EgonChp):
    """Adds new CHP plants to meet target value per federal state.

    The additional capacity for CHPs < 10 MW is distributed discretly.
    Therefore, existing CHPs and their parameters from Marktstammdatenregister
    are randomly selected and allocated in a district heating grid.
    In order to generate a reasonable distribution, new CHPs can only
    be assigned to a district heating grid which needs additional supply
    technologies. This is estimated by the substraction of demand, and the
    assumed dispatch oof a CHP considering the capacitiy and full load hours
    of each CHPs.

    Parameters
    ----------
    additional_capacity : float
        Capacity to distribute.
    federal_state : str
        Name of the federal state
    EgonChp : class
        ORM-class definition of CHP table

    Returns
    -------
    None.

    """
    sources = config.datasets()["chp_location"]["sources"]
    targets = config.datasets()["chp_location"]["targets"]

    flh_chp = 6000
    dh_areas_demand = db.select_dataframe(
        f"""
        SELECT SUM(residential_and_service_demand) as demand
        FROM
        {sources['district_heating_areas']['schema']}.
        {sources['district_heating_areas']['table']}
        WHERE scenario = 'eGon2035'
        AND ST_Intersects(ST_Transform(ST_Centroid(geom_polygon), 4326), (
            SELECT ST_Union(d.geometry)
            FROM
            {sources['vg250_lan']['schema']}.{sources['vg250_lan']['table']} d
            WHERE REPLACE(REPLACE(gen, '-', ''), 'ü', 'ue') ='{federal_state}'))
        AND area_id NOT IN (
            SELECT district_heating_area_id
            FROM {targets['chp_table']['schema']}.
            {targets['chp_table']['table']}
            WHERE scenario = 'eGon2035'
            AND district_heating = TRUE)
        """).demand[0]

    heat_elec_ratio_industry = 2.5 # According to Flexibilisierung der Kraft-Wärme-Kopplung, figure 6-3

    industry_heat_demand = db.select_dataframe(
        f"""
        SELECT
        SUM(demand) as demand
        FROM
        {sources['industrial_demand_osm']['schema']}.
        {sources['industrial_demand_osm']['table']} a,
        {sources['osm_landuse']['schema']}.
        {sources['osm_landuse']['table']} b
        WHERE a.scenario = 'eGon2035'
        AND b.gid = a.osm_id
        AND NOT ST_Intersects(
            ST_Transform(b.geom, 4326),
            (SELECT ST_Union(geom) FROM
              {targets['chp_table']['schema']}.
              {targets['chp_table']['table']}
              ))
        AND b.tags::json->>'landuse' = 'industrial'
        AND b.name NOT LIKE '%%kraftwerk%%'
        AND b.name NOT LIKE '%%Stadtwerke%%'
        AND b.name NOT LIKE '%%Müllverbrennung%%'
        AND b.name NOT LIKE '%%Müllverwertung%%'
        AND b.name NOT LIKE '%%Abfall%%'
        AND b.name NOT LIKE '%%Kraftwerk%%'
        AND b.name NOT LIKE '%%Wertstoff%%'
        AND b.name NOT LIKE '%%olarpark%%'
        AND b.name NOT LIKE '%%Gewerbegebiet%%'
        AND b.name NOT LIKE '%%Gewerbepark%%'
        AND ST_Intersects(
            ST_Transform(ST_Centroid(b.geom), 4326),
            (SELECT ST_Union(d.geometry)
             FROM {sources['vg250_lan']['schema']}.
             {sources['vg250_lan']['table']} d
             WHERE REPLACE(REPLACE(gen, '-', ''), 'ü', 'ue') ='{federal_state}'))
        """).demand[0]*heat_elec_ratio_industry



    share_dh = dh_areas_demand /(dh_areas_demand+industry_heat_demand)
    print(f"Distributing {additional_capacity} MW in {federal_state}")
    print(f"Distributing {additional_capacity*share_dh} MW to district heating")
    extension_district_heating(
        federal_state, additional_capacity*share_dh, flh_chp, EgonChp)
    print(f"Distributing {additional_capacity*(1-share_dh)} MW to industry")
    extension_industrial(
        federal_state, additional_capacity*(1-share_dh), flh_chp, EgonChp)


def assign_use_case(chp, sources):
    """Identifies CHPs used in district heating areas.

    A CHP plant is assigned to a district heating area if
    - it is closer than 1km to the borders of the district heating area
    - the name of the osm landuse area where the CHP is located indicates
    that it feeds in to a district heating area (e.g. 'Stadtwerke')
    - it is not closer than 100m to an industrial area

    Parameters
    ----------
    chp : pandas.DataFrame
        CHPs without district_heating flag

    Returns
    -------
    chp : pandas.DataFrame
        CHPs with identification of district_heating CHPs


    """
    # Select osm industrial areas which don't include power or heat supply
    # (name not includes 'Stadtwerke', 'Kraftwerk', 'Müllverbrennung'...)
    landuse_industrial = db.select_geodataframe(
        f"""
        SELECT ST_Buffer(geom, 100) as geom,
         tags::json->>'name' as name
         FROM {sources['osm_landuse']['schema']}.
        {sources['osm_landuse']['table']}
        WHERE tags::json->>'landuse' = 'industrial'
        AND(name NOT LIKE '%%kraftwerk%%'
        OR name NOT LIKE '%%Müllverbrennung%%'
        OR name LIKE '%%Müllverwertung%%'
        OR name NOT LIKE '%%Abfall%%'
        OR name NOT LIKE '%%Kraftwerk%%'
        OR name NOT LIKE '%%Wertstoff%%')
        """,
        epsg=4326)

    # Select osm polygons where a district heating chp is likely
    # (name includes 'Stadtwerke', 'Kraftwerk', 'Müllverbrennung'...)
    possible_dh_locations= db.select_geodataframe(
        f"""
        SELECT ST_Buffer(geom, 100) as geom,
         tags::json->>'name' as name
         FROM {sources['osm_polygon']['schema']}.
        {sources['osm_polygon']['table']}
        WHERE name LIKE '%%Stadtwerke%%'
        OR name LIKE '%%kraftwerk%%'
        OR name LIKE '%%Müllverbrennung%%'
        OR name LIKE '%%Müllverwertung%%'
        OR name LIKE '%%Abfall%%'
        OR name LIKE '%%Kraftwerk%%'
        OR name LIKE '%%Wertstoff%%'
        """,
        epsg=4326)

    # Initilize district_heating argument
    chp['district_heating'] = False
    #chp.loc[chp[chp.Nettonennleistung <= 0.15].index, 'use_case'] = 'individual'
    # Select district heating areas with buffer of 1 km
    district_heating = db.select_geodataframe(
        f"""
        SELECT area_id, ST_Buffer(geom_polygon, 1000) as geom
        FROM {sources['district_heating_areas']['schema']}.
        {sources['district_heating_areas']['table']}
        WHERE scenario = 'eGon2035'
        """,
        epsg=4326)

    # Select all CHP closer than 1km to a district heating area
    # these are possible district heating chp
    # Chps which are not close to a district heating area get use_case='industrial'
    close_to_dh = chp[chp.index.isin(
        gpd.sjoin(chp, district_heating).index)]

    # All chp which are close to a district heating grid and intersect with
    # osm polygons whoes name indicates that it could be a district heating location
    # (e.g. Stadtwerke, Heizraftwerk, Müllverbrennung)
    # are assigned as district heating chp
    district_heating_chp = chp[chp.index.isin(
        gpd.sjoin(close_to_dh, possible_dh_locations).index)]

    # Assigned district heating chps are dropped from list of possible
    # district heating chp
    close_to_dh.drop(district_heating_chp.index, inplace=True)

    # Select all CHP closer than 100m to a industrial location its name
    # doesn't indicate that it could be a district heating location
    # these chp get use_case='industrial'
    close_to_industry = chp[chp.index.isin(
        gpd.sjoin(close_to_dh, landuse_industrial).index)]

    # Chp which are close to a district heating area and not close to an
    # industrial location are assigned as district_heating_chp
    district_heating_chp = district_heating_chp.append(
        close_to_dh[~close_to_dh.index.isin(close_to_industry.index)])

    # Set district_heating = True for all district heating chp
    chp.loc[district_heating_chp.index, 'district_heating'] = True

    return chp
