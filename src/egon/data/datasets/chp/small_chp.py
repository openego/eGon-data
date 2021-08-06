"""
The module containing all code dealing with chp < 10MW.
"""
import pandas as pd
from egon.data import db, config
from egon.data.processing.power_plants import (
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
                el_capacity=row.Nettonennleistung,
                th_capacity= row.ThermischeNutzleistung,
                electrical_bus_id = row.bus_id,
                gas_bus_id = row.gas_bus_id,
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
        (MaStR_konv.Nettonennleistung<=1000)
        &(MaStR_konv.ThermischeNutzleistung>0)]

    targets = select_target('small_chp', 'eGon2035')

    additional_capacitiy = pd.Series()

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

        target = targets[federal_state]

        if mastr_chp.Nettonennleistung.sum() > target:

            additional_capacitiy[federal_state] = 0

        elif mastr_chp.Nettonennleistung.sum()< target:

            # Keep all existing CHP < 10MW
            insert_mastr_chp(mastr_chp, EgonChp)

            # Add new chp
            additional_capacitiy[federal_state] = (
                target - mastr_chp.Nettonennleistung.sum())

        else:
            # Keep all existing CHP < 10MW
            insert_mastr_chp(mastr_chp, EgonChp)

            additional_capacitiy[federal_state] = 0

    return additional_capacitiy



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
    flh_chp = 4000
    print(f"Distributing {additional_capacity} MW in {federal_state}")
    existing_chp = db.select_dataframe(
        f"""
        SELECT el_capacity, th_capacity, voltage_level, b.area_id
        FROM
        supply.egon_chp a,
        demand.district_heating_areas b
        WHERE a.scenario = 'eGon2035'
        AND b.scenario = 'eGon2035'
        AND district_heating = True
        AND ST_Intersects(ST_Transform(ST_Centroid(geom_polygon), 4326), (
            SELECT ST_Union(geometry) FROM boundaries.vg250_lan
            WHERE REPLACE(gen, '-', '') ='{federal_state}'))
        AND el_capacity < 10
        ORDER BY el_capacity, residential_and_service_demand

        """)

    # Select all district heating areas without CHP
    dh_areas = db.select_geodataframe(
        f"""
        SELECT
        residential_and_service_demand as demand, area_id,
        ST_Transform(ST_Centroid(geom_polygon), 4326)  as geom
        FROM
        demand.district_heating_areas
        WHERE scenario = 'eGon2035'
      ---  AND residential_and_service_demand > 2400
        AND ST_Intersects(ST_Transform(ST_Centroid(geom_polygon), 4326), (
            SELECT ST_Union(d.geometry) FROM boundaries.vg250_lan d
            WHERE REPLACE(gen, '-', '') ='{federal_state}'))
        AND area_id NOT IN (SELECT district_heating_area_id FROM
                             supply.egon_chp
                             WHERE scenario = 'eGon2035'
                             AND district_heating = TRUE)
        """)

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
                supply.egon_chp a,
                demand.district_heating_areas b
                WHERE b.scenario = 'eGon2035'
                AND a.scenario = 'eGon2035'
               --- AND b.residential_and_service_demand > 2400
                AND ST_Intersects(
                    ST_Transform(ST_Centroid(geom_polygon), 4326),
                    (SELECT ST_Union(d.geometry) FROM boundaries.vg250_lan d
                    WHERE REPLACE(gen, '-', '') ='{federal_state}'))
                AND a.district_heating_area_id = b.area_id
                GROUP BY (
                    b.residential_and_service_demand,
                    b.area_id, geom_polygon)
                """),ignore_index=True
                )

    # extended_chp = gpd.GeoDataFrame(
    #     columns = ['heat_bus_id', 'voltage_level',
    #                 'el_capacity', 'th_capacity'])

    session = sessionmaker(bind=db.engine())()

    np.random.seed(seed=123456)

    n = 0
    # Add new CHP as long as the additional capacity is not reached
    while additional_capacity > existing_chp.el_capacity.min():

        # Break loop after 500 iterations without a fitting CHP
        if n > 500:
            print(
                f'{additional_capacity} MW are not matched to a district heating grid.')
            break

        # Select random new build CHP from list of existing CHP
        # which is smaller than the remaining capacity to distribute
        id_chp = np.random.choice(range(len(existing_chp[
            existing_chp.el_capacity <= additional_capacity])))
        selected_chp = existing_chp[
            existing_chp.el_capacity <= additional_capacity].iloc[id_chp]

        # Select district heatung areas whoes remaining demand, which is not
        # covered by another CHP, fits to the selected CHP
        possible_dh = dh_areas[
                dh_areas.demand > selected_chp.th_capacity*flh_chp].to_crs(4326)


        # If there is no district heating area whoes demand (not covered by
        # another CHP) fit to the CHP, quit and select another CHP
        if len(possible_dh) > 0:

            # Assign gas bus_id
            possible_dh['gas_bus_id'] = assign_gas_bus_id(possible_dh.copy()).gas_bus_id

            # Assign bus_id
            possible_dh['voltage_level'] = selected_chp.voltage_level
            # TODO: Fix promplem in Kappeln (centroid on river, no mv grid)
            #possible_dh['bus_id'] = assign_bus_id(
            #possible_dh, config.datasets()["chp_location"]).bus_id


            # Select randomly one district heating area from the list
            # of possible district heating areas
            id_dh = np.random.choice(range(len(possible_dh)))
            selected_area = possible_dh.iloc[id_dh]

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
                        district_heating=True,
                        voltage_level=selected_chp.voltage_level,
                        #electrical_bus_id = int(selected_area.bus_id),
                        #gas_bus_id = int(selected_area.gas_bus_id),
                        district_heating_area_id = int(selected_area.area_id),
                        scenario='eGon2035',
                        geom=f"SRID=4326;POINT({selected_area.geom.x} {selected_area.geom.y})",
                    )
            session.add(entry)
            session.commit()
            # extended_chp = extended_chp.append({
            #     'heat_bus_id': int(selected_area.bus_id),
            #     'voltage_level': selected_chp.voltage_level,
            #     'el_capacity': selected_chp.el_capacity,
            #     'th_capacity': selected_chp.th_capacity},
            #     ignore_index=True)

            # Reduce additional capacity and district heating demand
            additional_capacity -= selected_chp.el_capacity
            dh_areas.loc[
                dh_areas.index[dh_areas.area_id == selected_area.area_id],
                'demand'] -= selected_chp.th_capacity*flh_chp
            dh_areas = dh_areas[dh_areas.demand > 0]
        else:
           # print('Selected CHP can not be assigned to a district heating area.')
            n+= 1

def assign_use_case(chp, sources):
    """ Intentifies CHPs used in district heating areas

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
