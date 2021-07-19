"""
The module containing all code dealing with chp < 10MW.
"""
import pandas as pd
import geopandas
from egon.data import db, config
from egon.data.processing.power_plants import (
    assign_voltage_level, assign_bus_id, assign_gas_bus_id,
    filter_mastr_geometry, select_target)
from sqlalchemy.orm import sessionmaker

def existing_chp_smaller_10mw(MaStR_konv, EgonChp):

    existsting_chp_smaller_10mw = MaStR_konv[
        (MaStR_konv.Nettonennleistung>0.1)
        &(MaStR_konv.Nettonennleistung<=10)]

    mastr_chp =  geopandas.GeoDataFrame(
        filter_mastr_geometry(existsting_chp_smaller_10mw))

    mastr_chp.crs = "EPSG:4326"

    # Assign gas bus_id
    mastr_chp_c = mastr_chp.copy()
    mastr_chp['gas_bus_id'] = assign_gas_bus_id(mastr_chp_c).gas_bus_id

    # Assign bus_id
    mastr_chp['bus_id'] = assign_bus_id(
        mastr_chp, config.datasets()["chp_location"]).bus_id



    mastr_chp = assign_use_case(mastr_chp)

    target = select_target('small_chp', 'eGon2035')['SchleswigHolstein']


    if mastr_chp.Nettonennleistung.sum() > target:
        # Remove small chp ?
        additional_capacitiy = 0

    elif mastr_chp.Nettonennleistung.sum()< target:

        # Keep all existing CHP < 10MW
        session = sessionmaker(bind=db.engine())()
        for i, row in mastr_chp.iterrows():
            entry = EgonChp(
                    sources={
                        "chp": "MaStR",
                        "el_capacity": "MaStR",
                        "th_capacity": "MaStR",
                    },
                    source_id={"MastrNummer": row.EinheitMastrNummer},
                    carrier=row.energietraeger_Ma,
                    el_capacity=row.Nettonennleistung,
                    th_capacity= row.ThermischeNutzleistung,
                    electrical_bus_id = row.bus_id,
                    gas_bus_id = row.gas_bus_id,
                    use_case=row.use_case,
                    scenario='eGon2035',
                    geom=f"SRID=4326;POINT({row.geometry.x} {row.geometry.y})",
                )
            session.add(entry)
        session.commit()

        # Add new chp
        additional_capacitiy = target - mastr_chp.Nettonennleistung.sum()
    else:
        # Keep all existing CHP < 10MW
        session = sessionmaker(bind=db.engine())()
        for i, row in mastr_chp.iterrows():
            entry = EgonChp(
                    sources={
                        "chp": "MaStR",
                        "el_capacity": "MaStR",
                        "th_capacity": "MaStR",
                    },
                    source_id={"MastrNummer": row.EinheitMastrNummer},
                    carrier=row.carrier,
                    el_capacity=row.Nettonennleistung,
                    th_capacity= row.ThermischeNutzleistung,
                    electrical_bus_id = row.bus_id,
                    gas_bus_id = row.gas_bus_id,
                    use_case=row.use_case,
                    scenario='eGon2035',
                    geom=f"SRID=4326;POINT({row.geometry.x} {row.geometry.y})",
                )
            session.add(entry)
        session.commit()

        additional_capacitiy = 0
    return additional_capacitiy

def nearest(row, geom_union, df1, df2,
            geom1_col='geometry', geom2_col='geometry', src_column=None):
    """Find the nearest point and return the corresponding value from specified column."""
    from shapely.ops import nearest_points
    # Find the geometry that is closest
    nearest = df2[geom2_col] == nearest_points(row[geom1_col], geom_union)[1]
    # Get the corresponding value from df2 (matching is based on the geometry)
    value = df2[nearest][src_column].values[0]
    return value

def assign_use_case(chp):
    # Select osm industrial areas which don't include power or heat supply
    # (name not includes 'Stadtwerke', 'Kraftwerk', 'Müllverbrennung'...)
    landuse_industrial = db.select_geodataframe(
        """
        SELECT ST_Buffer(geom, 100) as geom,
         tags::json->>'name' as name
         FROM openstreetmap.osm_landuse
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
        """
        SELECT ST_Buffer(geom, 100) as geom,
         tags::json->>'name' as name
        FROM openstreetmap.osm_polygon
        WHERE name LIKE '%%Stadtwerke%%'
        OR name LIKE '%%kraftwerk%%'
        OR name LIKE '%%Müllverbrennung%%'
        OR name LIKE '%%Müllverwertung%%'
        OR name LIKE '%%Abfall%%'
        OR name LIKE '%%Kraftwerk%%'
        OR name LIKE '%%Wertstoff%%'
        """,
        epsg=4326)

    # All chp < 150kWel are individual
    chp['use_case'] = ''
    #chp.loc[chp[chp.Nettonennleistung <= 0.15].index, 'use_case'] = 'individual'
    # Select district heating areas with buffer of 1 km
    district_heating = db.select_geodataframe(
        """
        SELECT area_id, ST_Buffer(geom_polygon, 1000) as geom
        FROM demand.district_heating_areas
        WHERE scenario = 'eGon2035'
        """,
        epsg=4326)

    # Select all CHP closer than 1km to a district heating area
    # these are possible district heating chp
    # Chps which are not close to a district heating area get use_case='industrial'
    close_to_dh = chp[chp.index.isin(
        geopandas.sjoin(chp[chp['use_case'] == ''], district_heating).index)]

    # All chp which are close to a district heating grid and intersect with
    # osm polygons whoes name indicates that it could be a district heating location
    # (e.g. Stadtwerke, Heizraftwerk, Müllverbrennung)
    # are assigned as district heating chp
    district_heating_chp = chp[chp.index.isin(
        geopandas.sjoin(close_to_dh, possible_dh_locations).index)]

    # Assigned district heating chps are dropped from list of possible
    # district heating chp
    close_to_dh.drop(district_heating_chp.index, inplace=True)

    # Select all CHP closer than 100m to a industrial location its name
    # doesn't indicate that it could be a district heating location
    # these chp get use_case='industrial'
    close_to_industry =  chp[chp.index.isin(
        geopandas.sjoin(close_to_dh, landuse_industrial).index)]

    # Chp which are close to a district heating area and not close to an
    # industrial location are assigned as district_heating_chp
    district_heating_chp = district_heating_chp.append(
        close_to_dh[~close_to_dh.index.isin(close_to_industry.index)])

    # Set use_case for all district heating chp
    chp.loc[district_heating_chp.index, 'use_case'] = 'district_heating'

    # Others get use_case='industrial'
    chp.loc[chp[chp.use_case == ''].index, 'use_case'] = 'industrial'

    return chp
