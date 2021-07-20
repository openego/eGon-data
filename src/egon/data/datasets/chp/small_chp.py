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
                carrier=row.energietraeger_Ma,
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

def existing_chp_smaller_10mw(MaStR_konv, EgonChp):
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
        (MaStR_konv.Nettonennleistung>0.1)
        &(MaStR_konv.Nettonennleistung<=10)]

    targets = select_target('small_chp', 'eGon2035')

    additional_capacitiy = pd.Series()

    for federal_state in targets.index:
        mastr_chp = geopandas.GeoDataFrame(
            filter_mastr_geometry(existsting_chp_smaller_10mw, federal_state))

        mastr_chp.crs = "EPSG:4326"

        # Assign gas bus_id
        mastr_chp_c = mastr_chp.copy()
        mastr_chp['gas_bus_id'] = assign_gas_bus_id(mastr_chp_c).gas_bus_id

        # Assign bus_id
        mastr_chp['bus_id'] = assign_bus_id(
            mastr_chp, config.datasets()["chp_location"]).bus_id

        mastr_chp = assign_use_case(mastr_chp)

        target = targets[federal_state]

        if mastr_chp.Nettonennleistung.sum() > target:
            # Remove small chp ?
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

def assign_use_case(chp):
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

    # Initilize district_heating argument
    chp['district_heating'] = False
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
        geopandas.sjoin(chp, district_heating).index)]

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
    close_to_industry = chp[chp.index.isin(
        geopandas.sjoin(close_to_dh, landuse_industrial).index)]

    # Chp which are close to a district heating area and not close to an
    # industrial location are assigned as district_heating_chp
    district_heating_chp = district_heating_chp.append(
        close_to_dh[~close_to_dh.index.isin(close_to_industry.index)])

    # Set district_heating = True for all district heating chp
    chp.loc[district_heating_chp.index, 'district_heating'] = True

    return chp
