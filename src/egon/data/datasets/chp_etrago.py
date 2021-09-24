
"""
The central module containing all code dealing with chp for eTraGo.
"""

import geopandas as gpd
from egon.data import db, config
from egon.data.datasets import Dataset

from egon.data.datasets.etrago_setup import link_geom_from_buses


class ChpEtrago(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="ChpEtrago",
            version="0.0.0",
            dependencies=dependencies,
            tasks=(insert),
        )

def insert():

    sources = config.datasets()["chp_etrago"]["sources"]

    targets = config.datasets()["chp_etrago"]["targets"]

    db.execute_sql(
        f"""
        DELETE FROM {targets['link']['schema']}.{targets['link']['table']}
        WHERE carrier LIKE '%%CHP%%'
        AND scn_name = 'eGon2035'
        """)

    chp_dh = db.select_dataframe(
        f"""
        SELECT electrical_bus_id, gas_bus_id, a.carrier,
        SUM(el_capacity) AS el_capacity, SUM(th_capacity) AS th_capacity,
        c.bus_id as heat_bus_id
        FROM {sources['chp_table']['schema']}.
        {sources['chp_table']['table']} a
        JOIN {sources['district_heating_areas']['schema']}.
        {sources['district_heating_areas']['table']}  b
        ON a.district_heating_area_id = b.area_id
        JOIN grid.egon_etrago_bus c
        ON ST_Transform(ST_Centroid(b.geom_polygon), 4326) = c.geom

        WHERE a.scenario='eGon2035'
        AND b.scenario = 'eGon2035'
        AND c.scn_name = 'eGon2035'
        AND c.carrier = 'central_heat'
        AND NOT district_heating_area_id IS NULL
        GROUP BY (
            electrical_bus_id, gas_bus_id, a.carrier, c.bus_id)
        """)

    # Create geodataframes for CHP plants
    chp_el = link_geom_from_buses(
        gpd.GeoDataFrame(
            index=chp_dh.index,
            data={
                'scn_name': 'eGon2035',
                'bus0': chp_dh.gas_bus_id,
                'bus1': chp_dh.electrical_bus_id,
                'p_nom': chp_dh.el_capacity,
                'carrier': 'urban central gas CHP'
                }),
        'eGon2035')

    chp_el['link_id'] = range(
        db.next_etrago_id('link'),
        len(chp_el)+db.next_etrago_id('link'))

    chp_el.to_postgis(
        targets["link"]["table"],
        schema=targets["link"]["schema"],
        con=db.engine(),
        if_exists="append")

    chp_heat = link_geom_from_buses(
        gpd.GeoDataFrame(
            index=chp_dh.index,
            data={
                'scn_name': 'eGon2035',
                'bus0': chp_dh.gas_bus_id,
                'bus1': chp_dh.heat_bus_id,
                'p_nom': chp_dh.th_capacity,
                'carrier': 'urban central gas CHP heat'
                }),
        'eGon2035')

    chp_heat['link_id'] = range(
        db.next_etrago_id('link'),
        len(chp_heat)+db.next_etrago_id('link'))

    chp_heat.to_postgis(
        targets["link"]["table"],
        schema=targets["link"]["schema"],
        con=db.engine(),
        if_exists="append")


    chp_industry = db.select_dataframe(
        f"""
        SELECT electrical_bus_id, gas_bus_id, carrier,
        SUM(el_capacity) AS el_capacity, SUM(th_capacity) AS th_capacity
        FROM {sources['chp_table']['schema']}.{sources['chp_table']['table']}
        WHERE scenario='eGon2035'
        AND district_heating_area_id IS NULL
        GROUP BY (electrical_bus_id, gas_bus_id, carrier)
        """)

    chp_el_ind = link_geom_from_buses(
        gpd.GeoDataFrame(
            index=chp_industry.index,
            data={
                'scn_name': 'eGon2035',
                'bus0': chp_industry.gas_bus_id,
                'bus1': chp_industry.electrical_bus_id,
                'p_nom': chp_industry.el_capacity,
                'carrier': 'industrial gas CHP'
                }),
        'eGon2035')

    chp_el_ind['link_id'] = range(
        db.next_etrago_id('link'),
        len(chp_el_ind)+db.next_etrago_id('link'))

    chp_el_ind.to_postgis(
    targets["link"]["table"],
            schema=targets["link"]["schema"],
        con=db.engine(),
        if_exists="append")


