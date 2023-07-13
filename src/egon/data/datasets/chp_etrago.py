"""
The central module containing all code dealing with chp for eTraGo.
"""

import geopandas as gpd
import pandas as pd

from egon.data import config, db
from egon.data.datasets import Dataset
from egon.data.datasets.etrago_setup import link_geom_from_buses
from egon.data.datasets.scenario_parameters import get_sector_parameters


class ChpEtrago(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="ChpEtrago",
            version="0.0.6",
            dependencies=dependencies,
            tasks=(insert),
        )


def insert_egon100re():
    """Insert combined heat and power plants into eTraGo tables
    for the eGon100RE scenario.

    Returns
    -------
    None.

    """

    sources = config.datasets()["chp_etrago"]["sources"]

    targets = config.datasets()["chp_etrago"]["targets"]

    db.execute_sql(
        f"""
        DELETE FROM {targets['link']['schema']}.{targets['link']['table']}
        WHERE carrier LIKE '%%CHP%%'
        AND scn_name = 'eGon100RE'
        AND bus0 IN
        (SELECT bus_id
         FROM {sources['etrago_buses']['schema']}.{sources['etrago_buses']['table']}
         WHERE scn_name = 'eGon100RE'
         AND country = 'DE')
        AND bus1 IN
        (SELECT bus_id
         FROM {sources['etrago_buses']['schema']}.{sources['etrago_buses']['table']}
         WHERE scn_name = 'eGon100RE'
         AND country = 'DE')
        """
    )

    # Select all CHP plants used in district heating
    chp_dh = db.select_dataframe(
        f"""
        SELECT electrical_bus_id, ch4_bus_id, a.carrier,
        SUM(el_capacity) AS el_capacity, SUM(th_capacity) AS th_capacity,
        c.bus_id as heat_bus_id
        FROM {sources['chp_table']['schema']}.
        {sources['chp_table']['table']} a
        JOIN {sources['district_heating_areas']['schema']}.
        {sources['district_heating_areas']['table']}  b
        ON a.district_heating_area_id = b.area_id
        JOIN grid.egon_etrago_bus c
        ON ST_Transform(ST_Centroid(b.geom_polygon), 4326) = c.geom

        WHERE a.scenario='eGon100RE'
        AND b.scenario = 'eGon100RE'
        AND c.scn_name = 'eGon100RE'
        AND c.carrier = 'central_heat'
        AND NOT district_heating_area_id IS NULL
        GROUP BY (
            electrical_bus_id, ch4_bus_id, a.carrier, c.bus_id)
        """
    )

    # Create geodataframes for gas CHP plants
    chp_el = link_geom_from_buses(
        gpd.GeoDataFrame(
            index=chp_dh.index,
            data={
                "scn_name": "eGon2035",
                "bus0": chp_dh.loc[:, "ch4_bus_id"].astype(int),
                "bus1": chp_dh.loc[:, "electrical_bus_id"].astype(int),
                "p_nom": chp_dh.loc[:, "el_capacity"],
                "carrier": "central_gas_CHP",
            },
        ),
        "eGon100RE",
    )
    # Set index
    chp_el["link_id"] = range(
        db.next_etrago_id("link"), len(chp_el) + db.next_etrago_id("link")
    )

    # Add marginal cost which is only VOM in case of gas chp
    chp_el["marginal_cost"] = get_sector_parameters("gas", "eGon100RE")[
        "marginal_cost"
    ]["chp_gas"]

    # Insert into database
    chp_el.to_postgis(
        targets["link"]["table"],
        schema=targets["link"]["schema"],
        con=db.engine(),
        if_exists="append",
    )

    #
    chp_heat = link_geom_from_buses(
        gpd.GeoDataFrame(
            index=chp_dh.index,
            data={
                "scn_name": "eGon100RE",
                "bus0": chp_dh.loc[:, "ch4_bus_id"].astype(int),
                "bus1": chp_dh.loc[:, "heat_bus_id"].astype(int),
                "p_nom": chp_dh.loc[:, "th_capacity"],
                "carrier": "central_gas_CHP_heat",
            },
        ),
        "eGon100RE",
    )

    chp_heat["link_id"] = range(
        db.next_etrago_id("link"), len(chp_heat) + db.next_etrago_id("link")
    )

    chp_heat.to_postgis(
        targets["link"]["table"],
        schema=targets["link"]["schema"],
        con=db.engine(),
        if_exists="append",
    )


def insert():
    """Insert combined heat and power plants into eTraGo tables.

    Gas CHP plants are modeled as links to the gas grid,
    biomass CHP plants (only in eGon2035) are modeled as generators

    Returns
    -------
    None.

    """

    sources = config.datasets()["chp_etrago"]["sources"]

    targets = config.datasets()["chp_etrago"]["targets"]

    db.execute_sql(
        f"""
        DELETE FROM {targets['link']['schema']}.{targets['link']['table']}
        WHERE carrier LIKE '%%CHP%%'
        AND scn_name = 'eGon2035'
        AND bus0 IN
        (SELECT bus_id
         FROM {sources['etrago_buses']['schema']}.{sources['etrago_buses']['table']}
         WHERE scn_name = 'eGon2035'
         AND country = 'DE')
        AND bus1 IN
        (SELECT bus_id
         FROM {sources['etrago_buses']['schema']}.{sources['etrago_buses']['table']}
         WHERE scn_name = 'eGon2035'
         AND country = 'DE')
        """
    )
    db.execute_sql(
        f"""
        DELETE FROM {targets['generator']['schema']}.{targets['generator']['table']}
        WHERE carrier LIKE '%%CHP%%'
        AND scn_name = 'eGon2035'
        """
    )
    # Select all CHP plants used in district heating
    chp_dh = db.select_dataframe(
        f"""
        SELECT electrical_bus_id, ch4_bus_id, a.carrier,
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
            electrical_bus_id, ch4_bus_id, a.carrier, c.bus_id)
        """
    )
    # Divide into biomass and gas CHP which are modelled differently
    chp_link_dh = chp_dh[chp_dh.carrier != "biomass"].index
    chp_generator_dh = chp_dh[chp_dh.carrier == "biomass"].index

    # Create geodataframes for gas CHP plants
    chp_el = link_geom_from_buses(
        gpd.GeoDataFrame(
            index=chp_link_dh,
            data={
                "scn_name": "eGon2035",
                "bus0": chp_dh.loc[chp_link_dh, "ch4_bus_id"].astype(int),
                "bus1": chp_dh.loc[chp_link_dh, "electrical_bus_id"].astype(
                    int
                ),
                "p_nom": chp_dh.loc[chp_link_dh, "el_capacity"],
                "carrier": "central_gas_CHP",
            },
        ),
        "eGon2035",
    )
    # Set index
    chp_el["link_id"] = range(
        db.next_etrago_id("link"), len(chp_el) + db.next_etrago_id("link")
    )

    # Add marginal cost which is only VOM in case of gas chp
    chp_el["marginal_cost"] = get_sector_parameters("gas", "eGon2035")[
        "marginal_cost"
    ]["chp_gas"]

    # Insert into database
    chp_el.to_postgis(
        targets["link"]["table"],
        schema=targets["link"]["schema"],
        con=db.engine(),
        if_exists="append",
    )

    #
    chp_heat = link_geom_from_buses(
        gpd.GeoDataFrame(
            index=chp_link_dh,
            data={
                "scn_name": "eGon2035",
                "bus0": chp_dh.loc[chp_link_dh, "ch4_bus_id"].astype(int),
                "bus1": chp_dh.loc[chp_link_dh, "heat_bus_id"].astype(int),
                "p_nom": chp_dh.loc[chp_link_dh, "th_capacity"],
                "carrier": "central_gas_CHP_heat",
            },
        ),
        "eGon2035",
    )

    chp_heat["link_id"] = range(
        db.next_etrago_id("link"), len(chp_heat) + db.next_etrago_id("link")
    )

    chp_heat.to_postgis(
        targets["link"]["table"],
        schema=targets["link"]["schema"],
        con=db.engine(),
        if_exists="append",
    )

    # Insert biomass CHP as generators
    # Create geodataframes for CHP plants
    chp_el_gen = pd.DataFrame(
        index=chp_generator_dh,
        data={
            "scn_name": "eGon2035",
            "bus": chp_dh.loc[chp_generator_dh, "electrical_bus_id"].astype(
                int
            ),
            "p_nom": chp_dh.loc[chp_generator_dh, "el_capacity"],
            "carrier": "central_biomass_CHP",
        },
    )

    chp_el_gen["generator_id"] = range(
        db.next_etrago_id("generator"),
        len(chp_el_gen) + db.next_etrago_id("generator"),
    )

    # Add marginal cost
    chp_el_gen["marginal_cost"] = get_sector_parameters(
        "electricity", "eGon2035"
    )["marginal_cost"]["biomass"]

    chp_el_gen.to_sql(
        targets["generator"]["table"],
        schema=targets["generator"]["schema"],
        con=db.engine(),
        if_exists="append",
        index=False,
    )

    chp_heat_gen = pd.DataFrame(
        index=chp_generator_dh,
        data={
            "scn_name": "eGon2035",
            "bus": chp_dh.loc[chp_generator_dh, "heat_bus_id"].astype(int),
            "p_nom": chp_dh.loc[chp_generator_dh, "th_capacity"],
            "carrier": "central_biomass_CHP_heat",
        },
    )

    chp_heat_gen["generator_id"] = range(
        db.next_etrago_id("generator"),
        len(chp_heat_gen) + db.next_etrago_id("generator"),
    )

    chp_heat_gen.to_sql(
        targets["generator"]["table"],
        schema=targets["generator"]["schema"],
        con=db.engine(),
        if_exists="append",
        index=False,
    )

    chp_industry = db.select_dataframe(
        f"""
        SELECT electrical_bus_id, ch4_bus_id, carrier,
        SUM(el_capacity) AS el_capacity, SUM(th_capacity) AS th_capacity
        FROM {sources['chp_table']['schema']}.{sources['chp_table']['table']}
        WHERE scenario='eGon2035'
        AND district_heating_area_id IS NULL
        GROUP BY (electrical_bus_id, ch4_bus_id, carrier)
        """
    )
    chp_link_ind = chp_industry[chp_industry.carrier != "biomass"].index

    chp_generator_ind = chp_industry[chp_industry.carrier == "biomass"].index

    chp_el_ind = link_geom_from_buses(
        gpd.GeoDataFrame(
            index=chp_link_ind,
            data={
                "scn_name": "eGon2035",
                "bus0": chp_industry.loc[chp_link_ind, "ch4_bus_id"].astype(
                    int
                ),
                "bus1": chp_industry.loc[
                    chp_link_ind, "electrical_bus_id"
                ].astype(int),
                "p_nom": chp_industry.loc[chp_link_ind, "el_capacity"],
                "carrier": "industrial_gas_CHP",
            },
        ),
        "eGon2035",
    )

    chp_el_ind["link_id"] = range(
        db.next_etrago_id("link"), len(chp_el_ind) + db.next_etrago_id("link")
    )

    # Add marginal cost which is only VOM in case of gas chp
    chp_el_ind["marginal_cost"] = get_sector_parameters("gas", "eGon2035")[
        "marginal_cost"
    ]["chp_gas"]

    chp_el_ind.to_postgis(
        targets["link"]["table"],
        schema=targets["link"]["schema"],
        con=db.engine(),
        if_exists="append",
    )

    # Insert biomass CHP as generators
    chp_el_ind_gen = pd.DataFrame(
        index=chp_generator_ind,
        data={
            "scn_name": "eGon2035",
            "bus": chp_industry.loc[
                chp_generator_ind, "electrical_bus_id"
            ].astype(int),
            "p_nom": chp_industry.loc[chp_generator_ind, "el_capacity"],
            "carrier": "industrial_biomass_CHP",
        },
    )

    chp_el_ind_gen["generator_id"] = range(
        db.next_etrago_id("generator"),
        len(chp_el_ind_gen) + db.next_etrago_id("generator"),
    )

    # Add marginal cost
    chp_el_ind_gen["marginal_cost"] = get_sector_parameters(
        "electricity", "eGon2035"
    )["marginal_cost"]["biomass"]

    chp_el_ind_gen.to_sql(
        targets["generator"]["table"],
        schema=targets["generator"]["schema"],
        con=db.engine(),
        if_exists="append",
        index=False,
    )

    insert_egon100re()
