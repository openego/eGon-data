"""
The central module containing all code dealing with chp for eTraGo.
"""

import geopandas as gpd
import pandas as pd

from egon.data import config, db
from egon.data.datasets import Dataset, DatasetSources, DatasetTargets
from egon.data.datasets.etrago_setup import link_geom_from_buses
from egon.data.datasets.scenario_parameters import get_sector_parameters


class ChpEtrago(Dataset):
    """
    Collect data related to combined heat and power plants for the eTraGo tool

    This dataset collects data for combined heat and power plants and puts it into a format that
    is needed for the transmission grid optimisation within the tool eTraGo.
    This data is then writting into the corresponding tables that are read by eTraGo.


    *Dependencies*
      * :py:class:`HeatEtrago <egon.data.datasets.heat_etrago.HeatEtrago>`
      * :py:class:`Chp <egon.data.datasets.chp.Chp>`

    *Resulting tables*
      * :py:class:`grid.egon_etrago_link <egon.data.datasets.etrago_setup.EgonPfHvLink>` is extended
      * :py:class:`grid.egon_etrago_generator <egon.data.datasets.etrago_setup.EgonPfHvGenerator>` is extended

    """

    #:
    name: str = "ChpEtrago"
    #:
    version: str = "0.0.7"
    sources = DatasetSources(
        tables={
            "chp_table": "supply.egon_chp_plants",
            "district_heating_areas": "demand.egon_district_heating_areas",
            "etrago_buses": "grid.egon_etrago_bus",
        }
    )
    targets = DatasetTargets(
        tables={
            "link": "grid.egon_etrago_link",
            "generator": "grid.egon_etrago_generator",
        }
    )
    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(insert),
        )


def insert_egon100re():


    db.execute_sql(
        f"""
        DELETE FROM {ChpEtrago.targets.tables['link']}
        WHERE carrier LIKE '%%CHP%%'
        AND scn_name = 'eGon100RE'
        AND bus0 IN
        (SELECT bus_id
         FROM {ChpEtrago.sources.tables['etrago_buses']}
         WHERE scn_name = 'eGon100RE'
         AND country = 'DE')
        AND bus1 IN
        (SELECT bus_id
         FROM {ChpEtrago.sources.tables['etrago_buses']}
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
        FROM {ChpEtrago.sources.tables['chp_table']} a
        JOIN {ChpEtrago.sources.tables['district_heating_areas']} b
        ON a.district_heating_area_id = b.area_id
        JOIN grid.egon_etrago_bus c
        ON ST_Transform(ST_Centroid(b.geom_polygon), 4326) = c.geom
        WHERE a.scenario='eGon100RE'
        AND b.scenario = 'eGon100RE'
        AND c.scn_name = 'eGon100RE'
        AND c.carrier = 'central_heat'
        AND NOT district_heating_area_id IS NULL
        GROUP BY (electrical_bus_id, ch4_bus_id, a.carrier, c.bus_id)
        """
    )

    if chp_dh.empty:
        print("No CHP for district heating in scenario eGon100RE")
        return

    # Create geodataframes for gas CHP plants
    chp_el = link_geom_from_buses(
        gpd.GeoDataFrame(
            index=chp_dh.index,
            data={
                "scn_name": "eGon100RE",
                "bus0": chp_dh.loc[:, "ch4_bus_id"].astype(int),
                "bus1": chp_dh.loc[:, "electrical_bus_id"].astype(int),
                "p_nom": chp_dh.loc[:, "el_capacity"],
                "carrier": "central_gas_CHP",
            },
        ),
        "eGon100RE",
    )
    # Set index
    chp_el["link_id"] = db.next_etrago_id("link", len(chp_el))

    # Add marginal cost which is only VOM in case of gas chp
    chp_el["marginal_cost"] = get_sector_parameters("gas", "eGon100RE")[
        "marginal_cost"
    ]["chp_gas"]

    # Insert into database
    chp_el.to_postgis(
        ChpEtrago.targets.get_table_name("link"),
        schema=ChpEtrago.targets.get_table_schema("link"),
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

    chp_heat["link_id"] = db.next_etrago_id("link", len(chp_heat))

    chp_heat.to_postgis(
        ChpEtrago.targets.get_table_name("link"),
        schema=ChpEtrago.targets.get_table_schema("link"),
        con=db.engine(),
        if_exists="append",
    )


def insert_scenario(scenario):


    db.execute_sql(
        f"""
        DELETE FROM {ChpEtrago.targets.tables['link']}
        WHERE carrier LIKE '%%CHP%%'
        AND scn_name = '{scenario}'
        AND bus0 IN
        (SELECT bus_id
         FROM {ChpEtrago.sources.tables['etrago_buses']}
         WHERE scn_name = '{scenario}'
         AND country = 'DE')
        AND bus1 IN
        (SELECT bus_id
         FROM {ChpEtrago.sources.tables['etrago_buses']}
         WHERE scn_name = '{scenario}'
         AND country = 'DE')
        """
    )
    db.execute_sql(
        f"""
        DELETE FROM {ChpEtrago.targets.tables['generator']}
        WHERE carrier LIKE '%%CHP%%'
        AND scn_name = '{scenario}'
        """
    )
    # Select all CHP plants used in district heating
    chp_dh = db.select_dataframe(
        f"""
        SELECT electrical_bus_id, ch4_bus_id, a.carrier,
        SUM(el_capacity) AS el_capacity, SUM(th_capacity) AS th_capacity,
        c.bus_id as heat_bus_id
        FROM {ChpEtrago.sources.tables['chp_table']} a
        JOIN {ChpEtrago.sources.tables['district_heating_areas']} b
        ON a.district_heating_area_id = b.area_id
        JOIN grid.egon_etrago_bus c
        ON ST_Transform(ST_Centroid(b.geom_polygon), 4326) = c.geom
        WHERE a.scenario='{scenario}'
        AND b.scenario = '{scenario}'
        AND c.scn_name = '{scenario}'
        AND c.carrier = 'central_heat'
        AND NOT district_heating_area_id IS NULL
        GROUP BY (electrical_bus_id, ch4_bus_id, a.carrier, c.bus_id)
        """
    )

    chp_dh.loc[chp_dh[chp_dh.carrier == "gas extended"].index, "carrier"] = (
        "gas"
    )

    # Divide into biomass and gas CHP which are modelled differently
    chp_link_dh = chp_dh[chp_dh.carrier == "gas"].index
    chp_generator_dh = chp_dh[chp_dh.carrier != "gas"].index

    # Create geodataframes for gas CHP plants
    chp_el = link_geom_from_buses(
        gpd.GeoDataFrame(
            index=chp_link_dh,
            data={
                "scn_name": scenario,
                "bus0": chp_dh.loc[chp_link_dh, "ch4_bus_id"].astype(int),
                "bus1": chp_dh.loc[chp_link_dh, "electrical_bus_id"].astype(
                    int
                ),
                "p_nom": chp_dh.loc[chp_link_dh, "el_capacity"],
                "carrier": "central_gas_CHP",
            },
        ),
        scenario,
    )
    # Set index
    chp_el["link_id"] = db.next_etrago_id("link", len(chp_el))

    # Add marginal cost which is only VOM in case of gas chp
    chp_el["marginal_cost"] = get_sector_parameters("gas", scenario)[
        "marginal_cost"
    ]["chp_gas"]

    # Insert into database
    chp_el.to_postgis(
        ChpEtrago.targets.get_table_name("link"),
        schema=ChpEtrago.targets.get_table_schema("link"),
        con=db.engine(),
        if_exists="append",
    )

    #
    chp_heat = link_geom_from_buses(
        gpd.GeoDataFrame(
            index=chp_link_dh,
            data={
                "scn_name": scenario,
                "bus0": chp_dh.loc[chp_link_dh, "ch4_bus_id"].astype(int),
                "bus1": chp_dh.loc[chp_link_dh, "heat_bus_id"].astype(int),
                "p_nom": chp_dh.loc[chp_link_dh, "th_capacity"],
                "carrier": "central_gas_CHP_heat",
            },
        ),
        scenario,
    )

    chp_heat["link_id"] = db.next_etrago_id("link", len(chp_heat))

    chp_heat.to_postgis(
        ChpEtrago.targets.get_table_name("link"),
        schema=ChpEtrago.targets.get_table_schema("link"),
        con=db.engine(),
        if_exists="append",
    )

    # Insert biomass, coal, oil and other CHP as generators
    # Create geodataframes for CHP plants
    chp_el_gen = pd.DataFrame(
        index=chp_generator_dh,
        data={
            "scn_name": scenario,
            "bus": chp_dh.loc[chp_generator_dh, "electrical_bus_id"].astype(
                int
            ),
            "p_nom": chp_dh.loc[chp_generator_dh, "el_capacity"],
            "carrier": chp_dh.loc[chp_generator_dh, "carrier"],
        },
    )

    chp_el_gen["generator_id"] = db.next_etrago_id(
        "generator", len(chp_el_gen))
    # Add marginal cost
    chp_el_gen["marginal_cost"] = (
        pd.Series(
            get_sector_parameters("electricity", scenario)["marginal_cost"]
        )
        .rename({"other_non_renewable": "others"})
        .loc[chp_el_gen["carrier"]]
    ).values

    chp_el_gen["carrier"] = (
        "central_" + chp_dh.loc[chp_generator_dh, "carrier"] + "_CHP"
    )

    chp_el_gen.to_sql(
        ChpEtrago.targets.get_table_name("generator"),
        schema=ChpEtrago.targets.get_table_schema("generator"),
        con=db.engine(),
        if_exists="append",
        index=False,
    )

    chp_heat_gen = pd.DataFrame(
        index=chp_generator_dh,
        data={
            "scn_name": scenario,
            "bus": chp_dh.loc[chp_generator_dh, "heat_bus_id"].astype(int),
            "p_nom": chp_dh.loc[chp_generator_dh, "th_capacity"],
            "carrier": "central_"
            + chp_dh.loc[chp_generator_dh, "carrier"]
            + "_CHP_heat",
        },
    )

    chp_heat_gen["generator_id"] = db.next_etrago_id(
        "generator", len(chp_heat_gen))

    chp_heat_gen.to_sql(
        ChpEtrago.targets.get_table_name("generator"),
        schema=ChpEtrago.targets.get_table_schema("generator"),
        con=db.engine(),
        if_exists="append",
        index=False,
    )

    chp_industry = db.select_dataframe(
        f"""
        SELECT electrical_bus_id, ch4_bus_id, carrier,
        SUM(el_capacity) AS el_capacity, SUM(th_capacity) AS th_capacity
        FROM {ChpEtrago.sources.tables['chp_table']}
        WHERE scenario='{scenario}'
        AND district_heating_area_id IS NULL
        GROUP BY (electrical_bus_id, ch4_bus_id, carrier)
        """
    )

    chp_industry.loc[
        chp_industry[chp_industry.carrier == "gas extended"].index, "carrier"
    ] = "gas"

    chp_link_ind = chp_industry[chp_industry.carrier == "gas"].index

    chp_generator_ind = chp_industry[chp_industry.carrier != "gas"].index

    chp_el_ind = link_geom_from_buses(
        gpd.GeoDataFrame(
            index=chp_link_ind,
            data={
                "scn_name": scenario,
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
        scenario,
    )

    chp_el_ind["link_id"] = db.next_etrago_id("link", len(chp_el_ind))

    # Add marginal cost which is only VOM in case of gas chp
    chp_el_ind["marginal_cost"] = get_sector_parameters("gas", scenario)[
        "marginal_cost"
    ]["chp_gas"]

    chp_el_ind.to_postgis(
        ChpEtrago.targets.get_table_name("link"),
        schema=ChpEtrago.targets.get_table_schema("link"),
        con=db.engine(),
        if_exists="append",
    )

    # Insert biomass CHP as generators
    chp_el_ind_gen = pd.DataFrame(
        index=chp_generator_ind,
        data={
            "scn_name": scenario,
            "bus": chp_industry.loc[
                chp_generator_ind, "electrical_bus_id"
            ].astype(int),
            "p_nom": chp_industry.loc[chp_generator_ind, "el_capacity"],
            "carrier": chp_industry.loc[chp_generator_ind, "carrier"],
        },
    )

    chp_el_ind_gen["generator_id"] = db.next_etrago_id(
        "generator", len(chp_el_ind_gen))

    # Add marginal cost
    chp_el_ind_gen["marginal_cost"] = (
        pd.Series(
            get_sector_parameters("electricity", scenario)["marginal_cost"]
        )
        .rename({"other_non_renewable": "others"})
        .loc[chp_el_ind_gen["carrier"]]
    ).values

    # Update carrier
    chp_el_ind_gen["carrier"] = "industrial_" + chp_el_ind_gen.carrier + "_CHP"

    chp_el_ind_gen.to_sql(
        ChpEtrago.targets.get_table_name("generator"),
        schema=ChpEtrago.targets.get_table_schema("generator"),
        con=db.engine(),
        if_exists="append",
        index=False,
    )


def insert():
    """Insert combined heat and power plants into eTraGo tables.

    Gas CHP plants are modeled as links to the gas grid,
    biomass CHP plants (only in eGon2035) are modeled as generators

    Returns
    -------
    None.

    """

    for scenario in config.settings()["egon-data"]["--scenarios"]:
        if scenario != "eGon100RE":
            insert_scenario(scenario)

        else:
            insert_egon100re()
