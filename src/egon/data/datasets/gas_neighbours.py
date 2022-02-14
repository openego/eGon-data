"""The central module containing all code dealing with gas neighbours
"""

import zipfile

from shapely.geometry import LineString
from sqlalchemy.orm import sessionmaker
import geopandas as gpd
import pandas as pd

from egon.data import config, db
from egon.data.datasets import Dataset
from egon.data.datasets.scenario_parameters import get_sector_parameters
from egon.data.datasets.electrical_neighbours import get_map_buses
import egon.data.datasets.etrago_setup as etrago


class GasNeighbours(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="GasNeighbours",
            version="0.0.0",
            dependencies=dependencies,
            tasks=({tyndp_gas_generation}), #grid, tyndp_gas_demand
        )

def get_foreign_bus_id():
    """Calculate the etrago bus id from gas nodes of TYNDP based on the geometry

    Returns
    -------
    pandas.Series
        List of mapped node_ids from TYNDP and etragos bus_id

    """

    sources = config.datasets()["gas_neighbours"]["sources"]

    bus_id = db.select_geodataframe(
        """SELECT bus_id, ST_Buffer(geom, 1) as geom, country
        FROM grid.egon_etrago_bus
        WHERE scn_name = 'eGon2035'
        AND carrier = 'CH4'
        AND country != 'DE'
        """,
        epsg=3035,
    )

    # insert installed capacities
    file = zipfile.ZipFile(f"tyndp/{sources['tyndp_capacities']}")

    # Select buses in neighbouring countries as geodataframe
    buses = pd.read_excel(
        file.open("TYNDP-2020-Scenario-Datafile.xlsx").read(),
        sheet_name="Nodes - Dict",
    ).query("longitude==longitude")
    buses = gpd.GeoDataFrame(
        buses,
        crs=4326,
        geometry=gpd.points_from_xy(buses.longitude, buses.latitude),
    ).to_crs(3035)

    buses["bus_id"] = 0

    # Select bus_id from etrago with shortest distance to TYNDP node
    for i, row in buses.iterrows():
        distance = bus_id.set_index("bus_id").geom.distance(row.geometry)
        buses.loc[i, "bus_id"] = distance[
            distance == distance.min()
        ].index.values[0]

    return buses.set_index("node_id").bus_id

def calc_capacities():
    """Calculates gas production capacities from TYNDP data

    Returns
    -------
    pandas.DataFrame
        Gas production capacities per foreign node and energy carrier

    """

    sources = config.datasets()["gas_neighbours"]["sources"]

    countries = [
        "AT",
        "BE",
        "CH",
        "CZ",
        "DK",
        "FR",
        "NL",
        "NO",
        "SE",
        "PL",
        "UK",
    ]

    # insert installed capacities
    file = zipfile.ZipFile(f"tyndp/{sources['tyndp_capacities']}")
    df = pd.read_excel(
        file.open("TYNDP-2020-Scenario-Datafile.xlsx").read(),
        sheet_name="Gas Data",
    )
    
    df_2030 = ( 
        df.query(
            'Scenario == "Distributed Energy" & '
            'Year == 2030 & '
            'Case == "Average" &' # Case: 2 Week/Average/DF/Peak
            'Category == "Production"'
        )
        .drop(
        columns=[
            "Generator_ID",
            "Climate Year",
            "Simulation_ID",
            "Node 1",
            "Path",
            "Direct/Indirect",
            "Sector",
            "Note",            
        ]
    ).set_index("Node/Line")
    )

    df_2040 = ( # Case: 2 Week/Average/DF/Peak
        df.query(
            'Scenario == "Distributed Energy" & '
            'Year == 2040 & '
            'Case == "Average" &' # Case: 2 Week/Average/DF/Peak
            'Category == "Production"'
        )
        .drop(
        columns=[
            "Generator_ID",
            "Climate Year",
            "Simulation_ID",
            "Node 1",
            "Path",
            "Direct/Indirect",
            "Sector",
            "Note",
        ]
    ).set_index("Node/Line")
    )

    print(df_2030)
    print(df_2040)

    # interpolate linear between 2030 and 2040 for 2035 accordning to
    # scenario report of TSO's and the approval by BNetzA
    df_2035 = pd.DataFrame(index=df_2030.index)
    print(df_2035)
    df_2035["cap_2030"] = df_2030.Value
    print(df_2035)
    df_2035["cap_2040"] = df_2040.Value
    print(df_2035)
    df_2035.fillna(0., inplace=True)
    df_2035["cap_2035"] = (
        df_2035["cap_2030"] + (df_2035["cap_2040"] - df_2035["cap_2030"]) / 2
    )
    df_2035 = df_2035.reset_index()
    df_2035["carrier"] = 'CH4' #df_2035.Generator_ID.map(map_carriers_tyndp())

    grouped_capacities = df_2035
    # # group capacities by new carriers
    # grouped_capacities = (
    #     df_2035.groupby(["carrier", "Node/Line"]).cap_2035.sum().reset_index()
    # )

    # Calculation of ratio Biomethane/Conventional to estimate CO2 content/cost

    # choose capacities for considered countries
    return grouped_capacities[
        grouped_capacities["Node/Line"].str[:2].isin(countries)
    ]


def insert_generators(capacities):
    """Insert gas generators for foreign countries based on TYNDP-data

    Parameters
    ----------
    capacities : pandas.DataFrame
        Gas production capacities per foreign node and energy carrier

    Returns
    -------
    None.

    """
    targets = config.datasets()["gas_neighbours"]["targets"]
    map_buses = get_map_buses()

    # Delete existing data
    db.execute_sql(
        f"""
        DELETE FROM
        {targets['generators']['schema']}.{targets['generators']['table']}
        WHERE bus IN (
            SELECT bus_id FROM
            {targets['buses']['schema']}.{targets['buses']['table']}
            WHERE country != 'DE'
            AND scn_name = 'eGon2035')
        AND scn_name = 'eGon2035'
        AND carrier = 'CH4'
        """
    )
    gen = capacities
    # # Select generators from TYNDP capacities
    # gen = capacities[
    #     capacities.carrier.isin(
    #         [
    #             "other_non_renewable",
    #             "wind_offshore",
    #             "wind_onshore",
    #             "solar",
    #             "other_renewable",
    #             "reservoir",
    #             "run_of_river",
    #             "lignite",
    #             "coal",
    #             "oil",
    #             "nuclear",
    #         ]
    #     )
    # ]

    # Set bus_id
    gen.loc[
        gen[gen["Node/Line"].isin(map_buses.keys())].index, "Node/Line"
    ] = gen.loc[
        gen[gen["Node/Line"].isin(map_buses.keys())].index, "Node/Line"
    ].map(
        map_buses
    )

    gen.loc[:, "bus"] = (
        get_foreign_bus_id().loc[gen.loc[:, "Node/Line"]].values
    )

    # insert data
    session = sessionmaker(bind=db.engine())()
    for i, row in gen.iterrows():
        entry = etrago.EgonPfHvGenerator(
            scn_name="eGon2035",
            generator_id=int(db.next_etrago_id("generator")),
            bus=row.bus,
            carrier=row.carrier,
            p_nom=row.cap_2035,
        )

        session.add(entry)
        session.commit()


def tyndp_gas_generation():
    """Insert data from TYNDP 2020 accordning to NEP 2021
    Scenario 'Distributed Energy', linear interpolate between 2030 and 2040

    Returns
    -------
    None.
    """
    capacities = calc_capacities()
    print(capacities)
    # insert_generators(capacities)

    # insert_storage(capacities)

# def grid():
#     """Insert gas grid compoenents for neighbouring countries

#     Returns
#     -------
#     None.

#     """

# def tyndp_gas_demand():
#     """Insert data from TYNDP 2020 accordning to NEP 2021
#     Scenario 'Distributed Energy', linear interpolate between 2030 and 2040

#     Returns
#     -------
#     None.
#     """
