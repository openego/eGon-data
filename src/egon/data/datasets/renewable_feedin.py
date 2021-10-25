"""
Central module containing all code dealing with processing era5 weather data.
"""

import numpy as np

import egon.data.config
import geopandas as gpd
import pandas as pd
from egon.data import db
from egon.data.datasets import Dataset
from egon.data.datasets.era5 import import_cutout
from egon.data.datasets.scenario_parameters import get_sector_parameters


class RenewableFeedin(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="RenewableFeedin",
            version="0.0.1",
            dependencies=dependencies,
            tasks=(wind, pv, solar_thermal),
        )


def weather_cells_in_germany(geom_column="geom"):
    """Get weather cells which intersect with Germany

    Returns
    -------
    GeoPandas.GeoDataFrame
        Index and points of weather cells inside Germany

    """

    cfg = egon.data.config.datasets()["renewable_feedin"]["sources"]

    return db.select_geodataframe(
        f"""SELECT w_id, geom_point, geom
        FROM {cfg['weather_cells']['schema']}.
        {cfg['weather_cells']['table']}
        WHERE ST_Intersects('SRID=4326;
        POLYGON((5 56, 15.5 56, 15.5 47, 5 47, 5 56))', geom)""",
        geom_col=geom_column,
        index_col="w_id",
    )


def federal_states_per_weather_cell():
    """Assings a federal state to each weather cell in Germany.

    Sets the federal state to the weather celss using the centroid.
    Weather cells at the borders whoes centroid is not inside Germany
    are assinged to the closest federal state.

    Returns
    -------
    GeoPandas.GeoDataFrame
        Index, points and federal state of weather cells inside Germany

    """

    cfg = egon.data.config.datasets()["renewable_feedin"]["sources"]

    # Select weather cells and ferear states from database
    weather_cells = weather_cells_in_germany(geom_column="geom_point")

    federal_states = db.select_geodataframe(
        f"""SELECT gen, geometry
        FROM {cfg['vg250_lan_union']['schema']}.
        {cfg['vg250_lan_union']['table']}""",
        geom_col="geometry",
        index_col="gen",
    )

    # Map federal state and onshore wind turbine to weather cells
    weather_cells["federal_state"] = gpd.sjoin(
        weather_cells, federal_states
    ).index_right

    # Assign a federal state to each cell inside Germany
    buffer = 1000

    while (buffer < 30000) & (
        len(weather_cells[weather_cells["federal_state"].isnull()]) > 0
    ):

        cells = weather_cells[weather_cells["federal_state"].isnull()]

        cells.loc[:, "geom_point"] = cells.geom_point.buffer(buffer)

        weather_cells.loc[cells.index, "federal_state"] = gpd.sjoin(
            cells, federal_states
        ).index_right

        buffer += 200

        weather_cells = (
            weather_cells.reset_index()
            .drop_duplicates(subset="w_id", keep="first")
            .set_index("w_id")
        )
        
        weather_cells = weather_cells.dropna(axis=0, subset=["federal_state"])

    return weather_cells.to_crs(4326)


def turbine_per_weather_cell():
    """Assign wind onshore turbine types to weather cells

    Returns
    -------
    weather_cells : GeoPandas.GeoDataFrame
        Weather cells in Germany including turbine type

    """

    # Select representative onshore wind turbines per federal state
    map_federal_states_turbines = {
        "Schleswig-Holstein": "E-126",
        "Bremen": "E-126",
        "Hamburg": "E-126",
        "Mecklenburg-Vorpommern": "E-126",
        "Niedersachsen": "E-126",
        "Berlin": "E-141",
        "Brandenburg": "E-141",
        "Hessen": "E-141",
        "Nordrhein-Westfalen": "E-141",
        "Sachsen": "E-141",
        "Sachsen-Anhalt": "E-141",
        "Thüringen": "E-141",
        "Baden-Württemberg": "E-141",
        "Bayern": "E-141",
        "Rheinland-Pfalz": "E-141",
        "Saarland": "E-141",
    }

    # Select weather cells and federal states
    weather_cells = federal_states_per_weather_cell()

    # Assign turbine type per federal state
    weather_cells["wind_turbine"] = weather_cells["federal_state"].map(
        map_federal_states_turbines
    )

    return weather_cells


def feedin_per_turbine():
    """Calculate feedin timeseries per turbine type and weather cell

    Returns
    -------
    gdf : GeoPandas.GeoDataFrame
        Feed-in timeseries per turbine type and weather cell

    """

    # Select weather data for Germany
    cutout = import_cutout(boundary="Germany")

    gdf = gpd.GeoDataFrame(geometry=cutout.grid_cells(), crs=4326)

    # Calculate feedin-timeseries for E-141
    # source: https://openenergy-platform.org/dataedit/view/supply/wind_turbine_library
    turbine_e141 = {
        "name": "E141 4200 kW",
        "hub_height": 129,
        "P": 4.200,
        "V": np.arange(1, 26, dtype=float),
        "POW": np.array(
            [
                0.0,
                0.022,
                0.104,
                0.26,
                0.523,
                0.92,
                1.471,
                2.151,
                2.867,
                3.481,
                3.903,
                4.119,
                4.196,
                4.2,
                4.2,
                4.2,
                4.2,
                4.2,
                4.2,
                4.2,
                4.2,
                4.2,
                4.2,
                4.2,
                4.2,
            ]
        ),
    }
    ts_e141 = cutout.wind(
        turbine_e141, per_unit=True, shapes=cutout.grid_cells()
    )

    gdf["E-141"] = ts_e141.to_pandas().transpose().values.tolist()

    # Calculate feedin-timeseries for E-126
    # source: https://openenergy-platform.org/dataedit/view/supply/wind_turbine_library
    turbine_e126 = {
        "name": "E126 4200 kW",
        "hub_height": 159,
        "P": 4.200,
        "V": np.arange(1, 26, dtype=float),
        "POW": np.array(
            [
                0.0,
                0.0,
                0.058,
                0.185,
                0.4,
                0.745,
                1.2,
                1.79,
                2.45,
                3.12,
                3.66,
                4.0,
                4.15,
                4.2,
                4.2,
                4.2,
                4.2,
                4.2,
                4.2,
                4.2,
                4.2,
                4.2,
                4.2,
                4.2,
                4.2,
            ]
        ),
    }
    ts_e126 = cutout.wind(
        turbine_e126, per_unit=True, shapes=cutout.grid_cells()
    )

    gdf["E-126"] = ts_e126.to_pandas().transpose().values.tolist()

    return gdf


def wind():
    """Insert feed-in timeseries for wind onshore turbines to database

    Returns
    -------
    None.

    """

    cfg = egon.data.config.datasets()["renewable_feedin"]["targets"]

    # Get weather cells with turbine type
    weather_cells = turbine_per_weather_cell()
    weather_cells = weather_cells[weather_cells.wind_turbine.notnull()]

    # Calculate feedin timeseries per turbine and weather cell
    timeseries_per_turbine = feedin_per_turbine()

    # Join weather cells and feedin-timeseries
    timeseries = gpd.sjoin(weather_cells, timeseries_per_turbine)[
        ["E-141", "E-126"]
    ]

    weather_year = get_sector_parameters("global", "eGon2035")["weather_year"]

    df = pd.DataFrame(
        index=weather_cells.index,
        columns=["weather_year", "carrier", "feedin"],
        data={"weather_year": weather_year, "carrier": "wind_onshore"},
    )

    # Insert feedin for selected turbine per weather cell
    for turbine in ["E-126", "E-141"]:
        idx = weather_cells.index[
            (weather_cells.wind_turbine == turbine)
            & (weather_cells.index.isin(timeseries.index))
        ]
        df.loc[idx, "feedin"] = timeseries.loc[idx, turbine].values

    db.execute_sql(
        f"""
                   DELETE FROM {cfg['feedin_table']['schema']}.
                   {cfg['feedin_table']['table']}
                   WHERE carrier = 'wind_onshore'"""
    )

    # Insert values into database
    df.to_sql(
        cfg["feedin_table"]["table"],
        schema=cfg["feedin_table"]["schema"],
        con=db.engine(),
        if_exists="append",
    )


def pv():
    """Insert feed-in timeseries for pv plants to database

    Returns
    -------
    None.

    """

    # Get weather cells in Germany
    weather_cells = weather_cells_in_germany()

    # Select weather data for Germany
    cutout = import_cutout(boundary="Germany")

    # Select weather year from cutout
    weather_year = cutout.name.split("-")[1]

    # Calculate feedin timeseries
    ts_pv = cutout.pv(
        "CSi",
        orientation={"slope": 35.0, "azimuth": 180.0},
        per_unit=True,
        shapes=weather_cells.to_crs(4326).geom,
    )

    # Create dataframe and insert to database
    insert_feedin(ts_pv, "pv", weather_year)


def solar_thermal():
    """Insert feed-in timeseries for pv plants to database

    Returns
    -------
    None.

    """

    # Get weather cells in Germany
    weather_cells = weather_cells_in_germany()

    # Select weather data for Germany
    cutout = import_cutout(boundary="Germany")

    # Select weather year from cutout
    weather_year = cutout.name.split("-")[1]

    # Calculate feedin timeseries
    ts_solar_thermal = cutout.solar_thermal(
        clearsky_model="simple",
        orientation={"slope": 45.0, "azimuth": 180.0},
        per_unit=True,
        shapes=weather_cells.to_crs(4326).geom,
    )

    # Create dataframe and insert to database
    insert_feedin(ts_solar_thermal, "solar_thermal", weather_year)


def insert_feedin(data, carrier, weather_year):
    """Insert feedin data into database

    Parameters
    ----------
    data : xarray.core.dataarray.DataArray
        Feedin timeseries data
    carrier : str
        Name of energy carrier
    weather_year : int
        Selected weather year

    Returns
    -------
    None.

    """
    # Transpose DataFrame
    data = data.transpose()

    # Load configuration
    cfg = egon.data.config.datasets()["renewable_feedin"]

    # Initialize DataFrame
    df = pd.DataFrame(
        index=data.to_pandas().index,
        columns=["weather_year", "carrier", "feedin"],
        data={"weather_year": weather_year, "carrier": carrier},
    )

    # Insert feedin into DataFrame
    df.feedin = data.to_pandas().values.tolist()

    # Delete existing rows for carrier
    db.execute_sql(
        f"""
                   DELETE FROM {cfg['targets']['feedin_table']['schema']}.
                   {cfg['targets']['feedin_table']['table']}
                   WHERE carrier = '{carrier}'"""
    )

    # Insert values into database
    df.to_sql(
        cfg["targets"]["feedin_table"]["table"],
        schema=cfg["targets"]["feedin_table"]["schema"],
        con=db.engine(),
        if_exists="append",
    )
