"""
Use the concept of dynamic line rating(DLR) to calculate temporal
depending capacity for HV transmission lines.
Inspired mainly on Planungsgrundsaetze-2020
Available at:
<https://www.transnetbw.de/files/pdf/netzentwicklung/netzplanungsgrundsaetze/UENB_PlGrS_Juli2020.pdf>
"""
from pathlib import Path

from shapely.geometry import Point
import geopandas as gpd
import numpy as np
import pandas as pd
import psycopg2
import rioxarray
import xarray as xr

from egon.data import db
from egon.data.datasets import Dataset
import egon.data.config


class Calculate_dlr(Dataset):
    """Calculate DLR and assign values to each line in the db

    Parameters
    ----------
    *No parameters required

    *Dependencies*
      * :py:class:`DataBundle <egon.data.datasets.data_bundle.DataBundle>`
      * :py:class:`Osmtgmod <egon.data.datasets.osmtgmod.Osmtgmod>`
      * :py:class:`WeatherData <egon.data.datasets.era5.WeatherData>`
      * :py:class:`FixEhvSubnetworks <egon.data.datasets.FixEhvSubnetworks>`

    *Resulting tables*
      * :py:class:`grid.egon_etrago_line_timeseries
        <egon.data.datasets.etrago_setup.EgonPfHvLineTimeseries>` is filled
    """

    #:
    name: str = "dlr"
    #:
    version: str = "0.0.1"

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(dlr,),
        )


def dlr():
    """Calculate DLR and assign values to each line in the db

    Parameters
    ----------
    *No parameters required

    """
    cfg = egon.data.config.datasets()["dlr"]
    weather_info_path = Path(".") / "cutouts" / "germany-2011-era5.nc"

    regions_shape_path = (
        Path(".")
        / "data_bundle_egon_data"
        / "regions_dynamic_line_rating"
        / "Germany_regions.shp"
    )

    # Calculate hourly DLR per region
    dlr_hourly_dic, dlr_hourly = DLR_Regions(
        weather_info_path, regions_shape_path
    )

    regions = gpd.read_file(regions_shape_path)
    regions = regions.sort_values(by=["Region"])

    # Connect to the data base
    con = db.engine()

    sql = f"""
    SELECT scn_name, line_id, topo, s_nom FROM
    {cfg['sources']['trans_lines']['schema']}.
    {cfg['sources']['trans_lines']['table']}
    """
    df = gpd.GeoDataFrame.from_postgis(
        sql, con, crs="EPSG:4326", geom_col="topo"
    )

    trans_lines_R = {}
    for i in regions.Region:
        shape_area = regions[regions["Region"] == i]
        trans_lines_R[i] = gpd.clip(df, shape_area)
    trans_lines = df[["s_nom"]]
    trans_lines["in_regions"] = [[] for i in range(len(df))]

    trans_lines[["line_id", "geometry", "scn_name"]] = df[
        ["line_id", "topo", "scn_name"]
    ]
    trans_lines = gpd.GeoDataFrame(trans_lines)
    # Assign to each transmission line the region to which it belongs
    for i in trans_lines_R:
        for j in trans_lines_R[i].index:
            trans_lines.loc[j][1] = trans_lines.loc[j][1].append(i)
    trans_lines["crossborder"] = ~trans_lines.within(regions.unary_union)

    DLR = []

    # Assign to each transmision line the final values of DLR based on location
    # and type of line (overhead or underground)
    for i in trans_lines.index:
        # The concept of DLR does not apply to crossborder lines
        if trans_lines.loc[i, "crossborder"] == True:
            DLR.append([1] * 8760)
            continue
        # Underground lines have DLR = 1
        if (
            trans_lines.loc[i][0] % 280 == 0
            or trans_lines.loc[i][0] % 550 == 0
            or trans_lines.loc[i][0] % 925 == 0
        ):
            DLR.append([1] * 8760)
            continue
        # Lines completely in one of the regions, have the DLR of the region
        if len(trans_lines.loc[i][1]) == 1:
            region = int(trans_lines.loc[i][1][0])
            DLR.append(dlr_hourly_dic["R" + str(region) + "-DLR"])
            continue
        # For lines crossing 2 or more regions, the lowest DLR between the
        # different regions per hour is assigned.
        if len(trans_lines.loc[i][1]) > 1:
            reg = []
            for j in trans_lines.loc[i][1]:
                reg.append("Reg_" + str(j))
            min_DLR_reg = dlr_hourly[reg].min(axis=1)
            DLR.append(list(min_DLR_reg))

    trans_lines["s_max_pu"] = DLR

    # delete unnecessary columns
    trans_lines.drop(
        columns=["in_regions", "s_nom", "geometry", "crossborder"],
        inplace=True,
    )

    # Modify column "s_max_pu" to fit the requirement of the table
    trans_lines["s_max_pu"] = trans_lines.apply(
        lambda x: list(x["s_max_pu"]), axis=1
    )
    trans_lines["temp_id"] = 1

    # Delete existing data
    db.execute_sql(
        f"""
        DELETE FROM {cfg['sources']['line_timeseries']['schema']}.
        {cfg['sources']['line_timeseries']['table']};
        """
    )

    # Insert into database
    trans_lines.to_sql(
        f"{cfg['targets']['line_timeseries']['table']}",
        schema=f"{cfg['targets']['line_timeseries']['schema']}",
        con=db.engine(),
        if_exists="append",
        index=False,
    )
    return 0


def DLR_Regions(weather_info_path, regions_shape_path):
    """Calculate DLR values for the given regions

    Parameters
    ----------
    weather_info_path: str, mandatory
        path of the weather data downloaded from ERA5
    regions_shape_path: str, mandatory
        path to the shape file with the shape of the regions to analyze

    """

    # load, index and sort shapefile with the 9 regions defined by NEP 2020
    regions = gpd.read_file(regions_shape_path)
    regions = regions.set_index(["Region"])
    regions = regions.sort_values(by=["Region"])

    # The data downloaded using Atlite is loaded in 'weather_data_raw'.
    path = Path(".") / "cutouts" / "germany-2011-era5.nc"
    weather_data_raw = xr.open_mfdataset(str(path))
    weather_data_raw = weather_data_raw.rio.write_crs(4326)
    weather_data_raw = weather_data_raw.rio.clip_box(
        minx=5.5, miny=47, maxx=15.5, maxy=55.5
    )

    wind_speed_raw = weather_data_raw.wnd100m.values
    temperature_raw = weather_data_raw.temperature.values
    roughness_raw = weather_data_raw.roughness.values
    index = weather_data_raw.indexes._indexes
    # The info in 'weather_data_raw' has 3 dimensions. In 'weather_data' will be
    # stored all the relevant data in a 2 dimensions array.
    weather_data = np.zeros(shape=(wind_speed_raw.size, 5))
    count = 0
    for hour in range(index["time"].size):
        for row in range(index["y"].size):
            for column in range(index["x"].size):
                rough = roughness_raw[hour, row, column]
                ws_100m = wind_speed_raw[hour, row, column]
                # Use Log Law to calculate wind speed at 50m height
                ws_50m = ws_100m * (np.log(50 / rough) / np.log(100 / rough))
                weather_data[count, 0] = hour
                weather_data[count, 1] = index["y"][row]
                weather_data[count, 2] = index["x"][column]
                weather_data[count, 3] = ws_50m
                weather_data[count, 4] = (
                    temperature_raw[hour, row, column] - 273.15
                )
                count += 1

    weather_data = pd.DataFrame(
        weather_data, columns=["hour", "lat", "lon", "wind_s", "temp"]
    )

    region_selec = weather_data[0 : index["x"].size * index["y"].size].copy()
    region_selec["geom"] = region_selec.apply(
        lambda x: Point(x["lon"], x["lat"]), axis=1
    )
    region_selec = gpd.GeoDataFrame(region_selec)
    region_selec = region_selec.set_geometry("geom")
    region_selec["region"] = np.zeros(index["x"].size * index["y"].size)

    # Mask weather information for each region defined by NEP 2020
    for reg in regions.index:
        weather_region = gpd.clip(region_selec, regions.loc[reg][0])
        region_selec["region"][
            region_selec.isin(weather_region).any(axis=1)
        ] = reg

    weather_data["region"] = (
        region_selec["region"].tolist() * index["time"].size
    )
    weather_data = weather_data[weather_data["region"] != 0]

    # Create data frame to save results(Min wind speed, max temperature and %DLR per region along 8760h in a year)
    time = pd.date_range("2011-01-01", "2011-12-31 23:00:00", freq="H")
    # time = time.transpose()
    dlr = pd.DataFrame(
        0,
        columns=[
            "R1-Wind_min",
            "R1-Temp_max",
            "R1-DLR",
            "R2-Wind_min",
            "R2-Temp_max",
            "R2-DLR",
            "R3-Wind_min",
            "R3-Temp_max",
            "R3-DLR",
            "R4-Wind_min",
            "R4-Temp_max",
            "R4-DLR",
            "R5-Wind_min",
            "R5-Temp_max",
            "R5-DLR",
            "R6-Wind_min",
            "R6-Temp_max",
            "R6-DLR",
            "R7-Wind_min",
            "R7-Temp_max",
            "R7-DLR",
            "R8-Wind_min",
            "R8-Temp_max",
            "R8-DLR",
            "R9-Wind_min",
            "R9-Temp_max",
            "R9-DLR",
        ],
        index=time,
    )

    # Calculate and save min wind speed and max temperature in a dataframe.
    # Since the dataframe generated by the function era5.weather_df_from_era5() is sorted by date,
    # it is faster to calculate the hourly results using blocks of data defined by "step", instead of
    # using a filter or a search function.
    for reg, df in weather_data.groupby(["region"]):
        for t in range(0, len(time)):
            step = df.shape[0] / len(time)
            low_limit = int(t * step)
            up_limit = int(step * (t + 1))
            dlr.iloc[t, 0 + int(reg - 1) * 3] = min(
                df.iloc[low_limit:up_limit, 3]
            )
            dlr.iloc[t, 1 + int(reg - 1) * 3] = max(
                df.iloc[low_limit:up_limit, 4]
            )

    # The next loop use the min wind speed and max temperature calculated previously to
    # define the hourly DLR for each region based on the table given by NEP 2020 pag 31
    for i in range(0, len(regions)):
        for j in range(0, len(time)):
            if dlr.iloc[j, 1 + i * 3] <= 5:
                if dlr.iloc[j, 0 + i * 3] < 3:
                    dlr.iloc[j, 2 + i * 3] = 1.30
                elif dlr.iloc[j, 0 + i * 3] < 4:
                    dlr.iloc[j, 2 + i * 3] = 1.35
                elif dlr.iloc[j, 0 + i * 3] < 5:
                    dlr.iloc[j, 2 + i * 3] = 1.45
                else:
                    dlr.iloc[j, 2 + i * 3] = 1.50
            elif dlr.iloc[j, 1 + i * 3] <= 15:
                if dlr.iloc[j, 0 + i * 3] < 3:
                    dlr.iloc[j, 2 + i * 3] = 1.20
                elif dlr.iloc[j, 0 + i * 3] < 4:
                    dlr.iloc[j, 2 + i * 3] = 1.25
                elif dlr.iloc[j, 0 + i * 3] < 5:
                    dlr.iloc[j, 2 + i * 3] = 1.35
                elif dlr.iloc[j, 0 + i * 3] < 6:
                    dlr.iloc[j, 2 + i * 3] = 1.45
                else:
                    dlr.iloc[j, 2 + i * 3] = 1.50
            elif dlr.iloc[j, 1 + i * 3] <= 25:
                if dlr.iloc[j, 0 + i * 3] < 3:
                    dlr.iloc[j, 2 + i * 3] = 1.10
                elif dlr.iloc[j, 0 + i * 3] < 4:
                    dlr.iloc[j, 2 + i * 3] = 1.15
                elif dlr.iloc[j, 0 + i * 3] < 5:
                    dlr.iloc[j, 2 + i * 3] = 1.20
                elif dlr.iloc[j, 0 + i * 3] < 6:
                    dlr.iloc[j, 2 + i * 3] = 1.30
                else:
                    dlr.iloc[j, 2 + i * 3] = 1.40
            elif dlr.iloc[j, 1 + i * 3] <= 35:
                if dlr.iloc[j, 0 + i * 3] < 3:
                    dlr.iloc[j, 2 + i * 3] = 1.00
                elif dlr.iloc[j, 0 + i * 3] < 4:
                    dlr.iloc[j, 2 + i * 3] = 1.05
                elif dlr.iloc[j, 0 + i * 3] < 5:
                    dlr.iloc[j, 2 + i * 3] = 1.10
                elif dlr.iloc[j, 0 + i * 3] < 6:
                    dlr.iloc[j, 2 + i * 3] = 1.15
                else:
                    dlr.iloc[j, 2 + i * 3] = 1.25
            else:
                dlr.iloc[j, 2 + i * 3] = 1.00

    DLR_hourly_df_dic = {}
    for i in dlr.columns[range(2, 29, 3)]:  # columns with DLR values
        DLR_hourly_df_dic[i] = dlr[i].values

    dlr_hourly = pd.DataFrame(index=time)
    for i in range(len(regions)):
        dlr_hourly["Reg_" + str(i + 1)] = dlr.iloc[:, 3 * i + 2]

    return DLR_hourly_df_dic, dlr_hourly
