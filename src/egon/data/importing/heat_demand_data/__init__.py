# -*- coding: utf-8 -*-

# This script is part of eGon-data.

# license text - to be added.

"""
Central module containing all code dealing with the future heat demand import.

This module obtains the residential and service-sector heat demand data for
2015 from Peta5.0.1, calculates future heat demands and saves them in the
database with assigned census cell IDs.

"""

from urllib.request import urlretrieve
import os
import zipfile

from egon.data import db, subprocess
import egon.data.config

import pandas as pd
import geopandas as gpd

import rasterio
from rasterio.mask import mask
# import matplotlib.pyplot as plt

from pathlib import Path  # for database import

# for metadata creation
import json
# import time


def download_peta5_0_1_heat_demands():
    """
    Download Peta5.0.1 tiff files.

    The downloaded data contain residential and service-sector heat demands
    per hectar grid cell for 2015.

    Parameters
    ----------
        None

    Returns
    -------
        None

    Notes
    -----
        None

    TODO
    ----
        Check if downloaded data already exists
    """

    data_config = egon.data.config.datasets()

    # residential heat demands 2015
    peta5_resheatdemands_config = (data_config["peta5_0_1_res_heat_demands"]
                                   ["original_data"])

    target_file_res = os.path.join(os.path.dirname(__file__),
                                   peta5_resheatdemands_config["target"]
                                   ["path"])

    if not os.path.isfile(target_file_res):
        urlretrieve(peta5_resheatdemands_config["source"]["url"],
                    target_file_res)

    # service-sector heat demands 2015
    peta5_serheatdemands_config = (data_config["peta5_0_1_ser_heat_demands"]
                                   ["original_data"])

    target_file_ser = os.path.join(os.path.dirname(__file__),
                                   peta5_serheatdemands_config["target"]
                                   ["path"])

    if not os.path.isfile(target_file_ser):
        urlretrieve(peta5_serheatdemands_config["source"]["url"],
                    target_file_ser)

    return None


def unzip_peta5_0_1_heat_demands():
    """
    Unzip the downloaded Peta5.0.1 tiff files.

    Parameters
    ----------
        None

    Returns
    -------
        None

    Notes
    -----
        None

    TODO
    ----
        Check if unzipped data already exists
    """

    # Get information from data configuration file
    data_config = egon.data.config.datasets()
    peta5_res_heatdemands_orig = (data_config["peta5_0_1_res_heat_demands"]
                                  ["original_data"])
    # path to the downloaded residential heat demand 2015 data
    filepath_zip_res = os.path.join(os.path.dirname(__file__),
                                    peta5_res_heatdemands_orig["target"]
                                    ["path"])

    peta5_ser_heatdemands_orig = (data_config["peta5_0_1_ser_heat_demands"]
                                  ["original_data"])
    # path to the downloaded service-sector heat demand 2015 data
    filepath_zip_ser = os.path.join(os.path.dirname(__file__),
                                    peta5_ser_heatdemands_orig["target"]
                                    ["path"])

    # Create a folder, if it does not exists already
    if not os.path.exists(os.path.join(os.path.dirname(__file__),
                                       'Peta_5_0_1')):
        os.mkdir(os.path.join(os.path.dirname(__file__), 'Peta_5_0_1'))

    directory_to_extract_to = os.path.join(os.path.dirname(__file__),
                                           "Peta_5_0_1")

    # Unzip the tiffs
    with zipfile.ZipFile(filepath_zip_res, 'r') as zf:
        zf.extractall(directory_to_extract_to)

    with zipfile.ZipFile(filepath_zip_ser, 'r') as zf:
        zf.extractall(directory_to_extract_to)

    return None


def cutout_heat_demand_germany():
    """
    Save cutouts of Germany's 2015 heat demand densities from Europe-wide tifs.

    1. Get the German state boundaries
    2. Load the unzip 2015 heat demand data (Peta5_0_1) and
    3. Cutout Germany's residential and service-sector heat demand densities
    4. Save the cutouts as tifs

    Parameters
    ----------
        None

    Returns
    -------
        None

    Notes
    -----
        None

    TODO
    ----
        It would be better to cutout Germany from the pan-European raster
        based on German census cells, instead of using state boundaries with
        low resolution, to avoid inaccuracies. All attempts to read, (union)
        and load cells from the local database failed, but are documented here.

        Depending on the process afterwards also a buffer around the boundaries
        could be used, but then it must be ensured that later only heat demands
        of cells belonging to Germany are used.

        Specify the crs of the created heat demand tiffs: EPSG 3035

        Check if cutcut already exists

    """

    # Load the census cells from the local database for mask definition
    # This approach could not be implemented for operation on a laptop.

    # table_name = "destatis_zensus_population_per_ha"
    # schema = "society"

    # local_engine = db.engine()

    # get the entire table for Germany - very large dataset
    # df_census = pd.read_sql_table(table_name, local_engine, schema=schema)

    # get a test dataset
    # df_census = pd.read_sql(f"SELECT * FROM {schema}.{table_name} WHERE gid<200",
    #             local_engine)

    # get only the geometries
    # df_census = pd.read_sql(f"SELECT DISTINCT tabelle.geom FROM"
    #                         f" {schema}.{table_name} AS tabelle WHERE gid<200",
    #                       local_engine)

    # get only the geometries, but already dissolved to one polygon
    # df_census = pd.read_sql(f"SELECT ST_MemUnion(T.geom) FROM"
    #                        f" {schema}.{table_name} AS T", local_engine)

    # better / alternative form to write it, easier to handle
    # does not work
    # test = db.execute_sql(f"SELECT * FROM {schema}.{table_name} WHERE gid<200;")
    # does not work
    # test2 = db.execute_sql("SELECT ST_Union(T.geom) FROM"
    #                       f" {schema}.{table_name} AS T")

    # loading the unioned census cells from the local database - with condition
    # does not work
    # mask = db.execute_sql(
    #    f"""SELECT ST_Union(inner_results.geom) FROM(
    #                SELECT T.geom
    #                FROM {schema}.{table_name} AS T
    #                WHERE T.gid<200) AS inner_results"""
    #)
    # with a buffer, but without where condition
    # does not work on a laptop
    # mask_all = db.execute_sql(
    #    f"SELECT ST_Union(ST_Buffer(T.geom,1)) FROM {schema}.{table_name} AS T")



    # Load the German boundaries from the local database using a dissolved
    # dataset which provides one multipolygon
    table_name = "vg250_sta_union"
    schema = "boundaries"
    local_engine = db.engine()

    # Recommened way: gpd.read_postgis()
    # https://geopandas.readthedocs.io/en/latest/docs/reference/api/geopandas.GeoDataFrame.from_postgis.html?highlight=postgis#geopandas.GeoDataFrame.from_postgis
    # multipolygon can be converted into polygons for outcut function
    # using ST_Dump: https://postgis.net/docs/ST_Dump.html

    # gdf_boundaries_multi = gpd.read_postgis(
    #                 (f"SELECT geometry"
    #                  f" FROM {schema}.{table_name}"),
    #                 local_engine, geom_col = "geometry")

    gdf_boundaries = gpd.read_postgis(
                    (f"SELECT (ST_Dump(geometry)).geom As geometry"
                     f" FROM {schema}.{table_name}"),
                    local_engine, geom_col = "geometry")

    # rasterio wants the mask to be a GeoJSON-like dict or an object that
    # implements the Python geo interface protocol (such as a Shapely Polygon)

    # look at the data, to understanding it
    # gdf_boundaries.head
    # gdf_boundaries_multi.head
    # len(gdf_boundaries)
    # type(gdf_boundaries)
    # gdf_boundaries.crs

    # type(gdf_boundaries_multi.iloc[0,0])
    # gdf_boundaries.iloc[:,0]
    # gdf_boundaries.iloc[0,:]
    # gdf_boundaries.plot()

    # Load the unzipped heat demand data and cutout Germany

    # path to the downloaded and unzipped rediential heat demand 2015 data
    res_hd_2015 = os.path.join(
        os.path.dirname(__file__), "Peta_5_0_1/HD_2015_res_Peta5_0_1_GJ.tif"
    )

    # path to the downloaded and unzipped service-sector heat demand 2015 data
    ser_hd_2015 = os.path.join(
        os.path.dirname(__file__), "Peta_5_0_1/HD_2015_ser_Peta5_0_1_GJ.tif"
    )

    with rasterio.open(res_hd_2015) as dataset:
        # https://rasterio.readthedocs.io/en/latest/topics/masking-by-shapefile.html
        out_image, out_transform = mask(dataset, gdf_boundaries.iloc[:, 0],
                                        crop=True)
        out_meta = dataset.meta

        # Understanding the outputs
        # show(out_image)
        # out_transform

    out_meta.update({"driver": "GTiff",
         "height": out_image.shape[1],
         "width": out_image.shape[2],
         "transform": out_transform})

    with rasterio.open("Peta_5_0_1/res_hd_2015_GER.tif", "w",
                       **out_meta) as dest:
        dest.write(out_image)

    # Do the same for the service-sector
    with rasterio.open(ser_hd_2015) as dataset:
        # https://rasterio.readthedocs.io/en/latest/topics/masking-by-shapefile.html
        out_image, out_transform = mask(dataset, gdf_boundaries.iloc[:, 0],
                                        crop=True)
        out_meta = dataset.meta

        # Understanding the outputs
        # show(out_image)
        # out_transform

    out_meta.update({"driver": "GTiff",
         "height": out_image.shape[1],
         "width": out_image.shape[2],
         "transform": out_transform})

    with rasterio.open("Peta_5_0_1/ser_hd_2015_GER.tif", "w",
                       **out_meta) as dest:
        dest.write(out_image)

    return None


def future_heat_demand_germany(scenario_name):
    """
    Calculate the future residential and service-sector heat demand per ha.

    The calculation is based on Peta5_0_1 heat demand densities, cutcut for
    Germany, for the year 2015. The given scenario name is used to read the
    adjustment factors for the heat demand rasters from a file.

    Parameters
    ----------
    scenario_name: str
        Selected scenario name for which assumptions will be loaded.

    Returns
    -------
        None

    Notes
    -----
        None

    TODO
    ----
        Error messeage for the case that the specified scenario name is not in
        the file with the scenario data.

        Check if future heat demands already exists

        Specify the crs of the created heat demand tiffs: EPSG 3035

        Check if the raster calculations are correct.

        Check, if there are populated cells without heat demand.

        Check, if there are unpoplated cells with residential heat demands.

    """

    # Load the adjustment factors from an excel files, and not from a csv
    # might be implemented later
    # pip install xlrd to make it work
    # xlsfilename = ""
    # df_reductions = pd.read_excel(xlsfilename,sheet_name="scenarios_for_raster_adjustment")

    # Load the csv file with the sceanario data for raster adjustment
    # to be adjusted

    csvfilename = os.path.join(os.path.dirname(__file__),
                               "scenarios_HD_raster_adjustments.csv")
    df_reductions = pd.read_csv(csvfilename)

    for index, row in df_reductions.iterrows():
        if scenario_name == df_reductions.loc[index, "scenario"]:
            res_hd_reduction = df_reductions.loc[index,
                                                 "HD_reduction_residential"]
            # print(res_hd_reduction)
            ser_hd_reduction = df_reductions.loc[index,
                                                 "HD_reduction_service_sector"]
            # print(ser_hd_reduction)

    # Define the directory where the created rasters will be saved
    if not os.path.exists(os.path.join(os.path.dirname(__file__),
                                       'scenario_raster')):
        os.mkdir(os.path.join(os.path.dirname(__file__), 'scenario_raster'))

    # Open, read and adjust the cutout heat demand distributions for Germany
    # https://rasterio.readthedocs.io/en/latest/topics/writing.html
    # https://gis.stackexchange.com/questions/338282/applying-equation-to-a-numpy-array-while-preserving-tiff-metadata-coordinates
    # Write an array as a raster band to a new 16-bit file. For
    # the new file's profile, the profile of the source is adjusted.
    # Residential heat demands first

    res_cutout = os.path.join(os.path.dirname(__file__),
                              "Peta_5_0_1/res_hd_2015_GER.tif")

    with rasterio.open(res_cutout) as src:  # open raster dataset
        res_hd_2015 = src.read(1)  # read as numpy array; band 1; masked=True??
        res_profile = src.profile

    res_scenario_raster = res_hd_reduction * res_hd_2015  # adjusting

    res_profile.update(
        dtype=rasterio.uint16,  # set the dtype to uint16
        count=1,  # change the band count to 1
        compress='lzw'  # specify LZW compression
        )
    # Save the scenario's residential heat demands as tif file
    # Define the filename for export
    res_result_filename = os.path.join(os.path.dirname(__file__),
                                       'scenario_raster/res_HD_' +
                                       scenario_name + '.tif')
    # Open raster dataset in 'w' write mode using the adjuste meta data
    with rasterio.open(res_result_filename, 'w', **res_profile) as dst:
        dst.write(res_scenario_raster.astype(rasterio.uint16), 1)

    # Do the same for the service-sector
    ser_cutout = os.path.join(os.path.dirname(__file__),
                              "Peta_5_0_1/ser_hd_2015_GER.tif")

    with rasterio.open(ser_cutout) as src:  # open raster dataset
        ser_hd_2015 = src.read(1)  # read as numpy array; band 1; masked=True??
        ser_profile = src.profile

    ser_scenario_raster = ser_hd_reduction * ser_hd_2015  # adjusting

    ser_profile.update(
        dtype=rasterio.uint16,
        count=1,
        compress='lzw'
        )
    # Save the scenario's service-sector heat demands as tif file
    # Define the filename for export
    ser_result_filename = os.path.join(os.path.dirname(__file__),
                                       'scenario_raster/ser_HD_' +
                                       scenario_name + '.tif')
    # Open raster dataset in 'w' write mode using the adjuste meta data
    with rasterio.open(ser_result_filename, 'w', **ser_profile) as dst:
        dst.write(ser_scenario_raster.astype(rasterio.uint16), 1)

    # Make some images
    # show(res_scenario_raster)

    # %matplotlib inline
    # Plot the raster
    # plt.imshow(res_scenario_raster, cmap='terrain_r')
    # Add colorbar to show the index
    # plt.colorbar()

    return None


def heat_demand_to_db_table():
    """
    Import heat demand rasters and convert them to vector data.

    Specify the rasters to import as raster file patterns (file type and
    directory containing raster files, which all will be imported).
    The rasters are stored in a temporary table called "heat_demand_rasters".
    The final demand data, having the census IDs as foreign key, are genetated
    by the provided sql script (raster2cells-and-centroids.sql) and
    are stored in the table "demand.heat_demands".


    Parameters
    ----------
        None

    Returns
    -------
        None

    Notes
    -----
        Please note that the table "demand.heat_demands" is dropped prior to
        the import, so make sure you're not loosing valuable data.

    TODO
    ----
        Check if data already exists in database and the function does not need
        to be executed again

        Add a column with version number for versioning!

    """

    # Define the raster file type to be imported
    sources = ["*.tif"]
    # Define the directory from with all raster files having the defined type
    # will be imported
    sources = [path for pattern in sources for path in
               Path(os.path.join(os.path.dirname(__file__), 'scenario_raster')
                    ).glob(pattern)]

    # Create the schema for the final table, if needed
    engine = db.engine()
    db.execute_sql("CREATE SCHEMA IF NOT EXISTS demand;")
    # Create a temporary table and fill the final table using the sql script
    rasters = "heat_demand_rasters"
    import_rasters = subprocess.run(
        ["raster2pgsql", "-e", "-s", "3035", "-I", "-C", "-F", "-a"]
        + sources
        + [f"{rasters}"],
        text=True,
    ).stdout
    with engine.begin() as connection:
        connection.execute(
            f'CREATE TEMPORARY TABLE "{rasters}"'
            ' ("rid" serial PRIMARY KEY,"rast" raster,"filename" text);'
        )
        connection.execute(import_rasters)
        connection.execute(f'ANALYZE "{rasters}"')
        with open("raster2cells-and-centroids.sql") as convert:
            connection.execute(convert.read())

    return None


def add_metadata():
    """
    Writes metadata JSON string into table comment.

    TODO
    ----
        Meta data must be check and adjusted to the egon_data standard.

        Meta data for Census Population Table must be added.
    """

    # Prepare variables
    license_peta5_0_1 = [
        {
            "name": "Creative Commons Attribution 4.0 International",
            "title": "CC BY 4.0",
            "path": "https://creativecommons.org/licenses/by/4.0/",
            "instruction": (
                "You are free: To Share, To Adapt;"
                " As long as you: Attribute!"
            ),
            "attribution": "© Flensburg, Halmstad and Aalborg universities",
        }
    ]
    url_peta = ("https://s-eenergies-open-data-euf.hub.arcgis.com/search?"
                "categories=seenergies_buildings")

    url_geodatenzentrum = ("https://daten.gdz.bkg.bund.de/produkte/vg/"
                           "vg250_ebenen_0101/2020/vg250_01-01.geo84.shape."
                           "ebenen.zip")
    license_heat = [
        {
            # this could be the license of "heat"
            "name": "Creative Commons Attribution 4.0 International",
            "title": "CC BY 4.0",
            "path": "https://creativecommons.org/licenses/by/4.0/",
            "instruction": (
                "You are free: To Share, To Adapt;"
                " As long as you: Attribute!"
            ),
            "attribution": "© Europa-Universität Flensburg",  # if all agree
        }
    ]
    license_BKG = [
        {
            "title": "Datenlizenz Deutschland – Namensnennung – Version 2.0",
            "path": "www.govdata.de/dl-de/by-2-0",
            "instruction": (
                "Jede Nutzung ist unter den Bedingungen dieser „Datenlizenz "
                "Deutschland - Namensnennung - Version 2.0 zulässig.\nDie "
                "bereitgestellten Daten und Metadaten dürfen für die "
                "kommerzielle und nicht kommerzielle Nutzung insbesondere:"
                "(1) vervielfältigt, ausgedruckt, präsentiert, verändert, "
                "bearbeitet sowie an Dritte übermittelt werden;\n "
                "(2) mit eigenen Daten und Daten Anderer zusammengeführt und "
                "zu selbständigen neuen Datensätzen verbunden werden;\n "
                "(3) in interne und externe Geschäftsprozesse, Produkte und "
                "Anwendungen in öffentlichen und nicht öffentlichen "
                "elektronischen Netzwerken eingebunden werden."
            ),
            "attribution": "© Bundesamt für Kartographie und Geodäsie",
        }
    ]

    # Metadata creation
    meta = {
        "name": "heat",
        "title": "eGo_n scenario-specific future heat demand data",
        "description": "Future heat demands per hectare grid cell of "
        "the residential and service sector",
        "language": ["EN"],
        "spatial": {
            "location": "",
            "extent": "Germany",
            "resolution": "100x100m",
            },
        "temporal": {
            "referenceDate": "scenario-specific",
            "timeseries": {
                "start": "",
                "end": "",
                "resolution": "",
                "alignment": "",
                "aggregationType": "",
                },
            },
        "sources": [
            {
                # for Peta5_0_1
                "title": "Peta5_0_1_HD_res and Peta5 0 1 HD ser",
                "description": "Der Datenbestand umfasst sämtliche "
                "Verwaltungseinheiten aller hierarchischen "
                "Verwaltungsebenen vom Staat bis zu den "
                "Gemeinden mit ihren Verwaltungsgrenzen, "
                "statistischen Schlüsselzahlen und dem "
                "Namen der Verwaltungseinheit sowie der "
                "spezifischen Bezeichnung der "
                "Verwaltungsebene des jeweiligen "
                "Bundeslandes.",
                "path": url_peta,
                "licenses": license_peta5_0_1,
            },
            {
                # for the vg250_sta_union used - Please check!
                "title": "Dienstleistungszentrum des Bundes für "
                "Geoinformation und Geodäsie - Open Data",
                "description": "Dieser Datenbestand steht über "
                "Geodatendienste gemäß "
                "Geodatenzugangsgesetz (GeoZG) "
                "(http://www.geodatenzentrum.de/auftrag/pdf"
                "/geodatenzugangsgesetz.pdf) für die "
                "kommerzielle und nicht kommerzielle "
                "Nutzung geldleistungsfrei zum Download "
                "und zur Online-Nutzung zur Verfügung. Die "
                "Nutzung der Geodaten und Geodatendienste "
                "wird durch die Verordnung zur Festlegung "
                "der Nutzungsbestimmungen für die "
                "Bereitstellung von Geodaten des Bundes "
                "(GeoNutzV) (http://www.geodatenzentrum.de"
                "/auftrag/pdf/geonutz.pdf) geregelt. "
                "Insbesondere hat jeder Nutzer den "
                "Quellenvermerk zu allen Geodaten, "
                "Metadaten und Geodatendiensten erkennbar "
                "und in optischem Zusammenhang zu "
                "platzieren. Veränderungen, Bearbeitungen, "
                "neue Gestaltungen oder sonstige "
                "Abwandlungen sind mit einem "
                "Veränderungshinweis im Quellenvermerk zu "
                "versehen. Quellenvermerk und "
                "Veränderungshinweis sind wie folgt zu "
                "gestalten. Bei der Darstellung auf einer "
                "Webseite ist der Quellenvermerk mit der "
                "URL http://www.bkg.bund.de zu verlinken. "
                "© GeoBasis-DE / BKG <Jahr des letzten "
                "Datenbezugs> © GeoBasis-DE / BKG "
                "<Jahr des letzten Datenbezugs> "
                "(Daten verändert) Beispiel: "
                "© GeoBasis-DE / BKG 2013",
                "path": url_geodatenzentrum,
                "licenses": "Geodatenzugangsgesetz (GeoZG)",
                "copyright": "© GeoBasis-DE / BKG 2016 (Daten verändert)",
            },
            {
                # for the vg250_sta_union used, too - Please check!
                "title": "BKG - Verwaltungsgebiete 1:250.000 (vg250)",
                "description": "Der Datenbestand umfasst sämtliche "
                "Verwaltungseinheiten aller hierarchischen "
                "Verwaltungsebenen vom Staat bis zu den "
                "Gemeinden mit ihren Verwaltungsgrenzen, "
                "statistischen Schlüsselzahlen und dem "
                "Namen der Verwaltungseinheit sowie der "
                "spezifischen Bezeichnung der "
                "Verwaltungsebene des jeweiligen "
                "Bundeslandes.",
                "path": "http://www.bkg.bund.de",
                "licenses": license_BKG,
            },
            ],
        "licenses": license_heat,
        "contributors": [
            {
                "title": "Eva, Günni, Clara",
                "email": "",
                "date": "2021-03-04",
                "object": "",
                "comment": "Processed data",
            }
        ],
        "metaMetadata": {  # https://github.com/OpenEnergyPlatform/oemetadata
            "metadataVersion": "OEP-1.4.0",
            "metadataLicense": {
                "name": "CC0-1.0",
                "title": "Creative Commons Zero v1.0 Universal",
                "path": (
                    "https://creativecommons.org/publicdomain/zero/1.0/"
                ),
            },
        },
    }
    meta_json = "'" + json.dumps(meta) + "'"

    db.submit_comment(
        meta_json, "demand", "heat"
        )


def future_heat_demand_data_import():
    """
    Call all heat demand import related functions.

    This function executes the functions that download, unzip and adjust
    the heat demand distributions from Peta5.0.1
    and that save the future heat demand distributions for Germany as tiffs
    as well as with census grid IDs as foreign key in the database.

    Parameters
    ----------
        None

    Returns
    -------
        None

    Notes
    -----
        None

    TODO
    ----
        check which tasks need to run (according to version number)
   """

    download_peta5_0_1_heat_demands()
    unzip_peta5_0_1_heat_demands()
    cutout_heat_demand_germany()
    # Specifiy the scenario names for loading factors from csv file
    future_heat_demand_germany("eGon2035")
    future_heat_demand_germany("eGon100RE")
    heat_demand_to_db_table()
    add_metadata()

    return None
