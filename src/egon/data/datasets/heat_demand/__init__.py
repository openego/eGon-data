# -*- coding: utf-8 -*-
"""
Central module containing all code dealing with the future heat demand import.

This module obtains the residential and service-sector heat demand data for
2015 from Peta5.0.1, calculates future heat demands and saves them in the
database with assigned census cell IDs.

"""

from pathlib import Path  # for database import
from urllib.request import urlretrieve

# for metadata creation
import json
import os
import zipfile

from jinja2 import Template
from rasterio.mask import mask

# packages for ORM class definition
from sqlalchemy import Column, Float, ForeignKey, Integer, Sequence, String
from sqlalchemy.ext.declarative import declarative_base
import geopandas as gpd

# for raster operations
import rasterio

from egon.data import db, subprocess
from egon.data.datasets import Dataset
from egon.data.datasets.scenario_parameters import (
    EgonScenario,
    get_sector_parameters,
)
import egon.data.config

# import time


class HeatDemandImport(Dataset):

    """
    Insert the annual heat demand per census cell for each scenario

    This dataset downloads the heat demand raster data for private households
    and CTS from Peta 5.0.1 (https://s-eenergies-open-data-euf.hub.arcgis.com/maps/
    d7d18b63250240a49eb81db972aa573e/about) and stores it into files in the working directory.
    The data from Peta 5.0.1 represents the status quo of the year 2015.
    To model future heat demands, the data is scaled to meet target values
    from external sources. These target values are defined for each scenario
    in :py:class:`ScenarioParameters <egon.data.datasets.scenario_parameters.ScenarioParameters>`.

    *Dependencies*
      * :py:class:`ScenarioParameters <egon.data.datasets.scenario_parameters.ScenarioParameters>`
      * :py:class:`Vg250 <egon.data.datasets.vg250.Vg250>`
      * :py:class:`ZensusVg250 <egon.data.datasets.zensus_vg250.ZensusVg250>`

    *Resulting tables*
      * :py:class:`demand.egon_peta_heat <egon.data.datasets.heat_demand.EgonPetaHeat>` is created and filled

    """

    
    #:
    name: str = "heat-demands"
    #:
    version: str = "0.0.1"

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            # version=self.target_files + "_0.0",
            version=self.version,  # maybe rethink the naming
            dependencies=dependencies,
            tasks=(scenario_data_import),
        )


Base = declarative_base()


# class for the final dataset in the database
class EgonPetaHeat(Base):
    __tablename__ = "egon_peta_heat"
    __table_args__ = {"schema": "demand"}
    id = Column(
        Integer,
        Sequence("egon_peta_heat_seq", schema="demand"),
        server_default=Sequence(
            "egon_peta_heat_seq", schema="demand"
        ).next_value(),
        primary_key=True,
    )
    demand = Column(Float)
    sector = Column(String)
    scenario = Column(String, ForeignKey(EgonScenario.name))
    zensus_population_id = Column(Integer)


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
        The heat demand data in the Peta5.0.1 dataset are assumed not change.
        An upgrade to a higher Peta version is currently not foreseen.
        Therefore, for the version management we can assume that the dataset
        will not change, unless the code is changed.

    """

    data_config = egon.data.config.datasets()

    # residential heat demands 2015
    peta5_resheatdemands_config = data_config["peta5_0_1_res_heat_demands"][
        "original_data"
    ]

    target_file_res = peta5_resheatdemands_config["target"]["path"]

    if not os.path.isfile(target_file_res):
        urlretrieve(
            peta5_resheatdemands_config["source"]["url"], target_file_res
        )

    # service-sector heat demands 2015
    peta5_serheatdemands_config = data_config["peta5_0_1_ser_heat_demands"][
        "original_data"
    ]

    target_file_ser = peta5_serheatdemands_config["target"]["path"]

    if not os.path.isfile(target_file_ser):
        urlretrieve(
            peta5_serheatdemands_config["source"]["url"], target_file_ser
        )

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
        It is assumed that the Peta5.0.1 dataset does not change and that the
        version number does not need to be checked.

    """

    # Get information from data configuration file
    data_config = egon.data.config.datasets()
    peta5_res_heatdemands_orig = data_config["peta5_0_1_res_heat_demands"][
        "original_data"
    ]
    # path to the downloaded residential heat demand 2015 data
    filepath_zip_res = peta5_res_heatdemands_orig["target"]["path"]

    peta5_ser_heatdemands_orig = data_config["peta5_0_1_ser_heat_demands"][
        "original_data"
    ]
    # path to the downloaded service-sector heat demand 2015 data
    filepath_zip_ser = peta5_ser_heatdemands_orig["target"]["path"]

    directory_to_extract_to = "Peta_5_0_1"
    # Create the folder, if it does not exists already
    if not os.path.exists(directory_to_extract_to):
        os.mkdir(directory_to_extract_to)

    # Unzip the tiffs, if they do not exist
    if not os.path.isfile(
        directory_to_extract_to + "/HD_2015_res_Peta5_0_1_GJ.tif"
    ):
        with zipfile.ZipFile(filepath_zip_res, "r") as zf:
            zf.extractall(directory_to_extract_to)
    if not os.path.isfile(
        directory_to_extract_to + "/HD_2015_ser_Peta5_0_1_GJ.tif"
    ):
        with zipfile.ZipFile(filepath_zip_ser, "r") as zf:
            zf.extractall(directory_to_extract_to)

    return None


def cutout_heat_demand_germany():
    """
    Save cutouts of Germany's 2015 heat demand densities from Europe-wide tifs.

    1. Get the German state boundaries
    2. Load the unzip 2015 heat demand data (Peta5_0_1) and
    3. Cutout Germany's residential and service-sector heat demand densities
    4. Save the cutouts as tiffs

    Parameters
    ----------
        None

    Returns
    -------
        None

    Notes
    ----
        The alternative of cutting out Germany from the pan-European raster
        based on German census cells, instead of using state boundaries with
        low resolution (to avoid inaccuracies), was not implemented in order to
        achieve consistency with other datasets (e.g. egon_mv_grid_district).
        Besides, all attempts to read, (union) and load cells from the local
        database failed, but were documented as commented code within this
        function and afterwards removed.
        If you want to have a look at the comments, please check out commit
        ec3391e182215b32cd8b741557a747118ab61664, which is the last commit
        still containing them.

        Also the usage of a buffer around the boundaries and the subsequent
        selection of German cells was not implemented.
        could be used, but then it must be ensured that later only heat demands
        of cells belonging to Germany are used.

    """

    # Load the German boundaries from the local database using a dissolved
    # dataset which provides one multipolygon
    table_name = "vg250_sta_union"
    schema = "boundaries"
    local_engine = db.engine()

    # Recommened way: gpd.read_postgis()
    # https://geopandas.readthedocs.io/en/latest/docs/reference/api/geopandas.GeoDataFrame.from_postgis.html?highlight=postgis#geopandas.GeoDataFrame.from_postgis
    # multipolygon can be converted into polygons for outcut function
    # using ST_Dump: https://postgis.net/docs/ST_Dump.html

    gdf_boundaries = gpd.read_postgis(
        (
            f"SELECT (ST_Dump(geometry)).geom As geometry"
            f" FROM {schema}.{table_name}"
        ),
        local_engine,
        geom_col="geometry",
    )

    # rasterio wants the mask to be a GeoJSON-like dict or an object that
    # implements the Python geo interface protocol (such as a Shapely Polygon)

    # look at the data, to understanding it
    # gdf_boundaries.head
    # len(gdf_boundaries)
    # type(gdf_boundaries)
    # gdf_boundaries.crs
    # gdf_boundaries.iloc[:,0]
    # gdf_boundaries.iloc[0,:]
    # gdf_boundaries.plot()

    # Load the unzipped heat demand data and cutout Germany

    # path to the downloaded and unzipped rediential heat demand 2015 data
    res_hd_2015 = "Peta_5_0_1/HD_2015_res_Peta5_0_1_GJ.tif"

    # path to the downloaded and unzipped service-sector heat demand 2015 data
    ser_hd_2015 = "Peta_5_0_1/HD_2015_ser_Peta5_0_1_GJ.tif"

    with rasterio.open(res_hd_2015) as dataset:
        # https://rasterio.readthedocs.io/en/latest/topics/masking-by-shapefile.html
        out_image, out_transform = mask(
            dataset, gdf_boundaries.iloc[:, 0], crop=True
        )
        out_meta = dataset.meta

        # Understanding the outputs
        # show(out_image)
        # out_transform

    out_meta.update(
        {
            "driver": "GTiff",
            "height": out_image.shape[1],
            "width": out_image.shape[2],
            "transform": out_transform,
        }
    )

    with rasterio.open(
        "Peta_5_0_1/res_hd_2015_GER.tif", "w", **out_meta
    ) as dest:
        dest.write(out_image)

    # Do the same for the service-sector
    with rasterio.open(ser_hd_2015) as dataset:
        # https://rasterio.readthedocs.io/en/latest/topics/masking-by-shapefile.html
        out_image, out_transform = mask(
            dataset, gdf_boundaries.iloc[:, 0], crop=True
        )
        out_meta = dataset.meta

        # Understanding the outputs
        # show(out_image)
        # out_transform

    out_meta.update(
        {
            "driver": "GTiff",
            "height": out_image.shape[1],
            "width": out_image.shape[2],
            "transform": out_transform,
        }
    )

    with rasterio.open(
        "Peta_5_0_1/ser_hd_2015_GER.tif", "w", **out_meta
    ) as dest:
        dest.write(out_image)

    return None


def future_heat_demand_germany(scenario_name):
    """
    Calculate the future residential and service-sector heat demand per ha.

    The calculation is based on Peta5_0_1 heat demand densities, cutout for
    Germany, for the year 2015. The given scenario name is used to read the
    adjustment factors for the heat demand rasters from the scenario table.

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

        Specify the crs of the created heat demand tiffs: EPSG 3035

        Check if the raster calculations are correct.

        Check, if there are populated cells without heat demand.

    """
    # Load the values
    if scenario_name == "eGon2015":
        res_hd_reduction = 1
        ser_hd_reduction = 1
    else:
        heat_parameters = get_sector_parameters("heat", scenario=scenario_name)

        res_hd_reduction = heat_parameters["DE_demand_reduction_residential"]
        ser_hd_reduction = heat_parameters["DE_demand_reduction_service"]

    # Define the directory where the created rasters will be saved
    scenario_raster_directory = "heat_scenario_raster"
    if not os.path.exists(scenario_raster_directory):
        os.mkdir(scenario_raster_directory)

    # Open, read and adjust the cutout heat demand distributions for Germany
    # https://rasterio.readthedocs.io/en/latest/topics/writing.html
    # https://gis.stackexchange.com/questions/338282/applying-equation-to-a-numpy-array-while-preserving-tiff-metadata-coordinates
    # Write an array as a raster band to a new 16-bit file. For
    # the new file's profile, the profile of the source is adjusted.

    # Residential heat demands first
    res_cutout = "Peta_5_0_1/res_hd_2015_GER.tif"

    with rasterio.open(res_cutout) as src:  # open raster dataset
        res_hd_2015 = src.read(1)  # read as numpy array; band 1; masked=True??
        res_profile = src.profile

    # adjusting and connversion to MWh
    res_scenario_raster = res_hd_reduction * res_hd_2015 / 3.6

    res_profile.update(
        dtype=rasterio.float32,  # set the dtype to float32
        count=1,  # change the band count to 1
        compress="lzw",  # specify LZW compression
    )
    # Save the scenario's residential heat demands as tif file
    # Define the filename for export
    res_result_filename = (
        scenario_raster_directory + "/res_HD_" + scenario_name + ".tif"
    )
    # Open raster dataset in 'w' write mode using the adjusted meta data
    with rasterio.open(res_result_filename, "w", **res_profile) as dst:
        dst.write(res_scenario_raster.astype(rasterio.float32), 1)

    # Do the same for the service-sector
    ser_cutout = "Peta_5_0_1/ser_hd_2015_GER.tif"

    with rasterio.open(ser_cutout) as src:  # open raster dataset
        ser_hd_2015 = src.read(1)  # read as numpy array; band 1; masked=True??
        ser_profile = src.profile

    # adjusting and connversion to MWh
    ser_scenario_raster = ser_hd_reduction * ser_hd_2015 / 3.6

    ser_profile.update(dtype=rasterio.float32, count=1, compress="lzw")
    # Save the scenario's service-sector heat demands as tif file
    # Define the filename for export
    ser_result_filename = (
        scenario_raster_directory + "/ser_HD_" + scenario_name + ".tif"
    )
    # Open raster dataset in 'w' write mode using the adjusted meta data
    with rasterio.open(ser_result_filename, "w", **ser_profile) as dst:
        dst.write(ser_scenario_raster.astype(rasterio.float32), 1)

    return None


def heat_demand_to_db_table():
    """
    Import heat demand rasters and convert them to vector data.

    Specify the rasters to import as raster file patterns (file type and
    directory containing raster files, which all will be imported).
    The rasters are stored in a temporary table called "heat_demand_rasters".
    The final demand data, having the census IDs as foreign key (from the
    census population table), are genetated
    by the provided sql script (raster2cells-and-centroids.sql) and
    are stored in the table "demand.egon_peta_heat".

    Parameters
    ----------
        None

    Returns
    -------
        None

    Notes
    -----
        Please note that the data from "demand.egon_peta_heat" is deleted
        prior to the import, so make sure you're not loosing valuable data.

    TODO
    ----
        Check if data already exists in database or if the function needs
        to be executed: data version management.

        Define version number correctly
    """

    # Define the raster file type to be imported
    sources = ["*.tif"]
    # Define the directory from with all raster files having the defined type
    # will be imported
    sources = [
        path
        for pattern in sources
        for path in Path("heat_scenario_raster").glob(pattern)
    ]

    # Create the schema for the final table, if needed
    engine = db.engine()
    db.execute_sql("CREATE SCHEMA IF NOT EXISTS demand;")
    sql_script = os.path.join(
        os.path.dirname(__file__), "raster2cells-and-centroids.sql"
    )

    db.execute_sql("DELETE FROM demand.egon_peta_heat;")

    for source in sources:
        if not "2015" in source.stem:
            # Create a temporary table and fill the final table using the sql script
            rasters = f"heat_demand_rasters_{source.stem.lower()}"
            import_rasters = subprocess.run(
                ["raster2pgsql", "-e", "-s", "3035", "-I", "-C", "-F", "-a"]
                + [source]
                + [f"{rasters}"],
                text=True,
            ).stdout
            with engine.begin() as connection:
                print(
                    f'CREATE TEMPORARY TABLE "{rasters}"'
                    ' ("rid" serial PRIMARY KEY,"rast" raster,"filename" text);'
                )
                connection.execute(
                    f'CREATE TEMPORARY TABLE "{rasters}"'
                    ' ("rid" serial PRIMARY KEY,"rast" raster,"filename" text);'
                )
                connection.execute(import_rasters)
                connection.execute(f'ANALYZE "{rasters}"')
                with open(sql_script) as convert:
                    connection.execute(
                        Template(convert.read()).render(source=rasters)
                    )
    return None


def adjust_residential_heat_to_zensus(scenario):
    """
    Adjust residential heat demands to fit to zensus population.

    In some cases, Peta assigns residential heat demand to unpopulated cells.
    This can be caused by the different population data used in Peta or
    buildings in zenus cells without a population
    (see :func:`egon.data.importing.zensus.adjust_zensus_misc`)

    Residential heat demand in cells without zensus population is dropped.
    Residential heat demand in cells with zensus population is scaled to meet
    the overall residential heat demands.

    Parameters
    ----------
    scenario : str
        Name of the scenario.

    Returns
    -------
        None
    """

    # Select overall residential heat demand
    overall_demand = db.select_dataframe(
        f"""SELECT SUM(demand) as overall_demand
        FROM  demand.egon_peta_heat
        WHERE scenario = {'scenario'} and sector = 'residential'
        """
    ).overall_demand[0]

    # Select heat demand in populated cells
    df = db.select_dataframe(
        f"""SELECT *
        FROM  demand.egon_peta_heat
        WHERE scenario = {'scenario'} and sector = 'residential'
        AND zensus_population_id IN (
            SELECT id
            FROM society.destatis_zensus_population_per_ha_inside_germany
            )""",
        index_col="id",
    )

    # Scale heat demands in populated cells
    df.loc[:, "demand"] *= overall_demand / df.loc[:, "demand"].sum()

    # Drop residential heat demands
    db.execute_sql(
        f"""DELETE FROM demand.egon_peta_heat
        WHERE scenario = {'scenario'} and sector = 'residential'"""
    )

    # Insert adjusted heat demands in populated cells
    df.to_sql(
        "egon_peta_heat", schema="demand", con=db.engine(), if_exists="append"
    )

    return None


def add_metadata():
    """
    Writes metadata JSON string into table comment.

    TODO
    ----
        Meta data must be check and adjusted to the egon_data standard:
            - Add context

        Meta data for Census Population Table must be added.

        Check how to reference the heat demand adjustment factors
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
    url_peta = (
        "https://s-eenergies-open-data-euf.hub.arcgis.com/search?"
        "categories=seenergies_buildings"
    )

    url_geodatenzentrum = (
        "https://daten.gdz.bkg.bund.de/produkte/vg/"
        "vg250_ebenen_0101/2020/vg250_01-01.geo84.shape."
        "ebenen.zip"
    )
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
        "name": "egon_peta_heat_metadata",
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
        "resources": [
            {
                "profile": "tabular-data-resource",
                "name": "egon_peta_heat",
                "path": "",
                "format": "PostgreSQL",
                "encoding": "UTF-8",
                "schema": {
                    "fields": [
                        {
                            "name": "id",
                            "description": "Unique identifier",
                            "type": "serial",
                            "unit": "none",
                        },
                        {
                            "name": "demand",
                            "description": "annual heat demand",
                            "type": "double precision",
                            "unit": "MWh",
                        },
                        {
                            "name": "sector",
                            "description": "sector e.g. residential",
                            "type": "text",
                            "unit": "none",
                        },
                        {
                            "name": "scenario",
                            "description": "scenario name",
                            "type": "text",
                            "unit": "none",
                        },
                        {
                            "name": "zensus_population_id",
                            "description": "census cell id",
                            "type": "integer",
                            "unit": "none",
                        },
                    ],
                    "primaryKey": ["id"],
                    "foreignKeys": [
                        {
                            "fields": ["zensus_population_id"],
                            "reference": {
                                "resource": "society.destatis_zensus_population_per_ha",
                                "fields": ["id"],
                            },
                        },
                        {
                            "fields": ["scenario"],
                            "reference": {
                                "resource": "scenario.egon_scenario_parameters",
                                "fields": ["name"],
                            },
                        },
                    ],
                },
                "dialect": {"delimiter": "none", "decimalSeparator": "."},
            }
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
                "path": ("https://creativecommons.org/publicdomain/zero/1.0/"),
            },
        },
    }
    meta_json = "'" + json.dumps(meta) + "'"

    db.submit_comment(meta_json, "demand", "egon_peta_heat")


def scenario_data_import():
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

    """
    # create schema if not exists
    db.execute_sql("CREATE SCHEMA IF NOT EXISTS demand;")
    # drop table if exists
    # can be removed when table structure doesn't change anymore
    db.execute_sql("DROP TABLE IF EXISTS demand.egon_peta_heat CASCADE")
    db.execute_sql("DROP SEQUENCE IF EXISTS demand.egon_peta_heat_seq CASCADE")
    # create table
    EgonPetaHeat.__table__.create(bind=db.engine(), checkfirst=True)

    download_peta5_0_1_heat_demands()
    unzip_peta5_0_1_heat_demands()
    cutout_heat_demand_germany()
    # Specifiy the scenario names for loading factors from csv file
    future_heat_demand_germany("eGon2035")
    future_heat_demand_germany("eGon100RE")
    # future_heat_demand_germany("eGon2015")
    heat_demand_to_db_table()
    adjust_residential_heat_to_zensus("eGon2035")
    adjust_residential_heat_to_zensus("eGon100RE")
    # future_heat_demand_germany("eGon2015")
    add_metadata()

    return None
