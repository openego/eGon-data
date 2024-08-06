"""The central module containing all code dealing with importing data from
the pysa-eur-sec scenario parameter creation
"""

from pathlib import Path
import json

from shapely.geometry import LineString
import geopandas as gpd
import importlib_resources as resources
import numpy as np
import pandas as pd
import pypsa
import shutil
import yaml
from urllib.request import urlretrieve

from egon.data import __path__, db, logger, config
from egon.data.datasets import Dataset
from egon.data.datasets.scenario_parameters import get_sector_parameters
from egon.data.datasets.scenario_parameters.parameters import (
    annualize_capital_costs,
)
import egon.data.config
import egon.data.subprocess as subproc


class PreparePypsaEur(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="PreparePypsaEur",
            version="0.0.6",
            dependencies=dependencies,
            tasks=(
                download,
                prepare_network,
            ),
        )


class RunPypsaEur(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="SolvePypsaEur",
            version="0.0.5",
            dependencies=dependencies,
            tasks=(
                execute,
                solve_network,
                clean_database,
                electrical_neighbours_egon100,
                overwrite_H2_pipeline_share,
            ),
        )


def download():
    cwd = Path(".")
    filepath = cwd / "run-pypsa-eur"
    filepath.mkdir(parents=True, exist_ok=True)

    pypsa_eur_repos = filepath / "pypsa-eur"

    if not pypsa_eur_repos.exists():
        subproc.run(
            [
                "git",
                "clone",
                "--branch",
                "v0.10.0",
                "https://github.com/PyPSA/pypsa-eur.git",
                pypsa_eur_repos,
            ]
        )

        # Add gurobi solver to environment:
        # Read YAML file
        path_to_env = pypsa_eur_repos / "envs" / "environment.yaml"
        with open(path_to_env, "r") as stream:
            env = yaml.safe_load(stream)

        env["dependencies"][-1]["pip"].append("gurobipy==10.0.0")

        # Limit geopandas version
        # our pypsa-eur version is not compatible to geopandas>1
        env = ["geopandas>=0.11.0, <1" if
               x=="geopandas>=0.11.0" else x for x in env["dependencies"]]


        # Write YAML file
        with open(path_to_env, "w", encoding="utf8") as outfile:
            yaml.dump(
                env, outfile, default_flow_style=False, allow_unicode=True
            )

        # Copy config file for egon-data to pypsa-eur directory
        shutil.copy(Path(
            __path__[0],
            "datasets",
            "pypsaeur",
            "config.yaml"),
            pypsa_eur_repos / "config",
        )

        with open(filepath / "Snakefile", "w") as snakefile:
            snakefile.write(
                resources.read_text("egon.data.datasets.pypsaeur", "Snakefile")
        )

    # Copy era5 weather data to folder for pypsaeur
    era5_pypsaeur_path = filepath / "pypsa-eur" / "cutouts"

    if not era5_pypsaeur_path.exists():
        era5_pypsaeur_path.mkdir(parents=True, exist_ok=True)
        copy_from = config.datasets()[
            "era5_weather_data"]["targets"]["weather_data"]["path"]
        filename = "europe-2011-era5.nc"
        shutil.copy(copy_from + "/" + filename, era5_pypsaeur_path / filename)

    # Workaround to download natura and shipdensity data, which is not working
    # in the regular snakemake workflow.
    # The same files are downloaded from the same directory as in pypsa-eur
    # version 0.10 here. Is is stored in the folders from pypsa-eur.
    if not (filepath / "pypsa-eur" / "resources").exists():
        (filepath / "pypsa-eur" / "resources"
         ).mkdir(parents=True, exist_ok=True)
    urlretrieve(
        "https://zenodo.org/record/4706686/files/natura.tiff",
        filepath / "pypsa-eur" / "resources" / "natura.tiff",
        )

    if not (filepath / "pypsa-eur" / "data").exists():
        (filepath / "pypsa-eur" / "data"
         ).mkdir(parents=True, exist_ok=True)
    urlretrieve(
        "https://zenodo.org/record/6953563/files/shipdensity_global.zip",
        filepath / "pypsa-eur" / "data" / "shipdensity_global.zip",
        )

def prepare_network():
    cwd = Path(".")
    filepath = cwd / "run-pypsa-eur"

    subproc.run(
        [
            "snakemake",
            "-j1",
            "--directory",
            filepath,
            "--snakefile",
            filepath / "Snakefile",
            "--use-conda",
            "--conda-frontend=conda",
            "prepare",
        ]
    )


def solve_network():
    cwd = Path(".")
    filepath = cwd / "run-pypsa-eur"

    if config.settings()["egon-data"]["--run-pypsa-eur"]:
        subproc.run(
            [
                "snakemake",
                "-j1",
                "--directory",
                filepath,
                "--snakefile",
                filepath / "Snakefile",
                "--use-conda",
                "--conda-frontend=conda",
                "solve",
            ]
        )
    else:
        print("Pypsa-eur is not executed due to the settings of egon-data")


def read_network():
    if config.settings()["egon-data"]["--run-pypsa-eur"]:
        with open(
            __path__[0] + "/datasets/pypsaeur/config.yaml", "r"
        ) as stream:
            data_config = yaml.safe_load(stream)

        target_file = (
            Path(".")
            / "run-pypsa-eur"
            / "pypsa-eur"
            / "results"
            / data_config["run"]["name"]
            / "postnetworks"
            / f"elec_s_{data_config['scenario']['clusters'][0]}"
            f"_l{data_config['scenario']['ll'][0]}"
            f"_{data_config['scenario']['opts'][0]}"
            f"_{data_config['scenario']['sector_opts'][0]}"
            f"_{data_config['scenario']['planning_horizons'][0]}.nc"
        )

    else:
        target_file = (
            Path(".")
            / "data_bundle_egon_data"
            / "pypsa_eur_sec"
            / "2022-07-26-egondata-integration"
            / "postnetworks"
            / "elec_s_37_lv2.0__Co2L0-1H-T-H-B-I-dist1_2050.nc"
        )

    return pypsa.Network(target_file)


def clean_database():
    """Remove all components abroad for eGon100RE of the database

    Remove all components abroad and their associated time series of
    the datase for the scenario 'eGon100RE'.

    Parameters
    ----------
    None

    Returns
    -------
    None

    """
    scn_name = "eGon100RE"

    comp_one_port = ["load", "generator", "store", "storage"]

    # delete existing components and associated timeseries
    for comp in comp_one_port:
        db.execute_sql(
            f"""
            DELETE FROM {"grid.egon_etrago_" + comp + "_timeseries"}
            WHERE {comp + "_id"} IN (
                SELECT {comp + "_id"} FROM {"grid.egon_etrago_" + comp}
                WHERE bus IN (
                    SELECT bus_id FROM grid.egon_etrago_bus
                    WHERE country != 'DE'
                    AND scn_name = '{scn_name}')
                AND scn_name = '{scn_name}'
            );

            DELETE FROM {"grid.egon_etrago_" + comp}
            WHERE bus IN (
                SELECT bus_id FROM grid.egon_etrago_bus
                WHERE country != 'DE'
                AND scn_name = '{scn_name}')
            AND scn_name = '{scn_name}';"""
        )

    comp_2_ports = [
        "line",
        "transformer",
        "link",
    ]

    for comp, id in zip(comp_2_ports, ["line_id", "trafo_id", "link_id"]):
        db.execute_sql(
            f"""
            DELETE FROM {"grid.egon_etrago_" + comp + "_timeseries"}
            WHERE scn_name = '{scn_name}'
            AND {id} IN (
                SELECT {id} FROM {"grid.egon_etrago_" + comp}
            WHERE "bus0" IN (
            SELECT bus_id FROM grid.egon_etrago_bus
                WHERE country != 'DE'
                AND scn_name = '{scn_name}')
            AND "bus1" IN (
            SELECT bus_id FROM grid.egon_etrago_bus
                WHERE country != 'DE'
                AND scn_name = '{scn_name}')
            );

            DELETE FROM {"grid.egon_etrago_" + comp}
            WHERE scn_name = '{scn_name}'
            AND "bus0" IN (
            SELECT bus_id FROM grid.egon_etrago_bus
                WHERE country != 'DE'
                AND scn_name = '{scn_name}')
            AND "bus1" IN (
            SELECT bus_id FROM grid.egon_etrago_bus
                WHERE country != 'DE'
                AND scn_name = '{scn_name}')
            ;"""
        )

    db.execute_sql(
        "DELETE FROM grid.egon_etrago_bus "
        "WHERE scn_name = '{scn_name}' "
        "AND country <> 'DE' "
        "AND carrier <> 'AC'"
    )


def electrical_neighbours_egon100():
    if "eGon100RE" in egon.data.config.settings()["egon-data"]["--scenarios"]:
        neighbor_reduction()

    else:
        print(
            "eGon100RE is not in the list of created scenarios, this task is skipped."
        )


def neighbor_reduction():
    network = read_network()

    #network.links.drop("pipe_retrofit", axis="columns", inplace=True)

    wanted_countries = [
        "DE",
        "AT",
        "CH",
        "CZ",
        "PL",
        "SE",
        "NO",
        "DK",
        "GB",
        "NL",
        "BE",
        "FR",
        "LU",
    ]
    foreign_buses = network.buses[
        ~network.buses.index.str.contains("|".join(wanted_countries))
    ]
    network.buses = network.buses.drop(
        network.buses.loc[foreign_buses.index].index
    )

    # drop foreign lines and links from the 2nd row

    network.lines = network.lines.drop(
        network.lines[
            (network.lines["bus0"].isin(network.buses.index) == False)
            & (network.lines["bus1"].isin(network.buses.index) == False)
        ].index
    )

    # select all lines which have at bus1 the bus which is kept
    lines_cb_1 = network.lines[
        (network.lines["bus0"].isin(network.buses.index) == False)
    ]

    # create a load at bus1 with the line's hourly loading
    for i, k in zip(lines_cb_1.bus1.values, lines_cb_1.index):
        network.add(
            "Load",
            "slack_fix " + i + " " + k,
            bus=i,
            p_set=network.lines_t.p1[k],
        )
        network.loads.carrier.loc["slack_fix " + i + " " + k] = (
            lines_cb_1.carrier[k]
        )

    # select all lines which have at bus0 the bus which is kept
    lines_cb_0 = network.lines[
        (network.lines["bus1"].isin(network.buses.index) == False)
    ]

    # create a load at bus0 with the line's hourly loading
    for i, k in zip(lines_cb_0.bus0.values, lines_cb_0.index):
        network.add(
            "Load",
            "slack_fix " + i + " " + k,
            bus=i,
            p_set=network.lines_t.p0[k],
        )
        network.loads.carrier.loc["slack_fix " + i + " " + k] = (
            lines_cb_0.carrier[k]
        )

    # do the same for links

    network.links = network.links.drop(
        network.links[
            (network.links["bus0"].isin(network.buses.index) == False)
            & (network.links["bus1"].isin(network.buses.index) == False)
        ].index
    )

    # select all links which have at bus1 the bus which is kept
    links_cb_1 = network.links[
        (network.links["bus0"].isin(network.buses.index) == False)
    ]

    # create a load at bus1 with the link's hourly loading
    for i, k in zip(links_cb_1.bus1.values, links_cb_1.index):
        network.add(
            "Load",
            "slack_fix_links " + i + " " + k,
            bus=i,
            p_set=network.links_t.p1[k],
        )
        network.loads.carrier.loc["slack_fix_links " + i + " " + k] = (
            links_cb_1.carrier[k]
        )

    # select all links which have at bus0 the bus which is kept
    links_cb_0 = network.links[
        (network.links["bus1"].isin(network.buses.index) == False)
    ]

    # create a load at bus0 with the link's hourly loading
    for i, k in zip(links_cb_0.bus0.values, links_cb_0.index):
        network.add(
            "Load",
            "slack_fix_links " + i + " " + k,
            bus=i,
            p_set=network.links_t.p0[k],
        )
        network.loads.carrier.loc["slack_fix_links " + i + " " + k] = (
            links_cb_0.carrier[k]
        )

    # drop remaining foreign components

    network.lines = network.lines.drop(
        network.lines[
            (network.lines["bus0"].isin(network.buses.index) == False)
            | (network.lines["bus1"].isin(network.buses.index) == False)
        ].index
    )

    network.links = network.links.drop(
        network.links[
            (network.links["bus0"].isin(network.buses.index) == False)
            | (network.links["bus1"].isin(network.buses.index) == False)
        ].index
    )

    network.transformers = network.transformers.drop(
        network.transformers[
            (network.transformers["bus0"].isin(network.buses.index) == False)
            | (network.transformers["bus1"].isin(network.buses.index) == False)
        ].index
    )
    network.generators = network.generators.drop(
        network.generators[
            (network.generators["bus"].isin(network.buses.index) == False)
        ].index
    )

    network.loads = network.loads.drop(
        network.loads[
            (network.loads["bus"].isin(network.buses.index) == False)
        ].index
    )

    network.storage_units = network.storage_units.drop(
        network.storage_units[
            (network.storage_units["bus"].isin(network.buses.index) == False)
        ].index
    )

    components = [
        "loads",
        "generators",
        "lines",
        "buses",
        "transformers",
        "links",
    ]
    for g in components:  # loads_t
        h = g + "_t"
        nw = getattr(network, h)  # network.loads_t
        for i in nw.keys():  # network.loads_t.p
            cols = [
                j
                for j in getattr(nw, i).columns
                if j not in getattr(network, g).index
            ]
            for k in cols:
                del getattr(nw, i)[k]

    # writing components of neighboring countries to etrago tables

    # Set country tag for all buses
    network.buses.country = network.buses.index.str[:2]
    neighbors = network.buses[network.buses.country != "DE"]

    neighbors["new_index"] = (
        db.next_etrago_id("bus") + neighbors.reset_index().index
    )

    # Use index of AC buses created by electrical_neigbors
    foreign_ac_buses = db.select_dataframe(
        """
        SELECT * FROM grid.egon_etrago_bus
        WHERE carrier = 'AC' AND v_nom = 380
        AND country!= 'DE' AND scn_name ='eGon100RE'
        AND bus_id NOT IN (SELECT bus_i FROM osmtgmod_results.bus_data)
        """)
    buses_with_defined_id = neighbors[
        (neighbors.carrier=="AC")
        &(neighbors.country.isin(foreign_ac_buses.country.values))].index
    neighbors.loc[
        buses_with_defined_id, "new_index"] = foreign_ac_buses.set_index(
            "x").loc[neighbors.loc[buses_with_defined_id, "x"]].bus_id.values

    # lines, the foreign crossborder lines
    # (without crossborder lines to Germany!)

    neighbor_lines = network.lines[
        network.lines.bus0.isin(neighbors.index)
        & network.lines.bus1.isin(neighbors.index)
    ]
    if not network.lines_t["s_max_pu"].empty:
        neighbor_lines_t = network.lines_t["s_max_pu"][neighbor_lines.index]

    neighbor_lines.reset_index(inplace=True)
    neighbor_lines.bus0 = (
        neighbors.loc[neighbor_lines.bus0, "new_index"].reset_index().new_index
    )
    neighbor_lines.bus1 = (
        neighbors.loc[neighbor_lines.bus1, "new_index"].reset_index().new_index
    )
    neighbor_lines.index += db.next_etrago_id("line")

    if not network.lines_t["s_max_pu"].empty:
        for i in neighbor_lines_t.columns:
            new_index = neighbor_lines[neighbor_lines["name"] == i].index
            neighbor_lines_t.rename(columns={i: new_index[0]}, inplace=True)

    # links
    neighbor_links = network.links[
        network.links.bus0.isin(neighbors.index)
        & network.links.bus1.isin(neighbors.index)
    ]

    neighbor_links.reset_index(inplace=True)
    neighbor_links.bus0 = (
        neighbors.loc[neighbor_links.bus0, "new_index"].reset_index().new_index
    )
    neighbor_links.bus1 = (
        neighbors.loc[neighbor_links.bus1, "new_index"].reset_index().new_index
    )
    neighbor_links.index += db.next_etrago_id("link")

    # generators
    neighbor_gens = network.generators[
        network.generators.bus.isin(neighbors.index)
    ]
    neighbor_gens_t = network.generators_t["p_max_pu"][
        neighbor_gens[
            neighbor_gens.index.isin(network.generators_t["p_max_pu"].columns)
        ].index
    ]

    neighbor_gens.reset_index(inplace=True)
    neighbor_gens.bus = (
        neighbors.loc[neighbor_gens.bus, "new_index"].reset_index().new_index
    )
    neighbor_gens.index += db.next_etrago_id("generator")

    for i in neighbor_gens_t.columns:
        new_index = neighbor_gens[neighbor_gens["Generator"] == i].index
        neighbor_gens_t.rename(columns={i: new_index[0]}, inplace=True)

    # loads

    neighbor_loads = network.loads[network.loads.bus.isin(neighbors.index)]
    neighbor_loads_t_index = neighbor_loads.index[
        neighbor_loads.index.isin(network.loads_t.p_set.columns)
    ]
    neighbor_loads_t = network.loads_t["p_set"][neighbor_loads_t_index]

    neighbor_loads.reset_index(inplace=True)
    neighbor_loads.bus = (
        neighbors.loc[neighbor_loads.bus, "new_index"].reset_index().new_index
    )
    neighbor_loads.index += db.next_etrago_id("load")

    for i in neighbor_loads_t.columns:
        new_index = neighbor_loads[neighbor_loads["Load"] == i].index
        neighbor_loads_t.rename(columns={i: new_index[0]}, inplace=True)

    # stores
    neighbor_stores = network.stores[network.stores.bus.isin(neighbors.index)]
    neighbor_stores_t_index = neighbor_stores.index[
        neighbor_stores.index.isin(network.stores_t.e_min_pu.columns)
    ]
    neighbor_stores_t = network.stores_t["e_min_pu"][neighbor_stores_t_index]

    neighbor_stores.reset_index(inplace=True)
    neighbor_stores.bus = (
        neighbors.loc[neighbor_stores.bus, "new_index"].reset_index().new_index
    )
    neighbor_stores.index += db.next_etrago_id("store")

    for i in neighbor_stores_t.columns:
        new_index = neighbor_stores[neighbor_stores["Store"] == i].index
        neighbor_stores_t.rename(columns={i: new_index[0]}, inplace=True)

    # storage_units
    neighbor_storage = network.storage_units[
        network.storage_units.bus.isin(neighbors.index)
    ]
    neighbor_storage_t_index = neighbor_storage.index[
        neighbor_storage.index.isin(network.storage_units_t.inflow.columns)
    ]
    neighbor_storage_t = network.storage_units_t["inflow"][
        neighbor_storage_t_index
    ]

    neighbor_storage.reset_index(inplace=True)
    neighbor_storage.bus = (
        neighbors.loc[neighbor_storage.bus, "new_index"]
        .reset_index()
        .new_index
    )
    neighbor_storage.index += db.next_etrago_id("storage")

    for i in neighbor_storage_t.columns:
        new_index = neighbor_storage[
            neighbor_storage["StorageUnit"] == i
        ].index
        neighbor_storage_t.rename(columns={i: new_index[0]}, inplace=True)

    # Connect to local database
    engine = db.engine()

    neighbors["scn_name"] = "eGon100RE"
    neighbors.index = neighbors["new_index"]

    # Correct geometry for non AC buses
    carriers = set(neighbors.carrier.to_list())
    carriers = [e for e in carriers if e not in ("AC", "biogas")]
    non_AC_neighbors = pd.DataFrame()
    for c in carriers:
        c_neighbors = neighbors[neighbors.carrier == c].set_index(
            "location", drop=False
        )
        for i in ["x", "y"]:
            c_neighbors = c_neighbors.drop(i, axis=1)
        coordinates = neighbors[neighbors.carrier == "AC"][
            ["location", "x", "y"]
        ].set_index("location")
        c_neighbors = pd.concat([coordinates, c_neighbors], axis=1).set_index(
            "new_index", drop=False
        )
        non_AC_neighbors = pd.concat([non_AC_neighbors, c_neighbors])
    neighbors = pd.concat(
        [neighbors[neighbors.carrier == "AC"], non_AC_neighbors]
    )

    for i in [
        "new_index",
        "control",
        "generator",
        "location",
        "sub_network",
        "unit",
    ]:
        neighbors = neighbors.drop(i, axis=1)

    # Add geometry column
    neighbors = (
        gpd.GeoDataFrame(
            neighbors, geometry=gpd.points_from_xy(neighbors.x, neighbors.y)
        )
        .rename_geometry("geom")
        .set_crs(4326)
    )

    # Unify carrier names
    neighbors.carrier = neighbors.carrier.str.replace(" ", "_")
    neighbors.carrier.replace(
        {
            "gas": "CH4",
            "gas_for_industry": "CH4_for_industry",
        },
        inplace=True,
    )

    neighbors[~neighbors.carrier.isin(["AC"])].to_postgis(
        "egon_etrago_bus",
        engine,
        schema="grid",
        if_exists="append",
        index=True,
        index_label="bus_id",
    )

    # prepare and write neighboring crossborder lines to etrago tables
    def lines_to_etrago(neighbor_lines=neighbor_lines, scn="eGon100RE"):
        neighbor_lines["scn_name"] = scn
        neighbor_lines["cables"] = 3 * neighbor_lines["num_parallel"].astype(
            int
        )
        neighbor_lines["s_nom"] = neighbor_lines["s_nom_min"]

        for i in [
            "Line",
            "x_pu_eff",
            "r_pu_eff",
            "sub_network",
            "x_pu",
            "r_pu",
            "g_pu",
            "b_pu",
            "s_nom_opt",
            "i_nom",
        ]:
            neighbor_lines = neighbor_lines.drop(i, axis=1)

        # Define geometry and add to lines dataframe as 'topo'
        gdf = gpd.GeoDataFrame(index=neighbor_lines.index)
        gdf["geom_bus0"] = neighbors.geom[neighbor_lines.bus0].values
        gdf["geom_bus1"] = neighbors.geom[neighbor_lines.bus1].values
        gdf["geometry"] = gdf.apply(
            lambda x: LineString([x["geom_bus0"], x["geom_bus1"]]), axis=1
        )

        neighbor_lines = (
            gpd.GeoDataFrame(neighbor_lines, geometry=gdf["geometry"])
            .rename_geometry("topo")
            .set_crs(4326)
        )

        neighbor_lines["lifetime"] = get_sector_parameters("electricity", scn)[
            "lifetime"
        ]["ac_ehv_overhead_line"]

        neighbor_lines.to_postgis(
            "egon_etrago_line",
            engine,
            schema="grid",
            if_exists="append",
            index=True,
            index_label="line_id",
        )

    lines_to_etrago(neighbor_lines=neighbor_lines, scn="eGon100RE")

    def links_to_etrago(neighbor_links, scn="eGon100RE", extendable=True):
        """Prepare and write neighboring crossborder links to eTraGo table

        This function prepare the neighboring crossborder links
        generated the PyPSA-eur-sec (p-e-s) run by:
          * Delete the useless columns
          * If extendable is false only (non default case):
              * Replace p_nom = 0 with the p_nom_op values (arrising
                from the p-e-s optimisation)
              * Setting p_nom_extendable to false
          * Add geomtry to the links: 'geom' and 'topo' columns
          * Change the name of the carriers to have the consistent in
            eGon-data

        The function insert then the link to the eTraGo table and has
        no return.

        Parameters
        ----------
        neighbor_links : pandas.DataFrame
            Dataframe containing the neighboring crossborder links
        scn_name : str
            Name of the scenario
        extendable : bool
            Boolean expressing if the links should be extendable or not

        Returns
        -------
        None

        """
        neighbor_links["scn_name"] = scn

        dropped_carriers = [
            "Link",
            "geometry",
            "tags",
            "under_construction",
            "underground",
            "underwater_fraction",
            "bus2",
            "bus3",
            "bus4",
            "efficiency2",
            "efficiency3",
            "efficiency4",
            "lifetime",
            "pipe_retrofit",
            "committable",
            "start_up_cost",
            "shut_down_cost",
            "min_up_time",
            "min_down_time",
            "up_time_before",
            "down_time_before",
            "ramp_limit_up",
            "ramp_limit_down",
            "ramp_limit_start_up",
            "ramp_limit_shut_down",
            "length_original",
            "reversed",
        ]

        if extendable:
            dropped_carriers.append("p_nom_opt")
            neighbor_links = neighbor_links.drop(
                columns=dropped_carriers,
                errors="ignore",
            )

        else:
            dropped_carriers.append("p_nom")
            dropped_carriers.append("p_nom_extendable")
            neighbor_links = neighbor_links.drop(
                columns=dropped_carriers,
                errors="ignore",
            )
            neighbor_links = neighbor_links.rename(
                columns={"p_nom_opt": "p_nom"}
            )
            neighbor_links["p_nom_extendable"] = False

        if neighbor_links.empty:
            print("No links selected")
            return

        # Define geometry and add to lines dataframe as 'topo'
        gdf = gpd.GeoDataFrame(
            index=neighbor_links.index,
            data = {
                "geom_bus0":neighbors.loc[neighbor_links.bus0, "geom"].values,
                "geom_bus1":neighbors.loc[neighbor_links.bus1, "geom"].values
                }
            )

        gdf["geometry"] = gdf.apply(
            lambda x: LineString([x["geom_bus0"], x["geom_bus1"]]), axis=1
        )

        neighbor_links = (
            gpd.GeoDataFrame(neighbor_links, geometry=gdf["geometry"])
            .rename_geometry("topo")
            .set_crs(4326)
        )

        # Unify carrier names
        neighbor_links.carrier = neighbor_links.carrier.str.replace(" ", "_")

        neighbor_links.carrier.replace(
            {
                "H2_Electrolysis": "power_to_H2",
                "H2_Fuel_Cell": "H2_to_power",
                "H2_pipeline_retrofitted": "H2_retrofit",
                "SMR": "CH4_to_H2",
                "Sabatier": "H2_to_CH4",
                "gas_for_industry": "CH4_for_industry",
                "gas_pipeline": "CH4",
            },
            inplace=True,
        )

        neighbor_links.to_postgis(
            "egon_etrago_link",
            engine,
            schema="grid",
            if_exists="append",
            index=True,
            index_label="link_id",
        )

    non_extendable_links_carriers = [
        "H2 pipeline retrofitted",
        "H2 pipeline",
        "gas pipeline",
        "biogas to gas",
    ]

    # delete unwanted carriers for eTraGo
    excluded_carriers = ["gas for industry CC", "SMR CC", "biogas to gas",
                         "DAC", "electricity distribution grid", ]
    neighbor_links = neighbor_links[
        ~neighbor_links.carrier.isin(excluded_carriers)
    ]

    links_to_etrago(
        neighbor_links[
            ~neighbor_links.carrier.isin(non_extendable_links_carriers)
        ],
        "eGon100RE",
    )
    links_to_etrago(
        neighbor_links[
            neighbor_links.carrier.isin(non_extendable_links_carriers)
        ],
        "eGon100RE",
        extendable=False,
    )

    # prepare neighboring generators for etrago tables
    neighbor_gens["scn_name"] = "eGon100RE"
    neighbor_gens["p_nom"] = neighbor_gens["p_nom_opt"]
    neighbor_gens["p_nom_extendable"] = False

    # Unify carrier names
    neighbor_gens.carrier = neighbor_gens.carrier.str.replace(" ", "_")

    neighbor_gens.carrier.replace(
        {
            "onwind": "wind_onshore",
            "ror": "run_of_river",
            "offwind-ac": "wind_offshore",
            "offwind-dc": "wind_offshore",
            "urban_central_solar_thermal": "urban_central_solar_thermal_collector",
            "residential_rural_solar_thermal": "residential_rural_solar_thermal_collector",
            "services_rural_solar_thermal": "services_rural_solar_thermal_collector",
        },
        inplace=True,
    )

    for i in [
        "Generator",
        "weight",
        "lifetime",
        "p_set",
        "q_set",
        "p_nom_opt",
    ]:
        neighbor_gens = neighbor_gens.drop(i, axis=1)

    neighbor_gens.to_sql(
        "egon_etrago_generator",
        engine,
        schema="grid",
        if_exists="append",
        index=True,
        index_label="generator_id",
    )

    # prepare neighboring loads for etrago tables
    neighbor_loads["scn_name"] = "eGon100RE"

    # Unify carrier names
    neighbor_loads.carrier = neighbor_loads.carrier.str.replace(" ", "_")

    neighbor_loads.carrier.replace(
        {
            "electricity": "AC",
            "DC": "AC",
            "industry_electricity": "AC",
            "H2_pipeline_retrofitted": "H2_system_boundary",
            "gas_pipeline": "CH4_system_boundary",
            "gas_for_industry": "CH4_for_industry",
        },
        inplace=True,
    )

    neighbor_loads = neighbor_loads.drop(
        columns=["Load"],
        errors="ignore",
    )

    neighbor_loads.to_sql(
        "egon_etrago_load",
        engine,
        schema="grid",
        if_exists="append",
        index=True,
        index_label="load_id",
    )

    # prepare neighboring stores for etrago tables
    neighbor_stores["scn_name"] = "eGon100RE"

    # Unify carrier names
    neighbor_stores.carrier = neighbor_stores.carrier.str.replace(" ", "_")

    neighbor_stores.carrier.replace(
        {
            "Li_ion": "battery",
            "gas": "CH4",
        },
        inplace=True,
    )
    neighbor_stores.loc[
        (
            (neighbor_stores.e_nom_max <= 1e9)
            & (neighbor_stores.carrier == "H2")
        ),
        "carrier",
    ] = "H2_underground"
    neighbor_stores.loc[
        (
            (neighbor_stores.e_nom_max > 1e9)
            & (neighbor_stores.carrier == "H2")
        ),
        "carrier",
    ] = "H2_overground"

    for i in [
        "Store",
        "p_set",
        "q_set",
        "e_nom_opt",
        "lifetime",
        "e_initial_per_period",
        "e_cyclic_per_period",
        "location",
    ]:
        neighbor_stores = neighbor_stores.drop(i, axis=1, errors="ignore")

    neighbor_stores.to_sql(
        "egon_etrago_store",
        engine,
        schema="grid",
        if_exists="append",
        index=True,
        index_label="store_id",
    )

    # prepare neighboring storage_units for etrago tables
    neighbor_storage["scn_name"] = "eGon100RE"

    # Unify carrier names
    neighbor_storage.carrier = neighbor_storage.carrier.str.replace(" ", "_")

    neighbor_storage.carrier.replace(
        {"PHS": "pumped_hydro", "hydro": "reservoir"}, inplace=True
    )

    for i in [
        "StorageUnit",
        "p_nom_opt",
        "state_of_charge_initial_per_period",
        "cyclic_state_of_charge_per_period",
    ]:
        neighbor_storage = neighbor_storage.drop(i, axis=1, errors="ignore")

    neighbor_storage.to_sql(
        "egon_etrago_storage",
        engine,
        schema="grid",
        if_exists="append",
        index=True,
        index_label="storage_id",
    )

    # writing neighboring loads_t p_sets to etrago tables

    neighbor_loads_t_etrago = pd.DataFrame(
        columns=["scn_name", "temp_id", "p_set"],
        index=neighbor_loads_t.columns,
    )
    neighbor_loads_t_etrago["scn_name"] = "eGon100RE"
    neighbor_loads_t_etrago["temp_id"] = 1
    for i in neighbor_loads_t.columns:
        neighbor_loads_t_etrago["p_set"][i] = neighbor_loads_t[
            i
        ].values.tolist()

    neighbor_loads_t_etrago.to_sql(
        "egon_etrago_load_timeseries",
        engine,
        schema="grid",
        if_exists="append",
        index=True,
        index_label="load_id",
    )

    # writing neighboring generator_t p_max_pu to etrago tables
    neighbor_gens_t_etrago = pd.DataFrame(
        columns=["scn_name", "temp_id", "p_max_pu"],
        index=neighbor_gens_t.columns,
    )
    neighbor_gens_t_etrago["scn_name"] = "eGon100RE"
    neighbor_gens_t_etrago["temp_id"] = 1
    for i in neighbor_gens_t.columns:
        neighbor_gens_t_etrago["p_max_pu"][i] = neighbor_gens_t[
            i
        ].values.tolist()

    neighbor_gens_t_etrago.to_sql(
        "egon_etrago_generator_timeseries",
        engine,
        schema="grid",
        if_exists="append",
        index=True,
        index_label="generator_id",
    )

    # writing neighboring stores_t e_min_pu to etrago tables
    neighbor_stores_t_etrago = pd.DataFrame(
        columns=["scn_name", "temp_id", "e_min_pu"],
        index=neighbor_stores_t.columns,
    )
    neighbor_stores_t_etrago["scn_name"] = "eGon100RE"
    neighbor_stores_t_etrago["temp_id"] = 1
    for i in neighbor_stores_t.columns:
        neighbor_stores_t_etrago["e_min_pu"][i] = neighbor_stores_t[
            i
        ].values.tolist()

    neighbor_stores_t_etrago.to_sql(
        "egon_etrago_store_timeseries",
        engine,
        schema="grid",
        if_exists="append",
        index=True,
        index_label="store_id",
    )

    # writing neighboring storage_units inflow to etrago tables
    neighbor_storage_t_etrago = pd.DataFrame(
        columns=["scn_name", "temp_id", "inflow"],
        index=neighbor_storage_t.columns,
    )
    neighbor_storage_t_etrago["scn_name"] = "eGon100RE"
    neighbor_storage_t_etrago["temp_id"] = 1
    for i in neighbor_storage_t.columns:
        neighbor_storage_t_etrago["inflow"][i] = neighbor_storage_t[
            i
        ].values.tolist()

    neighbor_storage_t_etrago.to_sql(
        "egon_etrago_storage_timeseries",
        engine,
        schema="grid",
        if_exists="append",
        index=True,
        index_label="storage_id",
    )

    # writing neighboring lines_t s_max_pu to etrago tables
    if not network.lines_t["s_max_pu"].empty:
        neighbor_lines_t_etrago = pd.DataFrame(
            columns=["scn_name", "s_max_pu"], index=neighbor_lines_t.columns
        )
        neighbor_lines_t_etrago["scn_name"] = "eGon100RE"

        for i in neighbor_lines_t.columns:
            neighbor_lines_t_etrago["s_max_pu"][i] = neighbor_lines_t[
                i
            ].values.tolist()

        neighbor_lines_t_etrago.to_sql(
            "egon_etrago_line_timeseries",
            engine,
            schema="grid",
            if_exists="append",
            index=True,
            index_label="line_id",
        )


def prepared_network():
    if egon.data.config.settings()["egon-data"]["--run-pypsa-eur"]:
        with open(
            __path__[0] + "/datasets/pypsaeur/config.yaml", "r"
        ) as stream:
            data_config = yaml.safe_load(stream)

        target_file = (
            Path(".")
            / "run-pypsa-eur"
            / "pypsa-eur"
            / "results"
            / data_config["run"]["name"]
            / "prenetworks"
            / f"elec_s_{data_config['scenario']['clusters'][0]}"
            f"_l{data_config['scenario']['ll'][0]}"
            f"_{data_config['scenario']['opts'][0]}"
            f"_{data_config['scenario']['sector_opts'][0]}"
            f"_{data_config['scenario']['planning_horizons'][0]}.nc"
        )

    else:
        target_file = (
            Path(".")
            / "data_bundle_egon_data"
            / "pypsa_eur_sec"
            / "2022-07-26-egondata-integration"
            / "postnetworks"
            / "elec_s_37_lv2.0__Co2L0-1H-T-H-B-I-dist1_2050.nc"
        )

    return pypsa.Network(target_file.absolute().as_posix())


def overwrite_H2_pipeline_share():
    """Overwrite retrofitted_CH4pipeline-to-H2pipeline_share value

    Overwrite retrofitted_CH4pipeline-to-H2pipeline_share in the
    scenario parameter table if p-e-s is run.
    This function write in the database and has no return.

    """
    scn_name = "eGon100RE"
    # Select source and target from dataset configuration
    target = egon.data.config.datasets()["pypsa-eur-sec"]["target"]

    n = read_network()

    H2_pipelines = n.links[n.links["carrier"] == "H2 pipeline retrofitted"]
    CH4_pipelines = n.links[n.links["carrier"] == "gas pipeline"]
    H2_pipes_share = np.mean(
        [
            (i / j)
            for i, j in zip(
                H2_pipelines.p_nom_opt.to_list(), CH4_pipelines.p_nom.to_list()
            )
        ]
    )
    logger.info(
        "retrofitted_CH4pipeline-to-H2pipeline_share = " + str(H2_pipes_share)
    )

    parameters = db.select_dataframe(
        f"""
        SELECT *
        FROM {target['scenario_parameters']['schema']}.{target['scenario_parameters']['table']}
        WHERE name = '{scn_name}'
        """
    )

    gas_param = parameters.loc[0, "gas_parameters"]
    gas_param["retrofitted_CH4pipeline-to-H2pipeline_share"] = H2_pipes_share
    gas_param = json.dumps(gas_param)

    # Update data in db
    db.execute_sql(
        f"""
    UPDATE {target['scenario_parameters']['schema']}.{target['scenario_parameters']['table']}
    SET gas_parameters = '{gas_param}'
    WHERE name = '{scn_name}';
    """
    )


def update_electrical_timeseries_germany(network):
    """Replace electrical demand time series in Germany with data from egon-data

    Parameters
    ----------
    network : pypsa.Network
        Network including demand time series from pypsa-eur

    Returns
    -------
    network : pypsa.Network
        Network including electrical demand time series in Germany from egon-data

    """

    df = pd.read_csv(
        "input-pypsa-eur-sec/electrical_demand_timeseries_DE_eGon100RE.csv"
    )

    network.loads_t.p_set.loc[:, "DE1 0"] = (
        df["residential_and_service"] + df["industry"]
    ).values

    return network


def geothermal_district_heating(network):
    """Add the option to build geothermal power plants in district heating in Germany

    Parameters
    ----------
    network : pypsa.Network
        Network from pypsa-eur without geothermal generators

    Returns
    -------
    network : pypsa.Network
        Updated network with geothermal generators

    """

    costs_and_potentials = pd.read_csv(
        "input-pypsa-eur-sec/geothermal_potential_germany.csv"
    )

    network.add("Carrier", "urban central geo thermal")

    for i, row in costs_and_potentials.iterrows():
        # Set lifetime of geothermal plant to 30 years based on:
        # Ableitung eines Korridors für den Ausbau der erneuerbaren Wärme im Gebäudebereich,
        # Beuth Hochschule für Technik, Berlin ifeu – Institut für Energie- und Umweltforschung Heidelberg GmbH
        # Februar 2017
        lifetime_geothermal = 30

        network.add(
            "Generator",
            f"DE1 0 urban central geo thermal {i}",
            bus="DE1 0 urban central heat",
            carrier="urban central geo thermal",
            p_nom_extendable=True,
            p_nom_max=row["potential [MW]"],
            capital_cost=annualize_capital_costs(
                row["cost [EUR/kW]"] * 1e6, lifetime_geothermal, 0.07
            ),
        )
    return network


def h2_overground_stores(network):
    """Add hydrogen overground stores to each hydrogen node

    In pypsa-eur, only countries without the potential of underground hydrogen
    stores have to option to build overground hydrogen tanks.
    Overground stores are more expensive, but are not resitcted by the geological
    potential. To allow higher hydrogen store capacities in each country, optional
    hydogen overground tanks are also added to node with a potential for
    underground stores.

    Parameters
    ----------
    network : pypsa.Network
        Network without hydrogen overground stores at each hydrogen node

    Returns
    -------
    network : pypsa.Network
        Network with hydrogen overground stores at each hydrogen node

    """

    underground_h2_stores = network.stores[
        (network.stores.carrier == "H2 Store")
        & (network.stores.e_nom_max != np.inf)
    ]

    overground_h2_stores = network.stores[
        (network.stores.carrier == "H2 Store")
        & (network.stores.e_nom_max == np.inf)
    ]

    network.madd(
        "Store",
        underground_h2_stores.bus + " overground Store",
        bus=underground_h2_stores.bus.values,
        e_nom_extendable=True,
        e_cyclic=True,
        carrier="H2 Store",
        capital_cost=overground_h2_stores.capital_cost.mean(),
    )

    return network


def update_heat_timeseries_germany(network):
    network.loads
    # Import heat demand curves for Germany from eGon-data
    df_egon_heat_demand = pd.read_csv(
        "input-pypsa-eur-sec/heat_demand_timeseries_DE_eGon100RE.csv"
    )

    # Replace heat demand curves in Germany with values from eGon-data
    network.loads_t.p_set.loc[:, "DE1 0 residential rural heat"] = (
        df_egon_heat_demand.loc[:, "residential rural"].values
    )

    network.loads_t.p_set.loc[:, "DE1 0 services rural heat"] = (
        df_egon_heat_demand.loc[:, "service rural"].values
    )

    network.loads_t.p_set.loc[:, "DE1 0 urban central heat"] = (
        df_egon_heat_demand.loc[:, "urban central"].values
    )

    return network


def drop_biomass(network):
    carrier = "biomass"

    for c in network.iterate_components():
        network.mremove(c.name, c.df[c.df.index.str.contains(carrier)].index)
    return network


def drop_urban_decentral_heat(network):
    carrier = "urban decentral"

    for c in network.iterate_components():
        network.mremove(c.name, c.df[c.df.index.str.contains(carrier)].index)
    return network


def district_heating_shares(network):
    df = pd.read_csv(
        "data_bundle_powerd_data/district_heating_shares_egon.csv"
    ).set_index("country_code")

    heat_demand_per_country = (
        network.loads_t.p_set[
            network.loads[
                (network.loads.carrier.str.contains("heat"))
                & network.loads.index.isin(network.loads_t.p_set.columns)
            ].index
        ]
        .groupby(network.loads.bus.str[:5], axis=1)
        .sum()
    )

    for country in heat_demand_per_country.columns:
        network.loads_t.p_set[f"{country} urban central heat"] = (
            heat_demand_per_country.loc[:, country].mul(
                df.loc[country[:2]].values[0]
            )
        )
        network.loads_t.p_set[f"{country} residential rural heat"] = (
            heat_demand_per_country.loc[:, country].mul(
                (1 - df.loc[country[:2]].values[0])
            )
        )
    return network


def drop_new_gas_pipelines(network):
    network.mremove(
        "Link",
        network.links[
            network.links.index.str.contains("gas pipeline new")
        ].index,
    )

    return network


def drop_fossil_gas(network):
    network.mremove(
        "Store", network.stores[network.stores.carrier == "gas"].index
    )

    return network


def rual_heat_technologies(network):
    network.mremove(
        "Link",
        network.links[
            network.links.index.str.contains("rural gas boiler")
        ].index,
    )

    network.mremove(
        "Generator",
        network.generators[
            network.generators.carrier.str.contains("rural solar thermal")
        ].index,
    )

    network.links.loc["DE1 0 services rural ground heat pump", "p_nom_min"] = 0

    return network


def execute():
    with open(
        __path__[0] + "/datasets/pypsaeur/config.yaml", "r"
    ) as stream:
        data_config = yaml.safe_load(stream)

    network_path = (
        Path(".")
        / "run-pypsa-eur"
        / "pypsa-eur"
        / "results"
        / data_config["run"]["name"]
        / "prenetworks"
        / f"elec_s_{data_config['scenario']['clusters'][0]}"
        f"_l{data_config['scenario']['ll'][0]}"
        f"_{data_config['scenario']['opts'][0]}"
        f"_{data_config['scenario']['sector_opts'][0]}"
        f"_{data_config['scenario']['planning_horizons'][0]}.nc"
    )

    network = pypsa.Network(network_path)

    network = drop_biomass(network)

    network = drop_urban_decentral_heat(network)

    network = district_heating_shares(network)

    network = update_heat_timeseries_germany(network)

    network = update_electrical_timeseries_germany(network)

    network = geothermal_district_heating(network)

    network = h2_overground_stores(network)

    network = drop_new_gas_pipelines(network)

    network = drop_fossil_gas(network)

    network = rual_heat_technologies(network)

    network.export_to_netcdf(network_path)
