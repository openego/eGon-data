"""The central module containing all code dealing with importing data from
the pysa-eur-sec scenario parameter creation
"""

from pathlib import Path
from urllib.request import urlretrieve
import os
import tarfile

from shapely.geometry import LineString
import geopandas as gpd
import importlib_resources as resources
import pandas as pd
import pypsa
import yaml

from egon.data import __path__, db
from egon.data.datasets import Dataset
from egon.data.datasets.scenario_parameters import get_sector_parameters
import egon.data.config
import egon.data.subprocess as subproc


def run_pypsa_eur_sec():

    cwd = Path(".")
    filepath = cwd / "run-pypsa-eur-sec"
    filepath.mkdir(parents=True, exist_ok=True)

    pypsa_eur_repos = filepath / "pypsa-eur"
    pypsa_eur_repos_data = pypsa_eur_repos / "data"
    technology_data_repos = filepath / "technology-data"
    pypsa_eur_sec_repos = filepath / "pypsa-eur-sec"
    pypsa_eur_sec_repos_data = pypsa_eur_sec_repos / "data"

    if not pypsa_eur_repos.exists():
        subproc.run(
            [
                "git",
                "clone",
                "--branch",
                "v0.4.0",
                "https://github.com/PyPSA/pypsa-eur.git",
                pypsa_eur_repos,
            ]
        )

        # subproc.run(
        #     ["git", "checkout", "4e44822514755cdd0289687556547100fba6218b"],
        #     cwd=pypsa_eur_repos,
        # )

        file_to_copy = os.path.join(
            __path__[0], "datasets", "pypsaeursec", "pypsaeur", "Snakefile"
        )

        subproc.run(["cp", file_to_copy, pypsa_eur_repos])

        # Read YAML file
        path_to_env = pypsa_eur_repos / "envs" / "environment.yaml"
        with open(path_to_env, "r") as stream:
            env = yaml.safe_load(stream)

        env["dependencies"].append("gurobi")

        # Write YAML file
        with open(path_to_env, "w", encoding="utf8") as outfile:
            yaml.dump(
                env, outfile, default_flow_style=False, allow_unicode=True
            )

        datafile = "pypsa-eur-data-bundle.tar.xz"
        datapath = pypsa_eur_repos / datafile
        if not datapath.exists():
            urlretrieve(
                f"https://zenodo.org/record/3517935/files/{datafile}", datapath
            )
            tar = tarfile.open(datapath)
            tar.extractall(pypsa_eur_repos_data)

    if not technology_data_repos.exists():
        subproc.run(
            [
                "git",
                "clone",
                "--branch",
                "v0.3.0",
                "https://github.com/PyPSA/technology-data.git",
                technology_data_repos,
            ]
        )

    if not pypsa_eur_sec_repos.exists():
        subproc.run(
            [
                "git",
                "clone",
                "https://github.com/openego/pypsa-eur-sec.git",
                pypsa_eur_sec_repos,
            ]
        )

    datafile = "pypsa-eur-sec-data-bundle.tar.gz"
    datapath = pypsa_eur_sec_repos_data / datafile
    if not datapath.exists():
        urlretrieve(
            f"https://zenodo.org/record/5824485/files/{datafile}", datapath
        )
        tar = tarfile.open(datapath)
        tar.extractall(pypsa_eur_sec_repos_data)

    with open(filepath / "Snakefile", "w") as snakefile:
        snakefile.write(
            resources.read_text("egon.data.datasets.pypsaeursec", "Snakefile")
        )

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
            "Main",
        ]
    )


def read_network():

    # Set execute_pypsa_eur_sec to False until optional task is implemented
    execute_pypsa_eur_sec = False
    cwd = Path(".")

    if execute_pypsa_eur_sec:
        filepath = cwd / "run-pypsa-eur-sec"
        pypsa_eur_sec_repos = filepath / "pypsa-eur-sec"
        # Read YAML file
        pes_egonconfig = pypsa_eur_sec_repos / "config_egon.yaml"
        with open(pes_egonconfig, "r") as stream:
            data_config = yaml.safe_load(stream)

        simpl = data_config["scenario"]["simpl"][0]
        clusters = data_config["scenario"]["clusters"][0]
        lv = data_config["scenario"]["lv"][0]
        opts = data_config["scenario"]["opts"][0]
        sector_opts = data_config["scenario"]["sector_opts"][0]
        planning_horizons = data_config["scenario"]["planning_horizons"][0]
        file = "elec_s{simpl}_{clusters}_lv{lv}_{opts}_{sector_opts}_{planning_horizons}.nc".format(
            simpl=simpl,
            clusters=clusters,
            opts=opts,
            lv=lv,
            sector_opts=sector_opts,
            planning_horizons=planning_horizons,
        )

        target_file = (
            pypsa_eur_sec_repos
            / "results"
            / data_config["run"]
            / "postnetworks"
            / file
        )

    else:
        target_file = (
            cwd
            / "data_bundle_egon_data"
            / "pypsa_eur_sec"
            / "2022-07-26-egondata-integration"
            / "postnetworks"
            / "elec_s_37_lv2.0__Co2L0-1H-T-H-B-I-dist1_2050.nc"
        )

    return pypsa.Network(str(target_file))


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
        "AND country <> 'DE'"
    )


def neighbor_reduction():

    network = read_network()

    network.links.drop("pipe_retrofit", axis="columns", inplace=True)

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
        network.loads.carrier.loc[
            "slack_fix " + i + " " + k
        ] = lines_cb_1.carrier[k]

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
        network.loads.carrier.loc[
            "slack_fix " + i + " " + k
        ] = lines_cb_0.carrier[k]

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
        network.loads.carrier.loc[
            "slack_fix_links " + i + " " + k
        ] = links_cb_1.carrier[k]

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
        network.loads.carrier.loc[
            "slack_fix_links " + i + " " + k
        ] = links_cb_0.carrier[k]

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
        new_index = neighbor_gens[neighbor_gens["name"] == i].index
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
        new_index = neighbor_loads[neighbor_loads["index"] == i].index
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
        new_index = neighbor_stores[neighbor_stores["name"] == i].index
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
        new_index = neighbor_storage[neighbor_storage["name"] == i].index
        neighbor_storage_t.rename(columns={i: new_index[0]}, inplace=True)

    # Connect to local database
    engine = db.engine()

    neighbors["scn_name"] = "eGon100RE"
    neighbors.index = neighbors["new_index"]

    # Correct geometry for non AC buses
    carriers = set(neighbors.carrier.to_list())
    carriers.remove("AC")
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
        non_AC_neighbors = non_AC_neighbors.append(c_neighbors)
    neighbors = neighbors[neighbors.carrier == "AC"].append(non_AC_neighbors)

    for i in ["new_index", "control", "generator", "location", "sub_network"]:
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

    neighbors.to_postgis(
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
            "name",
            "x_pu_eff",
            "r_pu_eff",
            "sub_network",
            "x_pu",
            "r_pu",
            "g_pu",
            "b_pu",
            "s_nom_opt",
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
    lines_to_etrago(neighbor_lines=neighbor_lines, scn="eGon2035")

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

        if extendable is True:
            neighbor_links = neighbor_links.drop(
                columns=[
                    "name",
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
                    "p_nom_opt",
                    "pipe_retrofit",
                ],
                errors="ignore",
            )

        elif extendable is False:
            neighbor_links = neighbor_links.drop(
                columns=[
                    "name",
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
                    "p_nom",
                    "p_nom_extendable",
                    "pipe_retrofit",
                ],
                errors="ignore",
            )
            neighbor_links = neighbor_links.rename(
                columns={"p_nom_opt": "p_nom"}
            )
            neighbor_links["p_nom_extendable"] = False

        # Define geometry and add to lines dataframe as 'topo'
        gdf = gpd.GeoDataFrame(index=neighbor_links.index)
        gdf["geom_bus0"] = neighbors.geom[neighbor_links.bus0].values
        gdf["geom_bus1"] = neighbors.geom[neighbor_links.bus1].values
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
        "gas pipeline",
        "biogas to gas",
    ]

    # delete unwanted carriers for eTraGo
    excluded_carriers = ["gas for industry CC", "SMR CC"]
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

    links_to_etrago(neighbor_links[neighbor_links.carrier == "DC"], "eGon2035")

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

    for i in ["name", "weight", "lifetime", "p_set", "q_set", "p_nom_opt"]:
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
            "H2_pipeline": "H2_system_boundary",
            "gas_for_industry": "CH4_for_industry",
        },
        inplace=True,
    )

    for i in ["index", "p_set", "q_set"]:
        neighbor_loads = neighbor_loads.drop(i, axis=1)

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

    for i in ["name", "p_set", "q_set", "e_nom_opt", "lifetime"]:
        neighbor_stores = neighbor_stores.drop(i, axis=1)

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

    for i in ["name", "p_nom_opt"]:
        neighbor_storage = neighbor_storage.drop(i, axis=1)

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


# Skip execution of pypsa-eur-sec by default until optional task is implemented
execute_pypsa_eur_sec = False

if execute_pypsa_eur_sec:
    tasks = (run_pypsa_eur_sec, clean_database, neighbor_reduction)
else:
    tasks = (clean_database, neighbor_reduction)


class PypsaEurSec(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="PypsaEurSec",
            version="0.0.8",
            dependencies=dependencies,
            tasks=tasks,
        )
