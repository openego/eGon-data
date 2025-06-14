"""The central module containing all code dealing with importing data from
the pysa-eur-sec scenario parameter creation
"""

from pathlib import Path
from urllib.request import urlretrieve
import json
import shutil

from shapely.geometry import LineString
import geopandas as gpd
import importlib_resources as resources
import numpy as np
import pandas as pd
import pypsa
import requests
import yaml

from egon.data import __path__, config, db, logger
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
            version="0.0.42",
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
            version="0.0.41",
            dependencies=dependencies,
            tasks=(
                prepare_network_2,
                execute,
                solve_network,
                clean_database,
                electrical_neighbours_egon100,
                # Dropped until we decided how we deal with the H2 grid
                # overwrite_H2_pipeline_share,
            ),
        )


def download():
    cwd = Path(".")
    filepath = cwd / "run-pypsa-eur"
    filepath.mkdir(parents=True, exist_ok=True)

    pypsa_eur_repos = filepath / "pypsa-eur"
    if config.settings()["egon-data"]["--run-pypsa-eur"]:
        if not pypsa_eur_repos.exists():
            subproc.run(
                [
                    "git",
                    "clone",
                    "--branch",
                    "master",
                    "https://github.com/PyPSA/pypsa-eur.git",
                    pypsa_eur_repos,
                ]
            )

            subproc.run(
                [
                    "git",
                    "checkout",
                    "2119f4cee05c256509f48d4e9fe0d8fd9e9e3632",
                ],
                cwd=pypsa_eur_repos,
            )

            # Add gurobi solver to environment:
            # Read YAML file
            # path_to_env = pypsa_eur_repos / "envs" / "environment.yaml"
            # with open(path_to_env, "r") as stream:
            #    env = yaml.safe_load(stream)

            # The version of gurobipy has to fit to the version of gurobi.
            # Since we mainly use gurobi 10.0 this is set here.
            # env["dependencies"][-1]["pip"].append("gurobipy==10.0.0")

            # Set python version to <3.12
            # Python<=3.12 needs gurobipy>=11.0, in case gurobipy is updated,
            # this can be removed
            # env["dependencies"] = [
            #    "python>=3.8,<3.12" if x == "python>=3.8" else x
            #    for x in env["dependencies"]
            # ]

            # Limit geopandas version
            # our pypsa-eur version is not compatible to geopandas>1
            # env["dependencies"] = [
            #    "geopandas>=0.11.0,<1" if x == "geopandas>=0.11.0" else x
            #    for x in env["dependencies"]
            # ]

            # Write YAML file
            # with open(path_to_env, "w", encoding="utf8") as outfile:
            #    yaml.dump(
            #        env, outfile, default_flow_style=False, allow_unicode=True
            #    )

            # Copy config file for egon-data to pypsa-eur directory
            shutil.copy(
                Path(
                    __path__[0], "datasets", "pypsaeur", "config_prepare.yaml"
                ),
                pypsa_eur_repos / "config" / "config.yaml",
            )

            # Copy custom_extra_functionality.py file for egon-data to pypsa-eur directory
            shutil.copy(
                Path(
                    __path__[0],
                    "datasets",
                    "pypsaeur",
                    "custom_extra_functionality.py",
                ),
                pypsa_eur_repos / "data",
            )

            with open(filepath / "Snakefile", "w") as snakefile:
                snakefile.write(
                    resources.read_text(
                        "egon.data.datasets.pypsaeur", "Snakefile"
                    )
                )

        # Copy era5 weather data to folder for pypsaeur
        era5_pypsaeur_path = filepath / "pypsa-eur" / "cutouts"

        if not era5_pypsaeur_path.exists():
            era5_pypsaeur_path.mkdir(parents=True, exist_ok=True)
            copy_from = config.datasets()["era5_weather_data"]["targets"][
                "weather_data"
            ]["path"]
            filename = "europe-2011-era5.nc"
            shutil.copy(
                copy_from + "/" + filename, era5_pypsaeur_path / filename
            )

        # Workaround to download natura, shipdensity and globalenergymonitor
        # data, which is not working in the regular snakemake workflow.
        # The same files are downloaded from the same directory as in pypsa-eur
        # version 0.10 here. Is is stored in the folders from pypsa-eur.
        if not (filepath / "pypsa-eur" / "resources").exists():
            (filepath / "pypsa-eur" / "resources").mkdir(
                parents=True, exist_ok=True
            )
        urlretrieve(
            "https://zenodo.org/record/4706686/files/natura.tiff",
            filepath / "pypsa-eur" / "resources" / "natura.tiff",
        )

        if not (filepath / "pypsa-eur" / "data").exists():
            (filepath / "pypsa-eur" / "data").mkdir(
                parents=True, exist_ok=True
            )
        urlretrieve(
            "https://zenodo.org/record/13757228/files/shipdensity_global.zip",
            filepath / "pypsa-eur" / "data" / "shipdensity_global.zip",
        )

        if not (
            filepath
            / "pypsa-eur"
            / "zenodo.org"
            / "records"
            / "13757228"
            / "files"
        ).exists():
            (
                filepath
                / "pypsa-eur"
                / "zenodo.org"
                / "records"
                / "13757228"
                / "files"
            ).mkdir(parents=True, exist_ok=True)

        urlretrieve(
            "https://zenodo.org/records/10356004/files/ENSPRESO_BIOMASS.xlsx",
            filepath
            / "pypsa-eur"
            / "zenodo.org"
            / "records"
            / "13757228"
            / "files"
            / "ENSPRESO_BIOMASS.xlsx",
        )

        if not (filepath / "pypsa-eur" / "data" / "gem").exists():
            (filepath / "pypsa-eur" / "data" / "gem").mkdir(
                parents=True, exist_ok=True
            )

        r = requests.get(
            "https://tubcloud.tu-berlin.de/s/LMBJQCsN6Ez5cN2/download/"
            "Europe-Gas-Tracker-2024-05.xlsx"
        )
        with open(
            filepath
            / "pypsa-eur"
            / "data"
            / "gem"
            / "Europe-Gas-Tracker-2024-05.xlsx",
            "wb",
        ) as outfile:
            outfile.write(r.content)

        if not (filepath / "pypsa-eur" / "data" / "gem").exists():
            (filepath / "pypsa-eur" / "data" / "gem").mkdir(
                parents=True, exist_ok=True
            )

        r = requests.get(
            "https://tubcloud.tu-berlin.de/s/Aqebo3rrQZWKGsG/download/"
            "Global-Steel-Plant-Tracker-April-2024-Standard-Copy-V1.xlsx"
        )
        with open(
            filepath
            / "pypsa-eur"
            / "data"
            / "gem"
            / "Global-Steel-Plant-Tracker-April-2024-Standard-Copy-V1.xlsx",
            "wb",
        ) as outfile:
            outfile.write(r.content)

    else:
        print("Pypsa-eur is not executed due to the settings of egon-data")


def prepare_network():
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
                "--cores",
                "8",
                "prepare",
            ]
        )
        execute()

        path = filepath / "pypsa-eur" / "results" / "prenetworks"

        path_2 = path / "prenetwork_post-manipulate_pre-solve"
        path_2.mkdir(parents=True, exist_ok=True)

        with open(
            __path__[0] + "/datasets/pypsaeur/config_prepare.yaml", "r"
        ) as stream:
            data_config = yaml.safe_load(stream)

        for i in range(0, len(data_config["scenario"]["planning_horizons"])):
            nc_file = (
                f"base_s_{data_config['scenario']['clusters'][0]}"
                f"_l{data_config['scenario']['ll'][0]}"
                f"_{data_config['scenario']['opts'][0]}"
                f"_{data_config['scenario']['sector_opts'][0]}"
                f"_{data_config['scenario']['planning_horizons'][i]}.nc"
            )

            shutil.copy(Path(path, nc_file), path_2)

    else:
        print("Pypsa-eur is not executed due to the settings of egon-data")


def prepare_network_2():
    cwd = Path(".")
    filepath = cwd / "run-pypsa-eur"

    if config.settings()["egon-data"]["--run-pypsa-eur"]:
        shutil.copy(
            Path(__path__[0], "datasets", "pypsaeur", "config_solve.yaml"),
            filepath / "pypsa-eur" / "config" / "config.yaml",
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
                "--cores",
                "8",
                "prepare",
            ]
        )
    else:
        print("Pypsa-eur is not executed due to the settings of egon-data")


def solve_network():
    cwd = Path(".")
    filepath = cwd / "run-pypsa-eur"

    if config.settings()["egon-data"]["--run-pypsa-eur"]:
        subproc.run(
            [
                "snakemake",
                "-j1",
                "--cores",
                "8",
                "--directory",
                filepath,
                "--snakefile",
                filepath / "Snakefile",
                "--use-conda",
                "--conda-frontend=conda",
                "solve",
            ]
        )

        postprocessing_biomass_2045()

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
                "summary",
            ]
        )
    else:
        print("Pypsa-eur is not executed due to the settings of egon-data")


def read_network(planning_horizon=3):
    if config.settings()["egon-data"]["--run-pypsa-eur"]:
        with open(
            __path__[0] + "/datasets/pypsaeur/config_solve.yaml", "r"
        ) as stream:
            data_config = yaml.safe_load(stream)

        target_file = (
            Path(".")
            / "run-pypsa-eur"
            / "pypsa-eur"
            / "results"
            / data_config["run"]["name"]
            / "postnetworks"
            / f"base_s_{data_config['scenario']['clusters'][0]}"
            f"_l{data_config['scenario']['ll'][0]}"
            f"_{data_config['scenario']['opts'][0]}"
            f"_{data_config['scenario']['sector_opts'][0]}"
            f"_{data_config['scenario']['planning_horizons'][planning_horizon]}.nc"
        )

    else:
        target_file = (
            Path(".")
            / "data_bundle_egon_data"
            / "pypsa_eur"
            / "postnetworks"
            / "base_s_39_lc1.25__cb40ex0-T-H-I-B-solar+p3-dist1_2045.nc"
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
        "link",
    ]

    for comp, id in zip(comp_2_ports, ["line_id", "link_id"]):
        db.execute_sql(
            f"""
            DELETE FROM {"grid.egon_etrago_" + comp + "_timeseries"}
            WHERE scn_name = '{scn_name}'
            AND {id} IN (
                SELECT {id} FROM {"grid.egon_etrago_" + comp}
            WHERE "bus0" IN (
            SELECT bus_id FROM grid.egon_etrago_bus
                WHERE country != 'DE'
                AND scn_name = '{scn_name}'
                AND bus_id NOT IN (SELECT bus_i FROM osmtgmod_results.bus_data))
            AND "bus1" IN (
            SELECT bus_id FROM grid.egon_etrago_bus
                WHERE country != 'DE'
                AND scn_name = '{scn_name}'
                AND bus_id NOT IN (SELECT bus_i FROM osmtgmod_results.bus_data))
            );


            DELETE FROM {"grid.egon_etrago_" + comp}
            WHERE scn_name = '{scn_name}'
            AND "bus0" IN (
            SELECT bus_id FROM grid.egon_etrago_bus
                WHERE country != 'DE'
                AND scn_name = '{scn_name}'
                AND bus_id NOT IN (SELECT bus_i FROM osmtgmod_results.bus_data))
            AND "bus1" IN (
            SELECT bus_id FROM grid.egon_etrago_bus
                WHERE country != 'DE'
                AND scn_name = '{scn_name}'
                AND bus_id NOT IN (SELECT bus_i FROM osmtgmod_results.bus_data))
            ;"""
        )

    db.execute_sql(
        f"""
        DELETE FROM grid.egon_etrago_bus
        WHERE scn_name = '{scn_name}'
        AND country <> 'DE'
        AND carrier <> 'AC'
        """
    )


def electrical_neighbours_egon100():
    if "eGon100RE" in egon.data.config.settings()["egon-data"]["--scenarios"]:
        neighbor_reduction()

    else:
        print(
            "eGon100RE is not in the list of created scenarios, this task is skipped."
        )


def combine_decentral_and_rural_heat(network_solved, network_prepared):

    for comp in network_solved.iterate_components():

        if comp.name in ["Bus", "Link", "Store"]:
            urban_decentral = comp.df[
                comp.df.carrier.str.contains("urban decentral")
            ]
            rural = comp.df[comp.df.carrier.str.contains("rural")]
            for i, row in urban_decentral.iterrows():
                if not "DE" in i:
                    if comp.name in ["Bus"]:
                        network_solved.remove("Bus", i)
                    if comp.name in ["Link", "Generator"]:
                        if (
                            i.replace("urban decentral", "rural")
                            in rural.index
                        ):
                            rural.loc[
                                i.replace("urban decentral", "rural"),
                                "p_nom_opt",
                            ] += urban_decentral.loc[i, "p_nom_opt"]
                            rural.loc[
                                i.replace("urban decentral", "rural"), "p_nom"
                            ] += urban_decentral.loc[i, "p_nom"]
                            network_solved.remove(comp.name, i)
                        else:
                            print(i)
                            comp.df.loc[i, "bus0"] = comp.df.loc[
                                i, "bus0"
                            ].replace("urban decentral", "rural")
                            comp.df.loc[i, "bus1"] = comp.df.loc[
                                i, "bus1"
                            ].replace("urban decentral", "rural")
                            comp.df.loc[i, "carrier"] = comp.df.loc[
                                i, "carrier"
                            ].replace("urban decentral", "rural")
                    if comp.name in ["Store"]:
                        if (
                            i.replace("urban decentral", "rural")
                            in rural.index
                        ):
                            rural.loc[
                                i.replace("urban decentral", "rural"),
                                "e_nom_opt",
                            ] += urban_decentral.loc[i, "e_nom_opt"]
                            rural.loc[
                                i.replace("urban decentral", "rural"), "e_nom"
                            ] += urban_decentral.loc[i, "e_nom"]
                            network_solved.remove(comp.name, i)

                        else:
                            print(i)
                            network_solved.stores.loc[i, "bus"] = (
                                network_solved.stores.loc[i, "bus"].replace(
                                    "urban decentral", "rural"
                                )
                            )
                            network_solved.stores.loc[i, "carrier"] = (
                                "rural water tanks"
                            )

    urban_decentral_loads = network_prepared.loads[
        network_prepared.loads.carrier.str.contains("urban decentral")
    ]

    for i, row in urban_decentral_loads.iterrows():
        if i in network_prepared.loads_t.p_set.columns:
            network_prepared.loads_t.p_set[
                i.replace("urban decentral", "rural")
            ] += network_prepared.loads_t.p_set[i]
    network_prepared.mremove("Load", urban_decentral_loads.index)

    return network_prepared, network_solved


def neighbor_reduction():
    network_solved = read_network()
    network_prepared = prepared_network(planning_horizon="2045")

    # network.links.drop("pipe_retrofit", axis="columns", inplace=True)

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

    foreign_buses = network_solved.buses[
        (~network_solved.buses.index.str.contains("|".join(wanted_countries)))
        | (network_solved.buses.index.str.contains("FR6"))
    ]
    network_solved.buses = network_solved.buses.drop(
        network_solved.buses.loc[foreign_buses.index].index
    )

    # Add H2 demand of Fischer-Tropsch process and methanolisation
    # to industrial H2 demands
    industrial_hydrogen = network_prepared.loads.loc[
        network_prepared.loads.carrier == "H2 for industry"
    ]
    fischer_tropsch = (
        network_solved.links_t.p0[
            network_solved.links.loc[
                network_solved.links.carrier == "Fischer-Tropsch"
            ].index
        ]
        .mul(network_solved.snapshot_weightings.generators, axis=0)
        .sum()
    )
    methanolisation = (
        network_solved.links_t.p0[
            network_solved.links.loc[
                network_solved.links.carrier == "methanolisation"
            ].index
        ]
        .mul(network_solved.snapshot_weightings.generators, axis=0)
        .sum()
    )
    for i, row in industrial_hydrogen.iterrows():
        network_prepared.loads.loc[i, "p_set"] += (
            fischer_tropsch[
                fischer_tropsch.index.str.startswith(row.bus[:5])
            ].sum()
            / 8760
        )
        network_prepared.loads.loc[i, "p_set"] += (
            methanolisation[
                methanolisation.index.str.startswith(row.bus[:5])
            ].sum()
            / 8760
        )
    # drop foreign lines and links from the 2nd row

    network_solved.lines = network_solved.lines.drop(
        network_solved.lines[
            (
                network_solved.lines["bus0"].isin(network_solved.buses.index)
                == False
            )
            & (
                network_solved.lines["bus1"].isin(network_solved.buses.index)
                == False
            )
        ].index
    )

    # select all lines which have at bus1 the bus which is kept
    lines_cb_1 = network_solved.lines[
        (
            network_solved.lines["bus0"].isin(network_solved.buses.index)
            == False
        )
    ]

    # create a load at bus1 with the line's hourly loading
    for i, k in zip(lines_cb_1.bus1.values, lines_cb_1.index):

        # Copy loading of lines into hourly resolution
        pset = pd.Series(
            index=network_prepared.snapshots,
            data=network_solved.lines_t.p1[k].resample("H").ffill(),
        )
        pset["2011-12-31 22:00:00"] = pset["2011-12-31 21:00:00"]
        pset["2011-12-31 23:00:00"] = pset["2011-12-31 21:00:00"]

        # Loads are all imported from the prepared network in the end
        network_prepared.add(
            "Load",
            "slack_fix " + i + " " + k,
            bus=i,
            p_set=pset,
            carrier=lines_cb_1.loc[k, "carrier"],
        )

    # select all lines which have at bus0 the bus which is kept
    lines_cb_0 = network_solved.lines[
        (
            network_solved.lines["bus1"].isin(network_solved.buses.index)
            == False
        )
    ]

    # create a load at bus0 with the line's hourly loading
    for i, k in zip(lines_cb_0.bus0.values, lines_cb_0.index):
        # Copy loading of lines into hourly resolution
        pset = pd.Series(
            index=network_prepared.snapshots,
            data=network_solved.lines_t.p0[k].resample("H").ffill(),
        )
        pset["2011-12-31 22:00:00"] = pset["2011-12-31 21:00:00"]
        pset["2011-12-31 23:00:00"] = pset["2011-12-31 21:00:00"]

        network_prepared.add(
            "Load",
            "slack_fix " + i + " " + k,
            bus=i,
            p_set=pset,
            carrier=lines_cb_0.loc[k, "carrier"],
        )

    # do the same for links
    network_solved.mremove(
        "Link",
        network_solved.links[
            (~network_solved.links.bus0.isin(network_solved.buses.index))
            | (~network_solved.links.bus1.isin(network_solved.buses.index))
        ].index,
    )

    # select all links which have at bus1 the bus which is kept
    links_cb_1 = network_solved.links[
        (
            network_solved.links["bus0"].isin(network_solved.buses.index)
            == False
        )
    ]

    # create a load at bus1 with the link's hourly loading
    for i, k in zip(links_cb_1.bus1.values, links_cb_1.index):
        pset = pd.Series(
            index=network_prepared.snapshots,
            data=network_solved.links_t.p1[k].resample("H").ffill(),
        )
        pset["2011-12-31 22:00:00"] = pset["2011-12-31 21:00:00"]
        pset["2011-12-31 23:00:00"] = pset["2011-12-31 21:00:00"]

        network_prepared.add(
            "Load",
            "slack_fix_links " + i + " " + k,
            bus=i,
            p_set=pset,
            carrier=links_cb_1.loc[k, "carrier"],
        )

    # select all links which have at bus0 the bus which is kept
    links_cb_0 = network_solved.links[
        (
            network_solved.links["bus1"].isin(network_solved.buses.index)
            == False
        )
    ]

    # create a load at bus0 with the link's hourly loading
    for i, k in zip(links_cb_0.bus0.values, links_cb_0.index):
        pset = pd.Series(
            index=network_prepared.snapshots,
            data=network_solved.links_t.p0[k].resample("H").ffill(),
        )
        pset["2011-12-31 22:00:00"] = pset["2011-12-31 21:00:00"]
        pset["2011-12-31 23:00:00"] = pset["2011-12-31 21:00:00"]

        network_prepared.add(
            "Load",
            "slack_fix_links " + i + " " + k,
            bus=i,
            p_set=pset,
            carrier=links_cb_0.carrier[k],
        )

    # drop remaining foreign components
    for comp in network_solved.iterate_components():
        if "bus0" in comp.df.columns:
            network_solved.mremove(
                comp.name,
                comp.df[~comp.df.bus0.isin(network_solved.buses.index)].index,
            )
            network_solved.mremove(
                comp.name,
                comp.df[~comp.df.bus1.isin(network_solved.buses.index)].index,
            )
        elif "bus" in comp.df.columns:
            network_solved.mremove(
                comp.name,
                comp.df[~comp.df.bus.isin(network_solved.buses.index)].index,
            )

    # Combine urban decentral and rural heat
    network_prepared, network_solved = combine_decentral_and_rural_heat(
        network_solved, network_prepared
    )

    # writing components of neighboring countries to etrago tables

    # Set country tag for all buses
    network_solved.buses.country = network_solved.buses.index.str[:2]
    neighbors = network_solved.buses[network_solved.buses.country != "DE"]

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
        """
    )
    buses_with_defined_id = neighbors[
        (neighbors.carrier == "AC")
        & (neighbors.country.isin(foreign_ac_buses.country.values))
    ].index
    neighbors.loc[buses_with_defined_id, "new_index"] = (
        foreign_ac_buses.set_index("x")
        .loc[neighbors.loc[buses_with_defined_id, "x"]]
        .bus_id.values
    )

    # lines, the foreign crossborder lines
    # (without crossborder lines to Germany!)

    neighbor_lines = network_solved.lines[
        network_solved.lines.bus0.isin(neighbors.index)
        & network_solved.lines.bus1.isin(neighbors.index)
    ]
    if not network_solved.lines_t["s_max_pu"].empty:
        neighbor_lines_t = network_prepared.lines_t["s_max_pu"][
            neighbor_lines.index
        ]

    neighbor_lines.reset_index(inplace=True)
    neighbor_lines.bus0 = (
        neighbors.loc[neighbor_lines.bus0, "new_index"].reset_index().new_index
    )
    neighbor_lines.bus1 = (
        neighbors.loc[neighbor_lines.bus1, "new_index"].reset_index().new_index
    )
    neighbor_lines.index += db.next_etrago_id("line")

    if not network_solved.lines_t["s_max_pu"].empty:
        for i in neighbor_lines_t.columns:
            new_index = neighbor_lines[neighbor_lines["name"] == i].index
            neighbor_lines_t.rename(columns={i: new_index[0]}, inplace=True)

    # links
    neighbor_links = network_solved.links[
        network_solved.links.bus0.isin(neighbors.index)
        & network_solved.links.bus1.isin(neighbors.index)
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
    neighbor_gens = network_solved.generators[
        network_solved.generators.bus.isin(neighbors.index)
    ]
    neighbor_gens_t = network_prepared.generators_t["p_max_pu"][
        neighbor_gens[
            neighbor_gens.index.isin(
                network_prepared.generators_t["p_max_pu"].columns
            )
        ].index
    ]

    gen_time = [
        "solar",
        "onwind",
        "solar rooftop",
        "offwind-ac",
        "offwind-dc",
        "solar-hsat",
        "urban central solar thermal",
        "rural solar thermal",
        "offwind-float",
    ]

    missing_gent = neighbor_gens[
        neighbor_gens["carrier"].isin(gen_time)
        & ~neighbor_gens.index.isin(neighbor_gens_t.columns)
    ].index

    gen_timeseries = network_prepared.generators_t["p_max_pu"].copy()
    for mgt in missing_gent:  # mgt: missing generator timeseries
        try:
            neighbor_gens_t[mgt] = gen_timeseries.loc[:, mgt[0:-5]]
        except:
            print(f"There are not timeseries for {mgt}")

    neighbor_gens.reset_index(inplace=True)
    neighbor_gens.bus = (
        neighbors.loc[neighbor_gens.bus, "new_index"].reset_index().new_index
    )
    neighbor_gens.index += db.next_etrago_id("generator")

    for i in neighbor_gens_t.columns:
        new_index = neighbor_gens[neighbor_gens["Generator"] == i].index
        neighbor_gens_t.rename(columns={i: new_index[0]}, inplace=True)

    # loads
    # imported from prenetwork in 1h-resolution
    neighbor_loads = network_prepared.loads[
        network_prepared.loads.bus.isin(neighbors.index)
    ]
    neighbor_loads_t_index = neighbor_loads.index[
        neighbor_loads.index.isin(network_prepared.loads_t.p_set.columns)
    ]
    neighbor_loads_t = network_prepared.loads_t["p_set"][
        neighbor_loads_t_index
    ]

    neighbor_loads.reset_index(inplace=True)
    neighbor_loads.bus = (
        neighbors.loc[neighbor_loads.bus, "new_index"].reset_index().new_index
    )
    neighbor_loads.index += db.next_etrago_id("load")

    for i in neighbor_loads_t.columns:
        new_index = neighbor_loads[neighbor_loads["Load"] == i].index
        neighbor_loads_t.rename(columns={i: new_index[0]}, inplace=True)

    # stores
    neighbor_stores = network_solved.stores[
        network_solved.stores.bus.isin(neighbors.index)
    ]
    neighbor_stores_t_index = neighbor_stores.index[
        neighbor_stores.index.isin(network_solved.stores_t.e_min_pu.columns)
    ]
    neighbor_stores_t = network_prepared.stores_t["e_min_pu"][
        neighbor_stores_t_index
    ]

    neighbor_stores.reset_index(inplace=True)
    neighbor_stores.bus = (
        neighbors.loc[neighbor_stores.bus, "new_index"].reset_index().new_index
    )
    neighbor_stores.index += db.next_etrago_id("store")

    for i in neighbor_stores_t.columns:
        new_index = neighbor_stores[neighbor_stores["Store"] == i].index
        neighbor_stores_t.rename(columns={i: new_index[0]}, inplace=True)

    # storage_units
    neighbor_storage = network_solved.storage_units[
        network_solved.storage_units.bus.isin(neighbors.index)
    ]
    neighbor_storage_t_index = neighbor_storage.index[
        neighbor_storage.index.isin(
            network_solved.storage_units_t.inflow.columns
        )
    ]
    neighbor_storage_t = network_prepared.storage_units_t["inflow"][
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
    carriers = [e for e in carriers if e not in ("AC")]
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
        "substation_lv",
        "substation_off",
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
            "urban_central_heat": "central_heat",
            "EV_battery": "Li_ion",
            "urban_central_water_tanks": "central_heat_store",
            "rural_water_tanks": "rural_heat_store",
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
            "dc",
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
            "location",
            "project_status",
            "dc",
            "voltage",
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
            data={
                "geom_bus0": neighbors.loc[neighbor_links.bus0, "geom"].values,
                "geom_bus1": neighbors.loc[neighbor_links.bus1, "geom"].values,
            },
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
                "urban_central_gas_boiler": "central_gas_boiler",
                "urban_central_resistive_heater": "central_resistive_heater",
                "urban_central_water_tanks_charger": "central_heat_store_charger",
                "urban_central_water_tanks_discharger": "central_heat_store_discharger",
                "rural_water_tanks_charger": "rural_heat_store_charger",
                "rural_water_tanks_discharger": "rural_heat_store_discharger",
                "urban_central_gas_CHP": "central_gas_CHP",
                "urban_central_air_heat_pump": "central_heat_pump",
                "rural_ground_heat_pump": "rural_heat_pump",
            },
            inplace=True,
        )

        H2_links = {
            "H2_to_CH4": "H2_to_CH4",
            "H2_to_power": "H2_to_power",
            "power_to_H2": "power_to_H2_system",
            "CH4_to_H2": "CH4_to_H2",
        }

        for c in H2_links.keys():

            neighbor_links.loc[
                (neighbor_links.carrier == c),
                "lifetime",
            ] = get_sector_parameters("gas", "eGon100RE")["lifetime"][
                H2_links[c]
            ]

        neighbor_links.to_postgis(
            "egon_etrago_link",
            engine,
            schema="grid",
            if_exists="append",
            index=True,
            index_label="link_id",
        )

    extendable_links_carriers = [
        "battery charger",
        "battery discharger",
        "home battery charger",
        "home battery discharger",
        "rural water tanks charger",
        "rural water tanks discharger",
        "urban central water tanks charger",
        "urban central water tanks discharger",
        "urban decentral water tanks charger",
        "urban decentral water tanks discharger",
        "H2 Electrolysis",
        "H2 Fuel Cell",
        "SMR",
        "Sabatier",
    ]

    # delete unwanted carriers for eTraGo
    excluded_carriers = [
        "gas for industry CC",
        "SMR CC",
        "DAC",
    ]
    neighbor_links = neighbor_links[
        ~neighbor_links.carrier.isin(excluded_carriers)
    ]

    # Combine CHP_CC and CHP
    chp_cc = neighbor_links[
        neighbor_links.carrier == "urban central gas CHP CC"
    ]
    for index, row in chp_cc.iterrows():
        neighbor_links.loc[
            neighbor_links.Link == row.Link.replace("CHP CC", "CHP"),
            "p_nom_opt",
        ] += row.p_nom_opt
        neighbor_links.loc[
            neighbor_links.Link == row.Link.replace("CHP CC", "CHP"), "p_nom"
        ] += row.p_nom
        neighbor_links.drop(index, inplace=True)

    # Combine heat pumps
    # Like in Germany, there are air heat pumps in central heat grids
    # and ground heat pumps in rural areas
    rural_air = neighbor_links[neighbor_links.carrier == "rural air heat pump"]
    for index, row in rural_air.iterrows():
        neighbor_links.loc[
            neighbor_links.Link == row.Link.replace("air", "ground"),
            "p_nom_opt",
        ] += row.p_nom_opt
        neighbor_links.loc[
            neighbor_links.Link == row.Link.replace("air", "ground"), "p_nom"
        ] += row.p_nom
        neighbor_links.drop(index, inplace=True)
    links_to_etrago(
        neighbor_links[neighbor_links.carrier.isin(extendable_links_carriers)],
        "eGon100RE",
    )
    links_to_etrago(
        neighbor_links[
            ~neighbor_links.carrier.isin(extendable_links_carriers)
        ],
        "eGon100RE",
        extendable=False,
    )
    # Include links time-series
    # For heat_pumps
    hp = neighbor_links[neighbor_links["carrier"].str.contains("heat pump")]

    neighbor_eff_t = network_prepared.links_t["efficiency"][
        hp[hp.Link.isin(network_prepared.links_t["efficiency"].columns)].index
    ]

    missing_hp = hp[~hp["Link"].isin(neighbor_eff_t.columns)].Link

    eff_timeseries = network_prepared.links_t["efficiency"].copy()
    for met in missing_hp:  # met: missing efficiency timeseries
        try:
            neighbor_eff_t[met] = eff_timeseries.loc[:, met[0:-5]]
        except:
            print(f"There are not timeseries for heat_pump {met}")

    for i in neighbor_eff_t.columns:
        new_index = neighbor_links[neighbor_links["Link"] == i].index
        neighbor_eff_t.rename(columns={i: new_index[0]}, inplace=True)

    # Include links time-series
    # For ev_chargers
    ev = neighbor_links[neighbor_links["carrier"].str.contains("BEV charger")]

    ev_p_max_pu = network_prepared.links_t["p_max_pu"][
        ev[ev.Link.isin(network_prepared.links_t["p_max_pu"].columns)].index
    ]

    missing_ev = ev[~ev["Link"].isin(ev_p_max_pu.columns)].Link

    ev_p_max_pu_timeseries = network_prepared.links_t["p_max_pu"].copy()
    for mct in missing_ev:  # evt: missing charger timeseries
        try:
            ev_p_max_pu[mct] = ev_p_max_pu_timeseries.loc[:, mct[0:-5]]
        except:
            print(f"There are not timeseries for EV charger {mct}")

    for i in ev_p_max_pu.columns:
        new_index = neighbor_links[neighbor_links["Link"] == i].index
        ev_p_max_pu.rename(columns={i: new_index[0]}, inplace=True)

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
            "offwind-float": "wind_offshore",
            "urban_central_solar_thermal": "urban_central_solar_thermal_collector",
            "residential_rural_solar_thermal": "residential_rural_solar_thermal_collector",
            "services_rural_solar_thermal": "services_rural_solar_thermal_collector",
            "solar-hsat": "solar",
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
        "e_sum_min",
        "e_sum_max",
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
            "urban_central_heat": "central_heat",
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
            "urban_central_water_tanks": "central_heat_store",
            "rural_water_tanks": "rural_heat_store",
            "EV_battery": "battery_storage",
        },
        inplace=True,
    )
    neighbor_stores.loc[
        (
            (neighbor_stores.e_nom_max <= 1e9)
            & (neighbor_stores.carrier == "H2_Store")
        ),
        "carrier",
    ] = "H2_underground"
    neighbor_stores.loc[
        (
            (neighbor_stores.e_nom_max > 1e9)
            & (neighbor_stores.carrier == "H2_Store")
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

    for c in ["H2_underground", "H2_overground"]:
        neighbor_stores.loc[
            (neighbor_stores.carrier == c),
            "lifetime",
        ] = get_sector_parameters("gas", "eGon100RE")["lifetime"][c]

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

    # writing neighboring link_t efficiency and p_max_pu to etrago tables
    neighbor_link_t_etrago = pd.DataFrame(
        columns=["scn_name", "temp_id", "p_max_pu", "efficiency"],
        index=neighbor_eff_t.columns.to_list() + ev_p_max_pu.columns.to_list(),
    )
    neighbor_link_t_etrago["scn_name"] = "eGon100RE"
    neighbor_link_t_etrago["temp_id"] = 1
    for i in neighbor_eff_t.columns:
        neighbor_link_t_etrago["efficiency"][i] = neighbor_eff_t[
            i
        ].values.tolist()
    for i in ev_p_max_pu.columns:
        neighbor_link_t_etrago["p_max_pu"][i] = ev_p_max_pu[i].values.tolist()

    neighbor_link_t_etrago.to_sql(
        "egon_etrago_link_timeseries",
        engine,
        schema="grid",
        if_exists="append",
        index=True,
        index_label="link_id",
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
    if not network_solved.lines_t["s_max_pu"].empty:
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


def prepared_network(planning_horizon=3):
    if egon.data.config.settings()["egon-data"]["--run-pypsa-eur"]:
        with open(
            __path__[0] + "/datasets/pypsaeur/config_prepare.yaml", "r"
        ) as stream:
            data_config = yaml.safe_load(stream)

        target_file = (
            Path(".")
            / "run-pypsa-eur"
            / "pypsa-eur"
            / "results"
            / data_config["run"]["name"]
            / "prenetworks"
            / f"base_s_{data_config['scenario']['clusters'][0]}"
            f"_l{data_config['scenario']['ll'][0]}"
            f"_{data_config['scenario']['opts'][0]}"
            f"_{data_config['scenario']['sector_opts'][0]}"
            f"_{data_config['scenario']['planning_horizons'][planning_horizon]}.nc"
        )

    else:
        target_file = (
            Path(".")
            / "data_bundle_egon_data"
            / "pypsa_eur"
            / "prenetworks"
            / "prenetwork_post-manipulate_pre-solve"
            / "base_s_39_lc1.25__cb40ex0-T-H-I-B-solar+p3-dist1_2045.nc"
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
    year = network.year
    skip = network.snapshot_weightings.objective.iloc[0].astype("int")
    df = pd.read_csv(
        "input-pypsa-eur-sec/electrical_demand_timeseries_DE_eGon100RE.csv"
    )

    annual_demand = pd.Series(index=[2019, 2037])
    annual_demand_industry = pd.Series(index=[2019, 2037])
    # Define values from status2019 for interpolation
    # Residential and service (in TWh)
    annual_demand.loc[2019] = 124.71 + 143.26
    # Industry (in TWh)
    annual_demand_industry.loc[2019] = 241.925

    # Define values from NEP 2023 scenario B 2037 for interpolation
    # Residential and service (in TWh)
    annual_demand.loc[2037] = 104 + 153.1
    # Industry (in TWh)
    annual_demand_industry.loc[2037] = 334.0

    # Set interpolated demands for years between 2019 and 2045
    if year < 2037:
        # Calculate annual demands for year by linear interpolating between
        # 2019 and 2037
        # Done seperatly for industry and residential and service to fit
        # to pypsa-eurs structure
        annual_rate = (annual_demand.loc[2037] - annual_demand.loc[2019]) / (
            2037 - 2019
        )
        annual_demand_year = annual_demand.loc[2019] + annual_rate * (
            year - 2019
        )

        annual_rate_industry = (
            annual_demand_industry.loc[2037] - annual_demand_industry.loc[2019]
        ) / (2037 - 2019)
        annual_demand_year_industry = annual_demand_industry.loc[
            2019
        ] + annual_rate_industry * (year - 2019)

        # Scale time series for 100% scenario with the annual demands
        # The shape of the curve is taken from the 100% scenario since the
        # same weather and calender year is used there
        network.loads_t.p_set.loc[:, "DE0 0"] = (
            df["residential_and_service"].loc[::skip]
            / df["residential_and_service"].sum()
            * annual_demand_year
            * 1e6
        ).values

        network.loads_t.p_set.loc[:, "DE0 0 industry electricity"] = (
            df["industry"].loc[::skip]
            / df["industry"].sum()
            * annual_demand_year_industry
            * 1e6
        ).values

    elif year == 2045:
        network.loads_t.p_set.loc[:, "DE0 0"] = df[
            "residential_and_service"
        ].loc[::skip]

        network.loads_t.p_set.loc[:, "DE0 0 industry electricity"] = (
            df["industry"].loc[::skip].values
        )

    else:
        print(
            "Scaling not implemented for years between 2037 and 2045 and beyond."
        )
        return

    network.loads.loc["DE0 0 industry electricity", "p_set"] = 0.0

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
            f"DE0 0 urban central geo thermal {i}",
            bus="DE0 0 urban central heat",
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
    network.loads_t.p_set.loc[:, "DE1 0 rural heat"] = (
        df_egon_heat_demand.loc[:, "residential rural"].values
        + df_egon_heat_demand.loc[:, "service rural"].values
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


def postprocessing_biomass_2045():

    network = read_network()
    network = drop_biomass(network)

    with open(
        __path__[0] + "/datasets/pypsaeur/config_solve.yaml", "r"
    ) as stream:
        data_config = yaml.safe_load(stream)

    target_file = (
        Path(".")
        / "run-pypsa-eur"
        / "pypsa-eur"
        / "results"
        / data_config["run"]["name"]
        / "postnetworks"
        / f"base_s_{data_config['scenario']['clusters'][0]}"
        f"_l{data_config['scenario']['ll'][0]}"
        f"_{data_config['scenario']['opts'][0]}"
        f"_{data_config['scenario']['sector_opts'][0]}"
        f"_{data_config['scenario']['planning_horizons'][3]}.nc"
    )

    network.export_to_netcdf(target_file)


def drop_urban_decentral_heat(network):
    carrier = "urban decentral heat"

    # Add urban decentral heat demand to urban central heat demand
    for country in network.loads.loc[
        network.loads.carrier == carrier, "bus"
    ].str[:5]:

        if f"{country} {carrier}" in network.loads_t.p_set.columns:
            network.loads_t.p_set[
                f"{country} rural heat"
            ] += network.loads_t.p_set[f"{country} {carrier}"]
        else:
            print(
                f"""No time series available for {country} {carrier}.
                  Using static p_set."""
            )

            network.loads_t.p_set[
                f"{country} rural heat"
            ] += network.loads.loc[f"{country} {carrier}", "p_set"]

    # In some cases low-temperature heat for industry is connected to the urban
    # decentral heat bus since there is no urban central heat bus.
    # These loads are connected to the representatiive rural heat bus:
    network.loads.loc[
        (network.loads.bus.str.contains(carrier))
        & (~network.loads.carrier.str.contains(carrier.replace(" heat", ""))),
        "bus",
    ] = network.loads.loc[
        (network.loads.bus.str.contains(carrier))
        & (~network.loads.carrier.str.contains(carrier.replace(" heat", ""))),
        "bus",
    ].str.replace(
        "urban decentral", "rural"
    )

    # Drop componentents attached to urban decentral heat
    for c in network.iterate_components():
        network.mremove(
            c.name, c.df[c.df.index.str.contains("urban decentral")].index
        )

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
        network.loads_t.p_set[f"{country} rural heat"] = (
            heat_demand_per_country.loc[:, country].mul(
                (1 - df.loc[country[:2]].values[0])
            )
        )

    # Drop links with undefined buses or carrier
    network.mremove(
        "Link",
        network.links[
            ~network.links.bus0.isin(network.buses.index.values)
        ].index,
    )
    network.mremove(
        "Link",
        network.links[network.links.carrier == ""].index,
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
        "Generator",
        network.generators[network.generators.carrier == "gas"].index,
    )

    return network


def drop_conventional_power_plants(network):

    # Drop lignite and coal power plants in Germany
    network.mremove(
        "Link",
        network.links[
            (network.links.carrier.isin(["coal", "lignite"]))
            & (network.links.bus1.str.startswith("DE"))
        ].index,
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

    return network


def coal_exit_D():

    df = pd.read_csv(
        "run-pypsa-eur/pypsa-eur/resources/powerplants_s_39.csv", index_col=0
    )
    df_de_coal = df[
        (df.Country == "DE")
        & ((df.Fueltype == "Lignite") | (df.Fueltype == "Hard Coal"))
    ]
    df_de_coal.loc[df_de_coal.DateOut.values >= 2035, "DateOut"] = 2034
    df.loc[df_de_coal.index] = df_de_coal

    df.to_csv("run-pypsa-eur/pypsa-eur/resources/powerplants_s_39.csv")


def offwind_potential_D(network, capacity_per_sqkm=4):

    offwind_ac_factor = 1942
    offwind_dc_factor = 10768
    offwind_float_factor = 134

    # set p_nom_max for German offshore with respect to capacity_per_sqkm = 4 instead of default 2 (which is applied for the rest of Europe)
    network.generators.loc[
        (network.generators.bus == "DE0 0")
        & (network.generators.carrier == "offwind-ac"),
        "p_nom_max",
    ] = (
        offwind_ac_factor * capacity_per_sqkm
    )
    network.generators.loc[
        (network.generators.bus == "DE0 0")
        & (network.generators.carrier == "offwind-dc"),
        "p_nom_max",
    ] = (
        offwind_dc_factor * capacity_per_sqkm
    )
    network.generators.loc[
        (network.generators.bus == "DE0 0")
        & (network.generators.carrier == "offwind-float"),
        "p_nom_max",
    ] = (
        offwind_float_factor * capacity_per_sqkm
    )

    return network


def additional_grid_expansion_2045(network):

    network.global_constraints.loc["lc_limit", "constant"] *= 1.05

    return network


def execute():
    if egon.data.config.settings()["egon-data"]["--run-pypsa-eur"]:
        with open(
            __path__[0] + "/datasets/pypsaeur/config.yaml", "r"
        ) as stream:
            data_config = yaml.safe_load(stream)

        if data_config["foresight"] == "myopic":

            print("Adjusting scenarios on the myopic pathway...")

            coal_exit_D()

            networks = pd.Series()

            for i in range(
                0, len(data_config["scenario"]["planning_horizons"])
            ):
                nc_file = pd.Series(
                    f"base_s_{data_config['scenario']['clusters'][0]}"
                    f"_l{data_config['scenario']['ll'][0]}"
                    f"_{data_config['scenario']['opts'][0]}"
                    f"_{data_config['scenario']['sector_opts'][0]}"
                    f"_{data_config['scenario']['planning_horizons'][i]}.nc"
                )
                networks = networks._append(nc_file)

            scn_path = pd.DataFrame(
                index=["2025", "2030", "2035", "2045"],
                columns=["prenetwork", "functions"],
            )

            for year in scn_path.index:
                scn_path.at[year, "prenetwork"] = networks[
                    networks.str.contains(year)
                ].values

            for year in ["2025", "2030", "2035"]:
                scn_path.loc[year, "functions"] = [
                    # drop_urban_decentral_heat,
                    update_electrical_timeseries_germany,
                    geothermal_district_heating,
                    h2_overground_stores,
                    drop_new_gas_pipelines,
                    offwind_potential_D,
                ]

            scn_path.loc["2045", "functions"] = [
                drop_biomass,
                # drop_urban_decentral_heat,
                update_electrical_timeseries_germany,
                geothermal_district_heating,
                h2_overground_stores,
                drop_new_gas_pipelines,
                drop_fossil_gas,
                offwind_potential_D,
                additional_grid_expansion_2045,
                # drop_conventional_power_plants,
                # rual_heat_technologies, #To be defined
            ]

            network_path = (
                Path(".")
                / "run-pypsa-eur"
                / "pypsa-eur"
                / "results"
                / data_config["run"]["name"]
                / "prenetworks"
            )

            for scn in scn_path.index:
                path = network_path / scn_path.at[scn, "prenetwork"]
                network = pypsa.Network(path)
                network.year = int(scn)
                for manipulator in scn_path.at[scn, "functions"]:
                    network = manipulator(network)
                network.export_to_netcdf(path)

        elif (data_config["foresight"] == "overnight") & (
            int(data_config["scenario"]["planning_horizons"][0]) > 2040
        ):

            print("Adjusting overnight long-term scenario...")

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

        else:
            print(
                f"""Adjustments on prenetworks are not implemented for
                foresight option {data_config['foresight']} and
                year int(data_config['scenario']['planning_horizons'][0].
                Please check the pypsaeur.execute function.
                """
            )
    else:
        print("Pypsa-eur is not executed due to the settings of egon-data")
