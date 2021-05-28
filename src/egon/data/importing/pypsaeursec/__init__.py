"""The central module containing all code dealing with importing data from
the pysa-eur-sec scenario parameter creation
"""

from pathlib import Path
from urllib.request import urlretrieve
import tarfile
import os

import importlib_resources as resources
import pandas as pd

from egon.data import db
from egon.data import __path__
import egon.data.config
import egon.data.subprocess as subproc
import yaml

import pypsa



def run_pypsa_eur_sec():

    cwd = Path(".")
    filepath = cwd / "run-pypsa-eur-sec"
    filepath.mkdir(parents=True, exist_ok=True)

    pypsa_eur_repos = filepath / "pypsa-eur"
    technology_data_repos = filepath / "technology-data"
    pypsa_eur_sec_repos = filepath / "pypsa-eur-sec"
    pypsa_eur_sec_repos_data = pypsa_eur_sec_repos / "data/"

    if not pypsa_eur_repos.exists():
        subproc.run(
            [
                "git",
                "clone",
                "--branch",
                "master",
                "https://github.com/PyPSA/pypsa-eur.git",
                pypsa_eur_repos,
            ],
        )

        subproc.run(
            [
                "git",
                "checkout",
                "4e44822514755cdd0289687556547100fba6218b",
            ],
            cwd=pypsa_eur_repos,
        )
        
        file_to_copy = os.path.join(
        __path__[0] + "/importing" + "/pypsaeursec/pypsaeur/Snakefile")
        
        subproc.run(
            [
                "cp",
                file_to_copy,
                pypsa_eur_repos,
            ],
        )
        
        # Read YAML file
        path_to_env = pypsa_eur_repos / "envs/environment.yaml"
        with open(path_to_env, 'r') as stream:
            env = yaml.safe_load(stream)
            
        env["dependencies"].append("gurobi")

        # Write YAML file
        with open(path_to_env, 'w', encoding='utf8') as outfile:
            yaml.dump(env, outfile, default_flow_style=False, allow_unicode=True)

    if not technology_data_repos.exists():
        subproc.run(
            [
                "git",
                "clone",
                "--branch",
                "v0.2.0",
                "https://github.com/PyPSA/technology-data.git",
                technology_data_repos,
            ],
        )

    if not pypsa_eur_sec_repos.exists():
        subproc.run(
            [
                "git",
                "clone",
                "https://github.com/openego/pypsa-eur-sec.git",
                pypsa_eur_sec_repos,
            ],
        )

    datafile = "pypsa-eur-sec-data-bundle-210418.tar.gz"
    datapath = pypsa_eur_sec_repos_data / datafile
    if not datapath.exists():
        urlretrieve(f"https://nworbmot.org/{datafile}", datapath)
        tar = tarfile.open(datapath)
        tar.extractall(pypsa_eur_sec_repos_data)

    with open(filepath / "Snakefile", "w") as snakefile:
        snakefile.write(
            resources.read_text("egon.data.importing.pypsaeursec", "Snakefile")
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
        ],
    )


def pypsa_eur_sec_eGon100_capacities():
    """Inserts installed capacities for the eGon100 scenario

    Returns
    -------
    None.

    """

    # Connect to local database
    engine = db.engine()

    # Delete rows if already exist
    db.execute_sql(
        "DELETE FROM supply.egon_scenario_capacities "
        "WHERE scenario_name = 'eGon100'"
    )

    # read-in installed capacities
    cwd = Path(".")
    filepath = cwd / "run-pypsa-eur-sec"
    pypsa_eur_sec_repos = filepath / "pypsa-eur-sec"
    data_config = egon.data.config.datasets(config_file=pypsa_eur_sec_repos 
                                            / "egon_config.yaml")
    target_file = (pypsa_eur_sec_repos / "results" / data_config["run"] 
                   / "csvs/nodal_capacities.csv"
    )

    df = pd.read_csv(target_file, skiprows=5)
    df.columns = ["component", "country", "carrier", "capacity"]
    df["scenario_name"] = "eGon100"
    
    desired_countries = ['DE', 'AT', 'CH', 'CZ', 'PL', 'SE', 'NO', 'DK',
                         'GB', 'NL', 'BE', 'FR', 'LU']
    
    new_df =pd.DataFrame(columns=(df.columns))
    for i in desired_countries:
        new_df_1 = df[df.country.str.startswith(i, na=False)]
        new_df = new_df.append(new_df_1)


    # Insert data to db
    new_df.to_sql(
        "egon_scenario_capacities",
        engine,
        schema="supply",
        if_exists="append",
        index=new_df.index,
    )


def neighbor_reduction():
    
    
    cwd = Path(".")
    filepath = cwd / "run-pypsa-eur-sec"
    pypsa_eur_sec_repos = filepath / "pypsa-eur-sec"
    data_config = egon.data.config.datasets(config_file=pypsa_eur_sec_repos 
                                            / "egon_config.yaml")
    
    # todo: file name should refer to the one from the egon_config.yaml
    target_file = (pypsa_eur_sec_repos / "results" / data_config["run"] 
                   / "postnetworks" 
                   / "elec_s_37_lv4__Co2L0-3H-T-H-B-solar3-dist1_go_2030.nc"
    )   
    
    network = pypsa.Network(target_file)
        
    foreign_buses = network.buses[~network.buses.country.isin(
        ['DE', 'AT', 'CH', 'CZ', 'PL', 'SE', 'NO', 'DK', 'GB', 'NL', 'BE',
         'FR', 'LU'])]
    network.buses = network.buses.drop(
        network.buses.loc[foreign_buses.index].index)
    
    
    # drop foreign lines and links from the 2nd row
    
    network.lines = network.lines.drop(network.lines[
        (network.lines['bus0'].isin(network.buses.index) == False) &
        (network.lines['bus1'].isin(network.buses.index) == False)].index)
        
    # select all lines which have at bus1 the bus which is kept
    lines_cb_1 = network.lines[
        (network.lines['bus0'].isin(network.buses.index) == False)]    
    
    # create a load at bus1 with the line's hourly loading
    for i,k in zip(lines_cb_1.bus1.values, lines_cb_1.index) :
        network.add("Load", "slack_fix "+i+' '+k, bus=i,
                    p_set = network.lines_t.p1[k]) 
        
    # select all lines which have at bus0 the bus which is kept
    lines_cb_0 = network.lines[
        (network.lines['bus1'].isin(network.buses.index) == False)]  
        
     # create a load at bus0 with the line's hourly loading
    for i,k in zip(lines_cb_0.bus0.values, lines_cb_0.index) :
        network.add("Load", "slack_fix "+i+' '+k, bus=i,
                    p_set = network.lines_t.p0[k]) 
    
     
    # do the same for links
       
    network.links = network.links.drop(network.links[
    (network.links['bus0'].isin(network.buses.index) == False) &
    (network.links['bus1'].isin(network.buses.index) == False)].index)
        
          
    # select all links which have at bus1 the bus which is kept
    links_cb_1 = network.links[
        (network.links['bus0'].isin(network.buses.index) == False)]    
    
    # create a load at bus1 with the link's hourly loading  
    for i,k in zip(links_cb_1.bus1.values, links_cb_1.index) :
        network.add("Load", "slack_fix_links "+i+' '+k, bus=i,
                    p_set = network.links_t.p1[k]) 
        
        
    # select all links which have at bus0 the bus which is kept
    links_cb_0 = network.links[
        (network.links['bus1'].isin(network.buses.index) == False)]  
    
    # create a load at bus0 with the link's hourly loading
    for i,k in zip(links_cb_0.bus0.values, links_cb_0.index) :
        network.add("Load", "slack_fix_links "+i+' '+k, bus=i,
                    p_set = network.links_t.p0[k])
      
    
    # drop remaining foreign components
    
    
    network.lines = network.lines.drop(network.lines[
        (network.lines['bus0'].isin(network.buses.index) == False) |
        (network.lines['bus1'].isin(network.buses.index) == False)].index)
        
    
    network.links = network.links.drop(network.links[
        (network.links['bus0'].isin(network.buses.index) == False) |
        (network.links['bus1'].isin(network.buses.index) == False)].index)
    
    network.transformers = network.transformers.drop(network.transformers[
        (network.transformers['bus0'].isin(network.buses.index) == False) |
        (network.transformers['bus1'].isin(network.
                                           buses.index) == False)].index)
    network.generators = network.generators.drop(network.generators[
        (network.generators['bus'].isin(network.buses.index) == False)].index)
    network.loads = network.loads.drop(network.loads[
        (network.loads['bus'].isin(network.buses.index) == False)].index)
    network.storage_units = network.storage_units.drop(network.storage_units[
        (network.storage_units['bus'].isin(network.
                                           buses.index) == False)].index)
    
    components = ['loads', 'generators', 'lines', 'buses', 'transformers',
                  'links']
    for g in components:  # loads_t
        h = g + '_t'
        nw = getattr(network, h)  # network.loads_t
        for i in nw.keys():  # network.loads_t.p
            cols = [j for j in getattr(
                nw, i).columns if j not in getattr(network, g).index]
            for k in cols:
                del getattr(nw, i)[k]
    # todo: write components into etrago tables, harmonize with capacity function
    # how to handle values for Germany