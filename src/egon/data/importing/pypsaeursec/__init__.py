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


def pypsa_eur_sec_eGon100_capacities(version="0.0.0"):
    """Inserts installed capacities for the eGon100 scenario

    Returns
    -------
    None.

    """

    # Connect to local database
    engine = db.engine()

    # read-in installed capacities
    cwd = Path(".")
    filepath = cwd / "run-pypsa-eur-sec"
    pypsa_eur_sec_repos = filepath / "pypsa-eur-sec"
    
    # Read YAML file
    pes_egonconfig = pypsa_eur_sec_repos / "config_egon.yaml"
    with open(pes_egonconfig, 'r') as stream:
        data_config = yaml.safe_load(stream)
    target_file = (pypsa_eur_sec_repos / "results" / data_config["run"] 
                   / "csvs/nodal_capacities.csv"
    )

    df = pd.read_csv(target_file, skiprows=5)
    df.columns = ["component", "country", "carrier", "p_nom"]
    df["scn_name"] = "eGon100RE"
    df["version"] = version
    # Todo: connection to bus!

    
    desired_countries = ['DE', 'AT', 'CH', 'CZ', 'PL', 'SE', 'NO', 'DK',
                         'GB', 'NL', 'BE', 'FR', 'LU']
    
    new_df =pd.DataFrame(columns=(df.columns))
    for i in desired_countries:
        new_df_1 = df[df.country.str.startswith(i, na=False)]
        new_df = new_df.append(new_df_1)



    # Insert generator data to db
    gens = new_df["component"] == "generators"
    df_gens = new_df.loc[gens]
    df_gens = df_gens.drop('component', axis=1)
    df_gens = df_gens.drop('country', axis=1) # use for bus connection!
    df_gens["p_nom_extendable"] = False
    
    # ToDo for all: 
    ## if conventional, then p_min_pu_fixed=0 and p_max_pu_fixed=1,
    ## set marginal costs fixed
    df_gens["sign"]= 1
    

    df_gens.to_sql(
        "egon_pf_hv_generator",
        engine,
        schema="grid",
        if_exists="append",
        index=True,
        index_label="generator_id"
    )


def neighbor_reduction(version="0.0.0"):
    
    
    cwd = Path(".")
    filepath = cwd / "run-pypsa-eur-sec"
    pypsa_eur_sec_repos = filepath / "pypsa-eur-sec"
    # Read YAML file
    pes_egonconfig = pypsa_eur_sec_repos / "config_egon.yaml"
    with open(pes_egonconfig, 'r') as stream:
        data_config = yaml.safe_load(stream)

        
    simpl = data_config["scenario"]["simpl"][0]
    clusters = data_config["scenario"]["clusters"][0]
    lv = data_config["scenario"]["lv"][0]
    opts = data_config["scenario"]["opts"][0]
    sector_opts = data_config["scenario"]["sector_opts"][0]
    planning_horizons = data_config["scenario"]["planning_horizons"][0]
    file = "elec_s{simpl}_{clusters}_lv{lv}_{opts}_{sector_opts}_{planning_horizons}.nc".format(simpl=simpl,
                             clusters=clusters,
                             opts=opts,
                             lv=lv,
                             sector_opts=sector_opts,
                             planning_horizons=planning_horizons)
    
    target_file = (pypsa_eur_sec_repos / "results" / data_config["run"] 
                   / "postnetworks" / file )   
    
    network = pypsa.Network(str(target_file))
        
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
    
    # writing components of neighboring countries to etrago tables
    
    neighbors = network.buses[~network.buses.country.isin(['DE'])]
    
    neighbor_gens = network.generators[network.generators.bus.isin(neighbors.index)]
    neighbors['new_index']=neighbors.reset_index().index
    neighbor_gens.reset_index(inplace=True)
    neighbor_gens.bus = neighbors.loc[neighbor_gens.bus, 'new_index'].reset_index().new_index
    
    
    # Connect to local database
    engine = db.engine()
    
    db.execute_sql("DELETE FROM grid.egon_pf_hv_bus "
                   "WHERE scn_name = 'eGon100RE' "
                   "AND country <> 'DE'")
    
    neighbors["scn_name"] = "eGon100RE"
    neighbors["version"] = version
    neighbors = neighbors.rename(columns={'v_mag_pu_set': 'v_mag_pu_set_fixed'})
    neighbors.index = neighbors['new_index']

    for i in ['new_index', 'control', 'generator', 'location', 'sub_network']:
        neighbors = neighbors.drop(i, axis=1)

    neighbors.to_sql(
        "egon_pf_hv_bus",
        engine,
        schema="grid",
        if_exists="append",
        index=True,
        index_label="bus_id"
    )
    
    neighbor_gens.bus
    neighbor_gens["scn_name"] = "eGon100RE"
    neighbor_gens["version"] = version
    neighbor_gens["p_nom"] = neighbor_gens["p_nom_opt"]
    neighbor_gens = neighbor_gens.rename(columns={'marginal_cost': 
                                                  'marginal_cost_fixed', 
                                                  'p_min_pu':
                                                  'p_min_pu_fixed',
                                                  'p_max_pu': 
                                                  'p_max_pu_fixed'})

    for i in ['name','weight', 'lifetime', 'p_set', 'q_set', 'p_nom_opt']:
        neighbor_gens = neighbor_gens.drop(i, axis=1)

    neighbor_gens.to_sql(
        "egon_pf_hv_generator",
        engine,
        schema="grid",
        if_exists="append",
        index=True,
        index_label="generator_id"
    )