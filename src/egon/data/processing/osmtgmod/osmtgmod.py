import egon.data.config
import egon.data.subprocess as subprocess

### writing conifg.cfg file


import os


def run_osmtgmod():

#    configfile_name = "egon.cfg"
#    
#    # Check if there is already a configurtion file
#    if not os.path.isfile(configfile_name):
#        # Create the configuration file as it doesn't exist yet
#        cfgfile = open(configfile_name, "w")
#    
#    
#    
#        ## db connection parameters
#    
#        Config = ConfigParser.ConfigParser()
#        Config.add_section("postgres_server")
#        Config.set("postgres_server", "host", "localhost")
#        Config.set("postgres_server", "port", "root")
#        Config.set("postgres_server", "user", "my secret password")
#        Config.set("postgres_server", "password", "write-math")
#        
#    
#        ## osm_data
#    
#        Config.add_section("osm_data")
#        Config.set("osm_data", "filtered_osm_pbf_path_to_file", filtered_osm_pbf_path_to_file)
#        Config.set("osm_data", "date_download", "2021-01-01")
#        Config.set("osm_data", "osmosis_path_to_binary", "osmosis/bin/osmosis")
#        
#    
#        # getting path to osm.pdf file
#        data_config = egon.data.config.datasets()
#        osm_config = data_config["openstreetmap"]["original_data"]
#    
#        filtered_osm_pbf_path_to_file = os.path.join(
#            os.path.dirname(__file__), osm_config["target"]["path"]
#        )
#    
#        Config.add_section("abstraction")
#        Config.set("abstraction", "main_station", "35176751")
#        Config.set("abstraction", "graph_dfs",  False)
#        Config.set("abstraction", "conn_subgraphs" , True)
#        Config.set("abstraction", "transfer_busses",  True)
#        Config.set("abstraction", "transfer_busses_path_to_file", "transfer_busses/161001_transfer_busses.csv")
#    
#        Config.write(cfgfile)
#        cfgfile.close()
#    
#    
#       ### get transfer buses
#    
#       SELECT DISTINCT ON (osm_id) * FROM ( SELECT * FROM model_draft.ego_grid_ehv_substation UNION SELECT subst_id, lon, lat, point, polygon, voltage, power_type, substation, osm_id, osm_www, frequency, subst_name, ref, operator, dbahn, status, otg_id FROM model_draft.ego_grid_hvmv_substation --GROUP BY osm_id ORDER BY osm_id ) as foo;
#    
       ### execute osmTGmod
       # todo git clone in den subprocess


    command = ["python", "egon_otg.py", "egon.cfg"]
    try:
        subprocess.run(command, cwd="osmTGmod/")
    except FileNotFoundError as fnfe:
        subprocess.run(["git", "clone", "https://github.com/openego/osmTGmod"])
    subprocess.run(command, cwd="osmTGmod/")

    
       ### copy data from the osmtgmod db to the main db
       # todo
