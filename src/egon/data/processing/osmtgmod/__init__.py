import sys
import configparser
import psycopg2
import os
import csv
import datetime
import logging
import codecs
import subprocess
import egon.data.config
from egon.data.config import settings
import egon.data.subprocess as subproc
from egon.data import db


def run_osmtgmod():

       ### execute osmTGmod
       # todo git clone in den subprocess
    osmtgmod_repos = os.path.dirname(__file__)+"/osmTGmod"

    if not os.path.exists(osmtgmod_repos):
        subproc.run(["git", "clone", "--single-branch", "--branch", 
                     "features/egon", 
                     "https://github.com/openego/osmTGmod.git"], 
                     cwd=os.path.dirname(__file__))
    
    data_config = egon.data.config.datasets()
    osm_config = data_config["openstreetmap"]["original_data"]
    
    if settings()['egon-data']['--dataset-boundary'] == 'Everything':
        target_path = osm_config["target"]["path"]
    else:
        target_path = osm_config["target"]["path_testmode"]
    
    filtered_osm_pbf_path_to_file = os.path.join(egon.data.__path__[0]+
                                                 "/importing"+"/openstreetmap/"
                                                 + target_path)
    docker_db_config = db.credentials()

    
    osmtgmod(config_database=docker_db_config['POSTGRES_DB'], 
             config_basepath=os.path.dirname(__file__)+"/osmTGmod/egon-data", 
             config_continue_run= False,
             filtered_osm_pbf_path_to_file=filtered_osm_pbf_path_to_file,
             docker_db_config=docker_db_config)
    




def osmtgmod(config_database='egon-data', 
             config_basepath=os.path.dirname(__file__)+"/osmTGmod/egon-data", 
             config_continue_run= False,
             filtered_osm_pbf_path_to_file=None,
             docker_db_config=None):
    
    os.chdir(egon.data.__path__[0]+"/processing/osmtgmod/osmTGmod/")
    # ==============================================================
    # Setup logging
    # ==============================================================
    log=logging.getLogger()
    log.setLevel(logging.INFO)
    logformat=logging.Formatter('%(asctime)s %(message)s','%m/%d/%Y %H:%M:%S')
    sh=logging.StreamHandler()
    sh.setFormatter(logformat)
    log.addHandler(sh)
    logging.info("\n\n======================\nego_otg\n======================")
    logging.info("Logging to standard output...")
    #catch up some log messages from evaluation of command line arguments
    logging.info("Database: {}".format(config_database))
    logging.info("Path for configuration file and results: {}".format(config_basepath))
    if config_continue_run:
        logging.info("Continuing abstraction at: {} (database will not be emptied)".format(config_continue_run_at))
    else:
        logging.info("Starting from scratch. Will remove database if exists.")
    
    # ==============================================================
    # read configuration from file and create folder structure
    # ==============================================================
    if docker_db_config is not None:
        logging.info("Taking db connection credentials from eGon-data workflow with respect to the given docker_db_config variable")
        config=configparser.ConfigParser()
        config.read(config_basepath+".cfg")
        config["postgres_server"]["host"]= docker_db_config['HOST']
        config["postgres_server"]["port"]= docker_db_config['PORT']
        config["postgres_server"]["user"]= docker_db_config['POSTGRES_USER']
        config["postgres_server"]["password"]= docker_db_config["POSTGRES_PASSWORD"]
    else:
        logging.info("Reading configuration from file {}.cfg".format(config_basepath))
        config=configparser.ConfigParser()
        config.read(config_basepath+".cfg")
        if config.get("postgres_server","host").lower()=="<ask>":
            config["postgres_server"]["host"]=input("Postgres server (host) to connect to? (default: localhost)  ") or "localhost"
        if config.get("postgres_server","port").lower()=="<ask>":
            config["postgres_server"]["port"]=input("Postgres port to connect to? (default: 5432)  ") or "5432"
        if config.get("postgres_server","user").lower()=="<ask>":
            config["postgres_server"]["user"]=input("Postgres username on server {0}? (default: postgres)  ".format(config["postgres_server"]["host"])) or "postgres"
        if config.get("postgres_server","password").lower()=="<ask>":
            config["postgres_server"]["password"]=input("Passwort for user {0} on {1}? (default: postgres)  ".format(config["postgres_server"]["user"],config["postgres_server"]["host"])) or "postgres"
    ## Setting osmTGmod folder structure:
    logging.info("Checking/Creating file directories")
    input_data_dir = os.path.join(config_basepath,"input_data")
    result_dir = os.path.join(config_basepath,"results")
    ## Basic folders are created if not existent
    if not os.path.exists(input_data_dir):
        os.makedirs(input_data_dir)
    if not os.path.exists(result_dir):
        os.makedirs(result_dir)
    #start logging to file
    logfile=os.path.join(config_basepath,config_database+".log")
    fh=logging.FileHandler(logfile)
    fh.setFormatter(logformat)
    log.addHandler(fh)
    logging.info("Logging to file '{0}' is set up".format(logfile))
    logging.info("Now logging both to standard output and to file '{0}'...".format(logfile))
    logging.info("\n\n======================\nego_otg\n======================")
    #copy config file
    logging.info("Copying configuration file to '{0}'.".format(os.path.join(config_basepath,config_database+".cfg")))
    os.system("cp {0} {1}".format(config_basepath+".cfg",os.path.join(config_basepath,config_database+".cfg")))
    #copy files (linux specific) ##egon edit will be obsolete therefore commented out
    #logging.info("Copying transfer busses input file to '{0}'".format(config["abstraction"]["transfer_busses_path_to_file"]))
    #os.system("cp {0} {1}".format(config["abstraction"]["transfer_busses_path_to_file"],input_data_dir))
    
    # ==============================================================
    # setup database
    # ==============================================================
    ## Server connection:
    logging.info("Testing connection to server {}".format(config["postgres_server"]["host"]))
    try:
        # Standard database "postgres" is used to check out server connection
        conn_server = psycopg2.connect( host=config["postgres_server"]["host"],
                                        port=config["postgres_server"]["port"],
                                        database="postgres",
                                        user=config["postgres_server"]["user"],
                                        password=config["postgres_server"]["password"])
        cur_server = conn_server.cursor()
        logging.info("Connection to server successful!")
    except:
        # Get the most recent exception
        logging.exception("Connection failed.")
        exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
        sys.exit(exceptionValue)
    
    ## Database connection
    if config_continue_run:
        logging.info("Testing connection to database {}".format(config_database))
        try:
            conn = psycopg2.connect( host=config["postgres_server"]["host"],
                                        port=config["postgres_server"]["port"],
                                        database=config_database,
                                        user=config["postgres_server"]["user"],
                                        password=config["postgres_server"]["password"])
            cur = conn.cursor()
            logging.info("Successfully connected to existing database.")
            for script in ["sql-scripts/functions.sql"]:
                logging.info("Running script {0} ...".format(script))
                with codecs.open(script, 'r', "utf-8-sig") as fd:
                    sqlfile = fd.read()
                cur.execute(sqlfile)
                conn.commit()
                logging.info("Done.")
        except:
            # Get the most recent exception
            logging.exception("Connection failed, unable to continue abstraction process! You might want to start from scratch by dropping second command line argument.")
            exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
            sys.exit(exceptionValue)
    else:
        #### egon Todo either leave out the database dropping or create a new database 
        # Start from empty database, drop if exists
    #    logging.info("Dropping database {} if exists.".format(config_database))
    #    conn_server.set_isolation_level(_il)
    #    cur_server.execute('DROP DATABASE IF EXISTS %s' %config_database)
    #    logging.info("Creating database: {}".format(config_database))
    #    cur_server.execute('CREATE DATABASE %s' %config_database)
    #    conn_server.close()
    #    logging.info("Database created!")
        # Connects to new Database
        logging.info("Connecting to database {} ...".format(config_database))
        conn = psycopg2.connect( host=config["postgres_server"]["host"],
                                        port=config["postgres_server"]["port"],
                                        database=config_database,
                                        user=config["postgres_server"]["user"],
                                        password=config["postgres_server"]["password"])
    
        cur = conn.cursor()
        logging.info("Connected.")
        logging.info("Creating status table ...")
        cur.execute ("""
    DROP TABLE IF EXISTS _db_status;
    CREATE TABLE _db_status (module TEXT, status BOOLEAN);
    INSERT INTO _db_status (module, status) VALUES ('grid_model', FALSE);
                                        """)
        conn.commit()
        logging.info("Status table created.")
        logging.info("Loading functions and result schema ...")
        scripts = ['sql-scripts/extensions.sql', 
                   'sql-scripts/functions.sql', 
                   'sql-scripts/admin_boundaries.sql', 
                   'sql-scripts/electrical_properties.sql', 
                   'sql-scripts/build_up_db.sql']
        for script in scripts:
            logging.info("Running script {0} ...".format(script))
            with codecs.open(script, 'r', "utf-8-sig") as fd:
                sqlfile = fd.read()
            cur.execute(sqlfile)
            conn.commit()
            logging.info("Done.")
        cur.execute ("""UPDATE _db_status SET status = TRUE WHERE module = 'grid_model'; """)
        conn.commit()
        logging.info('osmTGmod-database successfully built up!')
    
    logging.info("Database setup finished succesfully.")
    
    # ==============================================================
    # load osm-data to database
    # ==============================================================
    if not config_continue_run:
    
        logging.info('Importing OSM-data to database.')
        logging.info("Using pdf file: {}".format(filtered_osm_pbf_path_to_file))
        #logging.info("Download date (according to config file): {}".format(config["osm_data"]["date_download"]))
        logging.info("Assuming osmosis is avaliable at: {}".format(config["osm_data"]["osmosis_path_to_binary"]))
        
        # BUG: Python continues (and sets osm_metadata) even in case osmosis fails!!!
        proc = subprocess.Popen('%s --read-pbf %s --write-pgsql database=%s host=%s user=%s password=%s'
                         %(config["osm_data"]["osmosis_path_to_binary"], 
                           filtered_osm_pbf_path_to_file, 
                           config_database, 
                           config["postgres_server"]["host"]+":"+config["postgres_server"]["port"], 
                           config["postgres_server"]["user"], 
                           config["postgres_server"]["password"]), shell=True)
        logging.info('Importing OSM-Data...')
        proc.wait()
    
        # After updating OSM-Data, power_tables (for editing) have to be updated as well
        logging.info("Creating power-tables...")
        cur.execute("SELECT otg_create_power_tables ();")
        conn.commit()
    
        # Update OSM Metadata
        logging.info("Updating OSM metadata")
        v_date = datetime.datetime.now().strftime("%Y-%m-%d")
        cur.execute("UPDATE osm_metadata SET imported = %s", [v_date])
        conn.commit()
        logging.info("OSM data imported to database successfully.")
    
    # ==============================================================
    # excecute abstraction
    # ==============================================================
    min_voltage = 110000
    
    if not config_continue_run:
        logging.info("Setting min_voltage...")
        cur.execute("""
            UPDATE abstr_values
                SET val_int = %s
                WHERE val_description = 'min_voltage'""", (min_voltage,))
        conn.commit()
    
        logging.info("Setting main_station...")
        cur.execute("""
            UPDATE abstr_values
                SET val_int = %s
                WHERE val_description = 'main_station'""", (config.getint("abstraction","main_station"),))
        conn.commit()
    
        logging.info("Setting graph_dfs...")
        cur.execute("""
            UPDATE abstr_values
                SET val_bool = %s
                WHERE val_description = 'graph_dfs'""", (config.getboolean("abstraction","graph_dfs"),))
        conn.commit()
    
        logging.info("Setting conn_subgraphs...")
        cur.execute("""
            UPDATE abstr_values
                SET val_bool = %s
                WHERE val_description = 'conn_subgraphs'""", (config.getboolean("abstraction","conn_subgraphs"),))
        conn.commit()
    
        logging.info("Setting transfer_busses...")
        cur.execute("""
            UPDATE abstr_values
                SET val_bool = %s
                WHERE val_description = 'transfer_busses'""", (config.getboolean("abstraction","transfer_busses"),))
        conn.commit()
    
        #setting transfer busses
        path_for_transfer_busses = input_data_dir + '/transfer_busses.csv'
        logging.info("Reading transfer busses from file {} ...".format(path_for_transfer_busses))
        logging.info("Deleting all entries from transfer_busses table ...")
        cur.execute("""
        DELETE FROM transfer_busses;
        """)
        conn.commit()
    
        ### new
    
        # using the base egon database for transfer bus creation. 
        #import pdb; pdb.set_trace()
        cur.execute("""
        DROP TABLE if exists transfer_busses_complete;
        CREATE TABLE transfer_busses_complete as
        SELECT DISTINCT ON (osm_id) * FROM 
        (SELECT * FROM grid.egon_ehv_substation 
        UNION SELECT subst_id, lon, lat, point, polygon, voltage, power_type, substation, osm_id, osm_www, frequency, subst_name, ref, operator, dbahn, status 
        FROM grid.egon_hvmv_substation ORDER BY osm_id) as foo;
    
    										
    	
        """)
        conn.commit()
        ## todo: make path to transfer_busses.csv generic!
        with open(path_for_transfer_busses, 'w') as this_file:
            cur.copy_expert("""COPY transfer_busses_complete to STDOUT WITH CSV HEADER""", this_file)
            conn.commit()
    
        reader = csv.reader(open(path_for_transfer_busses, 'r'))
        next(reader, None) # Skips header
        logging.info("Copying transfer-busses from CSV to database...")
        for row in reader:
            osm_id = str(row[8])        
            if osm_id[:1] == 'w':
                object_type = 'way'
            elif osm_id[:1] == 'n':
                object_type = 'node'
            else:
                object_type = None        
            osm_id_int = int(osm_id[1:])            
            center_geom = str(row[3])        
            cur.execute("""
                INSERT INTO transfer_busses (osm_id, object_type, center_geom) 
                VALUES (%s, %s, %s); 
                """, (osm_id_int, object_type, center_geom))
            conn.commit()
        logging.info("All transfer busses imported successfully")
    
    # Execute power_script
    logging.info("Preparing execution of abstraction script 'sql-scripts/power_script.sql' ...")
    with codecs.open("sql-scripts/power_script.sql", 'r', "utf-8-sig") as fd:
        sqlfile = fd.read()
    #remove lines starting with "--" (comments), tabulators and empty line
    #beware: comments in C-like style (such as /* comment */) are not parsed!
    sqlfile_without_comments="".join([line.lstrip().split("--")[0]+"\n" if not line.lstrip().split("--")[0]=="" else "" for line in sqlfile.split("\n")])
    
    if config_continue_run:
        logging.info("Continuing power script, starting from command #{}...".format(config_continue_run_at))
    else:
        logging.info("Stating execution of  power script...")
        config_continue_run_at=-1
    
    if not config_continue_run: ##debugging - to be removed
        cur.execute("drop table if exists debug;create table debug (step_before int,max_bus_id int, num_bus int,max_branch_id int, num_branch int, num_110_bus int, num_220_bus int, num_380_bus int)") 
        conn.commit() 
    
    #split sqlfile in commands seperated by ";", while not considering symbols for splitting if "escaped" by single quoted strings. Drop everything after last semicolon.
    for i,command in enumerate("'".join([segment.replace(";","§") if i%2==0 else segment for i,segment in enumerate(sqlfile_without_comments.split("'"))]).split("§")[:-1]):
        if i>=config_continue_run_at:
            logging.info("Executing SQL statement {0}:{1}\n".format(i,command))
            try:
                cur.execute(command)
                conn.commit()
            except:
                logging.exception("Exception raised with command {0}. Check data and code and restart with 'python ego_otg.py {1} {0}'.".format(i,config_database))
                sys.exit()
            if i>16:##debugging - to be removed
                cur.execute("insert into debug values ({0},(select max(id) from bus_data),(select count(*) from bus_data),(select max(branch_id) from branch_data),(select count(*) from branch_data),(select count(*) from bus_data where voltage = 110000), (select count (*) from bus_data where voltage = 220000), (select count (*) from bus_data where voltage = 380000))".format(i))
                conn.commit()
        
    logging.info('Power-script executed successfully.')
    
    logging.info("Saving Results...")
    cur.execute("SELECT otg_save_results ();")
    conn.commit()
    
    logging.info("Abstraction process complete!")
    
    # ==============================================================
    # Write results
    # ==============================================================
    logging.info("Writing results")
    
    tables = ['bus_data', 'branch_data', 'dcline_data', 'results_metadata']
    for table in tables:
        logging.info('writing %s...' % table)
        filename = os.path.join(result_dir,table+".csv")
        logging.info("Writing contents of table {0} to {1}...".format(table,filename))
        query = 'SELECT * FROM results.%s ' %(table,)
        outputquery = "COPY ({0}) TO STDOUT WITH DELIMITER ',' CSV HEADER".format(query)
        with open(filename, encoding='utf-8', mode = "w") as fh:
            cur.copy_expert(outputquery, fh)
    
    logging.info('All tables written!')
    
    logging.info("EXECUTION FINISHED SUCCESSFULLY!")
