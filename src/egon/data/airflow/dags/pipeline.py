import os

from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import importlib_resources as resources

from egon.data.airflow.tasks import initdb
from egon.data.processing.zensus_vg250 import (
    zensus_population_inside_germany as zensus_vg250,
)
import airflow
import egon.data.importing.demandregio as import_dr
import egon.data.importing.demandregio.install_disaggregator as install_dr
import egon.data.importing.etrago as etrago
import egon.data.importing.heat_demand_data as import_hd
import egon.data.importing.mastr as mastr
import egon.data.importing.nep_input_data as nep_input
import egon.data.importing.openstreetmap as import_osm
import egon.data.importing.re_potential_areas as re_potential_areas
import egon.data.importing.vg250 as import_vg250
import egon.data.processing.demandregio as process_dr
import egon.data.processing.openstreetmap as process_osm
import egon.data.importing.zensus as import_zs
import egon.data.processing.zensus as process_zs
import egon.data.processing.osmtgmod as osmtgmod
import egon.data.processing.power_plants as power_plants
import egon.data.processing.substation as substation
import egon.data.processing.zensus_vg250.zensus_population_inside_germany as zensus_vg250
import egon.data.processing.mv_grid_districts as mvgd
import egon.data.importing.scenarios as import_scenarios
import egon.data.importing.industrial_sites as industrial_sites
import egon.data.processing.loadarea as loadarea
from egon.data import db


with airflow.DAG(
    "egon-data-processing-pipeline",
    description="The eGo^N data processing DAG.",
    default_args={"start_date": days_ago(1)},
    template_searchpath=[
        os.path.abspath(
            os.path.join(
                os.path.dirname(__file__), "..", "..", "processing", "vg250"
            )
        )
    ],
    is_paused_upon_creation=False,
    schedule_interval=None,
) as pipeline:
    setup = PythonOperator(task_id="initdb", python_callable=initdb)

    # Openstreetmap data import
    osm_download = PythonOperator(
        task_id="download-osm",
        python_callable=import_osm.download_pbf_file,
    )
    osm_import = PythonOperator(
        task_id="import-osm",
        python_callable=import_osm.to_postgres,
    )
    osm_migrate = PythonOperator(
        task_id="migrate-osm",
        python_callable=process_osm.modify_tables,
    )
    osm_add_metadata = PythonOperator(
        task_id="add-osm-metadata",
        python_callable=import_osm.add_metadata,
    )
    setup >> osm_download >> osm_import >> osm_migrate >> osm_add_metadata

    # VG250 (Verwaltungsgebiete 250) data import
    vg250_download = PythonOperator(
        task_id="download-vg250",
        python_callable=import_vg250.download_vg250_files,
    )
    vg250_import = PythonOperator(
        task_id="import-vg250",
        python_callable=import_vg250.to_postgres,
    )

    vg250_nuts_mview = PostgresOperator(
        task_id="vg250_nuts_mview",
        sql="vg250_lan_nuts_id_mview.sql",
        postgres_conn_id="egon_data",
        autocommit=True,
    )
    vg250_metadata = PythonOperator(
        task_id="add-vg250-metadata",
        python_callable=import_vg250.add_metadata,
    )
    vg250_clean_and_prepare = PostgresOperator(
        task_id="vg250_clean_and_prepare",
        sql="cleaning_and_preparation.sql",
        postgres_conn_id="egon_data",
        autocommit=True,
    )
    setup >> vg250_download >> vg250_import >> vg250_nuts_mview
    vg250_nuts_mview >> vg250_metadata >> vg250_clean_and_prepare

    # Zensus import
    zensus_download_population = PythonOperator(
        task_id="download-zensus-population",
        python_callable=import_zs.download_zensus_pop,
    )

    zensus_download_misc = PythonOperator(
        task_id="download-zensus-misc",
        python_callable=import_zs.download_zensus_misc,
    )

    zensus_tables = PythonOperator(
        task_id="create-zensus-tables",
        python_callable=import_zs.create_zensus_tables,
    )

    population_import = PythonOperator(
        task_id="import-zensus-population",
        python_callable=import_zs.population_to_postgres,
    )

    zensus_misc_import = PythonOperator(
        task_id="import-zensus-misc",
        python_callable=import_zs.zensus_misc_to_postgres,
    )
    setup >> zensus_download_population >> zensus_download_misc
    zensus_download_misc >> zensus_tables >> population_import
    vg250_clean_and_prepare >> population_import
    population_import >> zensus_misc_import

    # Combine Zensus and VG250 data
    map_zensus_vg250 = PythonOperator(
        task_id="map_zensus_vg250",
        python_callable=zensus_vg250.map_zensus_vg250,
    )

    zensus_inside_ger = PythonOperator(
        task_id="zensus-inside-germany",
        python_callable=zensus_vg250.inside_germany,
    )

    zensus_inside_ger_metadata = PythonOperator(
        task_id="zensus-inside-germany-metadata",
        python_callable=zensus_vg250.add_metadata_zensus_inside_ger,
    )

    vg250_population = PythonOperator(
        task_id="population-in-municipalities",
        python_callable=zensus_vg250.population_in_municipalities,
    )

    vg250_population_metadata = PythonOperator(
        task_id="population-in-municipalities-metadata",
        python_callable=zensus_vg250.add_metadata_vg250_gem_pop,
    )
    [
        vg250_clean_and_prepare,
        population_import,
    ] >> map_zensus_vg250 >> zensus_inside_ger >> zensus_inside_ger_metadata
    zensus_inside_ger >> vg250_population >> vg250_population_metadata

    # Scenario table
    scenario_input_tables = PythonOperator(
        task_id="create-scenario-parameters-table",
        python_callable=import_scenarios.create_table
    )

    scenario_input_import = PythonOperator(
        task_id="import-scenario-parameters",
        python_callable=import_scenarios.insert_scenarios
    )
    setup >> scenario_input_tables >> scenario_input_import

    # DemandRegio data import
    demandregio_tables = PythonOperator(
        task_id="demandregio-tables",
        python_callable=import_dr.create_tables,
    )

    scenario_input_tables >> demandregio_tables


    demandregio_installation = PythonOperator(
        task_id="demandregio-installation",
        python_callable=install_dr.clone_and_install,
    )

    setup >> demandregio_installation

    demandregio_society = PythonOperator(
        task_id="demandregio-society",
        python_callable=import_dr.insert_society_data,
    )

    demandregio_installation >> demandregio_society
    vg250_clean_and_prepare >> demandregio_society
    demandregio_tables >> demandregio_society
    scenario_input_import >> demandregio_society

    demandregio_demand_households = PythonOperator(
        task_id="demandregio-household-demands",
        python_callable=import_dr.insert_household_demand,
    )

    demandregio_installation >> demandregio_demand_households
    vg250_clean_and_prepare >> demandregio_demand_households
    demandregio_tables >> demandregio_demand_households
    scenario_input_import >> demandregio_demand_households

    demandregio_demand_cts_ind = PythonOperator(
        task_id="demandregio-cts-industry-demands",
        python_callable=import_dr.insert_cts_ind_demands,
    )

    demandregio_installation >> demandregio_demand_cts_ind
    vg250_clean_and_prepare >> demandregio_demand_cts_ind
    demandregio_tables >> demandregio_demand_cts_ind
    scenario_input_import >> demandregio_demand_cts_ind

    # Society prognosis
    prognosis_tables = PythonOperator(
        task_id="create-prognosis-tables",
        python_callable=process_zs.create_tables,
    )

    setup >> prognosis_tables

    population_prognosis = PythonOperator(
        task_id="zensus-population-prognosis",
        python_callable=process_zs.population_prognosis_to_zensus,
    )

    prognosis_tables >> population_prognosis
    map_zensus_vg250 >> population_prognosis
    demandregio_society >> population_prognosis
    population_import >> population_prognosis

    household_prognosis = PythonOperator(
        task_id="zensus-household-prognosis",
        python_callable=process_zs.household_prognosis_to_zensus,
    )
    prognosis_tables >> household_prognosis
    map_zensus_vg250 >> household_prognosis
    demandregio_society >> household_prognosis
    zensus_misc_import >> household_prognosis


    # Distribute electrical demands to zensus cells
    processed_dr_tables = PythonOperator(
        task_id="create-demand-tables",
        python_callable=process_dr.create_tables,
    )

    elec_household_demands_zensus = PythonOperator(
        task_id="electrical-household-demands-zensus",
        python_callable=process_dr.distribute_household_demands,
    )

    zensus_tables >> processed_dr_tables >> elec_household_demands_zensus
    population_prognosis >> elec_household_demands_zensus
    demandregio_demand_households >> elec_household_demands_zensus
    map_zensus_vg250 >> elec_household_demands_zensus

    # Power plant setup
    power_plant_tables = PythonOperator(
        task_id="create-power-plant-tables",
        python_callable=power_plants.create_tables,
    )
    setup >> power_plant_tables

    # NEP data import
    create_tables = PythonOperator(
        task_id="create-scenario-tables",
        python_callable=nep_input.create_scenario_input_tables,
    )

    nep_insert_data = PythonOperator(
        task_id="insert-nep-data",
        python_callable=nep_input.insert_data_nep,
    )

    setup >> create_tables >> nep_insert_data
    vg250_clean_and_prepare >> nep_insert_data
    population_import >> nep_insert_data

    # setting etrago input tables
    etrago_input_data = PythonOperator(
        task_id="setting-etrago-input-tables",
        python_callable=etrago.create_tables,
    )
    setup >> etrago_input_data

    # Retrieve MaStR data
    retrieve_mastr_data = PythonOperator(
        task_id="retrieve_mastr_data",
        python_callable=mastr.download_mastr_data,
    )
    setup >> retrieve_mastr_data

    # Substation extraction
    substation_tables = PythonOperator(
        task_id="create_substation_tables",
        python_callable=substation.create_tables,
    )

    substation_functions = PythonOperator(
        task_id="substation_functions",
        python_callable=substation.create_sql_functions,
    )

    hvmv_substation_extraction = PostgresOperator(
        task_id="hvmv_substation_extraction",
        sql=resources.read_text(substation, "hvmv_substation.sql"),
        postgres_conn_id="egon_data",
        autocommit=True,
    )

    ehv_substation_extraction = PostgresOperator(
        task_id="ehv_substation_extraction",
        sql=resources.read_text(substation, "ehv_substation.sql"),
        postgres_conn_id="egon_data",
        autocommit=True,
    )

    osm_add_metadata >> substation_tables >> substation_functions
    substation_functions >> hvmv_substation_extraction
    substation_functions >> ehv_substation_extraction
    vg250_clean_and_prepare >> hvmv_substation_extraction
    vg250_clean_and_prepare >> ehv_substation_extraction

    # osmTGmod ehv/hv grid model generation
    run_osmtgmod = PythonOperator(
        task_id="run_osmtgmod",
        python_callable=osmtgmod.run_osmtgmod,
    )

    osmtgmod_pypsa = PythonOperator(
        task_id="osmtgmod_pypsa",
        python_callable=osmtgmod.osmtgmmod_to_pypsa,
    )

    osmtgmod_substation = PostgresOperator(
        task_id="osmtgmod_substation",
        sql=resources.read_text(osmtgmod, "substation_otg.sql"),
        postgres_conn_id="egon_data",
        autocommit=True,
    )
    ehv_substation_extraction >> run_osmtgmod
    hvmv_substation_extraction >> run_osmtgmod
    run_osmtgmod >> osmtgmod_pypsa
    run_osmtgmod >> osmtgmod_substation

    # MV grid districts
    create_voronoi = PythonOperator(
        task_id="create_voronoi",
        python_callable=substation.create_voronoi
    )
    osmtgmod_substation >> create_voronoi

    
    define_mv_grid_districts = PythonOperator(
        task_id="define_mv_grid_districts",
        python_callable=mvgd.define_mv_grid_districts
    )
    create_voronoi >> define_mv_grid_districts

    # Import potential areas for wind onshore and ground-mounted PV
    download_re_potential_areas = PythonOperator(
        task_id="download_re_potential_area_data",
        python_callable=re_potential_areas.download_datasets,
    )
    create_re_potential_areas_tables = PythonOperator(
        task_id="create_re_potential_areas_tables",
        python_callable=re_potential_areas.create_tables,
    )
    insert_re_potential_areas = PythonOperator(
        task_id="insert_re_potential_areas",
        python_callable=re_potential_areas.insert_data,
    )
    setup >> download_re_potential_areas >> create_re_potential_areas_tables
    create_re_potential_areas_tables >> insert_re_potential_areas

    # Future heat demand calculation based on Peta5_0_1 data
    heat_demand_import = PythonOperator(
        task_id="import-heat-demand",
        python_callable=import_hd.future_heat_demand_data_import,
    )
    vg250_clean_and_prepare >> heat_demand_import
    zensus_inside_ger_metadata >> heat_demand_import
    scenario_input_import >> heat_demand_import

    # Power plant setup
    power_plant_tables = PythonOperator(
        task_id="create-power-plant-tables",
        python_callable=power_plants.create_tables,
    )

    power_plant_import = PythonOperator(
        task_id="import-power-plants",
        python_callable=power_plants.insert_power_plants,
    )

    setup >> power_plant_tables >> power_plant_import
    nep_insert_data >> power_plant_import
    retrieve_mastr_data >> power_plant_import

    # Import and merge data on industrial sites from different sources

    industrial_sites_import = PythonOperator(
        task_id="download-import-industrial-sites",
        python_callable=industrial_sites.download_import_industrial_sites
    )

    industrial_sites_merge = PythonOperator(
        task_id="merge-industrial-sites",
        python_callable=industrial_sites.merge_inputs
    )

    industrial_sites_nuts = PythonOperator(
        task_id="map-industrial-sites-nuts3",
        python_callable=industrial_sites.map_nuts3
    )
    vg250_clean_and_prepare >> industrial_sites_import
    industrial_sites_import >> industrial_sites_merge >> industrial_sites_nuts

    # Distribute electrical CTS demands to zensus grid

    elec_cts_demands_zensus = PythonOperator(
        task_id="electrical-cts-demands-zensus",
        python_callable=process_dr.distribute_cts_demands,
    )

    processed_dr_tables >> elec_cts_demands_zensus
    heat_demand_import >> elec_cts_demands_zensus
    demandregio_demand_cts_ind >> elec_cts_demands_zensus
    map_zensus_vg250 >> elec_cts_demands_zensus

    # Extract landuse areas from osm data set
    create_landuse_table = PythonOperator(
        task_id="create-landuse-table",
        python_callable=loadarea.create_landuse_table
    )

    landuse_extraction = PostgresOperator(
        task_id="extract-osm_landuse",
        sql=resources.read_text(loadarea, "osm_landuse_extraction.sql"),
        postgres_conn_id="egon_data",
        autocommit=True,
    )
    setup >> create_landuse_table
    create_landuse_table >> landuse_extraction
    osm_add_metadata >> landuse_extraction
    vg250_clean_and_prepare >> landuse_extraction

