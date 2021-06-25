import os

from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import importlib_resources as resources

from egon.data.datasets import database
from egon.data.datasets.data_bundle import DataBundle
from egon.data.datasets.osm import OpenStreetMap
from egon.data.datasets.vg250 import Vg250
from egon.data.processing.zensus_vg250 import (
    zensus_population_inside_germany as zensus_vg250,
)
import airflow
import egon.data.importing.demandregio as import_dr
import egon.data.importing.demandregio.install_disaggregator as install_dr
import egon.data.importing.era5 as import_era5
import egon.data.importing.etrago as etrago
import egon.data.importing.heat_demand_data as import_hd
import egon.data.importing.industrial_sites as industrial_sites
import egon.data.importing.mastr as mastr
import egon.data.importing.nep_input_data as nep_input
import egon.data.importing.re_potential_areas as re_potential_areas
import egon.data.importing.scenarios as import_scenarios
import egon.data.importing.zensus as import_zs
import egon.data.processing.boundaries_grid_districts as boundaries_grid_districts
import egon.data.processing.demandregio as process_dr
import egon.data.processing.district_heating_areas as district_heating_areas
import egon.data.processing.loadarea as loadarea
import egon.data.processing.osmtgmod as osmtgmod
import egon.data.processing.power_plants as power_plants
import egon.data.processing.renewable_feedin as import_feedin
import egon.data.processing.substation as substation
import egon.data.processing.zensus_vg250.zensus_population_inside_germany as zensus_vg250
import egon.data.importing.gas_grid as gas_grid
import egon.data.processing.mv_grid_districts as mvgd
import egon.data.processing.zensus as process_zs
import egon.data.processing.zensus_grid_districts as zensus_grid_districts

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

    tasks = pipeline.task_dict

    database_setup = database.Setup()
    database_setup.insert_into(pipeline)
    setup = tasks["database.setup"]

    osm = OpenStreetMap(dependencies=[setup])
    osm.insert_into(pipeline)
    osm_add_metadata = tasks["osm.add-metadata"]
    osm_download = tasks["osm.download"]

    data_bundle = DataBundle(dependencies=[setup])
    data_bundle.insert_into(pipeline)
    download_data_bundle = tasks["data_bundle.download"]

    # VG250 (Verwaltungsgebiete 250) data import
    vg250 = Vg250(dependencies=[setup])
    vg250.insert_into(pipeline)
    vg250_clean_and_prepare = tasks["vg250.cleaning-and-preperation"]

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
    download_data_bundle >> demandregio_demand_cts_ind

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
    download_data_bundle >> nep_insert_data

    # setting etrago input tables
    etrago_input_data = PythonOperator(
        task_id="setting-etrago-input-tables",
        python_callable=etrago.setup,
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
    osmtgmod_osm_import = PythonOperator(
        task_id="osmtgmod_osm_import",
        python_callable=osmtgmod.import_osm_data,
    )

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

    osm_download >> osmtgmod_osm_import >> run_osmtgmod
    ehv_substation_extraction >> run_osmtgmod
    hvmv_substation_extraction >> run_osmtgmod
    run_osmtgmod >> osmtgmod_pypsa
    etrago_input_data >> osmtgmod_pypsa
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
        task_id="import-hydro-biomass-power-plants",
        python_callable=power_plants.insert_power_plants,
    )

    setup >> power_plant_tables >> power_plant_import
    nep_insert_data >> power_plant_import
    retrieve_mastr_data >> power_plant_import
    define_mv_grid_districts >> power_plant_import

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


    # Gas grid import
    gas_grid_insert_data = PythonOperator(
        task_id="insert-gas-grid",
        python_callable=gas_grid.insert_gas_data,
    )

    etrago_input_data >> gas_grid_insert_data
    download_data_bundle >> gas_grid_insert_data

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

 # Import weather data
    download_era5 = PythonOperator(
        task_id="download-weather-data",
        python_callable=import_era5.download_era5,
    )
    scenario_input_import >> download_era5

    create_weather_tables = PythonOperator(
        task_id="create-weather-tables",
        python_callable=import_era5.create_tables,
    )
    setup >> create_weather_tables

    import_weather_cells = PythonOperator(
        task_id="insert-weather-cells",
        python_callable=import_era5.insert_weather_cells,
    )
    create_weather_tables >> import_weather_cells
    download_era5 >> import_weather_cells

    feedin_wind_onshore = PythonOperator(
        task_id="insert-feedin-wind",
        python_callable=import_feedin.wind_feedin_per_weather_cell,
    )

    feedin_pv = PythonOperator(
        task_id="insert-feedin-pv",
        python_callable=import_feedin.pv_feedin_per_weather_cell,
    )

    feedin_solar_thermal = PythonOperator(
        task_id="insert-feedin-solar-thermal",
        python_callable=import_feedin.solar_thermal_feedin_per_weather_cell,
    )

    import_weather_cells >> [feedin_wind_onshore,
                             feedin_pv, feedin_solar_thermal]
    vg250_clean_and_prepare >> [feedin_wind_onshore,
                             feedin_pv, feedin_solar_thermal]

    # District heating areas demarcation
    create_district_heating_areas_table = PythonOperator(
        task_id="create-district-heating-areas-table",
        python_callable=district_heating_areas.create_tables
    )
    import_district_heating_areas = PythonOperator(
        task_id="import-district-heating-areas",
        python_callable=district_heating_areas.
        district_heating_areas_demarcation
    )
    setup >> create_district_heating_areas_table
    create_district_heating_areas_table >> import_district_heating_areas
    zensus_misc_import >> import_district_heating_areas
    heat_demand_import >> import_district_heating_areas
    scenario_input_import >> import_district_heating_areas

    # Electrical load curves CTS
    map_zensus_grid_districts = PythonOperator(
        task_id="map_zensus_grid_districts",
        python_callable=zensus_grid_districts.map_zensus_mv_grid_districts,
    )
    population_import >> map_zensus_grid_districts
    define_mv_grid_districts >> map_zensus_grid_districts

    electrical_load_curves_cts = PythonOperator(
        task_id="electrical-load-curves-cts",
        python_callable=process_dr.insert_cts_load,
    )
    map_zensus_grid_districts >> electrical_load_curves_cts
    elec_cts_demands_zensus >> electrical_load_curves_cts
    demandregio_demand_cts_ind >> electrical_load_curves_cts
    map_zensus_vg250 >> electrical_load_curves_cts
    etrago_input_data >> electrical_load_curves_cts

    # Map federal states to mv_grid_districts
    map_boundaries_grid_districts = PythonOperator(
        task_id="map_vg250_grid_districts",
        python_callable=boundaries_grid_districts.map_mvgriddistricts_vg250,
    )
    define_mv_grid_districts >> map_boundaries_grid_districts
    vg250_clean_and_prepare >> map_boundaries_grid_districts

    # Solar rooftop per mv grid district
    solar_rooftop_etrago = PythonOperator(
        task_id="etrago_solar_rooftop",
        python_callable=power_plants.pv_rooftop_per_mv_grid,
    )
    map_boundaries_grid_districts >> solar_rooftop_etrago
    feedin_pv >> solar_rooftop_etrago
    elec_cts_demands_zensus >> solar_rooftop_etrago
    elec_household_demands_zensus >> solar_rooftop_etrago
    nep_insert_data >> solar_rooftop_etrago
    etrago_input_data >> solar_rooftop_etrago
    map_zensus_grid_districts >> solar_rooftop_etrago
