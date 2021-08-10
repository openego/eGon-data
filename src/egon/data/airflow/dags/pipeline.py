import os

from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import importlib_resources as resources

from egon.data.datasets import database
from egon.data.datasets.data_bundle import DataBundle
from egon.data.datasets.demandregio import DemandRegio
from egon.data.datasets.electricity_demand import (
    CtsElectricityDemand,
    HouseholdElectricityDemand,
)
from egon.data.datasets.electricity_demand_etrago import ElectricalLoadEtrago
from egon.data.datasets.era5 import WeatherData
from egon.data.datasets.heat_etrago import HeatEtrago
from egon.data.datasets.heat_supply import HeatSupply
from egon.data.datasets.industry import IndustrialDemandCurves
from egon.data.datasets.industrial_sites import MergeIndustrialSites
from egon.data.datasets.renewable_feedin import RenewableFeedin
from egon.data.datasets.osm import OpenStreetMap
from egon.data.datasets.mastr import mastr_data_setup
from egon.data.datasets.re_potential_areas import re_potential_area_setup
from egon.data.datasets.society_prognosis import SocietyPrognosis
from egon.data.datasets.mv_grid_districts import mv_grid_districts_setup
from egon.data.datasets.power_plants import PowerPlants
from egon.data.datasets.scenario_capacities import ScenarioCapacities
from egon.data.datasets.scenario_parameters import ScenarioParameters
from egon.data.datasets.vg250 import Vg250
from egon.data.datasets.vg250_mv_grid_districts import Vg250MvGridDistricts
from egon.data.datasets.zensus_mv_grid_districts import ZensusMvGridDistricts
from egon.data.datasets.zensus_vg250 import ZensusVg250
import airflow
import egon.data.importing.etrago as etrago
import egon.data.importing.heat_demand_data as import_hd
import egon.data.importing.zensus as import_zs
import egon.data.importing.gas_grid as gas_grid
import egon.data.processing.district_heating_areas as district_heating_areas
import egon.data.processing.osmtgmod as osmtgmod
import egon.data.processing.power2gas as power2gas
import egon.data.processing.substation as substation
import egon.data.processing.gas_areas as gas_areas
import egon.data.processing.loadarea as loadarea
import egon.data.processing.calculate_dlr as dlr


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

    # Scenario table
    scenario_parameters = ScenarioParameters(dependencies=[setup])
    scenario_input_import = tasks["scenario_parameters.insert-scenarios"]

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
    zensus_vg250 = ZensusVg250(dependencies=[vg250, population_import])

    zensus_inside_ger_metadata = tasks[
        "zensus_vg250.add-metadata-zensus-inside-ger"
    ]

    # DemandRegio data import
    demandregio = DemandRegio(
        dependencies=[setup, vg250, scenario_input_import, data_bundle]
    )
    demandregio_demand_cts_ind = tasks["demandregio.insert-cts-ind-demands"]

    # Society prognosis
    society_prognosis = SocietyPrognosis(
        dependencies=[
            demandregio,
            zensus_vg250,
            population_import,
            zensus_misc_import,
        ]
    )

    population_prognosis = tasks["society_prognosis.zensus-population"]

    # Distribute household electrical demands to zensus cells
    household_electricity_demand_annual = HouseholdElectricityDemand(
        dependencies=[
            demandregio,
            zensus_vg250,
            zensus_tables,
            society_prognosis,
        ]
    )

    elec_household_demands_zensus = tasks[
        "electricity_demand.distribute-household-demands"
    ]

    # NEP data import
    scenario_capacities = ScenarioCapacities(
        dependencies=[setup, vg250, data_bundle]
    )
    nep_insert_data = tasks["scenario_capacities.insert-data-nep"]

    population_import >> nep_insert_data

    # setting etrago input tables
    etrago_input_data = PythonOperator(
        task_id="setting-etrago-input-tables", python_callable=etrago.setup
    )
    setup >> etrago_input_data

    # Retrieve MaStR data
    mastr_data = mastr_data_setup(dependencies=[setup])
    mastr_data.insert_into(pipeline)
    retrieve_mastr_data = tasks["mastr.download-mastr-data"]

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
        task_id="osmtgmod_osm_import", python_callable=osmtgmod.import_osm_data
    )

    run_osmtgmod = PythonOperator(
        task_id="run_osmtgmod", python_callable=osmtgmod.run_osmtgmod
    )

    osmtgmod_pypsa = PythonOperator(
        task_id="osmtgmod_pypsa", python_callable=osmtgmod.osmtgmmod_to_pypsa
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

    # create Voronoi for MV grid districts
    create_voronoi_substation = PythonOperator(
        task_id="create-voronoi-substations",
        python_callable=substation.create_voronoi,
    )
    osmtgmod_substation >> create_voronoi_substation

    # MV grid districts
    mv_grid_districts = mv_grid_districts_setup(
        dependencies=[create_voronoi_substation]
    )
    mv_grid_districts.insert_into(pipeline)
    define_mv_grid_districts = tasks[
        "mv_grid_districts.define-mv-grid-districts"
    ]

    # Import potential areas for wind onshore and ground-mounted PV
    re_potential_areas = re_potential_area_setup(dependencies=[setup])
    re_potential_areas.insert_into(pipeline)
    insert_re_potential_areas = tasks["re_potential_areas.insert-data"]

    # Future heat demand calculation based on Peta5_0_1 data
    heat_demand_import = PythonOperator(
        task_id="import-heat-demand",
        python_callable=import_hd.future_heat_demand_data_import,
    )
    vg250_clean_and_prepare >> heat_demand_import
    zensus_inside_ger_metadata >> heat_demand_import
    scenario_input_import >> heat_demand_import

    # Distribute electrical CTS demands to zensus grid
    cts_electricity_demand_annual = CtsElectricityDemand(
        dependencies=[
            demandregio,
            zensus_vg250,
            heat_demand_import,
            etrago_input_data,
            household_electricity_demand_annual,
        ]
    )

    elec_cts_demands_zensus = tasks[
        "electricity_demand.distribute-cts-demands"
    ]

    # Gas grid import
    gas_grid_insert_data = PythonOperator(
        task_id="insert-gas-grid", python_callable=gas_grid.insert_gas_data
    )

    etrago_input_data >> gas_grid_insert_data
    download_data_bundle >> gas_grid_insert_data
    osmtgmod_pypsa >> gas_grid_insert_data

    # Power-to-gas installations creation
    insert_power2gas_installations = PythonOperator(
        task_id="insert-power-to-gas-installations",
        python_callable=power2gas.insert_power2gas,
    )

    gas_grid_insert_data >> insert_power2gas_installations
    osmtgmod_pypsa >> insert_power2gas_installations

    # Create gas voronoi
    create_gas_polygons = PythonOperator(
        task_id="create-gas-voronoi", python_callable=gas_areas.create_voronoi
    )

    gas_grid_insert_data >> create_gas_polygons
    vg250_clean_and_prepare >> create_gas_polygons

    # Extract landuse areas from osm data set
    create_landuse_table = PythonOperator(
        task_id="create-landuse-table",
        python_callable=loadarea.create_landuse_table,
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
    weather_data = WeatherData(dependencies=[setup, scenario_parameters])
    download_weather_data = tasks["era5.download-era5"]

    renewable_feedin = RenewableFeedin(dependencies=[weather_data, vg250])

    feedin_wind_onshore = tasks["renewable_feedin.wind"]
    feedin_pv = tasks["renewable_feedin.pv"]
    feedin_solar_thermal = tasks["renewable_feedin.solar-thermal"]

    # District heating areas demarcation
    create_district_heating_areas_table = PythonOperator(
        task_id="create-district-heating-areas-table",
        python_callable=district_heating_areas.create_tables,
    )
    import_district_heating_areas = PythonOperator(
        task_id="import-district-heating-areas",
        python_callable=district_heating_areas.district_heating_areas_demarcation,
    )
    setup >> create_district_heating_areas_table
    create_district_heating_areas_table >> import_district_heating_areas
    zensus_misc_import >> import_district_heating_areas
    heat_demand_import >> import_district_heating_areas
    scenario_input_import >> import_district_heating_areas

    # Calculate dynamic line rating for HV trans lines
    calculate_dlr = PythonOperator(
        task_id="calculate_dlr", python_callable=dlr.Calculate_DLR
    )
    osmtgmod_pypsa >> calculate_dlr
    download_data_bundle >> calculate_dlr
    download_weather_data >> calculate_dlr

    # Map zensus grid districts
    zensus_mv_grid_districts = ZensusMvGridDistricts(
        dependencies=[population_import, mv_grid_districts]
    )

    map_zensus_grid_districts = tasks["zensus_mv_grid_districts.mapping"]

    # Map federal states to mv_grid_districts
    vg250_mv_grid_districts = Vg250MvGridDistricts(
        dependencies=[vg250, mv_grid_districts]
    )

    # Power plants
    power_plants = PowerPlants(
        dependencies=[
            setup,
            renewable_feedin,
            mv_grid_districts,
            mastr_data,
            re_potential_areas,
            scenario_parameters,
            scenario_capacities,
            Vg250MvGridDistricts,
        ]
    )

    power_plant_import = tasks["power_plants.insert-hydro-biomass"]
    generate_wind_farms = tasks["power_plants.wind_farms.insert"]
    generate_pv_ground_mounted = tasks["power_plants.pv_ground_mounted.insert"]
    solar_rooftop_etrago = tasks[
        "power_plants.pv_rooftop.pv-rooftop-per-mv-grid"
    ]

    hvmv_substation_extraction >> generate_wind_farms
    hvmv_substation_extraction >> generate_pv_ground_mounted
    feedin_pv >> solar_rooftop_etrago
    elec_cts_demands_zensus >> solar_rooftop_etrago
    elec_household_demands_zensus >> solar_rooftop_etrago
    etrago_input_data >> solar_rooftop_etrago
    map_zensus_grid_districts >> solar_rooftop_etrago

    # Heat supply
    heat_supply = HeatSupply(
        dependencies=[data_bundle, zensus_mv_grid_districts]
    )

    import_district_heating_supply = tasks["heat_supply.district-heating"]
    import_individual_heating_supply = tasks["heat_supply.individual-heating"]
    heat_supply_tables = tasks["heat_supply.create-tables"]
    geothermal_potential = tasks["heat_supply.geothermal.potential-germany"]

    create_district_heating_areas_table >> heat_supply_tables
    import_district_heating_areas >> import_district_heating_supply
    import_district_heating_areas >> geothermal_potential
    import_district_heating_areas >> import_individual_heating_supply
    power_plant_import >> import_individual_heating_supply

    # Heat to eTraGo
    heat_etrago = HeatEtrago(dependencies=[heat_supply, mv_grid_districts])

    heat_etrago_buses = tasks["heat_etrago.buses"]
    heat_etrago_supply = tasks["heat_etrago.supply"]

    etrago_input_data >> heat_etrago_buses

    # Industry

    industrial_sites = MergeIndustrialSites(
        dependencies=[setup, vg250_clean_and_prepare]
    )

    demand_curves_industry = IndustrialDemandCurves(
        dependencies=[
            define_mv_grid_districts,
            industrial_sites,
            demandregio_demand_cts_ind,
            osm,
            landuse_extraction,
        ]
    )

    # Electrical loads to eTraGo

    electrical_load_etrago = ElectricalLoadEtrago(
        dependencies=[demand_curves_industry, elec_cts_demands_zensus]
    )
