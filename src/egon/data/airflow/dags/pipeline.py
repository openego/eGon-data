import os

from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import airflow
import importlib_resources as resources

from egon.data import db
from egon.data.datasets import database
from egon.data.datasets.calculate_dlr import Calculate_dlr
from egon.data.datasets.ch4_storages import CH4Storages
from egon.data.datasets.chp import Chp
from egon.data.datasets.chp_etrago import ChpEtrago
from egon.data.datasets.data_bundle import DataBundle
from egon.data.datasets.demandregio import DemandRegio
from egon.data.datasets.district_heating_areas import DistrictHeatingAreas
from egon.data.datasets.DSM_cts_ind import dsm_Potential
from egon.data.datasets.electricity_demand import (
    CtsElectricityDemand,
    HouseholdElectricityDemand,
)
from egon.data.datasets.electricity_demand_etrago import ElectricalLoadEtrago
from egon.data.datasets.era5 import WeatherData
from egon.data.datasets.etrago_setup import EtragoSetup
from egon.data.datasets.gas_grid import GasNodesandPipes
from egon.data.datasets.gas_prod import CH4Production
from egon.data.datasets.heat_demand import HeatDemandImport
from egon.data.datasets.heat_demand_timeseries.HTS import HeatTimeSeries
from egon.data.datasets.heat_etrago import HeatEtrago
from egon.data.datasets.heat_etrago.hts_etrago import HtsEtragoTable
from egon.data.datasets.heat_supply import HeatSupply
from egon.data.datasets.hh_demand_profiles import (
    hh_demand_setup,
    houseprofiles_in_census_cells,
    mv_grid_district_HH_electricity_load,
)
from egon.data.datasets.industrial_gas_demand import IndustrialGasDemand
from egon.data.datasets.industrial_sites import MergeIndustrialSites
from egon.data.datasets.industry import IndustrialDemandCurves
from egon.data.datasets.mastr import mastr_data_setup
from egon.data.datasets.mv_grid_districts import mv_grid_districts_setup
from egon.data.datasets.osm import OpenStreetMap
from egon.data.datasets.osmtgmod import Osmtgmod
from egon.data.datasets.power_plants import PowerPlants
from egon.data.datasets.re_potential_areas import re_potential_area_setup
from egon.data.datasets.renewable_feedin import RenewableFeedin
from egon.data.datasets.scenario_capacities import ScenarioCapacities
from egon.data.datasets.scenario_parameters import ScenarioParameters
from egon.data.datasets.society_prognosis import SocietyPrognosis
from egon.data.datasets.storages import PumpedHydro
from egon.data.datasets.substation import SubstationExtraction
from egon.data.datasets.substation_voronoi import SubstationVoronoi
from egon.data.datasets.vg250 import Vg250
from egon.data.datasets.vg250_mv_grid_districts import Vg250MvGridDistricts
from egon.data.datasets.zensus_mv_grid_districts import ZensusMvGridDistricts
from egon.data.datasets.zensus_vg250 import ZensusVg250
from egon.data.processing.gas_areas import GasAreas
from egon.data.processing.power_to_h2 import PowertoH2
import egon.data.datasets.gas_grid as gas_grid
import egon.data.importing.zensus as import_zs
import egon.data.processing.gas_areas as gas_areas
import egon.data.processing.loadarea as loadarea
import egon.data.processing.power_to_h2 as power_to_h2


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
    zensus_inside_ger = tasks["zensus_vg250.inside-germany"]

    zensus_inside_ger >> zensus_misc_import

    # DemandRegio data import
    demandregio = DemandRegio(
        dependencies=[setup, vg250, scenario_parameters, data_bundle]
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

    setup_etrago = EtragoSetup(dependencies=[setup])
    etrago_input_data = tasks["etrago_setup.create-tables"]

    # Retrieve MaStR data
    mastr_data = mastr_data_setup(dependencies=[setup])
    mastr_data.insert_into(pipeline)
    retrieve_mastr_data = tasks["mastr.download-mastr-data"]

    substation_extraction = SubstationExtraction(
        dependencies=[osm_add_metadata, vg250_clean_and_prepare])

    # osmTGmod ehv/hv grid model generation
    osmtgmod = Osmtgmod(
        dependencies=[
            osm_download,
            substation_extraction,
            setup_etrago,
        ]
    )
    osmtgmod.insert_into(pipeline)
    osmtgmod_pypsa = tasks["osmtgmod.to-pypsa"]
    osmtgmod_substation = tasks["osmtgmod_substation"]

    # create Voronoi polygons
    substation_voronoi = SubstationVoronoi(
        dependencies=[
            osmtgmod_substation,
            vg250,
            ])

    # MV grid districts
    mv_grid_districts = mv_grid_districts_setup(
        dependencies=[substation_voronoi]
    )
    mv_grid_districts.insert_into(pipeline)
    define_mv_grid_districts = tasks[
        "mv_grid_districts.define-mv-grid-districts"
    ]

    # Import potential areas for wind onshore and ground-mounted PV
    re_potential_areas = re_potential_area_setup(dependencies=[setup])
    re_potential_areas.insert_into(pipeline)

    # Future heat demand calculation based on Peta5_0_1 data
    heat_demand_Germany = HeatDemandImport(
        dependencies=[vg250, scenario_parameters, zensus_vg250]
    )

    # Gas grid import
    gas_grid_insert_data = GasNodesandPipes(
        dependencies=[etrago_input_data, download_data_bundle, osmtgmod_pypsa]
    )

    # Power-to-gas installations creation
    insert_power_to_h2_installations = PowertoH2(
        dependencies=[gas_grid_insert_data]
    )

    # Create gas voronoi
    create_gas_polygons = GasAreas(
        dependencies=[gas_grid_insert_data, vg250_clean_and_prepare]
    )

    # Gas prod import
    gas_production_insert_data = CH4Production(
        dependencies=[create_gas_polygons]
    )

    # CH4 storages import
    insert_data_ch4_storages = CH4Storages(
        dependencies=[create_gas_polygons])

    # Insert industrial gas demand
    industrial_gas_demand = IndustrialGasDemand(
        dependencies=[create_gas_polygons]
    )

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
    weather_data = WeatherData(
        dependencies=[setup, scenario_parameters, vg250]
    )
    download_weather_data = tasks["era5.download-era5"]

    renewable_feedin = RenewableFeedin(dependencies=[weather_data, vg250])

    feedin_wind_onshore = tasks["renewable_feedin.wind"]
    feedin_pv = tasks["renewable_feedin.pv"]
    feedin_solar_thermal = tasks["renewable_feedin.solar-thermal"]

    # District heating areas demarcation
    district_heating_areas = DistrictHeatingAreas(
        dependencies=[heat_demand_Germany, scenario_parameters]
    )
    import_district_heating_areas = tasks["district_heating_areas.demarcation"]

    zensus_misc_import >> import_district_heating_areas

    # Calculate dynamic line rating for HV trans lines
    dlr = Calculate_dlr(
        dependencies=[osmtgmod_pypsa,
                      download_data_bundle,
                      download_weather_data,
            ]
    )

    # Map zensus grid districts
    zensus_mv_grid_districts = ZensusMvGridDistricts(
        dependencies=[population_import, mv_grid_districts]
    )

    map_zensus_grid_districts = tasks["zensus_mv_grid_districts.mapping"]

    # Map federal states to mv_grid_districts
    vg250_mv_grid_districts = Vg250MvGridDistricts(
        dependencies=[vg250, mv_grid_districts]
    )

    # Distribute electrical CTS demands to zensus grid
    cts_electricity_demand_annual = CtsElectricityDemand(
        dependencies=[
            demandregio,
            zensus_vg250,
            zensus_mv_grid_districts,
            heat_demand_Germany,
            etrago_input_data,
            household_electricity_demand_annual,
        ]
    )

    elec_cts_demands_zensus = tasks[
        "electricity_demand.distribute-cts-demands"
    ]

    mv_hh_electricity_load_2035 = PythonOperator(
        task_id="MV-hh-electricity-load-2035",
        python_callable=mv_grid_district_HH_electricity_load,
        op_args=["eGon2035", 2035, "0.0.0"],
        op_kwargs={"drop_table": True},
    )

    mv_hh_electricity_load_2050 = PythonOperator(
        task_id="MV-hh-electricity-load-2050",
        python_callable=mv_grid_district_HH_electricity_load,
        op_args=["eGon100RE", 2050, "0.0.0"],
    )

    hh_demand = hh_demand_setup(
        dependencies=[
            vg250_clean_and_prepare,
            zensus_misc_import,
            map_zensus_grid_districts,
            zensus_inside_ger,
            demandregio,
        ],
        tasks=(
            houseprofiles_in_census_cells,
            mv_hh_electricity_load_2035,
            mv_hh_electricity_load_2050,
        ),
    )
    hh_demand.insert_into(pipeline)
    householdprofiles_in_cencus_cells = tasks[
        "hh_demand_profiles.houseprofiles-in-census-cells"
    ]
    mv_hh_electricity_load_2035 = tasks["MV-hh-electricity-load-2035"]
    mv_hh_electricity_load_2050 = tasks["MV-hh-electricity-load-2050"]

    # Industry

    industrial_sites = MergeIndustrialSites(
        dependencies=[setup, vg250_clean_and_prepare, data_bundle]
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
        dependencies=[demand_curves_industry, cts_electricity_demand_annual]
    )

    # CHP locations
    chp = Chp(dependencies=[mv_grid_districts, mastr_data, industrial_sites, create_gas_polygons])

    chp_locations_nep = tasks["chp.insert-chp-egon2035"]
    chp_heat_bus = tasks["chp.assign-heat-bus"]

    nep_insert_data >> chp_locations_nep
    import_district_heating_areas >> chp_locations_nep

    # Power plants
    power_plants = PowerPlants(
        dependencies=[
            setup,
            renewable_feedin,
            substation_extraction,
            mv_grid_districts,
            mastr_data,
            re_potential_areas,
            scenario_parameters,
            scenario_capacities,
            Vg250MvGridDistricts,
            chp,
        ]
    )

    power_plant_import = tasks["power_plants.insert-hydro-biomass"]
    generate_wind_farms = tasks["power_plants.wind_farms.insert"]
    generate_pv_ground_mounted = tasks["power_plants.pv_ground_mounted.insert"]
    solar_rooftop_etrago = tasks[
        "power_plants.pv_rooftop.pv-rooftop-per-mv-grid"
    ]

    feedin_pv >> solar_rooftop_etrago
    elec_cts_demands_zensus >> solar_rooftop_etrago
    elec_household_demands_zensus >> solar_rooftop_etrago
    etrago_input_data >> solar_rooftop_etrago
    map_zensus_grid_districts >> solar_rooftop_etrago

    # Heat supply
    heat_supply = HeatSupply(
        dependencies=[
            data_bundle,
            zensus_mv_grid_districts,
            district_heating_areas,
            zensus_mv_grid_districts,
            chp,
        ]
    )

    # Heat to eTraGo
    heat_etrago = HeatEtrago(
        dependencies=[heat_supply, mv_grid_districts, setup_etrago]
    )

    heat_etrago_buses = tasks["heat_etrago.buses"]
    heat_etrago_supply = tasks["heat_etrago.supply"]

    # CHP to eTraGo
    chp_etrago = ChpEtrago(dependencies=[chp, heat_etrago])

    # DSM
    components_dsm =  dsm_Potential(
        dependencies = [cts_electricity_demand_annual,
                        demand_curves_industry,
                        osmtgmod_pypsa])

    # Pumped hydro units

    pumped_hydro = PumpedHydro(
        dependencies=[
            setup,
            mv_grid_districts,
            mastr_data,
            scenario_parameters,
            scenario_capacities,
            Vg250MvGridDistricts,
            power_plants,
        ]
    )

    # Heat time Series
    heat_time_series = HeatTimeSeries(
        dependencies=[
            data_bundle,
            demandregio,
            heat_demand_Germany,
            import_district_heating_areas,
            import_district_heating_areas,
            vg250,
            map_zensus_grid_districts,
        ]
    )

    # HTS to etrago table
    hts_etrago_table = HtsEtragoTable(
                        dependencies = [heat_time_series,mv_grid_districts,
                                        district_heating_areas,heat_etrago])
