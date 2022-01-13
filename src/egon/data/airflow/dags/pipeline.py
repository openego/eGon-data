import os

from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import airflow


from egon.data.config import set_numexpr_threads
from egon.data.datasets import database
from egon.data.datasets.calculate_dlr import Calculate_dlr
from egon.data.datasets.ch4_storages import CH4Storages
from egon.data.datasets.chp import Chp
from egon.data.datasets.chp_etrago import ChpEtrago
from egon.data.datasets.data_bundle import DataBundle
from egon.data.datasets.demandregio import DemandRegio
from egon.data.datasets.district_heating_areas import DistrictHeatingAreas
from egon.data.datasets.DSM_cts_ind import dsm_Potential
from egon.data.datasets.electrical_neighbours import ElectricalNeighbours
from egon.data.datasets.electricity_demand import (
    CtsElectricityDemand,
    HouseholdElectricityDemand,
)
from egon.data.datasets.electricity_demand_etrago import ElectricalLoadEtrago
from egon.data.datasets.electricity_demand_timeseries import (
    hh_buildings,
    hh_profiles,
)
from egon.data.datasets.era5 import WeatherData
from egon.data.datasets.etrago_setup import EtragoSetup
from egon.data.datasets.fill_etrago_gen import Egon_etrago_gen
from egon.data.datasets.gas_areas import GasAreas
from egon.data.datasets.gas_grid import GasNodesandPipes
from egon.data.datasets.gas_prod import CH4Production
from egon.data.datasets.heat_demand import HeatDemandImport
from egon.data.datasets.heat_demand_europe import HeatDemandEurope
from egon.data.datasets.heat_demand_timeseries.HTS import HeatTimeSeries
from egon.data.datasets.heat_etrago import HeatEtrago
from egon.data.datasets.heat_etrago.hts_etrago import HtsEtragoTable
from egon.data.datasets.heat_supply import HeatSupply
from egon.data.datasets.hydrogen_etrago import (
    HydrogenBusEtrago,
    HydrogenMethaneLinkEtrago,
    HydrogenPowerLinkEtrago,
    HydrogenStoreEtrago,
)
from egon.data.datasets.industrial_gas_demand import IndustrialGasDemand
from egon.data.datasets.industrial_sites import MergeIndustrialSites
from egon.data.datasets.industry import IndustrialDemandCurves
from egon.data.datasets.loadarea import LoadArea
from egon.data.datasets.mastr import mastr_data_setup
from egon.data.datasets.mv_grid_districts import mv_grid_districts_setup
from egon.data.datasets.osm import OpenStreetMap
from egon.data.datasets.osm_buildings_streets import OsmBuildingsStreets
from egon.data.datasets.osmtgmod import Osmtgmod
from egon.data.datasets.power_etrago import OpenCycleGasTurbineEtrago
from egon.data.datasets.power_plants import PowerPlants
from egon.data.datasets.pypsaeursec import PypsaEurSec
from egon.data.datasets.re_potential_areas import re_potential_area_setup
from egon.data.datasets.renewable_feedin import RenewableFeedin
from egon.data.datasets.saltcavern import SaltcavernData
from egon.data.datasets.scenario_capacities import ScenarioCapacities
from egon.data.datasets.scenario_parameters import ScenarioParameters
from egon.data.datasets.society_prognosis import SocietyPrognosis
from egon.data.datasets.storages import PumpedHydro
from egon.data.datasets.storages_etrago import StorageEtrago
from egon.data.datasets.substation import SubstationExtraction
from egon.data.datasets.substation_voronoi import SubstationVoronoi
from egon.data.datasets.tyndp import Tyndp
from egon.data.datasets.vg250 import Vg250
from egon.data.datasets.vg250_mv_grid_districts import Vg250MvGridDistricts
from egon.data.datasets.zensus import ZensusMiscellaneous, ZensusPopulation
from egon.data.datasets.zensus_mv_grid_districts import ZensusMvGridDistricts
from egon.data.datasets.zensus_vg250 import ZensusVg250

# Set number of threads used by numpy and pandas
set_numexpr_threads()

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
    setup = tasks["database.setup"]

    osm = OpenStreetMap(dependencies=[setup])
    osm_download = tasks["osm.download"]

    data_bundle = DataBundle(dependencies=[setup])

    # VG250 (Verwaltungsgebiete 250) data import
    vg250 = Vg250(dependencies=[setup])

    # Scenario table
    scenario_parameters = ScenarioParameters(dependencies=[setup])

    tyndp_data = Tyndp(dependencies=[setup])

    # Zensus population import
    zensus_population = ZensusPopulation(dependencies=[setup, vg250])

    # Combine Zensus and VG250 data
    zensus_vg250 = ZensusVg250(dependencies=[vg250, zensus_population])

    # Download and import zensus data on households, buildings and apartments
    zensus_miscellaneous = ZensusMiscellaneous(
        dependencies=[zensus_population, zensus_vg250]
    )

    # DemandRegio data import
    demandregio = DemandRegio(
        dependencies=[setup, vg250, scenario_parameters, data_bundle]
    )

    # Society prognosis
    society_prognosis = SocietyPrognosis(
        dependencies=[demandregio, zensus_vg250, zensus_population]
    )

    # OSM buildings, streets, amenities
    osm_buildings_streets = OsmBuildingsStreets(
        dependencies=[osm, zensus_miscellaneous]
    )
    osm_buildings_streets_preprocessing = tasks[
        "osm_buildings_streets.preprocessing"
    ]

    # Distribute household electrical demands to zensus cells
    household_electricity_demand_annual = HouseholdElectricityDemand(
        dependencies=[
            demandregio,
            zensus_vg250,
            zensus_miscellaneous,
            society_prognosis,
        ]
    )

    # Saltcavern storages

    saltcavern_storage = SaltcavernData(dependencies=[data_bundle, vg250])

    # NEP data import
    scenario_capacities = ScenarioCapacities(
        dependencies=[setup, vg250, data_bundle, zensus_population]
    )

    # setting etrago input tables

    setup_etrago = EtragoSetup(dependencies=[setup])

    # Retrieve MaStR data
    mastr_data = mastr_data_setup(dependencies=[setup])

    substation_extraction = SubstationExtraction(
        dependencies=[osm, vg250]
    )

    # osmTGmod ehv/hv grid model generation
    osmtgmod = Osmtgmod(
        dependencies=[
            osm_download,
            substation_extraction,
            setup_etrago,
            scenario_parameters,
        ]
    )

    # create Voronoi polygons
    substation_voronoi = SubstationVoronoi(
        dependencies=[osmtgmod, vg250]
    )

    # MV grid districts
    mv_grid_districts = mv_grid_districts_setup(
        dependencies=[substation_voronoi]
    )

    # Import potential areas for wind onshore and ground-mounted PV
    re_potential_areas = re_potential_area_setup(dependencies=[setup])

    # Future heat demand calculation based on Peta5_0_1 data
    heat_demand_Germany = HeatDemandImport(
        dependencies=[vg250, scenario_parameters, zensus_vg250]
    )

    # Future national heat demands for foreign countries based on Hotmaps
    # download only, processing in PyPSA-Eur-Sec fork
    hd_abroad = HeatDemandEurope(dependencies=[setup])


    # Extract landuse areas from osm data set
    load_area = LoadArea(dependencies=[osm, vg250])

    # Import weather data
    weather_data = WeatherData(
        dependencies=[setup, scenario_parameters, vg250]
    )

    renewable_feedin = RenewableFeedin(dependencies=[weather_data, vg250])


    # District heating areas demarcation
    district_heating_areas = DistrictHeatingAreas(
        dependencies=[
            heat_demand_Germany,
            scenario_parameters,
            zensus_miscellaneous,
        ]
    )

    # Calculate dynamic line rating for HV trans lines
    dlr = Calculate_dlr(
        dependencies=[
            osmtgmod,
            data_bundle,
            weather_data,
        ]
    )

    # Map zensus grid districts
    zensus_mv_grid_districts = ZensusMvGridDistricts(
        dependencies=[zensus_population, mv_grid_districts]
    )


    # Map federal states to mv_grid_districts
    vg250_mv_grid_districts = Vg250MvGridDistricts(
        dependencies=[vg250, mv_grid_districts]
    )

    # Distribute electrical CTS demands to zensus grid
    cts_electricity_demand = CtsElectricityDemand(
        dependencies=[
            demandregio,
            zensus_vg250,
            zensus_mv_grid_districts,
            heat_demand_Germany,
            setup_etrago,
            household_electricity_demand_annual,
        ]
    )

    # Household electricity demands

    mv_hh_electricity_load_2035 = PythonOperator(
        task_id="MV-hh-electricity-load-2035",
        python_callable=hh_profiles.mv_grid_district_HH_electricity_load,
        op_args=["eGon2035", 2035],
        op_kwargs={"drop_table": True},
    )

    mv_hh_electricity_load_2050 = PythonOperator(
        task_id="MV-hh-electricity-load-2050",
        python_callable=hh_profiles.mv_grid_district_HH_electricity_load,
        op_args=["eGon100RE", 2050],
    )

    hh_demand_profiles_setup = hh_profiles.setup(
        dependencies=[
            vg250,
            zensus_miscellaneous,
            zensus_mv_grid_districts,
            zensus_vg250,
            demandregio,
            osm_buildings_streets_preprocessing,
        ],
        tasks=(
            hh_profiles.houseprofiles_in_census_cells,
            mv_hh_electricity_load_2035,
            mv_hh_electricity_load_2050,
        ),
    )
    householdprofiles_in_cencus_cells = tasks[
        "electricity_demand_timeseries.hh_profiles.houseprofiles-in-census-cells"
    ]
    mv_hh_electricity_load_2035 = tasks["MV-hh-electricity-load-2035"]
    mv_hh_electricity_load_2050 = tasks["MV-hh-electricity-load-2050"]

    # Household electricity demand buildings
    hh_demand_buildings_setup = hh_buildings.setup(
        dependencies=[householdprofiles_in_cencus_cells],
    )



    # Industry

    industrial_sites = MergeIndustrialSites(
        dependencies=[setup, vg250, data_bundle]
    )

    demand_curves_industry = IndustrialDemandCurves(
        dependencies=[
            mv_grid_districts,
            industrial_sites,
            demandregio,
            osm,
            load_area,
        ]
    )

    # Electrical loads to eTraGo

    electrical_load_etrago = ElectricalLoadEtrago(
        dependencies=[
            demand_curves_industry,
            cts_electricity_demand,
            hh_demand_buildings_setup,
        ]
    )

    # run pypsa-eur-sec
    run_pypsaeursec = PypsaEurSec(
        dependencies=[
            weather_data,
            hd_abroad,
            osmtgmod,
            setup_etrago,
            data_bundle,
        ]
    )

    foreign_lines = ElectricalNeighbours(
        dependencies=[run_pypsaeursec, tyndp_data]
    )

    # Gas grid import
    gas_grid_insert_data = GasNodesandPipes(
        dependencies=[
            setup_etrago,
            data_bundle,
            osmtgmod,
            foreign_lines,
            scenario_parameters,
        ]
    )

    # Insert hydrogen buses
    insert_hydrogen_buses = HydrogenBusEtrago(
        dependencies=[
            saltcavern_storage,
            gas_grid_insert_data,
            substation_voronoi,
        ]
    )

    # H2 steel tanks and saltcavern storage
    insert_H2_storage = HydrogenStoreEtrago(
        dependencies=[insert_hydrogen_buses]
    )

    # Power-to-gas-to-power chain installations
    insert_power_to_h2_installations = HydrogenPowerLinkEtrago(
        dependencies=[
            insert_hydrogen_buses,
        ]
    )

    # Link between methane grid and respective hydrogen buses
    insert_h2_to_ch4_grid_links = HydrogenMethaneLinkEtrago(
        dependencies=[
            insert_hydrogen_buses,
        ]
    )

    # Create gas voronoi
    create_gas_polygons = GasAreas(
        dependencies=[insert_hydrogen_buses, vg250]
    )

    # Gas prod import
    gas_production_insert_data = CH4Production(
        dependencies=[create_gas_polygons]
    )

    # CH4 storages import
    insert_data_ch4_storages = CH4Storages(dependencies=[create_gas_polygons])

    # Insert industrial gas demand
    industrial_gas_demand = IndustrialGasDemand(
        dependencies=[create_gas_polygons]
    )

    # CHP locations
    chp = Chp(
        dependencies=[
            mv_grid_districts,
            mastr_data,
            industrial_sites,
            create_gas_polygons,
            district_heating_areas,
        ]
    )


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
            setup_etrago,
            zensus_mv_grid_districts,
            cts_electricity_demand,
            household_electricity_demand_annual,
        ]
    )

    create_ocgt = OpenCycleGasTurbineEtrago(
        dependencies=[create_gas_polygons, power_plants]
    )


    # Fill eTraGo Generators tables
    fill_etrago_generators = Egon_etrago_gen(
        dependencies=[power_plants, weather_data]
    )

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
        dependencies=[
            heat_supply,
            mv_grid_districts,
            setup_etrago,
            renewable_feedin,
        ]
    )



    # CHP to eTraGo
    chp_etrago = ChpEtrago(dependencies=[chp, heat_etrago])

    # DSM
    components_dsm = dsm_Potential(
        dependencies=[
            cts_electricity_demand,
            demand_curves_industry,
            osmtgmod,
        ]
    )

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
            district_heating_areas,
            vg250,
            zensus_mv_grid_districts,
            hh_demand_buildings_setup,
        ]
    )

    # HTS to etrago table
    hts_etrago_table = HtsEtragoTable(
        dependencies=[
            heat_time_series,
            mv_grid_districts,
            district_heating_areas,
            heat_etrago,
        ]
    )

    # Storages to eTrago

    storage_etrago = StorageEtrago(
        dependencies=[
            pumped_hydro,
            setup_etrago,
            scenario_parameters,
        ]
    )
