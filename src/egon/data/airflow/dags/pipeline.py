import os

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
from egon.data.datasets.gas_aggregation import GasAggregation
from egon.data.datasets.gas_areas import GasAreaseGon100RE, GasAreaseGon2035
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
    HydrogenGridEtrago,
    HydrogenMethaneLinkEtrago,
    HydrogenPowerLinkEtrago,
    HydrogenStoreEtrago,
)
from egon.data.datasets.industrial_gas_demand import (
    IndustrialGasDemand,
    IndustrialGasDemandeGon100RE,
    IndustrialGasDemandeGon2035,
)
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

    # Download TYNDP data
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
    osm_buildings_streets_residential_zensus_mapping = tasks[
        "osm_buildings_streets.create-buildings-residential-zensus-mapping"
    ]

    # Import saltcavern storage potentials
    saltcavern_storage = SaltcavernData(dependencies=[data_bundle, vg250])

    # Import weather data
    weather_data = WeatherData(
        dependencies=[setup, scenario_parameters, vg250]
    )

    # Future national heat demands for foreign countries based on Hotmaps
    # download only, processing in PyPSA-Eur-Sec fork
    hd_abroad = HeatDemandEurope(dependencies=[setup])

    # setting etrago input tables

    setup_etrago = EtragoSetup(dependencies=[setup])
    etrago_input_data = tasks["etrago_setup.create-tables"]

    substation_extraction = SubstationExtraction(dependencies=[osm, vg250])

    # osmTGmod ehv/hv grid model generation
    osmtgmod = Osmtgmod(
        dependencies=[
            osm_download,
            substation_extraction,
            setup_etrago,
            scenario_parameters,
        ]
    )

    osmtgmod_substation = tasks["osmtgmod_substation"]

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

    # NEP data import
    scenario_capacities = ScenarioCapacities(
        dependencies=[
            setup,
            vg250,
            data_bundle,
            zensus_population,
            run_pypsaeursec,
        ]
    )

    # Retrieve MaStR data
    mastr_data = mastr_data_setup(dependencies=[setup])

    # create Voronoi polygons
    substation_voronoi = SubstationVoronoi(
        dependencies=[osmtgmod_substation, vg250]
    )

    # MV grid districts
    mv_grid_districts = mv_grid_districts_setup(
        dependencies=[substation_voronoi]
    )

    # Import potential areas for wind onshore and ground-mounted PV
    re_potential_areas = re_potential_area_setup(
        dependencies=[setup, data_bundle]
    )

    # Future heat demand calculation based on Peta5_0_1 data
    heat_demand_Germany = HeatDemandImport(
        dependencies=[vg250, scenario_parameters, zensus_vg250]
    )

    # Download industrial gas demand
    industrial_gas_demand = IndustrialGasDemand(dependencies=[setup])
    # Extract landuse areas from osm data set
    load_area = LoadArea(dependencies=[osm, vg250])

    # Calculate feedin from renewables
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
    dlr = Calculate_dlr(dependencies=[osmtgmod, data_bundle, weather_data])

    # Map zensus grid districts
    zensus_mv_grid_districts = ZensusMvGridDistricts(
        dependencies=[zensus_population, mv_grid_districts]
    )

    # Map federal states to mv_grid_districts
    vg250_mv_grid_districts = Vg250MvGridDistricts(
        dependencies=[vg250, mv_grid_districts]
    )

    # Create HH demand profiles on census level
    hh_demand_profiles_setup = hh_profiles.HouseholdDemands(
        dependencies=[
            vg250,
            zensus_miscellaneous,
            zensus_mv_grid_districts,
            zensus_vg250,
            demandregio,
            osm_buildings_streets_residential_zensus_mapping,
        ]
    )
    householdprofiles_in_cencus_cells = tasks[
        "electricity_demand_timeseries"
        ".hh_profiles"
        ".houseprofiles-in-census-cells"
    ]

    # Household electricity demand buildings
    hh_demand_buildings_setup = hh_buildings.setup(
        dependencies=[householdprofiles_in_cencus_cells]
    )

    map_houseprofiles_to_buildings = tasks[
        "electricity_demand_timeseries"
        ".hh_buildings"
        ".map-houseprofiles-to-buildings"
    ]

    # Get household electrical demands for cencus cells
    household_electricity_demand_annual = HouseholdElectricityDemand(
        dependencies=[map_houseprofiles_to_buildings]
    )

    elec_annual_household_demands_cells = tasks[
        "electricity_demand.get-annual-household-el-demand-cells"
    ]

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
            cts_electricity_demand_annual,
            hh_demand_buildings_setup,
        ]
    )

    # Deal with electrical neighbours
    foreign_lines = ElectricalNeighbours(
        dependencies=[run_pypsaeursec, tyndp_data]
    )

    # Gas grid import
    gas_grid_insert_data = GasNodesandPipes(
        dependencies=[
            etrago_input_data,
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

    # Create gas voronoi eGon2035
    create_gas_polygons_egon2035 = GasAreaseGon2035(
        dependencies=[insert_hydrogen_buses, vg250]
    )

    # Insert hydrogen grid
    insert_h2_grid = HydrogenGridEtrago(
        dependencies=[
            create_gas_polygons_egon2035,
            gas_grid_insert_data,
            insert_hydrogen_buses,
        ]
    )

    # H2 steel tanks and saltcavern storage
    insert_H2_storage = HydrogenStoreEtrago(
        dependencies=[insert_hydrogen_buses, insert_h2_grid]
    )

    # Power-to-gas-to-power chain installations
    insert_power_to_h2_installations = HydrogenPowerLinkEtrago(
        dependencies=[insert_hydrogen_buses, insert_h2_grid]
    )

    # Link between methane grid and respective hydrogen buses
    insert_h2_to_ch4_grid_links = HydrogenMethaneLinkEtrago(
        dependencies=[insert_hydrogen_buses, insert_h2_grid]
    )

    # Create gas voronoi eGon100RE
    create_gas_polygons_egon100RE = GasAreaseGon100RE(
        dependencies=[insert_h2_grid, vg250]
    )

    # Gas prod import
    gas_production_insert_data = CH4Production(
        dependencies=[create_gas_polygons_egon2035]
    )

    # CH4 storages import
    insert_data_ch4_storages = CH4Storages(
        dependencies=[create_gas_polygons_egon2035]
    )

    # Assign industrial gas demand eGon2035
    assign_industrial_gas_demand = IndustrialGasDemandeGon2035(
        dependencies=[industrial_gas_demand, create_gas_polygons_egon2035]
    )

    # Assign industrial gas demand eGon100RE
    assign_industrial_gas_demand = IndustrialGasDemandeGon100RE(
        dependencies=[industrial_gas_demand, create_gas_polygons_egon100RE]
    )
    # Aggregate gas loads, stores and generators
    aggrgate_gas = GasAggregation(
        dependencies=[gas_production_insert_data, insert_data_ch4_storages]
    )

    # CHP locations
    chp = Chp(
        dependencies=[
            mv_grid_districts,
            mastr_data,
            industrial_sites,
            create_gas_polygons_egon2035,
            create_gas_polygons_egon100RE,
            scenario_capacities,
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
            zensus_mv_grid_districts,
            cts_electricity_demand_annual,
            household_electricity_demand_annual,
            etrago_input_data,
        ]
    )

    create_ocgt = OpenCycleGasTurbineEtrago(
        dependencies=[create_gas_polygons_egon2035, power_plants]
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
            cts_electricity_demand_annual,
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
        dependencies=[pumped_hydro, setup_etrago, scenario_parameters]
    )
