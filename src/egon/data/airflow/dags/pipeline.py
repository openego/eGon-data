import os

from airflow.utils.dates import days_ago
import airflow

from egon.data.config import set_numexpr_threads
from egon.data.datasets import database
from egon.data.datasets.calculate_dlr import Calculate_dlr
from egon.data.datasets.ch4_prod import CH4Production
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
from egon.data.datasets.electricity_demand_timeseries.cts_buildings import (
    CtsDemandBuildings,
)
from egon.data.datasets.emobility.motorized_individual_travel import (
    MotorizedIndividualTravel,
)
from egon.data.datasets.era5 import WeatherData
from egon.data.datasets.etrago_setup import EtragoSetup
from egon.data.datasets.fill_etrago_gen import Egon_etrago_gen
from egon.data.datasets.fix_ehv_subnetworks import FixEhvSubnetworks
from egon.data.datasets.gas_areas import GasAreaseGon100RE, GasAreaseGon2035
from egon.data.datasets.gas_grid import GasNodesandPipes
from egon.data.datasets.gas_neighbours import GasNeighbours
from egon.data.datasets.heat_demand import HeatDemandImport
from egon.data.datasets.heat_demand_europe import HeatDemandEurope
from egon.data.datasets.heat_demand_timeseries import HeatTimeSeries
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
from egon.data.datasets.sanity_checks import SanityChecks
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

    setup = database.Setup()

    osm = OpenStreetMap(dependencies=[setup])

    data_bundle = DataBundle(dependencies=[setup])

    # Import VG250 (Verwaltungsgebiete 250) data
    vg250 = Vg250(dependencies=[setup])

    # Scenario table
    scenario_parameters = ScenarioParameters(dependencies=[setup])

    # Download TYNDP data
    tyndp_data = Tyndp(dependencies=[setup])

    # Import zensus population
    zensus_population = ZensusPopulation(dependencies=[setup, vg250])

    # Combine zensus and VG250 data
    zensus_vg250 = ZensusVg250(dependencies=[vg250, zensus_population])

    # Download and import zensus data on households, buildings and apartments
    zensus_miscellaneous = ZensusMiscellaneous(
        dependencies=[zensus_population, zensus_vg250]
    )

    # Import DemandRegio data
    demandregio = DemandRegio(
        dependencies=[data_bundle, scenario_parameters, setup, vg250]
    )

    # Society prognosis
    society_prognosis = SocietyPrognosis(
        dependencies=[demandregio, zensus_miscellaneous]
    )

    # OSM (OpenStreetMap) buildings, streets and amenities
    osm_buildings_streets = OsmBuildingsStreets(
        dependencies=[osm, zensus_miscellaneous]
    )

    # Import saltcavern storage potentials
    saltcavern_storage = SaltcavernData(dependencies=[data_bundle, vg250])

    # Import weather data
    weather_data = WeatherData(
        dependencies=[scenario_parameters, setup, vg250]
    )

    # Future national heat demands for foreign countries based on Hotmaps
    # download only, processing in PyPSA-Eur-Sec fork
    hd_abroad = HeatDemandEurope(dependencies=[setup])

    # Set eTraGo input tables
    setup_etrago = EtragoSetup(dependencies=[setup])

    substation_extraction = SubstationExtraction(dependencies=[osm, vg250])

    # Generate the osmTGmod ehv/hv grid model
    osmtgmod = Osmtgmod(
        dependencies=[
            scenario_parameters,
            setup_etrago,
            substation_extraction,
            tasks["osm.download"],
        ]
    )

    # Fix eHV subnetworks in Germany manually
    fix_subnetworks = FixEhvSubnetworks(dependencies=[osmtgmod])

    # Retrieve MaStR (Marktstammdatenregister) data
    mastr_data = mastr_data_setup(dependencies=[setup])

    # Create Voronoi polygons
    substation_voronoi = SubstationVoronoi(
        dependencies=[tasks["osmtgmod_substation"], vg250]
    )

    # MV (medium voltage) grid districts
    mv_grid_districts = mv_grid_districts_setup(
        dependencies=[substation_voronoi]
    )

    # Import potential areas for wind onshore and ground-mounted PV
    re_potential_areas = re_potential_area_setup(
        dependencies=[data_bundle, setup]
    )

    # Calculate future heat demand based on Peta5_0_1 data
    heat_demand_Germany = HeatDemandImport(
        dependencies=[scenario_parameters, vg250, zensus_vg250]
    )

    # Download industrial gas demand
    industrial_gas_demand = IndustrialGasDemand(
        dependencies=[scenario_parameters]
    )

    # Extract landuse areas from the `osm` dataset
    load_area = LoadArea(dependencies=[osm, vg250])

    # Calculate feedin from renewables
    renewable_feedin = RenewableFeedin(
        dependencies=[vg250, zensus_vg250, weather_data]
    )

    # Demarcate district heating areas
    district_heating_areas = DistrictHeatingAreas(
        dependencies=[
            heat_demand_Germany,
            scenario_parameters,
            zensus_miscellaneous,
        ]
    )

    # TODO: What does "trans" stand for?
    # Calculate dynamic line rating for HV (high voltage) trans lines
    dlr = Calculate_dlr(
        dependencies=[data_bundle, osmtgmod, weather_data, fix_subnetworks]
    )

    # Map zensus grid districts
    zensus_mv_grid_districts = ZensusMvGridDistricts(
        dependencies=[mv_grid_districts, zensus_population]
    )

    # Map federal states to mv_grid_districts
    vg250_mv_grid_districts = Vg250MvGridDistricts(
        dependencies=[mv_grid_districts, vg250]
    )

    # Create household demand profiles on zensus level
    hh_demand_profiles_setup = hh_profiles.HouseholdDemands(
        dependencies=[
            demandregio,
            tasks[
                "osm_buildings_streets"
                ".create-buildings-residential-zensus-mapping"
            ],
            vg250,
            zensus_miscellaneous,
            zensus_mv_grid_districts,
            zensus_vg250,
        ]
    )

    # Household electricity demand buildings
    hh_demand_buildings_setup = hh_buildings.setup(
        dependencies=[
            tasks[
                "electricity_demand_timeseries"
                ".hh_profiles"
                ".houseprofiles-in-census-cells"
            ]
        ]
    )

    # Get household electrical demands for cencus cells
    household_electricity_demand_annual = HouseholdElectricityDemand(
        dependencies=[
            tasks[
                "electricity_demand_timeseries"
                ".hh_buildings"
                ".map-houseprofiles-to-buildings"
            ]
        ]
    )

    # Distribute electrical CTS demands to zensus grid
    cts_electricity_demand_annual = CtsElectricityDemand(
        dependencies=[
            demandregio,
            heat_demand_Germany,
            # household_electricity_demand_annual,
            tasks["electricity_demand.create-tables"],
            tasks["etrago_setup.create-tables"],
            zensus_mv_grid_districts,
            zensus_vg250,
        ]
    )

    # Industry
    industrial_sites = MergeIndustrialSites(
        dependencies=[data_bundle, setup, vg250]
    )
    demand_curves_industry = IndustrialDemandCurves(
        dependencies=[
            demandregio,
            industrial_sites,
            load_area,
            mv_grid_districts,
            osm,
        ]
    )

    # Electrical loads to eTraGo
    electrical_load_etrago = ElectricalLoadEtrago(
        dependencies=[
            cts_electricity_demand_annual,
            demand_curves_industry,
            hh_demand_buildings_setup,
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
            weather_data,
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
            electrical_load_etrago,
            heat_time_series,
        ]
    )

    # Deal with electrical neighbours
    foreign_lines = ElectricalNeighbours(
        dependencies=[run_pypsaeursec, tyndp_data]
    )

    # Import NEP (Netzentwicklungsplan) data
    scenario_capacities = ScenarioCapacities(
        dependencies=[
            data_bundle,
            run_pypsaeursec,
            setup,
            vg250,
            zensus_population,
        ]
    )

    # Import gas grid
    gas_grid_insert_data = GasNodesandPipes(
        dependencies=[
            data_bundle,
            foreign_lines,
            osmtgmod,
            scenario_parameters,
            tasks["etrago_setup.create-tables"],
        ]
    )

    # Insert hydrogen buses
    insert_hydrogen_buses = HydrogenBusEtrago(
        dependencies=[
            gas_grid_insert_data,
            saltcavern_storage,
            substation_voronoi,
        ]
    )

    # Create gas voronoi eGon2035
    create_gas_polygons_egon2035 = GasAreaseGon2035(
        dependencies=[setup_etrago, insert_hydrogen_buses, vg250]
    )

    # Insert hydrogen grid
    insert_h2_grid = HydrogenGridEtrago(
        dependencies=[
            create_gas_polygons_egon2035,
            gas_grid_insert_data,
            insert_hydrogen_buses,
        ]
    )

    h2_infrastructure = [insert_h2_grid, insert_hydrogen_buses]

    # H2 steel tanks and saltcavern storage
    insert_H2_storage = HydrogenStoreEtrago(dependencies=h2_infrastructure)

    # Power-to-gas-to-power chain installations
    insert_power_to_h2_installations = HydrogenPowerLinkEtrago(
        dependencies=h2_infrastructure
    )

    # Link between methane grid and respective hydrogen buses
    insert_h2_to_ch4_grid_links = HydrogenMethaneLinkEtrago(
        dependencies=[h2_infrastructure, insert_power_to_h2_installations]
    )

    # Create gas voronoi eGon100RE
    create_gas_polygons_egon100RE = GasAreaseGon100RE(
        dependencies=[create_gas_polygons_egon2035, insert_h2_grid, vg250]
    )

    # Gas abroad
    gas_abroad_insert_data = GasNeighbours(
        dependencies=[
            gas_grid_insert_data,
            run_pypsaeursec,
            foreign_lines,
            insert_hydrogen_buses,
            create_gas_polygons_egon100RE
        ]
    )

    # Import gas production
    gas_production_insert_data = CH4Production(
        dependencies=[create_gas_polygons_egon2035]
    )

    # Import CH4 storages
    insert_data_ch4_storages = CH4Storages(
        dependencies=[create_gas_polygons_egon2035]
    )

    # Assign industrial gas demand eGon2035
    IndustrialGasDemandeGon2035(
        dependencies=[create_gas_polygons_egon2035, industrial_gas_demand]
    )

    # Assign industrial gas demand eGon100RE
    IndustrialGasDemandeGon100RE(
        dependencies=[create_gas_polygons_egon100RE, industrial_gas_demand]
    )

    # CHP locations
    chp = Chp(
        dependencies=[
            create_gas_polygons_egon100RE,
            create_gas_polygons_egon2035,
            demand_curves_industry,
            district_heating_areas,
            industrial_sites,
            load_area,
            mastr_data,
            mv_grid_districts,
            scenario_capacities,
        ]
    )

    # Power plants
    power_plants = PowerPlants(
        dependencies=[
            chp,
            cts_electricity_demand_annual,
            household_electricity_demand_annual,
            mastr_data,
            mv_grid_districts,
            re_potential_areas,
            renewable_feedin,
            scenario_capacities,
            scenario_parameters,
            setup,
            substation_extraction,
            tasks["etrago_setup.create-tables"],
            vg250_mv_grid_districts,
            zensus_mv_grid_districts,
        ]
    )

    create_ocgt = OpenCycleGasTurbineEtrago(
        dependencies=[create_gas_polygons_egon2035, power_plants]
    )

    # Fill eTraGo generators tables
    fill_etrago_generators = Egon_etrago_gen(
        dependencies=[power_plants, weather_data]
    )

    # Heat supply
    heat_supply = HeatSupply(
        dependencies=[
            chp,
            data_bundle,
            district_heating_areas,
            zensus_mv_grid_districts,
        ]
    )

    # DSM (demand site management)
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
            mastr_data,
            mv_grid_districts,
            power_plants,
            scenario_capacities,
            scenario_parameters,
            setup,
            vg250_mv_grid_districts,
        ]
    )

    # Heat to eTraGo
    heat_etrago = HeatEtrago(
        dependencies=[
            heat_supply,
            mv_grid_districts,
            renewable_feedin,
            setup_etrago,
            heat_time_series,
        ]
    )

    # CHP to eTraGo
    chp_etrago = ChpEtrago(dependencies=[chp, heat_etrago])

    # HTS to eTraGo table
    hts_etrago_table = HtsEtragoTable(
        dependencies=[
            district_heating_areas,
            heat_etrago,
            heat_time_series,
            mv_grid_districts,
        ]
    )

    # Storages to eTraGo
    storage_etrago = StorageEtrago(
        dependencies=[pumped_hydro, scenario_parameters, setup_etrago]
    )

    # eMobility: motorized individual travel
    emobility_mit = MotorizedIndividualTravel(
        dependencies=[
            data_bundle,
            mv_grid_districts,
            scenario_parameters,
            setup_etrago,
            zensus_mv_grid_districts,
            zensus_vg250,
            storage_etrago,
            hts_etrago_table,
            chp_etrago,
            components_dsm,
            heat_etrago,
            fill_etrago_generators,
            create_ocgt,
            insert_H2_storage,
            insert_power_to_h2_installations,
            insert_h2_to_ch4_grid_links,
            create_gas_polygons_egon100RE,
            gas_production_insert_data,
            insert_data_ch4_storages,
        ]
    )

    cts_demand_buildings = CtsDemandBuildings(
        dependencies=[
            osm_buildings_streets,
            cts_electricity_demand_annual,
            hh_demand_buildings_setup,
        ]
    )

    # ########## Keep this dataset at the end
    # Sanity Checks
    sanity_checks = SanityChecks(
        dependencies=[
            storage_etrago,
            hts_etrago_table,
            fill_etrago_generators,
            emobility_mit,
        ]
    )
