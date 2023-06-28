import os

from airflow.utils.dates import days_ago
import airflow

from egon.data.config import set_numexpr_threads
from egon.data.datasets import database
from egon.data.datasets.ch4_prod import CH4Production
from egon.data.datasets.ch4_storages import CH4Storages
from egon.data.datasets.chp import Chp
from egon.data.datasets.chp_etrago import ChpEtrago
from egon.data.datasets.data_bundle import DataBundle
from egon.data.datasets.demandregio import DemandRegio
from egon.data.datasets.district_heating_areas import DistrictHeatingAreas
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
from egon.data.datasets.emobility.motorized_individual_travel_charging_infrastructure import (  # noqa: E501
    MITChargingInfrastructure,
)
from egon.data.datasets.era5 import WeatherData
from egon.data.datasets.etrago_setup import EtragoSetup
from egon.data.datasets.fill_etrago_gen import Egon_etrago_gen
from egon.data.datasets.fix_ehv_subnetworks import FixEhvSubnetworks
from egon.data.datasets.gas_areas import GasAreasstatus2019
from egon.data.datasets.gas_grid import GasNodesAndPipes
from egon.data.datasets.gas_neighbours import GasNeighbours
from egon.data.datasets.heat_demand import HeatDemandImport
from egon.data.datasets.heat_demand_europe import HeatDemandEurope
from egon.data.datasets.heat_demand_timeseries import HeatTimeSeries
from egon.data.datasets.heat_etrago import HeatEtrago
from egon.data.datasets.heat_etrago.hts_etrago import HtsEtragoTable
from egon.data.datasets.heat_supply import HeatSupply
from egon.data.datasets.heat_supply.individual_heating import HeatPumps2019
from egon.data.datasets.industrial_sites import MergeIndustrialSites
from egon.data.datasets.industry import IndustrialDemandCurves
from egon.data.datasets.loadarea import LoadArea, OsmLanduse
from egon.data.datasets.mastr import mastr_data_setup
from egon.data.datasets.mv_grid_districts import mv_grid_districts_setup
from egon.data.datasets.osm import OpenStreetMap
from egon.data.datasets.osm_buildings_streets import OsmBuildingsStreets
from egon.data.datasets.osmtgmod import Osmtgmod
from egon.data.datasets.power_etrago import OpenCycleGasTurbineEtrago
from egon.data.datasets.power_plants import PowerPlants
from egon.data.datasets.pypsaeursec import PypsaEurSec
from egon.data.datasets.renewable_feedin import RenewableFeedin
from egon.data.datasets.scenario_capacities import ScenarioCapacities
from egon.data.datasets.scenario_parameters import ScenarioParameters
from egon.data.datasets.society_prognosis import SocietyPrognosis
from egon.data.datasets.storages import Storages
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
    "powerd-status-quo-processing-pipeline",
    description="The PoWerD Status Quo data processing DAG.",
    default_args={"start_date": days_ago(1),
                  "email_on_failure": True,
                  "email":"ulf.p.mueller@hs-flensburg.de"},
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
    tyndp_data = Tyndp(dependencies=[setup]) #TODO: kick out or adjust

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
        dependencies=[tasks["osmtgmod.substation.extract"], vg250]
    )

    # MV (medium voltage) grid districts
    mv_grid_districts = mv_grid_districts_setup(
        dependencies=[substation_voronoi]
    )

    # Calculate future heat demand based on Peta5_0_1 data
    heat_demand_Germany = HeatDemandImport(
        dependencies=[scenario_parameters, vg250, zensus_vg250]
    )

    # Extract landuse areas from the `osm` dataset
    osm_landuse = OsmLanduse(dependencies=[osm, vg250])

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
    # dlr = Calculate_dlr(
    #    dependencies=[data_bundle, osmtgmod, weather_data] # , fix_subnetworks]
    #)

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
            osm_landuse,
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

    cts_demand_buildings = CtsDemandBuildings(
        dependencies=[
            osm_buildings_streets,
            cts_electricity_demand_annual,
            hh_demand_buildings_setup,
            tasks["heat_demand_timeseries.export-etrago-cts-heat-profiles"],
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
    gas_grid_insert_data = GasNodesAndPipes(
        dependencies=[
            data_bundle,
            foreign_lines,
            osmtgmod,
            scenario_parameters,
            tasks["etrago_setup.create-tables"],
        ]
    )
    # Create gas voronoi status2019
    create_gas_polygons_status2019 = GasAreasstatus2019(
        dependencies=[setup_etrago, vg250, gas_grid_insert_data, substation_voronoi]
    )

    # Gas abroad
    gas_abroad_insert_data = GasNeighbours(
        dependencies=[
            gas_grid_insert_data,
            run_pypsaeursec,
            foreign_lines,
            create_gas_polygons_status2019,
        ]
    )

    # Import gas production
    gas_production_insert_data = CH4Production(
        dependencies=[create_gas_polygons_status2019]
    )

    # Import CH4 storages
    insert_data_ch4_storages = CH4Storages(
        dependencies=[create_gas_polygons_status2019]
    )

    # CHP locations
    chp = Chp(
        dependencies=[
            create_gas_polygons_status2019,
            demand_curves_industry,
            district_heating_areas,
            industrial_sites,
            osm_landuse,
            mastr_data,
            mv_grid_districts,
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
            renewable_feedin,
            scenario_parameters,
            setup,
            substation_extraction,
            tasks["etrago_setup.create-tables"],
            vg250_mv_grid_districts,
            zensus_mv_grid_districts,
        ]
    )

    create_ocgt = OpenCycleGasTurbineEtrago(
        dependencies=[create_gas_polygons_status2019, power_plants]
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
            scenario_capacities,
        ]
    )
    
    
    # Pumped hydro units
    pumped_hydro = Storages(
        dependencies=[
            mastr_data,
            mv_grid_districts,
            power_plants,
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

    # Heat pump disaggregation for status2019
    heat_pumps_2019 = HeatPumps2019(
        dependencies=[
            cts_demand_buildings,
            DistrictHeatingAreas,
            heat_supply,
            heat_time_series,
            power_plants,
        ]
    )

    # HTS to eTraGo table
    hts_etrago_table = HtsEtragoTable(
        dependencies=[
            district_heating_areas,
            heat_etrago,
            heat_time_series,
            mv_grid_districts,
            heat_pumps_2019,
        ]
    )

    # Storages to eTraGo
    storage_etrago = StorageEtrago(
        dependencies=[pumped_hydro, scenario_parameters, setup_etrago]
    )

    # eMobility: motorized individual travel TODO: adjust for SQ
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
            heat_etrago,
            fill_etrago_generators,
            create_ocgt,
            gas_production_insert_data,
            insert_data_ch4_storages,
        ]
    )

    mit_charging_infrastructure = MITChargingInfrastructure(
        dependencies=[mv_grid_districts, hh_demand_buildings_setup]
    )

    cts_demand_buildings = CtsDemandBuildings(
        dependencies=[
            osm_buildings_streets,
            cts_electricity_demand_annual,
            hh_demand_buildings_setup,
            tasks["heat_demand_timeseries.export-etrago-cts-heat-profiles"],
        ]
    )

    # Create load areas
    load_areas = LoadArea(
        dependencies=[
            osm_landuse,
            zensus_vg250,
            household_electricity_demand_annual,
            tasks[
                "electricity_demand_timeseries"
                ".hh_buildings"
                ".get-building-peak-loads"
            ],
            cts_demand_buildings,
            demand_curves_industry,
        ]
    )


