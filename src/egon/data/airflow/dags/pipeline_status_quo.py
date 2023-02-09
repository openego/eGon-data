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
from egon.data.datasets.DSM_cts_ind import DsmPotential
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
from egon.data.datasets.emobility.heavy_duty_transport import (
    HeavyDutyTransport,
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
from egon.data.datasets.saltcavern import SaltcavernData
from egon.data.datasets.sanity_checks import SanityChecks
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


    # run pypsa-eur-sec
    run_pypsaeursec = PypsaEurSec(
        dependencies=[
            weather_data,
            hd_abroad,
            osmtgmod,
            setup_etrago,
            data_bundle,
            electrical_load_etrago,
        ]
    )

    # Deal with electrical neighbours
    foreign_lines = ElectricalNeighbours(
        dependencies=[run_pypsaeursec, tyndp_data]
    )


    # CHP locations
    chp = Chp(
        dependencies=[
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
        dependencies=[power_plants]
    )

    # Fill eTraGo generators tables
    fill_etrago_generators = Egon_etrago_gen(
        dependencies=[power_plants, weather_data]
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

    # CHP to eTraGo
    chp_etrago = ChpEtrago(dependencies=[chp])

    # Storages to eTraGo
    storage_etrago = StorageEtrago(
        dependencies=[pumped_hydro, scenario_parameters, setup_etrago]
    )

