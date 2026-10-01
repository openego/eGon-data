"""
The module containing the allocation of the hydrogen power plants of the
reGon scenarios.

The hydrogen capacity per federal state of the NEP Strom is placed at
existing power plant sites: conversions of natural gas sites, freed grid
connections and, in 2045, load-near sites. The method is described in the
documentation of the gas sector (gas-fired and hydrogen power plants).

"""

from scipy.optimize import linear_sum_assignment
from scipy.spatial import cKDTree
from sqlalchemy.orm import sessionmaker
import geopandas as gpd
import numpy as np
import pandas as pd

from egon.data import db
from egon.data.datasets import load_sources_and_targets
from egon.data.datasets.scenario_parameters import get_scenario_year
import egon.data.config

# Minimum net capacity [MW] of a site for new builds and load-near plants
MIN_SITE_CAPACITY = 10

# Pairing cost of a timing mismatch, in units of |ln(capacity ratio)|
TIMING_MISMATCH_COST = np.log(2)

# Maximum capacity ratio of a project and its site
MAX_CAPACITY_RATIO = 4

# MaStR carriers without plants in the NEP Strom C 2037: freed connections
FREED_CARRIERS = ["Steinkohle", "Braunkohle"]

# Distance [m] to snap MaStR units without location to a site
SNAP_DISTANCE = 1000

# Federal states as named in the target values (see
# egon.data.datasets.power_plants.select_target) and the MaStR
FEDERAL_STATES = {
    "BadenWuerttemberg": "BW",
    "Bayern": "BY",
    "Berlin": "BE",
    "Brandenburg": "BB",
    "Bremen": "HB",
    "Hamburg": "HH",
    "Hessen": "HE",
    "MecklenburgVorpommern": "MV",
    "Niedersachsen": "NI",
    "NordrheinWestfalen": "NW",
    "RheinlandPfalz": "RP",
    "Saarland": "SL",
    "Sachsen": "SN",
    "SachsenAnhalt": "ST",
    "SchleswigHolstein": "SH",
    "Thueringen": "TH",
}


def select_power_plant_sites():
    """
    Select the power plant sites (MaStR locations with combustion units) and
    add the natural gas units of the NEP 2025 list

    Returns
    -------
    geopandas.GeoDataFrame
        Sites with total and natural gas net capacity in MW, the natural gas
        units of the NEP list, federal state and point geometry

    """
    from egon.data.datasets.power_plants import filter_mastr_geometry

    sources, _ = load_sources_and_targets("PowerPlants")

    mastr = pd.read_csv(
        sources.files["mastr_combustion"],
        usecols=[
            "EinheitMastrNummer",
            "LokationMastrNummer",
            "Energietraeger",
            "Nettonennleistung",
            "Bundesland",
            "Laengengrad",
            "Breitengrad",
            "NameKraftwerk",
            "Gemeinde",
        ],
    )
    mastr = assign_missing_locations(mastr)
    # MaStR capacities are given in kW
    mastr["capacity"] = mastr.Nettonennleistung / 1e3
    mastr["gas_capacity"] = mastr.capacity.where(
        mastr.Energietraeger == "Erdgas", 0
    )
    coal = mastr.Energietraeger.isin(FREED_CARRIERS)
    mastr["coal_capacity"] = mastr.capacity.where(coal, 0)
    mastr["coal_units"] = mastr.EinheitMastrNummer.where(coal)

    # Natural gas units of the NEP 2025 list, capacity in the list (2037)
    # and in the MaStR (net capacity)
    nep_gas = db.select_dataframe(
        f"""
        SELECT mastr_id, capacity, c2037_capacity, h2_site
        FROM {sources.tables['nep_conv']}
        WHERE carrier = 'gas'
        AND scenario = 'reGon'
        """
    )
    nep_gas = nep_gas.merge(
        mastr[["EinheitMastrNummer", "LokationMastrNummer"]],
        left_on="mastr_id",
        right_on="EinheitMastrNummer",
    )
    nep_gas["h2_site"] = nep_gas.h2_site.fillna(False).astype(bool)
    in_2037 = nep_gas.c2037_capacity > 0
    nep_gas["h2_site_leaving"] = nep_gas.capacity.where(
        nep_gas.h2_site & ~in_2037, 0
    )
    nep_gas["h2_site_staying"] = nep_gas.capacity.where(
        nep_gas.h2_site & in_2037, 0
    )
    nep_gas["h2_units"] = nep_gas.mastr_id.where(nep_gas.h2_site)
    nep_gas["units_2037"] = nep_gas.mastr_id.where(in_2037)
    nep_gas["units_leaving"] = nep_gas.mastr_id.where(~in_2037)
    nep_sites = nep_gas.groupby("LokationMastrNummer").agg(
        gas_2037=("c2037_capacity", "sum"),
        h2_site_leaving=("h2_site_leaving", "sum"),
        h2_site_staying=("h2_site_staying", "sum"),
        h2_units=("h2_units", join_units),
        units_2037=("units_2037", join_units),
        units_leaving=("units_leaving", join_units),
    )

    sites = (
        mastr.groupby("LokationMastrNummer")
        .agg(
            capacity=("capacity", "sum"),
            gas_capacity=("gas_capacity", "sum"),
            coal_capacity=("coal_capacity", "sum"),
            coal_units=("coal_units", join_units),
            Bundesland=("Bundesland", "first"),
            Laengengrad=("Laengengrad", "first"),
            Breitengrad=("Breitengrad", "first"),
        )
        .join(nep_sites)
        .reset_index()
    )
    for column in ["gas_2037", "h2_site_leaving", "h2_site_staying"]:
        sites[column] = sites[column].fillna(0)
    for column in ["h2_units", "units_2037", "units_leaving"]:
        sites[column] = sites[column].fillna("")
    sites["h2_site_capacity"] = sites.h2_site_leaving + sites.h2_site_staying
    sites["federal_state"] = sites.Bundesland.map(FEDERAL_STATES)

    # Only sites of power plants: reported in the hydrogen market survey or
    # of a minimum size
    sites = sites[
        (sites.h2_site_capacity > 0) | (sites.capacity >= MIN_SITE_CAPACITY)
    ]

    # Drop sites outside of Germany or of the test area
    return filter_mastr_geometry(sites).set_crs(4326, allow_override=True)


def assign_missing_locations(mastr):
    """
    Assign a site to the MaStR units without location

    Parameters
    ----------
    mastr : pandas.DataFrame
        Combustion units of the MaStR

    Returns
    -------
    pandas.DataFrame
        Units with coordinates, all with a LokationMastrNummer

    """
    mastr = mastr[mastr.Laengengrad.notnull() & mastr.Breitengrad.notnull()]
    points = gpd.GeoSeries(
        gpd.points_from_xy(mastr.Laengengrad, mastr.Breitengrad), crs=4326
    ).to_crs(3035)
    xy = np.column_stack([points.x, points.y])

    missing = mastr.LokationMastrNummer.isnull().values
    if not missing.any():
        return mastr
    mastr = mastr.copy()

    distance, nearest = cKDTree(xy[~missing]).query(xy[missing])
    snapped = np.where(
        distance <= SNAP_DISTANCE,
        mastr.LokationMastrNummer.values[~missing][nearest],
        None,
    )
    own_site = (
        "no location: "
        + mastr.NameKraftwerk[missing].fillna("").astype(str)
        + " ("
        + mastr.Gemeinde[missing].fillna("").astype(str)
        + ")"
    )
    mastr.loc[missing, "LokationMastrNummer"] = np.where(
        pd.isnull(snapped), own_site, snapped
    )
    return mastr


def join_units(units):
    """Join the MaStR numbers of units, without missing values"""
    return ", ".join(sorted(units.dropna()))


def population_per_ehv_area():
    """Population per EHV substation area (Voronoi polygon)

    Returns
    -------
    pandas.Series
        Population per bus_id of the EHV substations

    """
    sources, _ = load_sources_and_targets("PowerPlants")

    return db.select_dataframe(
        f"""
        WITH voronoi AS (
            SELECT bus_id, ST_Transform(geom, 3035) AS geom
            FROM {sources.tables['ehv_voronoi']}
        )
        SELECT voronoi.bus_id, SUM(zensus.population) AS population
        FROM voronoi
        JOIN {sources.tables['zensus_population']} zensus
        ON ST_Contains(voronoi.geom, zensus.geom_point)
        WHERE zensus.population > 0
        GROUP BY voronoi.bus_id
        """,
        index_col="bus_id",
    ).population


def pair_by_capacity(capacity, site_capacity, extra_cost=None):
    """
    Pair projects with sites by capacity (linear sum assignment)

    Parameters
    ----------
    capacity : pandas.Series
        Capacity per project in MW
    site_capacity : pandas.Series
        Capacity per site (LokationMastrNummer) in MW, all > 0
    extra_cost : numpy.ndarray, optional
        Extra cost per project (rows) and site (columns)

    Returns
    -------
    pandas.Series
        LokationMastrNummer of the site per project, only paired projects

    """
    if capacity.empty or site_capacity.empty:
        return pd.Series(dtype=object)

    ratio = np.abs(
        np.log(capacity.values[:, None] / site_capacity.values[None, :])
    )
    cost = ratio if extra_cost is None else ratio + extra_cost
    allowed = ratio <= np.log(MAX_CAPACITY_RATIO)
    # Pairs that are not allowed get a cost above any allowed assignment
    cost = np.where(allowed, cost, cost.max() * len(cost) + 1)

    rows, columns = linear_sum_assignment(cost)
    rows, columns = (
        rows[allowed[rows, columns]],
        columns[allowed[rows, columns]],
    )
    return pd.Series(
        site_capacity.index[columns],
        index=capacity.index[rows],
    )


def pair_conversion_projects(projects, sites):
    """
    Pair the conversion projects of a federal state with reported sites

    Parameters
    ----------
    projects : pandas.DataFrame
        Conversion projects with columns el_capacity (the larger of 2037
        and 2045) and in_2037
    sites : pandas.DataFrame
        Sites of the federal state

    Returns
    -------
    pandas.Series
        LokationMastrNummer of the site per project, only paired projects

    """
    sites = sites[sites.h2_site_capacity > 0]

    # A project starting in 2037 needs a site whose units leave by 2037, a
    # project starting in 2045 one whose units are still there in 2037
    units_in_2037 = (sites.h2_site_staying > sites.h2_site_leaving).values
    mismatch = projects.in_2037.values[:, None] == units_in_2037[None, :]

    return pair_by_capacity(
        projects.el_capacity,
        sites.set_index("LokationMastrNummer").h2_site_capacity,
        TIMING_MISMATCH_COST * mismatch,
    )


def place_listed_projects(projects, sites):
    """
    Place the listed hydrogen projects of a federal state at sites

    Parameters
    ----------
    projects : pandas.DataFrame
        Projects of the federal state with columns name, h2_conversion,
        c2037_capacity and c2045_capacity
    sites : geopandas.GeoDataFrame
        Sites of the federal state

    Returns
    -------
    pandas.DataFrame
        Projects with the columns LokationMastrNummer of their site and
        site ("conversion", "freed connection" or "existing site")

    """
    placed = projects.copy()
    placed["el_capacity"] = placed[["c2037_capacity", "c2045_capacity"]].max(
        axis=1
    )
    placed["in_2037"] = placed.c2037_capacity > 0

    paired = pair_conversion_projects(placed[placed.h2_conversion], sites)
    placed["LokationMastrNummer"] = paired
    placed["site"] = np.where(
        placed.LokationMastrNummer.notnull(), "conversion", None
    )

    candidates = sites[sites.capacity >= MIN_SITE_CAPACITY]
    if candidates.empty:
        candidates = sites
    candidates = candidates.set_index("LokationMastrNummer")

    def hydrogen_per_site():
        return (
            placed.groupby("LokationMastrNummer")
            .el_capacity.sum()
            .reindex(candidates.index, fill_value=0)
        )

    # Freed connections: pairing
    freed = (
        candidates.gas_capacity
        - candidates.gas_2037
        + candidates.coal_capacity
        - hydrogen_per_site()
    )
    unplaced = placed[placed.LokationMastrNummer.isnull()]
    paired = pair_by_capacity(unplaced.el_capacity, freed[freed > 0])
    placed.loc[paired.index, "LokationMastrNummer"] = paired
    placed.loc[paired.index, "site"] = "freed connection"

    # Rest: largest remaining freed connection, preferably at a site
    # without hydrogen plant, else the largest remaining capacity
    hydrogen = hydrogen_per_site()
    freed = freed.sub(
        placed[placed.site == "freed connection"]
        .groupby("LokationMastrNummer")
        .el_capacity.sum(),
        fill_value=0,
    )[candidates.index]
    remaining = candidates.capacity - hydrogen
    unplaced = placed[placed.LokationMastrNummer.isnull()]
    for i, project in unplaced.sort_values(
        "el_capacity", ascending=False
    ).iterrows():
        fits = freed >= project.el_capacity / MAX_CAPACITY_RATIO
        if (fits & (hydrogen == 0)).any():
            site, choice = (
                freed[fits & (hydrogen == 0)].idxmax(),
                "freed connection",
            )
        elif fits.any():
            site, choice = freed[fits].idxmax(), "freed connection"
        else:
            site, choice = remaining.idxmax(), "existing site"
        placed.loc[i, ["LokationMastrNummer", "site"]] = [site, choice]
        freed[site] -= project.el_capacity
        remaining[site] -= project.el_capacity
        hydrogen[site] += project.el_capacity

    return placed.drop(columns=["el_capacity", "in_2037"])


def allocate_load_near(extra, sites, listed, year):
    """
    Allocate the hydrogen capacity of a federal state beyond the list

    Parameters
    ----------
    extra : float
        Capacity to allocate in MW
    sites : geopandas.GeoDataFrame
        Sites of the federal state
    listed : pandas.Series
        Listed hydrogen capacity per site (LokationMastrNummer) in MW
    year : int
        Year of the scenario

    Returns
    -------
    pandas.DataFrame
        Plants with the columns LokationMastrNummer, el_capacity and site
        ("gas site 2037" or "population")

    """
    plants = []

    if year >= 2045:
        free = (
            sites.set_index("LokationMastrNummer")
            .gas_2037.sub(listed, fill_value=0)
            .clip(lower=0)
        )
        reported = (
            sites.set_index("LokationMastrNummer")
            .h2_site_staying.reindex(free.index)
            .gt(0)
        )
        for group in [free[reported], free[~reported]]:
            group = group[group > 0]
            if extra <= 1e-3 or group.empty:
                continue
            share = min(1, extra / group.sum())
            plants.append(
                pd.DataFrame(
                    {
                        "LokationMastrNummer": group.index,
                        "el_capacity": (group * share).values,
                        "site": "gas site 2037",
                    }
                )
            )
            extra -= group.sum() * share

    weighted = sites[sites.capacity >= MIN_SITE_CAPACITY]
    if extra > 1e-3 and weighted.weight.sum() > 0:
        plants.append(
            pd.DataFrame(
                {
                    "LokationMastrNummer": weighted.LokationMastrNummer.values,
                    "el_capacity": (
                        extra * weighted.weight / weighted.weight.sum()
                    ).values,
                    "site": "population",
                }
            )
        )
        extra = 0

    if not plants:
        return pd.DataFrame(), extra

    plants = pd.concat(plants, ignore_index=True)
    return plants[plants.el_capacity > 0], extra


def allocate_hydrogen_power_plants():
    """
    Allocate the hydrogen power plants of the reGon scenarios

    Returns
    -------
    None

    """
    from egon.data.datasets.power_plants import (
        EgonPowerPlants,
        assign_bus_id,
        assign_voltage_level_by_capacity,
        select_target,
    )

    sources, targets = load_sources_and_targets("PowerPlants")

    scenarios = [
        scn
        for scn in ["reGon2037", "reGon2045"]
        if scn in egon.data.config.settings()["egon-data"]["--scenarios"]
    ]
    if not scenarios:
        return

    db.execute_sql(
        f"""
        DELETE FROM {targets.tables['power_plants']}
        WHERE carrier = 'hydrogen'
        AND scenario IN ({", ".join(f"'{s}'" for s in scenarios)});
        """
    )

    sites = select_power_plant_sites()
    ehv_voronoi = db.select_geodataframe(
        f"SELECT bus_id, geom FROM {sources.tables['ehv_voronoi']}",
        epsg=4326,
    )
    sites = gpd.sjoin(sites, ehv_voronoi, how="left").drop(
        columns="index_right"
    )
    sites = sites.rename(columns={"bus_id": "ehv_bus_id"}).drop_duplicates(
        "LokationMastrNummer"
    )
    # Population weight of the sites of the minimum size, shared between
    # the sites of the same EHV substation area
    population = population_per_ehv_area()
    large = sites.capacity >= MIN_SITE_CAPACITY
    sites["weight"] = (
        sites.ehv_bus_id.map(population).fillna(0)
        / large.groupby(sites.ehv_bus_id).transform("sum").clip(lower=1)
    ).where(large, 0)

    # Placement of the listed projects, the same for all scenarios
    projects = db.select_dataframe(
        f"""
        SELECT name, federal_state, h2_conversion,
        c2037_capacity, c2045_capacity
        FROM {sources.tables['nep_conv']}
        WHERE carrier = 'hydrogen'
        AND scenario = 'reGon'
        AND c2037_capacity + c2045_capacity > 0
        """
    )
    projects["h2_conversion"] = projects.h2_conversion.fillna(False).astype(
        bool
    )
    placed = [
        place_listed_projects(
            projects[projects.federal_state == state],
            sites[sites.federal_state == state],
        )
        for state in sites.federal_state.dropna().unique()
        if (projects.federal_state == state).any()
    ]
    projects = (
        pd.concat(placed, ignore_index=True)
        if placed
        else projects.assign(LokationMastrNummer=None, site=None).iloc[:0]
    )

    for scn in scenarios:
        year = get_scenario_year(scn)
        capacity_column = f"c{year}_capacity"

        target = select_target("hydrogen", scn)
        target.index = target.index.map(FEDERAL_STATES)

        plants = []
        for state in sites.federal_state.dropna().unique():
            state_sites = sites[sites.federal_state == state]
            state_projects = projects[
                (projects.federal_state == state)
                & (projects[capacity_column] > 0)
            ].assign(el_capacity=lambda df: df[capacity_column])
            listed = state_projects.el_capacity.sum()
            total = target.get(state, 0)

            # Listed projects: scaled to the NEP capacity of the federal
            # state in 2037, only scaled down in 2045
            if listed > 0:
                factor = total / listed
                if year >= 2045:
                    factor = min(factor, 1)
                state_projects = state_projects.assign(
                    el_capacity=state_projects.el_capacity * factor,
                    siting="listed",
                )
                plants.append(state_projects)

            # Load-near plants: the rest of the capacity of the federal
            # state (2045, or a federal state without listed projects)
            extra = total - state_projects.el_capacity.sum()
            if extra > 1e-3:
                load_near, extra = allocate_load_near(
                    extra,
                    state_sites,
                    state_projects.groupby(
                        "LokationMastrNummer"
                    ).el_capacity.sum(),
                    year,
                )
                if not load_near.empty:
                    load_near["name"] = None
                    load_near["siting"] = "load-near"
                    plants.append(load_near)
            if extra > 1e-3:
                print(
                    f"{scn}: {extra:.0f} MW of hydrogen power plants in "
                    f"{state} have no site and are not allocated."
                )

        if not plants:
            continue

        plants = pd.concat(plants, ignore_index=True)

        # One plant per site, siting and choice of the site
        plants = (
            plants.groupby(["LokationMastrNummer", "siting", "site"])
            .agg(
                el_capacity=("el_capacity", "sum"),
                projects=("name", lambda x: ", ".join(x.dropna())),
            )
            .reset_index()
            .merge(
                sites[
                    [
                        "LokationMastrNummer",
                        "h2_units",
                        "units_2037",
                        "units_leaving",
                        "coal_units",
                        "geometry",
                    ]
                ],
                on="LokationMastrNummer",
            )
        )
        # Natural gas units of the list replaced at the site
        freed_units = (
            plants.units_leaving + ", " + plants.coal_units
        ).str.strip(", ")
        plants["replaced_units"] = np.select(
            [
                plants.site == "conversion",
                plants.site == "freed connection",
                plants.site == "gas site 2037",
            ],
            [plants.h2_units, freed_units, plants.units_2037],
            "",
        )
        plants = gpd.GeoDataFrame(plants, geometry="geometry", crs=4326)

        plants["voltage_level"] = None
        plants["voltage_level"] = assign_voltage_level_by_capacity(
            plants.rename(columns={"el_capacity": "Nettonennleistung"})
        )
        plants = assign_bus_id(plants, sources, drop_missing=True)

        summary = plants.groupby("site").el_capacity.sum().round()
        print(
            f"{scn}: {plants.el_capacity.sum():.0f} MW of hydrogen power "
            f"plants allocated, MW per choice of the site: "
            f"{summary.to_dict()}"
        )

        session = sessionmaker(bind=db.engine())()
        for _, row in plants.iterrows():
            session.add(
                EgonPowerPlants(
                    sources={
                        "el_capacity": (
                            "NEP 2025 list of power plants (Annex 1)"
                            if row.siting == "listed"
                            else "NEP Strom 2037/2045 (2025) capacity per "
                            "federal state minus listed projects"
                        ),
                        "siting": row.siting,
                        "site": row.site,
                    },
                    source_id={
                        "LokationMastrNummer": row.LokationMastrNummer,
                        "projects": row.projects,
                        "replaced_units": row.replaced_units,
                    },
                    carrier="hydrogen",
                    el_capacity=row.el_capacity,
                    voltage_level=row.voltage_level,
                    bus_id=row.bus_id,
                    scenario=scn,
                    geom=f"SRID=4326;POINT({row.geometry.x} {row.geometry.y})",
                )
            )
        session.commit()
