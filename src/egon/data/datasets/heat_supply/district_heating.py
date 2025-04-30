"""The central module containing all code dealing with heat supply
for district heating areas.

"""
import geopandas as gpd
import pandas as pd
from egon.data import config, db
from egon.data.datasets.heat_supply.geothermal import calc_geothermal_costs


def capacity_per_district_heating_category(district_heating_areas, scenario):
    """Calculates target values per district heating category and technology

    Parameters
    ----------
    district_heating_areas : geopandas.geodataframe.GeoDataFrame
        District heating areas per scenario
    scenario : str
        Name of the scenario

    Returns
    -------
    capacity_per_category : pandas.DataFrame
        Installed capacities per technology and size category

    """
    sources = config.datasets()["heat_supply"]["sources"]

    target_values = db.select_dataframe(
        f"""
        SELECT capacity, split_part(carrier, 'urban_central_', 2) as technology
        FROM {sources['scenario_capacities']['schema']}.
        {sources['scenario_capacities']['table']}
        WHERE carrier IN (
            'urban_central_heat_pump',
            'urban_central_resistive_heater',
            'urban_central_geo_thermal',
            'urban_central_solar_thermal_collector')
        AND scenario_name = '{scenario}'
        """,
        index_col="technology",
    )

    capacity_per_category = pd.DataFrame(
        index=["small", "medium", "large"],
        columns=[
            "solar_thermal_collector",
            "resistive_heater",
            "heat_pump",
            "geo_thermal",
            "demand",
        ],
    )

    capacity_per_category.demand = district_heating_areas.groupby(
        district_heating_areas.category
    ).demand.sum()

    capacity_per_category.loc[
        ["small", "medium"], "solar_thermal_collector"
    ] = (
        target_values.capacity["solar_thermal_collector"]
        * capacity_per_category.demand
        / capacity_per_category.demand[["small", "medium"]].sum()
    )

    capacity_per_category.loc[:, "heat_pump"] = (
        target_values.capacity["heat_pump"]
        * capacity_per_category.demand
        / capacity_per_category.demand.sum()
    )

    capacity_per_category.loc[:, "resistive_heater"] = (
        target_values.capacity["resistive_heater"]
        * capacity_per_category.demand
        / capacity_per_category.demand.sum()
    )

    capacity_per_category.loc["large", "geo_thermal"] = target_values.capacity[
        "geo_thermal"
    ]

    return capacity_per_category


def set_technology_data():
    """Set data per technology according to Kurzstudie KWK

    Returns
    -------
    pandas.DataFrame
        List of parameters per technology

    """
    return pd.DataFrame(
        index=[
            "CHP",
            "solar_thermal_collector",
            "heat_pump",
            "geo_thermal",
            "resistive_heater",
        ],
        columns=["estimated_flh", "priority"],
        data={
            "estimated_flh": [8760, 1330, 7000, 3000, 400],
            "priority": [5, 3, 2, 4, 1],
        },
    )


def select_district_heating_areas(scenario):
    """Selects district heating areas per scenario and assigns size-category

    Parameters
    ----------
    scenario : str
        Name of the scenario

    Returns
    -------
    district_heating_areas : geopandas.geodataframe.GeoDataFrame
        District heating areas per scenario

    """

    sources = config.datasets()["heat_supply"]["sources"]

    max_demand_medium_district_heating = 96000

    max_demand_small_district_heating = 2400

    district_heating_areas = db.select_geodataframe(
        f"""
         SELECT id as district_heating_id,
         residential_and_service_demand as demand,
         geom_polygon as geom
         FROM {sources['district_heating_areas']['schema']}.
        {sources['district_heating_areas']['table']}
         WHERE scenario = '{scenario}'
         """,
        index_col="district_heating_id",
    )

    district_heating_areas["category"] = "large"

    district_heating_areas.loc[
        district_heating_areas[
            district_heating_areas.demand < max_demand_medium_district_heating
        ].index,
        "category",
    ] = "medium"

    district_heating_areas.loc[
        district_heating_areas[
            district_heating_areas.demand < max_demand_small_district_heating
        ].index,
        "category",
    ] = "small"

    return district_heating_areas


def cascade_per_technology(
    scenario,
    areas,
    technologies,
    capacity_per_category,
    size_dh,
    max_geothermal_costs=2,
):
    """Add plants of one technology suppliing district heating

    Parameters
    ----------
    areas : geopandas.geodataframe.GeoDataFrame
        District heating areas which need to be supplied
    technologies : pandas.DataFrame
        List of supply technologies and their parameters
    capacity_per_category : pandas.DataFrame
        Target installed capacities per size-category
    size_dh : str
        Category of the district heating areas
    max_geothermal_costs : float, optional
        Maxiumal costs of MW geothermal in EUR/MW. The default is 2.

    Returns
    -------
    areas : geopandas.geodataframe.GeoDataFrame
        District heating areas which need additional supply technologies
    technologies : pandas.DataFrame
        List of supply technologies and their parameters
    append_df : pandas.DataFrame
        List of plants per district heating grid for the selected technology

    """
    sources = config.datasets()["heat_supply"]["sources"]

    tech = technologies[technologies.priority == technologies.priority.max()]

    # Assign CHP plants inside district heating area
    # TODO: This has to be updaten when all chp plants are available!
    if tech.index == "CHP":
        # Select chp plants from database
        gdf_chp = db.select_geodataframe(
            f"""SELECT a.geom, th_capacity as capacity, c.area_id
            FROM {sources['chp']['schema']}.
            {sources['chp']['table']} a,
            {sources['district_heating_areas']['schema']}.
            {sources['district_heating_areas']['table']} c
            WHERE a.district_heating = True
            AND a.district_heating_area_id = c.area_id
            AND a.scenario = '{scenario}'
            AND c.scenario = '{scenario}'
            """
        )

        gdf_chp = gdf_chp[gdf_chp.area_id.isin(areas.index)]

        append_df = (
            pd.DataFrame(gdf_chp.groupby("area_id").capacity.sum())
            .reset_index()
            .rename({"area_id": "district_heating_id"}, axis=1)
        )

    # Distribute solar thermal and heatpumps linear to remaining demand.
    # Geothermal plants are distributed to areas with geothermal potential.
    if tech.index in [
        "resistive_heater",
        "solar_thermal_collector",
        "heat_pump",
        "geo_thermal",
    ]:
        if tech.index == "geo_thermal":
            # Select areas with geothermal potential considering costs
            gdf_geothermal = calc_geothermal_costs(max_geothermal_costs)
            # Select areas which intersect with district heating areas
            join = gpd.sjoin(
                gdf_geothermal.to_crs(4326), areas, rsuffix="area"
            )
            # Calculate share of installed capacity
            share_per_area = (
                join.groupby("index_area")["remaining_demand"].sum()
                / join["remaining_demand"].sum().sum()
            )

        else:
            share_per_area = (
                areas["remaining_demand"] / areas["remaining_demand"].sum()
            )
        # Prepare list of heat supply technologies
        append_df = pd.DataFrame(
            (share_per_area).mul(
                capacity_per_category.loc[size_dh, tech.index].values[0]
            )
        ).reset_index()
        # Rename columns
        append_df.rename(
            {
                "index_area": "district_heating_id",
                "remaining_demand": "capacity",
            },
            axis=1,
            inplace=True,
        )
    # Add heat supply to overall list
    if append_df.size > 0:
        append_df["carrier"] = tech.index[0]
        append_df["category"] = size_dh
        areas.loc[
            append_df.district_heating_id, "remaining_demand"
        ] -= append_df.set_index("district_heating_id").capacity.mul(
            tech.estimated_flh.values[0]
        )
    # Select district heating areas which need an additional supply technology
    areas = areas[areas.remaining_demand >= 0]

    # Delete inserted technology from list
    technologies = technologies.drop(tech.index)

    return areas, technologies, append_df


def cascade_heat_supply(scenario, plotting=True):
    """Assigns supply strategy for ditsrict heating areas.

    Different technologies are selected for three categories of district
    heating areas (small, medium and large annual demand).
    The technologies are priorized according to
    Flexibilisierung der Kraft-Wärme-Kopplung; 2017;
    Forschungsstelle für Energiewirtschaft e.V. (FfE)

    Parameters
    ----------
    scenario : str
        Name of scenario
    plotting : bool, optional
        Choose if district heating supply is plotted. The default is True.

    Returns
    -------
    resulting_capacities : pandas.DataFrame
        List of plants per district heating grid

    """

    # Select district heating areas from database
    district_heating_areas = select_district_heating_areas(scenario)

    # Select technolgies per district heating size
    if "status" not in scenario:
        map_dh_technologies = {
            "small": [
                "CHP",
                "solar_thermal_collector",
                "heat_pump",
                "resistive_heater",
            ],
            "medium": [
                "CHP",
                "solar_thermal_collector",
                "heat_pump",
                "resistive_heater",
            ],
            "large": ["CHP", "geo_thermal", "heat_pump", "resistive_heater"],
        }

        # Assign capacities per district heating category
        capacity_per_category = capacity_per_district_heating_category(
            district_heating_areas, scenario
        )
    else:
        map_dh_technologies = {
            "small": [
                "CHP",
            ],
            "medium": [
                "CHP",
            ],
            "large": ["CHP"],
        }
        capacity_per_category = pd.DataFrame()

    # Initalize Dataframe for results
    resulting_capacities = pd.DataFrame(
        columns=["district_heating_id", "carrier", "capacity", "category"]
    )

    # Set technology data according to Kurzstudie KWK, NEP 2021
    technology_data = set_technology_data()

    for size_dh in ["small", "medium", "large"]:
        # Select areas in size-category
        areas = district_heating_areas[
            district_heating_areas.category == size_dh
        ].to_crs(4326)

        # Set remaining_demand to demand for first iteration
        areas["remaining_demand"] = areas["demand"]

        # Select technologies which can be use in this size-category
        technologies = technology_data.loc[map_dh_technologies[size_dh], :]

        # Assign new supply technologies to district heating areas
        # as long as the demand is not covered and there are technologies left
        while (len(technologies) > 0) and (len(areas) > 0):
            areas, technologies, append_df = cascade_per_technology(
                scenario, areas, technologies, capacity_per_category, size_dh
            )

            resulting_capacities = pd.concat(
                [resulting_capacities, append_df], ignore_index=True
            )

    # Plot results per district heating area
    if plotting:
        plot_heat_supply(resulting_capacities)

    return gpd.GeoDataFrame(
        resulting_capacities,
        geometry=district_heating_areas.geom[
            resulting_capacities.district_heating_id
        ].centroid.values,
    )


def backup_gas_boilers(scenario):
    """Adds backup gas boilers to district heating grids.

    Parameters
    ----------
    scenario : str
        Name of the scenario.

    Returns
    -------
    Geopandas.GeoDataFrame
        List of gas boilers for district heating

    """

    # Select district heating areas from database
    district_heating_areas = select_district_heating_areas(scenario)

    return gpd.GeoDataFrame(
        data={
            "district_heating_id": district_heating_areas.index,
            "capacity": district_heating_areas.demand.div(100),
            "carrier": "gas_boiler",
            "category": district_heating_areas.category,
            "geometry": district_heating_areas.geom.centroid,
            "scenario": scenario,
        }
    )


def backup_resistive_heaters(scenario):
    """Adds backup resistive heaters to district heating grids to
    meet target values of installed capacities.

    Parameters
    ----------
    scenario : str
        Name of the scenario.

    Returns
    -------
    Geopandas.GeoDataFrame
        List of gas boilers for district heating

    """

    # Select district heating areas from database
    district_heating_areas = select_district_heating_areas(scenario)

    # Select target value
    target_value = db.select_dataframe(
        f"""
        SELECT capacity
        FROM supply.egon_scenario_capacities
        WHERE carrier = 'urban_central_resistive_heater'
        AND scenario_name = '{scenario}'
        """
    ).capacity[0]

    distributed = db.select_dataframe(
        f"""
        SELECT SUM(capacity) as capacity
        FROM supply.egon_district_heating
        WHERE carrier = 'resistive_heater'
        AND scenario = '{scenario}'
        """
    ).capacity[0]

    if not distributed:
        distributed = 0

    if target_value > distributed:
        df = gpd.GeoDataFrame(
            data={
                "district_heating_id": district_heating_areas.index,
                "capacity": district_heating_areas.demand.div(
                    district_heating_areas.demand.sum()
                ).mul(target_value - distributed),
                "carrier": "resistive_heater",
                "category": district_heating_areas.category,
                "geometry": district_heating_areas.geom.centroid,
                "scenario": scenario,
            }
        )

    else:
        df = gpd.GeoDataFrame()

    return df


def plot_heat_supply(resulting_capacities):
    from matplotlib import pyplot as plt

    district_heating_areas = select_district_heating_areas("eGon2035")

    for c in ["CHP", "solar_thermal_collector", "geo_thermal", "heat_pump"]:
        district_heating_areas[c] = (
            resulting_capacities[resulting_capacities.carrier == c]
            .set_index("district_heating_id")
            .capacity
        )

        fig, ax = plt.subplots(1, 1)
        district_heating_areas.boundary.plot(
            linewidth=0.2, ax=ax, color="black"
        )
        district_heating_areas.plot(
            ax=ax,
            column=c,
            cmap="magma_r",
            legend=True,
            legend_kwds={
                "label": f"Installed {c} in MW",
                "orientation": "vertical",
            },
        )
        plt.savefig(f"plots/heat_supply_{c}.png", dpi=300)
