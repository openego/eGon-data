"""The central module containing all code dealing with importing data from
the pysa-eur-sec scenario parameter creation
"""

import pypsa
import pandas as pd
import numpy as np
import yaml

from pathlib import Path

from egon.data import __path__
from egon.data.datasets.scenario_parameters.parameters import (
    annualize_capital_costs,
)


def update_electrical_timeseries_germany(network):
    """Replace electrical demand time series in Germany with data from egon-data

    Parameters
    ----------
    network : pypsa.Network
        Network including demand time series from pypsa-eur

    Returns
    -------
    network : pypsa.Network
        Network including electrical demand time series in Germany from egon-data

    """

    df = pd.read_csv(
        "input-pypsa-eur-sec/electrical_demand_timeseries_DE_eGon100RE.csv"
    )

    network.loads_t.p_set.loc[:, "DE1 0"] = (
        df["residential_and_service"] + df["industry"]
    )

    return network


def geothermal_district_heating(network):
    """Add the option to build geothermal power plants in district heating in Germany

    Parameters
    ----------
    network : pypsa.Network
        Network from pypsa-eur without geothermal generators

    Returns
    -------
    network : pypsa.Network
        Updated network with geothermal generators

    """

    costs_and_potentials = pd.read_csv(
        "input-pypsa-eur-sec/geothermal_potential_germany.csv"
    )

    network.add("Carrier", "urban central geo thermal")

    for i, row in costs_and_potentials.iterrows():
        # Set lifetime of geothermal plant to 30 years based on:
        # Ableitung eines Korridors für den Ausbau der erneuerbaren Wärme im Gebäudebereich,
        # Beuth Hochschule für Technik, Berlin ifeu – Institut für Energie- und Umweltforschung Heidelberg GmbH
        # Februar 2017
        lifetime_geothermal = 30

        network.add(
            "Generator",
            f"DE1 0 urban central geo thermal {i}",
            bus="DE1 0 urban central heat",
            carrier="urban central geo thermal",
            p_nom_extendable=True,
            p_nom_max=row["potential [MW]"],
            capital_cost=annualize_capital_costs(
                row["cost [EUR/kW]"] * 1e6, lifetime_geothermal, 0.07
            ),
        )
    return network


def h2_overground_stores(network):
    """Add hydrogen overground stores to each hydrogen node

    In pypsa-eur, only countries without the potential of underground hydrogen
    stores have to option to build overground hydrogen tanks.
    Overground stores are more expensive, but are not resitcted by the geological
    potential. To allow higher hydrogen store capacities in each country, optional
    hydogen overground tanks are also added to node with a potential for
    underground stores.

    Parameters
    ----------
    network : pypsa.Network
        Network without hydrogen overground stores at each hydrogen node

    Returns
    -------
    network : pypsa.Network
        Network with hydrogen overground stores at each hydrogen node

    """

    underground_h2_stores = network.stores[
        (network.stores.carrier == "H2 Store")
        & (network.stores.e_nom_max != np.inf)
    ]

    overground_h2_stores = network.stores[
        (network.stores.carrier == "H2 Store")
        & (network.stores.e_nom_max == np.inf)
    ]

    network.madd(
        "Store",
        underground_h2_stores.bus + " overground Store",
        bus=underground_h2_stores.bus.values,
        e_nom_extendable=True,
        e_cyclic=True,
        carrier="H2 Store",
        capital_cost=overground_h2_stores.capital_cost.mean(),
    )

    return network


def update_heat_timeseries_germany(network):
    network.loads
    # Import heat demand curves for Germany from eGon-data
    df_egon_heat_demand = pd.read_csv(
        "input-pypsa-eur-sec/heat_demand_timeseries_DE_eGon100RE.csv"
    )

    # Replace heat demand curves in Germany with values from eGon-data
    network.loads_t.p_set.loc[
        :, "DE1 0 residential rural heat"
    ] = df_egon_heat_demand.loc[:, "residential rural"]

    network.loads_t.p_set.loc[
        :, "DE1 0 services rural heat"
    ] = df_egon_heat_demand.loc[:, "services rural"]

    network.loads_t.p_set.loc[
        :, "DE1 0 urban central heat"
    ] = df_egon_heat_demand.loc[:, "urban central"]

    return network


def drop_biomass(network):
    carrier = "biomass"

    for c in network.iterate_components():
        network.mremove(c.name, c.df[c.df.index.str.contains(carrier)].index)
    return network


def drop_urban_decentral_heat(network):
    carrier = "urban decentral"

    for c in network.iterate_components():
        network.mremove(c.name, c.df[c.df.index.str.contains(carrier)].index)
    return network


def district_heating_shares(network):
    df = pd.read_csv(
        "data_bundle_powerd_data/district_heating_shares_egon.csv"
    ).set_index("country_code")

    heat_demand_per_country = (
        network.loads_t.p_set[
            network.loads[
                (network.loads.carrier.str.contains("heat"))
                & network.loads.index.isin(network.loads_t.p_set.columns)
            ].index
        ]
        .groupby(network.loads.bus.str[:5], axis=1)
        .sum()
    )

    for country in heat_demand_per_country.columns:
        network.loads_t.p_set[
            f"{country} urban central heat"
        ] = heat_demand_per_country.loc[:, country].mul(
            df.loc[country[:2]].values[0]
        )
        network.loads_t.p_set[
            f"{country} residential rural heat"
        ] = heat_demand_per_country.loc[:, country].mul(
            (1 - df.loc[country[:2]].values[0])
        )
    return network


def drop_new_gas_pipelines(network):
    network.mremove(
        "Link",
        network.links[
            network.links.index.str.contains("gas pipeline new")
        ].index,
    )

    return network


def drop_fossil_gas(network):
    network.mremove(
        "Store", network.stores[network.stores.carrier == "gas"].index
    )

    return network


def rual_heat_technologies(network):
    network.mremove(
        "Link",
        network.links[
            network.links.index.str.contains("rural gas boiler")
        ].index,
    )

    network.mremove(
        "Generator",
        network.generators[
            network.generators.carrier.str.contains("rural solar thermal")
        ].index,
    )

    network.links.loc["DE1 0 services rural ground heat pump", "p_nom_min"] = 0


def execute():
    with open(
        __path__[0] + "/datasets/pypsaeursec/config.yaml", "r"
    ) as stream:
        data_config = yaml.safe_load(stream)

    network_path = (
        Path(".")
        / "run-pypsa-eur"
        / "pypsa-eur"
        / "results"
        / data_config["run"]["name"]
        / "prenetworks"
        / f"elec_s_{data_config['scenario']['clusters'][0]}"
        f"_l{data_config['scenario']['ll'][0]}"
        f"_{data_config['scenario']['opts'][0]}"
        f"_{data_config['scenario']['sector_opts'][0]}"
        f"_{data_config['scenario']['planning_horizons'][0]}.nc"
    )

    network = pypsa.Network(network_path)

    network = drop_biomass(network)

    network = drop_urban_decentral_heat(network)

    network = district_heating_shares(network)

    network = update_heat_timeseries_germany(network)

    network = update_electrical_timeseries_germany(network)

    network = geothermal_district_heating(network)

    network = h2_overground_stores(network)

    network = drop_new_gas_pipelines(network)

    network = drop_fossil_gas(network)

    network.export_to_netcdf(network_path)
