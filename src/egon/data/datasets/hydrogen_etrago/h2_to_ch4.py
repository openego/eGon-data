# -*- coding: utf-8 -*-
"""
Module containing the definition of the links between H2 and CH4 buses

In this module the functions used to define and insert the links between
H2 and CH4 buses into the database are to be found.
These links are modelling:

* Methanisation (carrier name: 'H2_to_CH4'): technology to produce CH4 from H2
* H2_feedin: Injection of H2 into the CH4 grid
* Steam Methane Reaction (SMR, carrier name: 'CH4_to_H2'): techonology
  to produce CH4 from H2

"""

from geoalchemy2.types import Geometry

from egon.data import db, config
from egon.data.datasets.etrago_setup import link_geom_from_buses
from egon.data.datasets.scenario_parameters import get_sector_parameters


def insert_h2_to_ch4_to_h2():
    """
    Inserts methanisation, feedin and SMR links into the database

    Define the potentials for methanisation and Steam Methane Reaction
    (SMR) modelled as extendable links as well as the H2 feedin
    capacities modelled as non extendable links and insert all of them
    into the database.
    These tree technologies are connecting CH4 and H2 buses only.

    The capacity of the H2_feedin links is considerated as constant and
    calculated as the sum of the capacities of the CH4 links connected
    to the CH4 bus multiplied by the H2 energy share allowed to be fed in.
    This share is calculated in the function :py:func:`H2_CH4_mix_energy_fractions`.

    Returns
    -------
    None

    """
    scenarios = config.settings()["egon-data"]["--scenarios"]

    if "status2019" in scenarios:
        scenarios.remove("status2019")

    for scn_name in scenarios:
        # Connect to local database
        engine = db.engine()

        # Select CH4 and corresponding H2 buses
        # No geometry required in this case!
        buses = db.select_dataframe(
            f"""
            SELECT * FROM grid.egon_etrago_ch4_h2 WHERE scn_name = '{scn_name}'
            """
        )

        methanation = buses.copy().rename(
            columns={"bus_H2": "bus0", "bus_CH4": "bus1"}
        )
        SMR = buses.copy().rename(
            columns={"bus_H2": "bus1", "bus_CH4": "bus0"}
        )

        # Delete old entries
        db.execute_sql(
            f"""
                DELETE FROM grid.egon_etrago_link WHERE "carrier" IN
                ('H2_to_CH4', 'H2_feedin', 'CH4_to_H2') AND scn_name = '{scn_name}'
                AND bus0 NOT IN (
                   SELECT bus_id FROM grid.egon_etrago_bus
                   WHERE scn_name = '{scn_name}' AND country != 'DE'
                ) AND bus1 NOT IN (
                   SELECT bus_id FROM grid.egon_etrago_bus
                   WHERE scn_name = '{scn_name}' AND country != 'DE'
                );
            """
        )

        scn_params = get_sector_parameters("gas", scn_name)

        technology = [methanation, SMR]
        links_names = ["H2_to_CH4", "CH4_to_H2"]

        if scn_name == "eGon2035":
            feed_in = methanation.copy()
            pipeline_capacities = db.select_dataframe(
                f"""
                SELECT bus0, bus1, p_nom FROM grid.egon_etrago_link
                WHERE scn_name = '{scn_name}' AND carrier = 'CH4'
                AND (
                    bus0 IN (
                        SELECT bus_id FROM grid.egon_etrago_bus
                        WHERE scn_name = '{scn_name}' AND country = 'DE'
                    ) OR bus1 IN (
                        SELECT bus_id FROM grid.egon_etrago_bus
                        WHERE scn_name = '{scn_name}' AND country = 'DE'
                    )
                );
                """
            )

            feed_in["p_nom"] = 0
            feed_in["p_nom_extendable"] = False
            # calculation of H2 energy share via volumetric share outsourced
            # in a mixture of H2 and CH4 with 15 %vol share
            H2_share = scn_params["H2_feedin_volumetric_fraction"]
            H2_energy_share = H2_CH4_mix_energy_fractions(H2_share)

            for bus in feed_in["bus1"].values:
                # calculate the total pipeline capacity connected to a specific bus
                nodal_capacity = pipeline_capacities.loc[
                    (pipeline_capacities["bus0"] == bus)
                    | (pipeline_capacities["bus1"] == bus),
                    "p_nom",
                ].sum()
                # multiply total pipeline capacity with H2 energy share corresponding
                # to volumetric share
                feed_in.loc[feed_in["bus1"] == bus, "p_nom"] = (
                    nodal_capacity * H2_energy_share
                )
            technology.append(feed_in)
            links_names.append("H2_feedin")

        # Write new entries
        for table, carrier in zip(technology, links_names):
            # set parameters according to carrier name
            table["carrier"] = carrier
            table["efficiency"] = scn_params["efficiency"][carrier]
            if carrier != "H2_feedin":
                table["p_nom_extendable"] = True
                table["capital_cost"] = scn_params["capital_cost"][carrier]
                table["lifetime"] = scn_params["lifetime"][carrier]
            new_id = db.next_etrago_id("link")
            table["link_id"] = range(new_id, new_id + len(table))

            table = link_geom_from_buses(table, scn_name)

            table.to_postgis(
                "egon_etrago_link",
                engine,
                schema="grid",
                index=False,
                if_exists="append",
                dtype={"topo": Geometry()},
            )


def H2_CH4_mix_energy_fractions(x, T=25, p=50):
    """
    Calculate the fraction of H2 with respect to energy in a H2 CH4 mixture.

    Given the volumetric fraction of H2 in a H2 and CH4 mixture, the fraction
    of H2 with respect to energy is calculated with the ideal gas mixture law.
    Beware, that changing the fraction of H2 changes the overall energy within
    a specific volume of the mixture. If H2 is fed into CH4, the pipeline
    capacity (based on energy) therefore decreases if the volumetric flow
    does not change. This effect is neglected in eGon. At 15 vol% H2 the
    decrease in capacity equals about 10 % if volumetric flow does not change.

    Parameters
    ----------
    x : float
        Volumetric fraction of H2 in the mixture
    T : int, optional
        Temperature of the mixture in °C, by default 25
    p : int, optional
        Pressure of the mixture in bar, by default 50

    Returns
    -------
    float
        Fraction of H2 in mixture with respect to energy (LHV)

    """

    # molar masses
    M_H2 = 0.00201588
    M_CH4 = 0.0160428

    # universal gas constant (fluid independent!)
    R_u = 8.31446261815324
    # individual gas constants
    R_H2 = R_u / M_H2
    R_CH4 = R_u / M_CH4

    # volume is fixed: 1m^3, use ideal gas law at 25 °C, 50 bar
    V = 1
    T += 273.15
    p *= 1e5
    # volumetric shares of gases (specify share of H2)
    V_H2 = x
    V_CH4 = 1 - x

    # calculate data of mixture
    M_mix = V_H2 * M_H2 + V_CH4 * M_CH4
    R_mix = R_u / M_mix
    m_mix = p * V / (R_mix * T)

    # calulate masses with volumetric shares at mixture pressure
    m_H2 = p * V_H2 / (R_H2 * T)
    m_CH4 = p * V_CH4 / (R_CH4 * T)

    msg = (
        "Consistency check faild, individual masses are not equal to sum of "
        "masses. Residual is: " + str(m_mix - m_H2 - m_CH4)
    )
    assert round(m_mix - m_H2 - m_CH4, 6) == 0.0, msg

    LHV = {"CH4": 50e6, "H2": 120e6}

    return m_H2 * LHV["H2"] / (m_H2 * LHV["H2"] + m_CH4 * LHV["CH4"])
