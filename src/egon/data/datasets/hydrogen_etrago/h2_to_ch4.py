# -*- coding: utf-8 -*-
"""
Module containing the definition of the CH4 grid to H2 links
"""

from egon.data import db


def insert_h2_to_ch4_to_h2():
    """
    Define methanisation, feed in and SMR capacities and insert in etrago_link.

    The potentials for methanisation and SMR are created between CH4 and H2
    buses of the CH4 grid.
    """
    # Connect to local database
    engine = db.engine()

    # Select CH4 and corresponding H2 buses
    # No geometry required in this case!
    buses = db.select_dataframe(f"""SELECT * FROM grid.egon_etrago_ch4_h2""")

    methanation = buses.copy().rename(
        columns={"bus_H2": "bus0", "bus_CH4": "bus1"}
    )
    SMR = buses.copy().rename(columns={"bus_H2": "bus1", "bus_CH4": "bus0"})
    feed_in = methanation.copy()

    # Cost basis specified correctly?
    methanation["carrier"] = "H2-to-CH4"
    methanation["capital_cost"] = 1e6  # 1000€ / kW
    methanation["efficiency_fixed"] = 0.6

    # 10.1016/j.ijhydene.2017.05.219
    # SALKUYEH_2017_Techno-economic analysis and life cycle assessment of hydrogen production from natural gas using curernt and emerging technologies
    SMR["carrier"] = "CH4-to-H2"
    # capital cost ($ 2016) per MW hydrogen: 0.383  Mio$ / MW
    # 1.0537 $ 2016 (eoy) = 1 €
    # 1 € (2016) = 1 € * 1.025 ** 14 = 1.41 € (2030) check interest rate value (p. 35 Abschlussbericht eGo)
    # -> for 2035: ** 19
    # -> for 2050: ** 34
    SMR["capital_cost"] = 383540 / 1.0537 * 1.41  # pp. 18903-18904
    # CO2 emissions?
    SMR["efficiency_fixed"] = 0.66  # pp. 18903-18904

    # How to implement feed in restriction?
    feed_in["carrier"] = "H2-feedin"
    feed_in["capital_cost"] = 0
    feed_in["efficiency_fixed"] = 1

    # Delete old entries
    db.execute_sql(
        """
        DELETE FROM grid.egon_etrago_link WHERE "carrier" IN
        ('H2-to-CH4', 'H2-feedin', 'CH4-to-H2');
        """
    )

    # Write new entries
    for table in [methanation, SMR, feed_in]:

        new_id = db.next_etrago_id("link")
        table["link_id"] = range(new_id, new_id + len(table))

        table.to_sql(
            "egon_etrago_link",
            engine,
            schema="grid",
            index=False,
            if_exists="append",
        )
