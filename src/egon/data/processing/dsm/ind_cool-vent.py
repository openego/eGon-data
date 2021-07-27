from egon.data import db
import psycopg2
import numpy as np
import pandas as pd
import geopandas as gpd


def dsm_ind_processing():
    def data_import(con):

        """
        Import data necessary to identify DSM-potential.
            ----------
        con :
            Connection to database
        """

        # import load data

        sql = (
            "SELECT scn_name, bus, p_set FROM demand.egon_osm_ind_load_curves"
        )
        dsm = pd.read_sql_query(sql, con)

        return dsm

    def calculate_potentials(
        ind_share, s_flex, s_util, s_inc, s_dec, delta_t, dsm
    ):

        """
        Calculate DSM-potential per bus.
        Parameters
            ----------
        ind_share: float
            Share of application of CTS demand
        s_flex: float
            Feasability factor to account for socio-technical restrictions
        s_util: float
            Average annual utilisation rate
        s_inc: float
            Shiftable share of installed capacity up to which load can be increased considering technical limitations
        s_dec: float
            Shiftable share of installed capacity up to which load can be decreased considering technical limitations
        delta_t: int
            Maximum shift duration in hours
        dsm: DataFrame
            List of existing buses with DSM-potential
        """

        # timeseries for cooling or ventilation out of industry-data

        # calculate share of timeseries
        timeseries = dsm["p_set"].copy()
        for index, liste in timeseries.iteritems():
            share = []
            for item in liste:
                share.append(float(item) * ind_share)
            timeseries.loc[index] = share

        # calculate scheduled load L(t)

        scheduled_load = timeseries.copy()

        for index, liste in scheduled_load.iteritems():
            share = []
            for item in liste:
                share.append(item * s_flex)
            scheduled_load.loc[index] = share

        # calculate maximum capacity Lambda

        # calculate energy annual requirement
        energy_annual = pd.Series(index=timeseries.index, dtype=float)
        for index, liste in timeseries.iteritems():
            energy_annual.loc[index] = sum(liste)

        # calculate Lambda
        lam = (energy_annual * s_flex) / (8760 * s_util)

        # calculation of P_max and P_min

        # P_max
        p_max = scheduled_load.copy()
        for index, liste in scheduled_load.iteritems():
            lamb = lam.loc[index]
            p = []
            for item in liste:
                value = lamb * s_inc - item
                if value < 0:
                    value = 0
                p.append(value)
            p_max.loc[index] = p

        # P_min
        p_min = scheduled_load.copy()
        for index, liste in scheduled_load.iteritems():
            lamb = lam.loc[index]
            p = []
            for item in liste:
                value = -(item - lamb * s_dec)
                if value > 0:
                    value = 0
                p.append(value)
            p_min.loc[index] = p

        # calculation of E_max and E_min

        e_max = scheduled_load.copy()
        e_min = scheduled_load.copy()

        for index, liste in scheduled_load.iteritems():
            emin = []
            emax = []
            for i in range(0, len(liste)):
                if i + delta_t > len(liste):
                    emax.append(
                        sum(liste[i : len(liste)])
                        + sum(liste[0 : delta_t - (len(liste) - i)])
                    )
                else:
                    emax.append(sum(liste[i : i + delta_t]))
                if i - delta_t < 0:
                    emin.append(
                        -1
                        * (
                            sum(liste[0:i])
                            + sum(liste[len(liste) - delta_t + i : len(liste)])
                        )
                    )
                else:
                    emin.append(-1 * sum(liste[i - delta_t : i]))
            e_max.loc[index] = emax
            e_min.loc[index] = emin

        return p_max, p_min, e_max, e_min

    def create_dsm_components(con, p_max, p_min, e_max, e_min, dsm):

        """
        Create components representing DSM.
        Parameters
            ----------
        con :
            Connection to database
        p_max: DataFrame
            Timeseries identifying maximum load increase
        p_min: DataFrame
            Timeseries identifying maximum load decrease
        e_max: DataFrame
            Timeseries identifying maximum energy amount to be preponed
        e_min: DataFrame
            Timeseries identifying maximum energy amount to be postponed
        dsm: DataFrame
            List of existing buses with DSM-potential
        """

        # calculate P_nom and P per unit
        p_nom = pd.Series(index=p_max.index, dtype=float)
        for index, row in p_max.iteritems():
            nom = max(max(row), abs(min(p_min.loc[index])))
            p_nom.loc[index] = nom
            new = [element / nom for element in row]
            p_max.loc[index] = new
            new = [element / nom for element in p_min.loc[index]]
            p_min.loc[index] = new

        # calculate E_nom and E per unit
        e_nom = pd.Series(index=p_min.index, dtype=float)
        for index, row in e_max.iteritems():
            nom = max(max(row), abs(min(e_min.loc[index])))
            e_nom.loc[index] = nom
            new = [element / nom for element in row]
            e_max.loc[index] = new
            new = [element / nom for element in e_min.loc[index]]
            e_min.loc[index] = new

        # add DSM-buses to "original" buses

        dsm_buses = gpd.GeoDataFrame(index=dsm.index)
        dsm_buses["original_bus"] = dsm["bus"].copy()
        dsm_buses["scn_name"] = dsm["scn_name"].copy()

        # get original buses and add copy relevant information
        sql = "SELECT bus_id, v_nom, scn_name, x, y, geom FROM grid.egon_pf_hv_bus"
        original_buses = gpd.GeoDataFrame.from_postgis(sql, con)

        # copy v_nom, x, y and geom
        v_nom = pd.Series(index=dsm_buses.index, dtype=float)
        x = pd.Series(index=dsm_buses.index, dtype=float)
        y = pd.Series(index=dsm_buses.index, dtype=float)
        geom = gpd.GeoSeries(index=dsm_buses.index)

        originals = dsm_buses["original_bus"].unique()
        for i in originals:
            o_bus = original_buses[original_buses["bus_id"] == i]
            dsm_bus = dsm_buses[dsm_buses["original_bus"] == i]
            v_nom[dsm_bus.index[0]] = o_bus.iloc[0]["v_nom"]
            x[dsm_bus.index[0]] = o_bus.iloc[0]["x"]
            y[dsm_bus.index[0]] = o_bus.iloc[0]["y"]
            geom[dsm_bus.index[0]] = o_bus.iloc[0]["geom"]
            v_nom[dsm_bus.index[1]] = o_bus.iloc[0]["v_nom"]
            x[dsm_bus.index[1]] = o_bus.iloc[0]["x"]
            y[dsm_bus.index[1]] = o_bus.iloc[0]["y"]
            geom[dsm_bus.index[1]] = o_bus.iloc[0]["geom"]

        dsm_buses["v_nom"] = v_nom
        dsm_buses["x"] = x
        dsm_buses["y"] = y
        dsm_buses["geom"] = geom

        # new bus_ids for DSM-buses
        max_id = original_buses["bus_id"].max()
        if np.isnan(max_id):
            max_id = 0
        dsm_id = max_id + 1
        bus_id = pd.Series(index=dsm_buses.index, dtype=int)
        bus_id.iloc[0 : int((len(bus_id) / 2))] = range(
            dsm_id, int((dsm_id + len(dsm_buses) / 2))
        )
        bus_id.iloc[int((len(bus_id) / 2)) : len(bus_id)] = range(
            dsm_id, int((dsm_id + len(dsm_buses) / 2))
        )
        dsm_buses["bus_id"] = bus_id

        # add links from "orignal" buses to DSM-buses

        dsm_links = pd.DataFrame(index=dsm_buses.index)
        dsm_links["original_bus"] = dsm_buses["original_bus"].copy()
        dsm_links["dsm_bus"] = dsm_buses["bus_id"].copy()
        dsm_links["scn_name"] = dsm_buses["scn_name"].copy()

        # set link_id
        sql = "SELECT link_id FROM grid.egon_pf_hv_link"
        max_id = pd.read_sql_query(sql, con)
        max_id = max_id["link_id"].max()
        if np.isnan(max_id):
            max_id = 0
        dsm_id = max_id + 1
        link_id = pd.Series(index=dsm_buses.index, dtype=int)
        link_id.iloc[0 : int((len(link_id) / 2))] = range(
            dsm_id, int((dsm_id + len(dsm_links) / 2))
        )
        link_id.iloc[int((len(link_id) / 2)) : len(link_id)] = range(
            dsm_id, int((dsm_id + len(dsm_links) / 2))
        )
        dsm_links["link_id"] = link_id

        # timeseries
        dsm_links["p_nom"] = p_nom
        dsm_links["p_min"] = p_min
        dsm_links["p_max"] = p_max

        # add stores

        dsm_stores = pd.DataFrame(index=dsm_buses.index)
        dsm_stores["bus"] = dsm_buses["bus_id"].copy()
        dsm_stores["scn_name"] = dsm_buses["scn_name"].copy()

        # set store_id
        sql = "SELECT store_id FROM grid.egon_pf_hv_store"
        max_id = pd.read_sql_query(sql, con)
        max_id = max_id["store_id"].max()
        if np.isnan(max_id):
            max_id = 0
        dsm_id = max_id + 1
        store_id = pd.Series(index=dsm_buses.index, dtype=int)
        store_id.iloc[0 : int((len(store_id) / 2))] = range(
            dsm_id, int((dsm_id + len(dsm_stores) / 2))
        )
        store_id.iloc[int((len(store_id) / 2)) : len(store_id)] = range(
            dsm_id, int((dsm_id + len(dsm_stores) / 2))
        )
        dsm_stores["store_id"] = store_id

        # timeseries
        dsm_stores["e_nom"] = e_nom
        dsm_stores["e_min"] = e_min
        dsm_stores["e_max"] = e_max

        return dsm_buses, dsm_links, dsm_stores

    def data_export(con, dsm_buses, dsm_links, dsm_stores):

        """
        Export new components to database.
        Parameters
            ----------
        con :
            Connection to database
        dsm_buses: DataFrame
            Buses representing locations of DSM-potential
        dsm_links: DataFrame
            Links connecting DSM-buses and DSM-stores
        dsm_stores: DataFrame
            Stores representing DSM-potential
        """

        # dsm_buses

        insert_buses = gpd.GeoDataFrame(index=dsm_buses.index)
        insert_buses["version"] = "0.0.0"
        insert_buses["scn_name"] = dsm_buses["scn_name"]
        insert_buses["bus_id"] = dsm_buses["bus_id"]
        insert_buses["v_nom"] = dsm_buses["v_nom"]
        insert_buses["carrier"] = "dsm-ind"
        insert_buses["x"] = dsm_buses["x"]
        insert_buses["y"] = dsm_buses["y"]
        insert_buses["geom"] = dsm_buses["geom"]
        insert_buses = insert_buses.set_geometry("geom").set_crs(4326)

        # insert into database
        insert_buses.to_postgis(
            "egon_pf_hv_bus", ### _etrago_ statt _pf_hv_
            con=db.engine(),
            schema="grid",
            if_exists="append",
            index=False,
        )

        # dsm_links

        insert_links = pd.DataFrame(index=dsm_links.index)
        insert_links["version"] = "0.0.0"
        insert_links["scn_name"] = dsm_links["scn_name"]
        insert_links["link_id"] = dsm_links["link_id"]
        insert_links["bus0"] = dsm_links["original_bus"]
        insert_links["bus1"] = dsm_links["dsm_bus"]
        insert_links["carrier"] = "dsm-ind"
        insert_links["p_nom"] = dsm_links["p_nom"]

        # insert into database
        insert_links.to_sql(
            "egon_pf_hv_link", ### _etrago_ statt _pf_hv_
            con=db.engine(),
            schema="grid",
            if_exists="append",
            index=False,
        )

        insert_links_timeseries = pd.DataFrame(index=dsm_links.index)
        insert_links_timeseries["version"] = "0.0.0"
        insert_links_timeseries["scn_name"] = dsm_links["scn_name"]
        insert_links_timeseries["link_id"] = dsm_links["link_id"]
        insert_links_timeseries["p_min_pu"] = dsm_links["p_min"]
        insert_links_timeseries["p_max_pu"] = dsm_links["p_max"]
        insert_links_timeseries["temp_id"] = 1

        # insert into database
        insert_links_timeseries.to_sql(
            "egon_pf_hv_link_timeseries", ### _etrago_ statt _pf_hv_
            con=db.engine(),
            schema="grid",
            if_exists="append",
            index=False,
        )

        # dsm_stores

        insert_stores = pd.DataFrame(index=dsm_stores.index)
        insert_stores["version"] = "0.0.0"
        insert_stores["scn_name"] = dsm_stores["scn_name"]
        insert_stores["store_id"] = dsm_stores["store_id"]
        insert_stores["bus"] = dsm_stores["bus"]
        insert_stores["carrier"] = "dsm-ind"
        insert_stores["e_nom"] = dsm_stores["e_nom"]

        # insert into database
        insert_stores.to_sql(
            "egon_pf_hv_store", ### _etrago_ statt _pf_hv_
            con=db.engine(),
            schema="grid",
            if_exists="append",
            index=False,
        )

        insert_stores_timeseries = pd.DataFrame(index=dsm_stores.index)
        insert_stores_timeseries["version"] = "0.0.0"
        insert_stores_timeseries["scn_name"] = dsm_stores["scn_name"]
        insert_stores_timeseries["store_id"] = dsm_stores["store_id"]
        insert_stores_timeseries["e_min_pu"] = dsm_stores["e_min"]
        insert_stores_timeseries["e_max_pu"] = dsm_stores["e_max"]
        insert_stores_timeseries["temp_id"] = 1

        # insert into database
        insert_stores_timeseries.to_sql(
            "egon_pf_hv_store_timeseries", ### _etrago_ statt _pf_hv_
            con=db.engine(),
            schema="grid",
            if_exists="append",
            index=False,
        )

    def dsm_ind_cool(
        con=db.engine(),
        ind_share=0.022,
        s_flex=0.63,
        s_util=0.67,
        s_inc=0.9,
        s_dec=0.5,
        delta_t=2,
    ):

        """
        Execute methodology to create and implement components for DSM-industry cooling.
        Parameters
            ----------
        con :
            Connection to database
        ind_share: float
            Share of cooling of CTS demand
        s_flex: float
            Feasability factor to account for socio-technical restrictions
        s_util: float
            Average annual utilisation rate
        s_inc: float
            Shiftable share of installed capacity up to which load can be increased considering technical limitations
        s_dec: float
            Shiftable share of installed capacity up to which load can be decreased considering technical limitations
        delta_t: int
            Maximum shift duration in hours
        """

        dsm = data_import(con)

        p_max, p_min, e_max, e_min = calculate_potentials(
            ind_share, s_flex, s_util, s_inc, s_dec, delta_t, dsm
        )

        dsm_buses, dsm_links, dsm_stores = create_dsm_components(
            con, p_max, p_min, e_max, e_min, dsm
        )

        data_export(con, dsm_buses, dsm_links, dsm_stores)
        
    def dsm_ind_vent(
        con=db.engine(),
        ind_share=0.017,
        s_flex=0.5,
        s_util=0.8,
        s_inc=1,
        s_dec=0.5,
        delta_t=1,
    ):

        """
        Execute methodology to create and implement components for DSM-industry ventilation.
        Parameters
            ----------
        con :
            Connection to database
        ind_share: float
            Share of ventilation of CTS demand
        s_flex: float
            Feasability factor to account for socio-technical restrictions
        s_util: float
            Average annual utilisation rate
        s_inc: float
            Shiftable share of installed capacity up to which load can be increased considering technical limitations
        s_dec: float
            Shiftable share of installed capacity up to which load can be decreased considering technical limitations
        delta_t: int
            Maximum shift duration in hours
        """

        dsm = data_import(con)

        p_max, p_min, e_max, e_min = calculate_potentials(
            ind_share, s_flex, s_util, s_inc, s_dec, delta_t, dsm
        )

        dsm_buses, dsm_links, dsm_stores = create_dsm_components(
            con, p_max, p_min, e_max, e_min, dsm
        )

        data_export(con, dsm_buses, dsm_links, dsm_stores)

    dsm_ind_cool()
    
    dsm_ind_vent()
