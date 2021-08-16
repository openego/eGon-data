from egon.data import db
import psycopg2
import numpy as np
import pandas as pd
import geopandas as gpd


def dsm_cts_ind_processing():
    def cts_data_import(con,cts_share):

        """
        Import CTS data necessary to identify DSM-potential.
            ----------
        con :
            Connection to database
        cts_share: float
            Share of cooling, ventilation and AC in CTS demand
        """

        # import load data

        sql = (
            "SELECT scn_name, load_id, bus, carrier FROM grid.egon_etrago_load" 
        )
        l = pd.read_sql_query(sql, con)

        sql = "SELECT scn_name, load_id, p_set FROM grid.egon_etrago_load_timeseries" 
        t = pd.read_sql_query(sql, con)

        # select CTS load

        l = l[l["carrier"] == "AC-cts"]
        load_ids = list(l["load_id"])
        idx = pd.Series(t.index)
        for index, row in t.iterrows():
            if row["load_id"] in load_ids:
                idx.drop(index, inplace=True)
        t.drop(index=idx, axis=1, inplace=True)

        # get relevant data

        dsm = pd.DataFrame(index=t.index)

        dsm["load_id"] = t["load_id"].copy()
        dsm["scn_name"] = t["scn_name"].copy()
        dsm["load_p_set"] = t["p_set"].copy()
        # q_set is empty

        dsm["bus"] = pd.Series(dtype=int)
        for index, row in dsm.iterrows():
            load_id = row["load_id"]
            scn_name = row["scn_name"]
            x = l[l["load_id"] == load_id]
            dsm.loc[index, "bus"] = x[x["scn_name"] == scn_name]["bus"].iloc[0]
            
        # timeseries for air conditioning, cooling and ventilation out of CTS-data

        # calculate share of timeseries
        timeseries = dsm["load_p_set"].copy()
        for index, liste in timeseries.iteritems():
            share = []
            for item in liste:
                share.append(float(item) * cts_share)
            timeseries.loc[index] = share
        dsm["p_set"] = timeseries.copy()

        return dsm
    
    def ind_osm_data_import(con,ind_share):

        """
        Import industry data per osm-area necessary to identify DSM-potential.
            ----------
        con :
            Connection to database
        ind_share: float
            Share of considered application in industry demand
        """

        # import load data

        sql = (
            "SELECT scn_name, bus, p_set FROM demand.egon_osm_ind_load_curves"
        )
        dsm = pd.read_sql_query(sql, con)
        
        # timeseries for cooling or ventilation out of industry-data

        # calculate share of timeseries
        timeseries = dsm["p_set"].copy()
        for index, liste in timeseries.iteritems():
            share = []
            for item in liste:
                share.append(float(item) * ind_share)
            timeseries.loc[index] = share
        dsm["p_set"] = timeseries.copy()

        return dsm
    
    def ind_sites_data_import(con,wz):

        """
        Import industry sites data necessary to identify DSM-potential.
            ----------
        con :
            Connection to database
        wz: int
            Wirtschaftszweig of considered industry application
        """

        # import load data

        sql = (
            "SELECT scn_name, bus, p_set, wz FROM demand.egon_sites_ind_load_curves"
        )
        dsm = pd.read_sql_query(sql, con)
        
        # select load for considered applications
        
        dsm = dsm[dsm['wz']==wz]
        
        return dsm

    def calculate_potentials(
        s_flex, s_util, s_inc, s_dec, delta_t, dsm
    ):

        """
        Calculate DSM-potential per bus.
        Parameters
            ----------
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
            List of existing buses with DSM-potential including timeseries of loads
        """

        timeseries = dsm["p_set"].copy()

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
            List of existing buses with DSM-potential including timeseries of loads
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
        sql = "SELECT bus_id, v_nom, scn_name, x, y, geom FROM grid.egon_etrago_bus" 
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
        sql = "SELECT link_id FROM grid.egon_etrago_link" 
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
        sql = "SELECT store_id FROM grid.egon_etrago_store" 
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

    def data_export(con, dsm_buses, dsm_links, dsm_stores, carrier):

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
        carrier: String
            Remark to be filled in column 'carrier' identifying DSM-potential
        """

        # dsm_buses

        insert_buses = gpd.GeoDataFrame(index=dsm_buses.index)
        insert_buses["version"] = "0.0.0"
        insert_buses["scn_name"] = dsm_buses["scn_name"]
        insert_buses["bus_id"] = dsm_buses["bus_id"]
        insert_buses["v_nom"] = dsm_buses["v_nom"]
        insert_buses["carrier"] = carrier
        insert_buses["x"] = dsm_buses["x"]
        insert_buses["y"] = dsm_buses["y"]
        insert_buses["geom"] = dsm_buses["geom"]
        insert_buses = insert_buses.set_geometry("geom").set_crs(4326)

        # insert into database
        insert_buses.to_postgis(
            "egon_etrago_bus", 
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
        insert_links["carrier"] = carrier
        insert_links["p_nom"] = dsm_links["p_nom"]

        # insert into database
        insert_links.to_sql(
            "egon_etrago_link", 
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
            "egon_etrago_link_timeseries", 
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
        insert_stores["carrier"] = carrier
        insert_stores["e_nom"] = dsm_stores["e_nom"]

        # insert into database
        insert_stores.to_sql(
            "egon_etrago_store", 
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
            "egon_etrago_store_timeseries", 
            con=db.engine(),
            schema="grid",
            if_exists="append",
            index=False,
        )

    def dsm_cts_ind(
        con=db.engine(),
        cts_share=0.22,
        ind_osm_share=0.039,
        # TODO: Festlegen der Anteile der Anwendungen an den WZ
        #ind_paper_share=1,
        #ind_cement_share=1,
        #ind_air_share=1
    ):

        """
        Execute methodology to create and implement components for DSM-CTS cooling, ventilation and AC.
        Parameters
            ----------
        con :
            Connection to database
        cts_share: float
            Share of cooling, ventilation and AC in CTS demand
        ind_cool_share: float
            Share of cooling in industry demand
        ind_vent_share: float
            Share of ventilation in industry demand

        """

        # CTS: cooling, ventilation and air conditioning
        
        print(' ')
        print('CTS: cooling, ventilation and air conditioning')
        print(' ')

        dsm = cts_data_import(con,cts_share)

        p_max, p_min, e_max, e_min = calculate_potentials(
            s_flex=0.5, s_util=0.67, s_inc=1, s_dec=0, delta_t=1, dsm=dsm
        )

        dsm_buses, dsm_links, dsm_stores = create_dsm_components(
            con, p_max, p_min, e_max, e_min, dsm
        )

        data_export(con, dsm_buses, dsm_links, dsm_stores, carrier="dsm-cts")
        
        # industry per osm-area: cooling and ventilation
        
        print(' ')
        print('industry per osm-area: cooling and ventilation')
        print(' ')
        
        dsm = ind_osm_data_import(con,ind_osm_share)

        p_max, p_min, e_max, e_min = calculate_potentials(
            s_flex=0.5, s_util=0.67, s_inc=0.9, s_dec=0.5, delta_t=1, dsm=dsm
        )
        
        # TODO: Überprüfung der Parameter nach Zusammenführung cool & vent

        dsm_buses, dsm_links, dsm_stores = create_dsm_components(
            con, p_max, p_min, e_max, e_min, dsm
        )

        data_export(con, dsm_buses, dsm_links, dsm_stores, carrier="dsm-ind-osm")
        
        # industry sites: paper
        
        print(' ')
        print('industry sites: paper')
        print(' ')
        
        dsm = ind_sites_data_import(con,wz=17)
        
        # TODO: Parameter festlegen für Pulp, Paper & Recycled Paper zusammengefasst als Anwendung Paper

        p_max, p_min, e_max, e_min = calculate_potentials(
            s_flex=0.5, s_util=0.85, s_inc=0.95, s_dec=0, delta_t=3, dsm=dsm
        )

        dsm_buses, dsm_links, dsm_stores = create_dsm_components(
            con, p_max, p_min, e_max, e_min, dsm
        )

        data_export(con, dsm_buses, dsm_links, dsm_stores, carrier="dsm-ind-sites")
        
        # industry sites: cement
        
        print(' ')
        print('industry sites: cement')
        print(' ')
        
        dsm = ind_sites_data_import(con,wz=23)

        p_max, p_min, e_max, e_min = calculate_potentials(
            s_flex=0.61, s_util=0.65, s_inc=0.95, s_dec=0, delta_t=4, dsm=dsm
        )

        dsm_buses, dsm_links, dsm_stores = create_dsm_components(
            con, p_max, p_min, e_max, e_min, dsm
        )

        data_export(con, dsm_buses, dsm_links, dsm_stores, carrier="dsm-ind-sites")
        
        # industry sites: air separation
        
        print(' ')
        print('industry sites: air separation')
        print(' ')
        
        dsm = ind_sites_data_import(con,wz=20)

        p_max, p_min, e_max, e_min = calculate_potentials(
            s_flex=0.3, s_util=0.86, s_inc=0.95, s_dec=0.4, delta_t=4, dsm=dsm
        )

        dsm_buses, dsm_links, dsm_stores = create_dsm_components(
            con, p_max, p_min, e_max, e_min, dsm
        )

        data_export(con, dsm_buses, dsm_links, dsm_stores, carrier="dsm-ind-sites")

    # TODO: Parameter überprüfen

    dsm_cts_ind()
