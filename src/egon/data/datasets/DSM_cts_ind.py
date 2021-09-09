import egon.data.config
from egon.data import db
import psycopg2
import numpy as np
import pandas as pd
import geopandas as gpd
from egon.data.datasets.electricity_demand.temporal import calc_load_curve
from egon.data.datasets.industry.temporal import identify_bus
from egon.data.datasets import Dataset


class dsm_Potential(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="DSM_potentials",
            version="0.0.0.dev",
            dependencies=dependencies,
            tasks=(dsm_cts_ind_processing),
        )


def dsm_cts_ind_processing():
    def cts_data_import(con, cts_cool_vent_ac_share):

        """
        Import CTS data necessary to identify DSM-potential.
            ----------
        con :
            Connection to database
        cts_share: float
            Share of cooling, ventilation and AC in CTS demand
        """

        # import load data

        sources = egon.data.config.datasets()["DSM_CTS_industry"]["sources"][
            "cts_loadcurves"
        ]

        ts = db.select_dataframe(
            f"""SELECT subst_id, scn_name, p_set FROM
            {sources['schema']}.{sources['table']}"""
        )

        # get relevant data

        dsm = pd.DataFrame(index=ts.index)

        dsm["bus"] = ts["subst_id"].copy()
        dsm["scn_name"] = ts["scn_name"].copy()
        dsm["p_set"] = ts["p_set"].copy()

        # timeseries for air conditioning, cooling and ventilation out of CTS-data

        # calculate share of timeseries
        timeseries = dsm["p_set"].copy()
        for index, liste in timeseries.iteritems():
            share = []
            for item in liste:
                share.append(float(item) * cts_cool_vent_ac_share)
            timeseries.loc[index] = share
        dsm["p_set"] = timeseries.copy()

        return dsm

    def ind_osm_data_import(con, ind_vent_cool_share):

        """
        Import industry data per osm-area necessary to identify DSM-potential.
            ----------
        con :
            Connection to database
        ind_share: float
            Share of considered application in industry demand
        """

        # import load data

        sources = egon.data.config.datasets()["DSM_CTS_industry"]["sources"][
            "ind_osm_loadcurves"
        ]

        dsm = db.select_dataframe(
            f"""SELECT bus, scn_name, p_set FROM
            {sources['schema']}.{sources['table']}"""
        )

        # timeseries for cooling or ventilation out of industry-data

        # calculate share of timeseries
        timeseries = dsm["p_set"].copy()
        for index, liste in timeseries.iteritems():
            share = []
            for item in liste:
                share.append(float(item) * ind_vent_cool_share)
            timeseries.loc[index] = share
        dsm["p_set"] = timeseries.copy()

        return dsm

    def ind_sites_vent_data_import(con, ind_vent_share, wz):

        """
        Import industry sites necessary to identify DSM-potential.
            ----------
        con :
            Connection to database
        ind_vent_share: float
            Share of considered application in industry demand
        wz: int
            Wirtschaftszweig to be considered within industry sites
        """

        # import load data

        sources = egon.data.config.datasets()["DSM_CTS_industry"]["sources"][
            "ind_sites_loadcurves"
        ]

        dsm = db.select_dataframe(
            f"""SELECT bus, scn_name, p_set, wz FROM
            {sources['schema']}.{sources['table']}"""
        )

        # select load for considered applications

        dsm = dsm[dsm["wz"] == wz]        

        # calculate share of timeseries
        timeseries = dsm["p_set"].copy()
        for index, liste in timeseries.iteritems():
            share = []
            for item in liste:
                share.append(float(item) * ind_vent_share)
            timeseries.loc[index] = share
        dsm["p_set"] = timeseries.copy()

        return dsm

    def ind_sites_data_import(con):

        """
        Import industry sites data necessary to identify DSM-potential.
            ----------
        con :
            Connection to database
        """

        def calc_ind_site_timeseries(scenario):

            # calculate timeseries per site
            # -> using code from egon.data.datasets.industry.temporal: calc_load_curves_ind_sites

            # select demands per industrial site including the subsector information

            source1 = egon.data.config.datasets()["DSM_CTS_industry"][
                "sources"
            ]["demandregio_ind_sites"]

            demands_ind_sites = db.select_dataframe(
                f"""SELECT industrial_sites_id, wz, demand
                    FROM {source1['schema']}.{source1['table']}
                    WHERE scenario = '{scenario}'
                    AND demand > 0
                    """
            ).set_index(["industrial_sites_id"])

            # select industrial sites as demand_areas from database

            source2 = egon.data.config.datasets()["DSM_CTS_industry"][
                "sources"
            ]["ind_sites"]

            demand_area = db.select_geodataframe(
                f"""SELECT id, geom FROM
                    {source2['schema']}.{source2['table']}""",
                index_col="id",
                geom_col="geom",
                epsg=3035,
            )

            # replace entries to bring it in line with demandregio's subsector definitions
            demands_ind_sites.replace(1718, 17, inplace=True)
            share_wz_sites = demands_ind_sites.copy()

            # create additional df on wz_share per industrial site, which is always set to one
            # as the industrial demand per site is subsector specific
            share_wz_sites.demand = 1
            share_wz_sites.reset_index(inplace=True)

            share_transpose = pd.DataFrame(
                index=share_wz_sites.industrial_sites_id.unique(),
                columns=share_wz_sites.wz.unique(),
            )
            share_transpose.index.rename("industrial_sites_id", inplace=True)
            for wz in share_transpose.columns:
                share_transpose[wz] = (
                    share_wz_sites[share_wz_sites.wz == wz]
                    .set_index("industrial_sites_id")
                    .demand
                )

            # calculate load curves
            load_curves = calc_load_curve(
                share_transpose, demands_ind_sites["demand"]
            )

            # identify bus per industrial site
            curves_bus = identify_bus(load_curves, demand_area)
            curves_bus.index = curves_bus["id"].astype(int)

            # initialize dataframe to be returned
            ts = gpd.GeoDataFrame(
                index=curves_bus["id"].astype(int), data=demand_area["geom"], 
                geometry="geom", crs=3035
            )
            ts["subst_id"] = curves_bus["subst_id"].astype(int)
            curves_bus.drop({"id", "subst_id"}, axis=1, inplace=True)
            ts["scenario_name"] = scenario
            ts["p_set"] = curves_bus.values.tolist()

            return ts

        def relate_to_Schmidt_sites(dsm):

            # import industrial sites by Schmidt

            source = egon.data.config.datasets()["DSM_CTS_industry"][
                "sources"
            ]["ind_sites_schmidt"]

            schmidt = db.select_geodataframe(
                f"""SELECT application, geom FROM
                    {source['schema']}.{source['table']}""",
                geom_col="geom",
                epsg=3035,
            )

            dsm = gpd.overlay(dsm, schmidt)

            dsm.rename(
                columns={"scenario_name": "scn_name", "subst_id": "bus"},
                inplace=True,
            )

            return dsm

        dsm_2035 = calc_ind_site_timeseries("eGon2035")
        dsm_2035.reset_index(inplace=True)

        dsm_100 = calc_ind_site_timeseries("eGon100RE")
        dsm_100.reset_index(inplace=True)
        dsm_100.index = range(len(dsm_2035), (len(dsm_2035) + len((dsm_100))))

        dsm = dsm_2035.append(dsm_100)

        dsm = relate_to_Schmidt_sites(dsm)
        
        dsm = pd.DataFrame(dsm)
        dsm.drop("geometry", axis=1, inplace=True) 

        return dsm

    def calculate_potentials(s_flex, s_util, s_inc, s_dec, delta_t, dsm):

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
        dsm_buses = gpd.GeoDataFrame(index=dsm.index, crs=4326)
        dsm_buses["original_bus"] = dsm["bus"].copy()
        dsm_buses["scn_name"] = dsm["scn_name"].copy()

        # get original buses and add copy relevant information
        target1 = egon.data.config.datasets()["DSM_CTS_industry"]["targets"]['bus']
        original_buses = db.select_geodataframe(
            f"""SELECT bus_id, v_nom, scn_name, x, y, geom FROM
                {target1['schema']}.{target1['table']}""",
                geom_col="geom",
                epsg=4326)

        # copy v_nom, x, y and geom
        v_nom = pd.Series(index=dsm_buses.index, dtype=float)
        x = pd.Series(index=dsm_buses.index, dtype=float)
        y = pd.Series(index=dsm_buses.index, dtype=float)
        geom = gpd.GeoSeries(index=dsm_buses.index, crs=4326)

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
        dsm_buses.set_geometry("geom",inplace=True)
        
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
        # get original buses and add copy relevant information
        target1 = egon.data.config.datasets()["DSM_CTS_industry"]["targets"]['bus']
        original_buses = db.select_geodataframe(
            f"""SELECT bus_id, v_nom, scn_name, x, y, geom FROM
                {target1['schema']}.{target1['table']}""")
        
        target2 = egon.data.config.datasets()["DSM_CTS_industry"]["targets"]['link']
        sql = f"""SELECT link_id FROM {target2['schema']}.{target2['table']}"""
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
        target3 = egon.data.config.datasets()["DSM_CTS_industry"]["targets"]['store']
        sql = f"""SELECT store_id FROM {target3['schema']}.{target3['table']}"""
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

        targets = egon.data.config.datasets()["DSM_CTS_industry"]["targets"]
        
        # dsm_buses

        insert_buses = gpd.GeoDataFrame(index=dsm_buses.index, crs=4326)
        insert_buses["scn_name"] = dsm_buses["scn_name"]
        insert_buses["bus_id"] = dsm_buses["bus_id"]
        insert_buses["v_nom"] = dsm_buses["v_nom"]
        insert_buses["carrier"] = carrier
        insert_buses["x"] = dsm_buses["x"]
        insert_buses["y"] = dsm_buses["y"]
        insert_buses["geom"] = dsm_buses["geom"]
        insert_buses.set_geometry("geom",inplace=True)

        # insert into database
        insert_buses.to_postgis(
            targets["bus"]["table"],
            con=db.engine(),
            schema=targets["bus"]["schema"],
            if_exists="append",
            index=False,
        )

        # dsm_links

        insert_links = pd.DataFrame(index=dsm_links.index)
        insert_links["scn_name"] = dsm_links["scn_name"]
        insert_links["link_id"] = dsm_links["link_id"]
        insert_links["bus0"] = dsm_links["original_bus"]
        insert_links["bus1"] = dsm_links["dsm_bus"]
        insert_links["carrier"] = carrier
        insert_links["p_nom"] = dsm_links["p_nom"]

        # insert into database
        insert_links.to_sql(
            targets["link"]["table"],
            con=db.engine(),
            schema=targets["link"]["schema"],
            if_exists="append",
            index=False,
        )

        insert_links_timeseries = pd.DataFrame(index=dsm_links.index)
        insert_links_timeseries["scn_name"] = dsm_links["scn_name"]
        insert_links_timeseries["link_id"] = dsm_links["link_id"]
        insert_links_timeseries["p_min_pu"] = dsm_links["p_min"]
        insert_links_timeseries["p_max_pu"] = dsm_links["p_max"]
        insert_links_timeseries["temp_id"] = 1

        # insert into database
        insert_links_timeseries.to_sql(
            targets["link_timeseries"]["table"],
            con=db.engine(),
            schema=targets["link_timeseries"]["schema"],
            if_exists="append",
            index=False,
        )

        # dsm_stores

        insert_stores = pd.DataFrame(index=dsm_stores.index)
        insert_stores["scn_name"] = dsm_stores["scn_name"]
        insert_stores["store_id"] = dsm_stores["store_id"]
        insert_stores["bus"] = dsm_stores["bus"]
        insert_stores["carrier"] = carrier
        insert_stores["e_nom"] = dsm_stores["e_nom"]

        # insert into database
        insert_stores.to_sql(
            targets["store"]["table"],
            con=db.engine(),
            schema=targets["store"]["schema"],
            if_exists="append",
            index=False,
        )

        insert_stores_timeseries = pd.DataFrame(index=dsm_stores.index)
        insert_stores_timeseries["scn_name"] = dsm_stores["scn_name"]
        insert_stores_timeseries["store_id"] = dsm_stores["store_id"]
        insert_stores_timeseries["e_min_pu"] = dsm_stores["e_min"]
        insert_stores_timeseries["e_max_pu"] = dsm_stores["e_max"]
        insert_stores_timeseries["temp_id"] = 1

        # insert into database
        insert_stores_timeseries.to_sql(
            targets["store_timeseries"]["table"],
            con=db.engine(),
            schema=targets["store_timeseries"]["schema"],
            if_exists="append",
            index=False,
        )
        
    def delete_dsm_entries(carrier):

        """
        Deletes DSM-components from databse if they are there already.
        Parameters
            ----------
         carrier: String
            Remark in column 'carrier' identifying DSM-potential
        """      
        
        targets = egon.data.config.datasets()["DSM_CTS_industry"]["targets"]
        
        # buses
        
        sql = f"""DELETE FROM {targets["bus"]["schema"]}.{targets["bus"]["table"]} b
         WHERE (b.carrier LIKE '{carrier}');"""
        db.execute_sql(sql)
        
        # links
        
        sql = f"""DELETE FROM {targets["link_timeseries"]["schema"]}.{targets["link_timeseries"]["table"]} t
        WHERE t.link_id IN 
                 (SELECT l.link_id FROM {targets["link"]["schema"]}.{targets["link"]["table"]} l
              WHERE l.carrier LIKE '{carrier}');"""
        db.execute_sql(sql)
        sql = f"""DELETE FROM {targets["link"]["schema"]}.{targets["link"]["table"]} l
         WHERE (l.carrier LIKE '{carrier}');"""
        db.execute_sql(sql)
        
        # stores
        
        sql = f"""DELETE FROM {targets["store_timeseries"]["schema"]}.{targets["store_timeseries"]["table"]} t
        WHERE t.store_id IN 
                 (SELECT s.store_id FROM {targets["store"]["schema"]}.{targets["store"]["table"]} s
              WHERE s.carrier LIKE '{carrier}');"""
        db.execute_sql(sql)
        sql = f"""DELETE FROM {targets["store"]["schema"]}.{targets["store"]["table"]} s
         WHERE (s.carrier LIKE '{carrier}');"""
        db.execute_sql(sql)

    def dsm_cts_ind(
        con=db.engine(),
        cts_cool_vent_ac_share=0.22,
        ind_cool_vent_share=0.039,
        ind_vent_share=0.017,
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

        # CTS per osm-area: cooling, ventilation and air conditioning

        print(" ")
        print("CTS per osm-area: cooling, ventilation and air conditioning")
        print(" ")
        
        delete_dsm_entries('dsm-cts')

        dsm = cts_data_import(con, cts_cool_vent_ac_share)

        p_max, p_min, e_max, e_min = calculate_potentials(
            s_flex=0.5, s_util=0.67, s_inc=1, s_dec=0, delta_t=1, dsm=dsm
        )

        dsm_buses, dsm_links, dsm_stores = create_dsm_components(
            con, p_max, p_min, e_max, e_min, dsm
        )

        data_export(con, dsm_buses, dsm_links, dsm_stores, carrier="dsm-cts")

        # industry per osm-area: cooling and ventilation

        print(" ")
        print("industry per osm-area: cooling and ventilation")
        print(" ")
        
        delete_dsm_entries('dsm-ind-osm')

        dsm = ind_osm_data_import(con, ind_cool_vent_share)

        p_max, p_min, e_max, e_min = calculate_potentials(
            s_flex=0.5, s_util=0.73, s_inc=0.9, s_dec=0.5, delta_t=1, dsm=dsm
        )

        dsm_buses, dsm_links, dsm_stores = create_dsm_components(
            con, p_max, p_min, e_max, e_min, dsm
        )

        data_export(
            con, dsm_buses, dsm_links, dsm_stores, carrier="dsm-ind-osm"
        )

        # industry sites

        # industry sites: different applications
        
        delete_dsm_entries('dsm-ind-sites')

        dsm = ind_sites_data_import(con)

        print(" ")
        print("industry sites: paper")
        print(" ")

        dsm_paper = gpd.GeoDataFrame(
            dsm[
                dsm["application"].isin(
                    [
                        "Graphic Paper",
                        "Packing Paper and Board",
                        "Hygiene Paper",
                        "Technical/Special Paper and Board",
                    ]
                )
            ]
        )

        p_max, p_min, e_max, e_min = calculate_potentials(
            s_flex=0.15,
            s_util=0.86,
            s_inc=0.95,
            s_dec=0,
            delta_t=3,
            dsm=dsm_paper,
        )

        dsm_buses, dsm_links, dsm_stores = create_dsm_components(
            con, p_max, p_min, e_max, e_min, dsm_paper
        )

        data_export(
            con, dsm_buses, dsm_links, dsm_stores, carrier="dsm-ind-sites"
        )

        print(" ")
        print("industry sites: recycled paper")
        print(" ")

        dsm_recycled_paper = gpd.GeoDataFrame(
            dsm[dsm["application"] == "Recycled Paper"]
        )

        p_max, p_min, e_max, e_min = calculate_potentials(
            s_flex=0.7,
            s_util=0.85,
            s_inc=0.95,
            s_dec=0,
            delta_t=3,
            dsm=dsm_recycled_paper,
        )

        dsm_buses, dsm_links, dsm_stores = create_dsm_components(
            con, p_max, p_min, e_max, e_min, dsm_recycled_paper
        )

        data_export(
            con, dsm_buses, dsm_links, dsm_stores, carrier="dsm-ind-sites"
        )

        print(" ")
        print("industry sites: pulp")
        print(" ")

        dsm_pulp = gpd.GeoDataFrame(
            dsm[dsm["application"] == "Mechanical Pulp"]
        )

        p_max, p_min, e_max, e_min = calculate_potentials(
            s_flex=0.7,
            s_util=0.83,
            s_inc=0.95,
            s_dec=0,
            delta_t=2,
            dsm=dsm_pulp,
        )

        dsm_buses, dsm_links, dsm_stores = create_dsm_components(
            con, p_max, p_min, e_max, e_min, dsm_pulp
        )

        data_export(
            con, dsm_buses, dsm_links, dsm_stores, carrier="dsm-ind-sites"
        )

        # industry sites: cement

        print(" ")
        print("industry sites: cement")
        print(" ")

        dsm_cement = gpd.GeoDataFrame(dsm[dsm["application"] == "Cement Mill"])

        p_max, p_min, e_max, e_min = calculate_potentials(
            s_flex=0.61,
            s_util=0.65,
            s_inc=0.95,
            s_dec=0,
            delta_t=4,
            dsm=dsm_cement,
        )

        dsm_buses, dsm_links, dsm_stores = create_dsm_components(
            con, p_max, p_min, e_max, e_min, dsm_cement
        )

        data_export(
            con, dsm_buses, dsm_links, dsm_stores, carrier="dsm-ind-sites"
        )
        
        # industry sites: ventilation in WZ23

        print(" ")
        print("industry sites: ventilation in WZ23")
        print(" ")

        dsm = ind_sites_vent_data_import(con, ind_vent_share, wz=23)
        
        # drop entries of Cement Mills whose DSM-potentials have already been modelled
        cement = np.unique(dsm_cement['bus'].values)
        index_names = np.array(dsm[dsm['bus'].isin(cement)].index)
        dsm.drop(index_names,inplace=True)

        p_max, p_min, e_max, e_min = calculate_potentials(
            s_flex=0.5, s_util=0.8, s_inc=1, s_dec=0.5, delta_t=1, dsm=dsm
        )

        dsm_buses, dsm_links, dsm_stores = create_dsm_components(
            con, p_max, p_min, e_max, e_min, dsm
        )

        data_export(
            con, dsm_buses, dsm_links, dsm_stores, carrier="dsm-ind-sites"
        )

    dsm_cts_ind()
