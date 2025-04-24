import geopandas as gpd
import numpy as np
import pandas as pd

from egon.data import db
from egon.data.datasets.mastr import WORKING_DIR_MASTR_NEW
import egon.data.config


def insert():
    def mastr_existing_pv(pow_per_area):
        """Import MaStR data from csv-files.

        Parameters
        ----------
        pow_per_area: int
            Assumption for areas of existing pv farms and power of new built
            pv farms depending on area in kW/m²

        """
        # get config
        cfg = egon.data.config.datasets()["power_plants"]

        # import MaStR data: locations, grid levels and installed capacities

        # get relevant pv plants: ground mounted
        df = pd.read_csv(
            WORKING_DIR_MASTR_NEW / cfg["sources"]["mastr_pv"],
            usecols=[
                "Lage",
                "Laengengrad",
                "Breitengrad",
                "Nettonennleistung",
                "EinheitMastrNummer",
                "LokationMastrNummer",
            ],
        )
        df = df[df["Lage"] == "Freifläche"]

        # examine data concerning geographical locations and drop NaNs
        x1 = df["Laengengrad"].isnull().sum()
        x2 = df["Breitengrad"].isnull().sum()
        print(" ")
        print("Examination of MaStR data set:")
        print("original number of rows in the data set: " + str(len(df)))
        print("NaNs for longitude and latitude: " + str(x1) + " & " + str(x2))
        df.dropna(inplace=True)
        print("Number of rows after neglecting NaNs: " + str(len(df)))
        print(" ")

        # derive dataframe for locations
        mastr = gpd.GeoDataFrame(
            index=df.index,
            geometry=gpd.points_from_xy(df["Laengengrad"], df["Breitengrad"]),
            crs={"init": "epsg:4326"},
        )
        mastr = mastr.to_crs(3035)

        # derive installed capacities
        mastr["installed capacity in kW"] = df["Nettonennleistung"]

        # create buffer around locations

        # calculate bufferarea and -radius considering installed capacity
        df_radius = (
            mastr["installed capacity in kW"].div(pow_per_area * np.pi) ** 0.5
        )  # in m

        # create buffer
        mastr["buffer"] = mastr["geometry"].buffer(df_radius)
        mastr["buffer"].crs = 3035

        # derive MaStR-Nummer
        mastr["LokationMastrNummer"] = df["LokationMastrNummer"]

        # derive voltage level

        mastr["voltage_level"] = pd.Series(dtype=int)
        lvl = pd.read_csv(
            WORKING_DIR_MASTR_NEW / cfg["sources"]["mastr_location"],
            usecols=["Spannungsebene", "MaStRNummer"],
        )

        # assign voltage_level to MaStR-unit:

        vlevel_mapping = {
            "Höchstspannung": 1,
            "UmspannungZurHochspannung": 2,
            "Hochspannung": 3,
            "UmspannungZurMittelspannung": 4,
            "Mittelspannung": 5,
            "UmspannungZurNiederspannung": 6,
            "Niederspannung": 7,
        }

        mastr = mastr.merge(
            lvl[["MaStRNummer", "Spannungsebene"]],
            left_on="LokationMastrNummer",
            right_on="MaStRNummer",
            how="left",
        )

        mastr["voltage_level"] = mastr.Spannungsebene.replace(vlevel_mapping)

        mastr.drop(["MaStRNummer", "Spannungsebene"], axis=1, inplace=True)

        # ### examine data concerning voltage level
        x1 = mastr["voltage_level"].isnull().sum()
        print(" ")
        print("Examination of voltage levels in MaStR data set:")
        print("Original number of rows in MaStR: " + str(len(mastr)))
        print(
            "NaNs in voltage level caused by a) a missing assignment to the "
            "number or b) insufficient data: " + str(x1)
        )
        # drop PVs with missing values due to a) no assignment of
        # MaStR-numbers or b) missing data in row
        mastr.dropna(inplace=True)
        print("Number of rows after neglecting NaNs: " + str(len(mastr)))

        # drop PVs in low voltage level
        index_names = mastr[mastr["voltage_level"] == "Niederspannung"].index
        x2 = len(index_names)
        mastr.drop(index_names, inplace=True)
        index_names = mastr[
            mastr["voltage_level"] == "UmspannungZurNiederspannung"
        ].index
        x3 = len(index_names)
        mastr.drop(index_names, inplace=True)

        # ### further examination
        print("Number of PVs in low voltage level: " + str(x2))
        print("Number of PVs in LVMV level: " + str(x3))
        print(
            "Number of rows after dropping entries assigned to these levels: "
            + str(len(mastr))
        )
        print(" ")

        return mastr

    def potential_areas(con, join_buffer):
        """Import potential areas and choose and prepare areas suitable for PV
        ground mounted.

        Parameters
        ----------
        con:
            Connection to database
        join_buffer: int
            Maximum distance for joining of potential areas (only small ones
            to big ones) in m

        """

        # import potential areas: railways and roads & agriculture

        # roads and railway
        sql = (
            "SELECT id, geom FROM "
            "supply.egon_re_potential_area_pv_road_railway"
        )
        potentials_rora = gpd.GeoDataFrame.from_postgis(sql, con)
        potentials_rora = potentials_rora.set_index("id")

        # agriculture
        sql = (
            "SELECT id, geom FROM "
            "supply.egon_re_potential_area_pv_agriculture"
        )
        potentials_agri = gpd.GeoDataFrame.from_postgis(sql, con)
        potentials_agri = potentials_agri.set_index("id")

        # add areas < 1 ha to bigger areas if they are very close, otherwise
        # exclude areas < 1 ha

        # calculate area
        potentials_rora["area"] = potentials_rora.area
        potentials_agri["area"] = potentials_agri.area

        # roads and railways

        # ### counting variable for examination
        before = len(potentials_rora)

        # get small areas and create buffer for joining around them
        small_areas = potentials_rora[potentials_rora["area"] < 10000]
        small_buffers = small_areas.copy()
        small_buffers["geom"] = small_areas["geom"].buffer(join_buffer)

        # drop small areas in potential areas
        index_names = potentials_rora[potentials_rora["area"] < 10000].index
        potentials_rora.drop(index_names, inplace=True)

        # check intersection of small areas with other potential areas
        overlay = gpd.sjoin(potentials_rora, small_buffers)
        o = overlay["index_right"]
        o.drop_duplicates(inplace=True)

        # add small areas to big ones if buffer intersects
        for i in range(0, len(o)):
            index_potentials = o.index[i]
            index_small = o.iloc[i]
            x = potentials_rora["geom"].loc[index_potentials]
            y = small_areas["geom"].loc[index_small]
            join = gpd.GeoSeries(data=[x, y])
            potentials_rora["geom"].loc[index_potentials] = join.unary_union

        # ### examination of joining of areas
        count_small = len(small_buffers)
        count_join = len(o)
        count_delete = count_small - count_join
        print(" ")
        print(
            "Examination of potential areas in category 'Roads and Railways'"
        )
        print("Length of original data frame: " + str(before))
        print("Number of small areas: " + str(count_small))
        print("Number of joins: " + str(count_join))
        print("Deleted areas (not joined): " + str(count_delete))
        print("Length of resulting data frame: " + str(len(potentials_rora)))
        print(" ")

        # agriculture

        # ### counting variable for examination
        before = len(potentials_agri)

        # get small areas and create buffer for joining around them
        small_areas = potentials_agri[potentials_agri["area"] < 10000]
        small_buffers = small_areas.copy()
        small_buffers["geom"] = small_areas["geom"].buffer(join_buffer)

        # drop small areas in potential areas
        index_names = potentials_agri[potentials_agri["area"] < 10000].index
        potentials_agri.drop(index_names, inplace=True)

        # check intersection of small areas with other potential areas
        overlay = gpd.sjoin(potentials_agri, small_buffers)
        o = overlay["index_right"]
        o.drop_duplicates(inplace=True)

        # add small areas to big ones if buffer intersects
        for i in range(0, len(o)):
            index_potentials = o.index[i]
            index_small = o.iloc[i]
            x = potentials_agri["geom"].loc[index_potentials]
            y = small_areas["geom"].loc[index_small]
            join = gpd.GeoSeries(data=[x, y])
            potentials_agri["geom"].loc[index_potentials] = join.unary_union

        # ### examination of joining of areas
        count_small = len(small_buffers)
        count_join = len(o)
        count_delete = count_small - count_join
        print(" ")
        print("Examination of potential areas in category 'Agriculture'")
        print("Length of original data frame: " + str(before))
        print("Number of small areas: " + str(count_small))
        print("Number of joins: " + str(count_join))
        print("Deleted areas (not joined): " + str(count_delete))
        print("Length of resulting data frame: " + str(len(potentials_agri)))
        print(" ")

        # calculate new areas
        potentials_rora["area"] = potentials_rora.area
        potentials_agri["area"] = potentials_agri.area

        # check intersection of potential areas

        # ### counting variable
        agri_vorher = len(potentials_agri)

        # if areas intersect, keep road & railway potential areas and drop
        # agricultural ones
        overlay = gpd.sjoin(potentials_rora, potentials_agri)
        o = overlay["index_right"]
        o.drop_duplicates(inplace=True)
        for i in range(0, len(o)):
            index = o.iloc[i]
            potentials_agri.drop([index], inplace=True)

        # ### examination of intersection of areas
        print(" ")
        print("Review function to avoid intersection of potential areas:")
        print("Initial length potentials_agri: " + str(agri_vorher))
        print("Number of occurred cases: " + str(len(o)))
        print("Resulting length potentials_agri: " + str(len(potentials_agri)))
        print(" ")

        return potentials_rora, potentials_agri

    def select_pot_areas(mastr, potentials_pot):
        """Select potential areas where there are existing pv parks
        (MaStR-data).

        Parameters
        ----------
        mastr: gpd.GeoDataFrame()
            MaStR-DataFrame with existing pv parks
        potentials_pot: gpd.GeoDataFrame()
            Suitable potential areas

        """

        # select potential areas with existing pv parks
        # (potential areas intersect buffer around existing plants)

        # prepare dataframes to check intersection
        pvs = gpd.GeoDataFrame()
        pvs["geom"] = mastr["buffer"].copy()
        pvs.set_geometry("geom", inplace=True)
        pvs.crs = 3035
        pvs = pvs.set_geometry("geom")
        potentials = gpd.GeoDataFrame()
        potentials["geom"] = potentials_pot["geom"].copy()
        potentials.set_geometry("geom", inplace=True)
        potentials.crs = 3035
        potentials = potentials.set_geometry("geom")

        # check intersection of potential areas with exisiting PVs (MaStR)
        overlay = gpd.sjoin(pvs, potentials)
        o = overlay["index_right"]
        o.drop_duplicates(inplace=True)

        # define selected potentials areas
        pot_sel = potentials_pot.copy()
        pot_sel["selected"] = pd.Series()
        pot_sel["voltage_level"] = pd.Series(dtype=int)
        for i in range(0, len(o)):
            index_pot = o.iloc[i]
            pot_sel["selected"].loc[index_pot] = True
            # get voltage level of existing PVs
            index_pv = o.index[i]
            pot_sel["voltage_level"] = mastr["voltage_level"].loc[index_pv]
        pot_sel = pot_sel[pot_sel["selected"] == True]
        pot_sel.drop("selected", axis=1, inplace=True)

        # drop selected existing pv parks from mastr
        mastr.drop(index=o.index, inplace=True)

        return (pot_sel, mastr)

    def build_pv(pv_pot, pow_per_area):
        """Build new pv parks in selected potential areas.

        Parameters
        ----------
        pv_pot: gpd.GeoDataFrame()
            Selected potential areas
        pow_per_area: int
            Assumption for areas of existing pv farms and power of new built
            pv farms depending on area in kW/m²

        """

        # build pv farms in selected areas

        # calculation of centroids
        pv_pot["centroid"] = pv_pot["geom"].representative_point()

        # calculation of power in kW
        pv_pot["installed capacity in kW"] = pd.Series()
        pv_pot["installed capacity in kW"] = pv_pot["area"] * pow_per_area

        # check for maximal capacity for PV ground mounted
        limit_cap = 120000  # in kW
        pv_pot["installed capacity in kW"] = pv_pot[
            "installed capacity in kW"
        ].apply(lambda x: x if x < limit_cap else limit_cap)

        return pv_pot

    def adapt_grid_level(pv_pot, max_dist_hv, con):
        """Check and if needed adapt grid level of newly built pv parks.

        Parameters
        ----------
        pv_pot: gpd.GeoDataFrame()
            Newly built pv parks on selected potential areas
        max_dist_hv: int
            Assumption for maximum distance of park with hv-power to next
            substation in m
        con:
            Connection to database

        """

        # divide dataframe in MV and HV
        pv_pot_mv = pv_pot[pv_pot["voltage_level"] == 5]
        pv_pot_hv = pv_pot[pv_pot["voltage_level"] == 4]

        # check installed capacity in MV

        max_cap_mv = 5500  # in kW

        # find PVs which need to be HV or to have reduced capacity
        pv_pot_mv_to_hv = pv_pot_mv[
            pv_pot_mv["installed capacity in kW"] > max_cap_mv
        ]

        if len(pv_pot_mv_to_hv) > 0:
            # import data for HV substations

            sql = "SELECT point, voltage FROM grid.egon_hvmv_substation"
            hvmv_substation = gpd.GeoDataFrame.from_postgis(
                sql, con, geom_col="point"
            )
            hvmv_substation = hvmv_substation.to_crs(3035)
            hvmv_substation["voltage"] = hvmv_substation["voltage"].apply(
                lambda x: int(x.split(";")[0])
            )
            hv_substations = hvmv_substation[
                hvmv_substation["voltage"] >= 110000
            ]
            hv_substations = (
                hv_substations.unary_union
            )  # join all the hv_substations

            # check distance to HV substations of PVs with too high installed
            # capacity for MV

            # calculate distance to substations
            pv_pot_mv_to_hv["dist_to_HV"] = (
                pv_pot_mv_to_hv["geom"].to_crs(3035).distance(hv_substations)
            )

            # adjust grid level and keep capacity if transmission lines are
            # close
            pv_pot_mv_to_hv = pv_pot_mv_to_hv[
                pv_pot_mv_to_hv["dist_to_HV"] <= max_dist_hv
            ]
            pv_pot_mv_to_hv = pv_pot_mv_to_hv.drop(columns=["dist_to_HV"])
            pv_pot_hv = pd.concat([pv_pot_hv, pv_pot_mv_to_hv])

            # delete PVs which are now HV from MV dataframe
            for index, pot in pv_pot_mv_to_hv.iterrows():
                pv_pot_mv = pv_pot_mv.drop([index])
            pv_pot_hv["voltage_level"] = 4

            # keep grid level adjust capacity if transmission lines are too
            # far
            pv_pot_mv["installed capacity in kW"] = pv_pot_mv[
                "installed capacity in kW"
            ].apply(lambda x: x if x < max_cap_mv else max_cap_mv)
            pv_pot_mv["voltage_level"] = 5

            pv_pot = pd.concat([pv_pot_mv, pv_pot_hv])

        return pv_pot

    def build_additional_pv(potentials, pv, pow_per_area, con):
        """Build additional pv parks if pv parks on selected potential areas
        do not hit the target value.

         Parameters
         ----------
         potenatials: gpd.GeoDataFrame()
             All suitable potential areas
         pv: gpd.GeoDataFrame()
             Newly built pv parks on selected potential areas
        pow_per_area: int
             Assumption for areas of existing pv farms and power of new built
             pv farms depending on area in kW/m²
         con:
             Connection to database

        """

        # get MV grid districts
        sql = "SELECT bus_id, geom FROM grid.egon_mv_grid_district"
        distr = gpd.GeoDataFrame.from_postgis(sql, con)
        distr = distr.set_index("bus_id")

        # identify potential areas where there are no PV parks yet
        for index, pv in pv.iterrows():
            potentials = potentials.drop([index])

        # aggregate potential area per MV grid district
        pv_per_distr = gpd.GeoDataFrame()
        pv_per_distr["geom"] = distr["geom"].copy()
        centroids = potentials.copy()
        centroids["geom"] = centroids["geom"].representative_point()

        overlay = gpd.sjoin(centroids, distr)

        # ### examine potential area per grid district
        anz = len(overlay)
        anz_distr = len(overlay["index_right"].unique())
        size = 137500  # m2 Fläche für > 5,5 MW: (5500 kW / (0,04 kW/m2))
        anz_big = len(overlay[overlay["area"] >= size])
        anz_small = len(overlay[overlay["area"] < size])

        print(" ")
        print(
            "Examination of remaining potential areas in MV grid districts: "
        )
        print("Number of potential areas: " + str(anz))
        print(" -> distributed to " + str(anz_distr) + " districts")
        print("Number of areas with a potential >= 5,5 MW: " + str(anz_big))
        print("Number of areas with a potential < 5,5 MW: " + str(anz_small))
        print(" ")

        for index, dist in distr.iterrows():
            pots = overlay[overlay["index_right"] == index]["geom"].index
            p = gpd.GeoSeries(index=pots)
            for i in pots:
                p.loc[i] = potentials["geom"].loc[i]
            pv_per_distr["geom"].loc[index] = p.unary_union

        # calculate area per MV grid district and linearly distribute needed
        # capacity considering pow_per_area
        pv_per_distr["area"] = pv_per_distr["geom"].area
        pv_per_distr["installed capacity in kW"] = (
            pv_per_distr["area"] * pow_per_area
        )

        # calculate centroid
        pv_per_distr["centroid"] = pv_per_distr["geom"].representative_point()

        return pv_per_distr

    def check_target(
        pv_rora_i,
        pv_agri_i,
        pv_exist_i,
        potentials_rora_i,
        potentials_agri_i,
        target_power,
        pow_per_area,
        con,
    ):
        """Check target value per scenario and per state.

         Parameters
         ----------
         pv_rora_i: gpd.GeoDataFrame()
             Newly built pv parks on selected potential areas of road and
             railways p
         pv_agri_i: gpd.GeoDataFrame()
             Newly built pv parks on selected potential areas of agriculture
         pv_exist_i: gpd.GeoDataFrame()
             existing pv parks that don't intercept any potential area
         potenatials_rora_i: gpd.GeoDataFrame()
             All suitable potential areas of road and railway
         potenatials_rora_i: gpd.GeoDataFrame()
             All suitable potential areas of agriculture
         target_power: int
             Target for installed capacity of pv ground mounted in referenced
             state
        pow_per_area: int
             Assumption for areas of existing pv farms and power of new built
             pv farms depending on area in kW/m²
         con:
             Connection to database

        """

        # sum overall installed capacity for MV and HV

        total_pv_power = (
            pv_rora_i["installed capacity in kW"].sum()
            + pv_agri_i["installed capacity in kW"].sum()
            + pv_exist_i["installed capacity in kW"].sum()
        )

        pv_per_distr_i = gpd.GeoDataFrame()

        # check target value

        ###
        print(" ")
        print(
            "Installed capacity on areas with existing plants: "
            + str(total_pv_power / 1000)
            + " MW"
        )

        # linear scale farms to meet target if sum of installed capacity is
        # too high
        if total_pv_power >= target_power:
            scale_factor = target_power / total_pv_power
            pv_rora_i["installed capacity in kW"] = (
                pv_rora_i["installed capacity in kW"] * scale_factor
            )
            pv_agri_i["installed capacity in kW"] = (
                pv_agri_i["installed capacity in kW"] * scale_factor
            )
            pv_exist_i["installed capacity in kW"] = (
                pv_exist_i["installed capacity in kW"] * scale_factor
            )

            pv_per_distr_i["grid_district"] = pd.Series()
            pv_per_distr_i["installed capacity in kW"] = pd.Series(0)

            ###
            print(
                "Expansion of existing PV parks on potential areas to "
                "achieve target capacity is sufficient."
            )
            print(
                "Installed power is greater than the target value, scaling "
                "is applied:"
            )
            print("Scaling factor: " + str(scale_factor))

        # build new pv parks if sum of installed capacity is below target
        # value
        elif total_pv_power < target_power:
            rest_cap = target_power - total_pv_power

            ###
            print(
                "Expansion of existing PV parks on potential areas to "
                "achieve target capacity is unsufficient:"
            )
            print("Residual capacity: " + str(rest_cap / 1000) + " MW")
            print(
                "Residual capacity will initially be distributed via "
                "remaining potential areas 'Road & Railway'."
            )

            # build pv parks in potential areas road & railway
            pv_per_distr_i = build_additional_pv(
                potentials_rora_i, pv_rora_i, pow_per_area, con
            )
            # change index to add different Dataframes in the end
            pv_per_distr_i["grid_district"] = pv_per_distr_i.index.copy()
            pv_per_distr_i.index = range(0, len(pv_per_distr_i))
            # delete empty grid districts
            index_names = pv_per_distr_i[
                pv_per_distr_i["installed capacity in kW"].isna()
            ].index
            pv_per_distr_i.drop(index_names, inplace=True)

            if pv_per_distr_i["installed capacity in kW"].sum() > rest_cap:
                scale_factor = (
                    rest_cap / pv_per_distr_i["installed capacity in kW"].sum()
                )
                pv_per_distr_i["installed capacity in kW"] = (
                    pv_per_distr_i["installed capacity in kW"] * scale_factor
                )

                ###
                print(
                    "Residual capacity got distributed via scaling factor "
                    + str(scale_factor)
                    + " to remaining potential areas 'Road & Railway'."
                )

            # build pv parks on potential areas agriculture if still necessary
            elif pv_per_distr_i["installed capacity in kW"].sum() < rest_cap:
                rest_cap = (
                    target_power
                    - total_pv_power
                    - pv_per_distr_i["installed capacity in kW"].sum()
                )

                ###
                print(
                    "Distribution via potential areas Road & Railway "
                    "unsufficient to achieve target capacity:"
                )
                print("Residual capacity: " + str(rest_cap / 1000) + " MW")
                print(
                    "Residual capacity is distributed to remaining potential "
                    "areas 'Agriculture'."
                )

                pv_per_distr_i_2 = build_additional_pv(
                    potentials_agri_i, pv_agri_i, pow_per_area, con
                )
                # change index to add different Dataframes in the end
                pv_per_distr_i_2["grid_district"] = pv_per_distr_i_2.index
                pv_per_distr_i_2.index = range(len(pv_per_distr_i_2))

                # delete empty grid districts
                index_names = pv_per_distr_i_2[
                    pv_per_distr_i_2["installed capacity in kW"].isna()
                ].index
                pv_per_distr_i_2.drop(index_names, inplace=True)

                if (
                    pv_per_distr_i_2["installed capacity in kW"].sum()
                    > rest_cap
                ):
                    scale_factor = (
                        rest_cap
                        / pv_per_distr_i_2["installed capacity in kW"].sum()
                    )
                    pv_per_distr_i_2["installed capacity in kW"] = (
                        pv_per_distr_i_2["installed capacity in kW"]
                        * scale_factor
                    )

                    ###
                    print(
                        "Residual capacity got distributed via scaling "
                        "factor "
                        + str(scale_factor)
                        + " to remaining potential areas 'Road & Railway' "
                        "and 'Agriculture'."
                    )

                pv_per_distr_i = pd.concat(
                    [pv_per_distr_i, pv_per_distr_i_2], ignore_index=True
                )

            # assign grid level to pv_per_distr
            v_lvl = pd.Series(dtype=int, index=pv_per_distr_i.index)
            for index, distr in pv_per_distr_i.iterrows():
                if distr["installed capacity in kW"] > 5500:  # > 5 MW
                    v_lvl[index] = 4
                else:
                    v_lvl[index] = 5
            pv_per_distr_i["voltage_level"] = v_lvl

            # new overall installed capacity
            total_pv_power = (
                pv_rora_i["installed capacity in kW"].sum()
                + pv_agri_i["installed capacity in kW"].sum()
                + pv_exist_i["installed capacity in kW"].sum()
                + pv_per_distr_i["installed capacity in kW"].sum()
            )

            ###
            print(
                "Total installed capacity of PV farms: "
                + str(total_pv_power / 1000)
                + " MW"
            )
            print(" ")

        pv_rora_i = pv_rora_i[pv_rora_i["installed capacity in kW"] > 0]
        pv_agri_i = pv_agri_i[pv_agri_i["installed capacity in kW"] > 0]
        pv_exist_i = pv_exist_i[pv_exist_i["installed capacity in kW"] > 0]
        pv_per_distr_i = pv_per_distr_i[
            pv_per_distr_i["installed capacity in kW"] > 0
        ]

        return pv_rora_i, pv_agri_i, pv_exist_i, pv_per_distr_i

    def keep_existing_pv(mastr, con):
        pv_exist = mastr[
            [
                "geometry",
                "installed capacity in kW",
                "voltage_level",
            ]
        ]
        pv_exist.rename(columns={"geometry": "centroid"}, inplace=True)
        pv_exist = gpd.GeoDataFrame(pv_exist, geometry="centroid", crs=3035)

        # German states
        sql = "SELECT geometry as geom, gf FROM boundaries.vg250_lan"
        land = gpd.GeoDataFrame.from_postgis(sql, con).to_crs(3035)
        land = land[(land["gf"] != 1) & (land["gf"] != 2)]
        land = land.unary_union
        pv_exist = gpd.clip(pv_exist, land)

        return pv_exist

    def run_methodology(
        con=db.engine(),
        pow_per_area=0.04,
        join_buffer=10,
        max_dist_hv=20000,
        show_map=False,
    ):
        """Execute methodology to distribute pv ground mounted.

         Parameters
         ----------
         con:
             Connection to database
         pow_per_area: int, default 0.4
             Assumption for areas of existing pv farms and power of new built
             pv farms depending on area in kW/m²
         join_buffer : int, default 10
             Maximum distance for joining of potential areas (only small ones
             to big ones) in m
         max_dist_hv : int, default 20000
             Assumption for maximum distance of park with hv-power to next
             substation in m
        show_map:  boolean
            Optional creation of map to show distribution of installed
            capacity

        """
        ###
        print(" ")
        print("MaStR-Data")
        print(" ")

        # MaStR-data: existing PV farms
        mastr = mastr_existing_pv(pow_per_area)

        ###
        print(" ")
        print("potential area")
        print(" ")

        # database-data: potential areas for new PV farms
        potentials_rora, potentials_agri = potential_areas(con, join_buffer)

        ###
        print(" ")
        print("select potentials area")
        print(" ")

        # select potential areas with existing PV farms to build new PV farms
        pv_rora, mastr = select_pot_areas(mastr, potentials_rora)
        pv_agri, mastr = select_pot_areas(mastr, potentials_agri)

        ###
        print(" ")
        print(
            "build PV parks where there is PV ground mounted already "
            "(-> MaStR) on potential area"
        )
        print(" ")

        # build new PV farms
        pv_rora = build_pv(pv_rora, pow_per_area)
        pv_agri = build_pv(pv_agri, pow_per_area)

        # keep the existing pv_farms that don't intercept potential areas
        exist = keep_existing_pv(mastr, con)

        ###
        print(" ")
        print("adapt grid level of PV parks")
        print(" ")

        # adapt grid level to new farms
        rora = adapt_grid_level(pv_rora, max_dist_hv, con)
        agri = adapt_grid_level(pv_agri, max_dist_hv, con)

        ###
        print(" ")
        print(
            "check target value and build more PV parks on potential area if "
            "necessary"
        )
        print(" ")

        # initialize final dataframe
        pv_rora = gpd.GeoDataFrame()
        pv_agri = gpd.GeoDataFrame()
        pv_exist = gpd.GeoDataFrame()
        pv_per_distr = gpd.GeoDataFrame()

        # 1) scenario: eGon2035
        if (
            "eGon2035"
            in egon.data.config.settings()["egon-data"]["--scenarios"]
        ):
            ###
            print(" ")
            print("scenario: eGon2035")
            print(" ")

            # German states
            sql = "SELECT geometry as geom, nuts FROM boundaries.vg250_lan"
            states = gpd.GeoDataFrame.from_postgis(sql, con)

            # assumption for target value of installed capacity
            sql = (
                "SELECT capacity,scenario_name,nuts FROM "
                "supply.egon_scenario_capacities WHERE carrier='solar'"
            )
            target = pd.read_sql(sql, con)
            target = target[target["scenario_name"] == "eGon2035"]
            nuts = np.unique(target["nuts"])

            # prepare selection per state
            rora = rora.set_geometry("centroid")
            agri = agri.set_geometry("centroid")
            potentials_rora = potentials_rora.set_geometry("geom")
            potentials_agri = potentials_agri.set_geometry("geom")

            # check target value per state
            for i in nuts:
                target_power = (
                    target[target["nuts"] == i]["capacity"].iloc[0] * 1000
                )

                ###
                land = target[target["nuts"] == i]["nuts"].iloc[0]
                print(" ")
                print("Bundesland (NUTS): " + land)
                print("target power: " + str(target_power / 1000) + " MW")

                # select state
                state = states[states["nuts"] == i]
                state = state.to_crs(3035)

                # select PVs in state
                rora_i = gpd.sjoin(rora, state)
                agri_i = gpd.sjoin(agri, state)
                exist_i = gpd.sjoin(exist, state)
                rora_i.drop("index_right", axis=1, inplace=True)
                agri_i.drop("index_right", axis=1, inplace=True)
                exist_i.drop("index_right", axis=1, inplace=True)
                rora_i.drop_duplicates(inplace=True)
                agri_i.drop_duplicates(inplace=True)
                exist_i.drop_duplicates(inplace=True)

                # select potential areas in state
                potentials_rora_i = gpd.sjoin(potentials_rora, state)
                potentials_agri_i = gpd.sjoin(potentials_agri, state)
                potentials_rora_i.drop("index_right", axis=1, inplace=True)
                potentials_agri_i.drop("index_right", axis=1, inplace=True)
                potentials_rora_i.drop_duplicates(inplace=True)
                potentials_agri_i.drop_duplicates(inplace=True)

                # check target value and adapt installed capacity if necessary
                rora_i, agri_i, exist_i, distr_i = check_target(
                    rora_i,
                    agri_i,
                    exist_i,
                    potentials_rora_i,
                    potentials_agri_i,
                    target_power,
                    pow_per_area,
                    con,
                )

                if len(distr_i) > 0:
                    distr_i["nuts"] = target[target["nuts"] == i]["nuts"].iloc[
                        0
                    ]

                # ### examination of built PV parks per state
                rora_i_mv = rora_i[rora_i["voltage_level"] == 5]
                rora_i_hv = rora_i[rora_i["voltage_level"] == 4]
                agri_i_mv = agri_i[agri_i["voltage_level"] == 5]
                agri_i_hv = agri_i[agri_i["voltage_level"] == 4]
                print(
                    "eGon2035: Examination of voltage level per federal state:"
                )
                print("a) PVs on potential areas Road & Railway: ")
                print(
                    "Total installed capacity: "
                    + str(rora_i["installed capacity in kW"].sum() / 1000)
                    + " MW"
                )
                print("Number of PV farms: " + str(len(rora_i)))
                print(" - thereof MV: " + str(len(rora_i_mv)))
                print(" - thereof HV: " + str(len(rora_i_hv)))
                print("b) PVs on potential areas Agriculture: ")
                print(
                    "Total installed capacity: "
                    + str(agri_i["installed capacity in kW"].sum() / 1000)
                    + " MW"
                )
                print("Number of PV farms: " + str(len(agri_i)))
                print(" - thereof MV: " + str(len(agri_i_mv)))
                print(" - dthereof HV: " + str(len(agri_i_hv)))
                print("c) Existing PVs not in potential areas: ")
                print("Number of PV farms: " + str(len(exist_i)))
                print("d) PVs on additional potential areas per MV-District: ")
                if len(distr_i) > 0:
                    distr_i_mv = distr_i[distr_i["voltage_level"] == 5]
                    distr_i_hv = distr_i[distr_i["voltage_level"] == 4]
                    print(
                        "Total installed capacity: "
                        + str(distr_i["installed capacity in kW"].sum() / 1000)
                        + " MW"
                    )
                    print("Number of PV farms: " + str(len(distr_i)))
                    print(" - thereof MV: " + str(len(distr_i_mv)))
                    print(" - thereof HV: " + str(len(distr_i_hv)))
                else:
                    print(" -> No additional expansion necessary")
                print(" ")

                pv_rora = pv_rora.append(rora_i)
                pv_agri = pv_agri.append(agri_i)
                pv_exist = pv_exist.append(exist_i)
                if len(distr_i) > 0:
                    pv_per_distr = pd.concat([pv_per_distr, distr_i])

        # initialize final dataframe
        pv_rora_nep2037_2025 = gpd.GeoDataFrame()
        pv_agri_nep2037_2025 = gpd.GeoDataFrame()
        pv_exist_nep2037_2025 = gpd.GeoDataFrame()
        pv_per_distr_nep2037_2025 = gpd.GeoDataFrame()

        if (
            "nep2037_2025"
            in egon.data.config.settings()["egon-data"]["--scenarios"]
        ):
            ###
            print(" ")
            print("scenario: nep2037_2025")
            print(" ")

            # German states
            sql = "SELECT geometry as geom, nuts FROM boundaries.vg250_lan"
            states = gpd.GeoDataFrame.from_postgis(sql, con)

            # assumption for target value of installed capacity
            sql = (
                "SELECT capacity,scenario_name,nuts FROM "
                "supply.egon_scenario_capacities WHERE carrier='solar'"
            )
            target = pd.read_sql(sql, con)
            target = target[target["scenario_name"] == "nep2037_2025"]
            nuts = np.unique(target["nuts"])

            # prepare selection per state
            rora = rora.set_geometry("centroid")
            agri = agri.set_geometry("centroid")
            potentials_rora = potentials_rora.set_geometry("geom")
            potentials_agri = potentials_agri.set_geometry("geom")

            # check target value per state
            for i in nuts:
                target_power = (
                    target[target["nuts"] == i]["capacity"].iloc[0] * 1000
                )

                ###
                land = target[target["nuts"] == i]["nuts"].iloc[0]
                print(" ")
                print("Bundesland (NUTS): " + land)
                print("target power: " + str(target_power / 1000) + " MW")

                # select state
                state = states[states["nuts"] == i]
                state = state.to_crs(3035)

                # select PVs in state
                rora_i = gpd.sjoin(rora, state)
                agri_i = gpd.sjoin(agri, state)
                exist_i = gpd.sjoin(exist, state)
                rora_i.drop("index_right", axis=1, inplace=True)
                agri_i.drop("index_right", axis=1, inplace=True)
                exist_i.drop("index_right", axis=1, inplace=True)
                rora_i.drop_duplicates(inplace=True)
                agri_i.drop_duplicates(inplace=True)
                exist_i.drop_duplicates(inplace=True)

                # select potential areas in state
                potentials_rora_i = gpd.sjoin(potentials_rora, state)
                potentials_agri_i = gpd.sjoin(potentials_agri, state)
                potentials_rora_i.drop("index_right", axis=1, inplace=True)
                potentials_agri_i.drop("index_right", axis=1, inplace=True)
                potentials_rora_i.drop_duplicates(inplace=True)
                potentials_agri_i.drop_duplicates(inplace=True)

                # check target value and adapt installed capacity if necessary
                rora_i, agri_i, exist_i, distr_i = check_target(
                    rora_i,
                    agri_i,
                    exist_i,
                    potentials_rora_i,
                    potentials_agri_i,
                    target_power,
                    pow_per_area,
                    con,
                )

                if len(distr_i) > 0:
                    distr_i["nuts"] = target[target["nuts"] == i]["nuts"].iloc[
                        0
                    ]

                # ### examination of built PV parks per state
                rora_i_mv = rora_i[rora_i["voltage_level"] == 5]
                rora_i_hv = rora_i[rora_i["voltage_level"] == 4]
                agri_i_mv = agri_i[agri_i["voltage_level"] == 5]
                agri_i_hv = agri_i[agri_i["voltage_level"] == 4]
                print(
                    "nep2037_2025: Examination of voltage level per federal state:"
                )
                print("a) PVs on potential areas Road & Railway: ")
                print(
                    "Total installed capacity: "
                    + str(rora_i["installed capacity in kW"].sum() / 1000)
                    + " MW"
                )
                print("Number of PV farms: " + str(len(rora_i)))
                print(" - thereof MV: " + str(len(rora_i_mv)))
                print(" - thereof HV: " + str(len(rora_i_hv)))
                print("b) PVs on potential areas Agriculture: ")
                print(
                    "Total installed capacity: "
                    + str(agri_i["installed capacity in kW"].sum() / 1000)
                    + " MW"
                )
                print("Number of PV farms: " + str(len(agri_i)))
                print(" - thereof MV: " + str(len(agri_i_mv)))
                print(" - dthereof HV: " + str(len(agri_i_hv)))
                print("c) Existing PVs not in potential areas: ")
                print("Number of PV farms: " + str(len(exist_i)))
                print("d) PVs on additional potential areas per MV-District: ")
                if len(distr_i) > 0:
                    distr_i_mv = distr_i[distr_i["voltage_level"] == 5]
                    distr_i_hv = distr_i[distr_i["voltage_level"] == 4]
                    print(
                        "Total installed capacity: "
                        + str(distr_i["installed capacity in kW"].sum() / 1000)
                        + " MW"
                    )
                    print("Number of PV farms: " + str(len(distr_i)))
                    print(" - thereof MV: " + str(len(distr_i_mv)))
                    print(" - thereof HV: " + str(len(distr_i_hv)))
                else:
                    print(" -> No additional expansion necessary")
                print(" ")

                pv_rora_nep2037_2025 = pv_rora_nep2037_2025.append(rora_i)
                pv_agri_nep2037_2025 = pv_agri_nep2037_2025.append(agri_i)
                pv_exist_nep2037_2025 = pv_exist_nep2037_2025.append(exist_i)
                if len(distr_i) > 0:
                    pv_per_distr_nep2037_2025 = pd.concat([pv_per_distr_nep2037_2025, distr_i])

        if (
            "eGon100RE"
            in egon.data.config.settings()["egon-data"]["--scenarios"]
        ):
            # 3) scenario: eGon100RE

            # assumption for target value of installed capacity in Germany per
            # scenario
            sql = (
                "SELECT capacity,scenario_name FROM "
                "supply.egon_scenario_capacities WHERE carrier='solar'"
            )
            target_power = pd.read_sql(sql, con)
            target_power = target_power[
                target_power["scenario_name"] == "eGon100RE"
            ]
            target_power = target_power["capacity"].sum() * 1000

            ###
            print(" ")
            print("scenario: eGon100RE")
            print("target power: " + str(target_power) + " kW")
            print(" ")

            # check target value and adapt installed capacity if necessary
            (
                pv_rora_100RE,
                pv_agri_100RE,
                pv_exist_100RE,
                pv_per_distr_100RE,
            ) = check_target(
                rora,
                agri,
                exist,
                potentials_rora,
                potentials_agri,
                target_power,
                pow_per_area,
                con,
            )

        # ### create map to show distribution of installed capacity
        if show_map == True:
            # 1) eGon2035

            # get MV grid districts
            sql = "SELECT bus_id, geom FROM grid.egon_mv_grid_district"
            distr = gpd.GeoDataFrame.from_postgis(sql, con)
            distr = distr.set_index("bus_id")

            # assign pv_per_distr-power to districts
            distr["capacity"] = pd.Series()
            for index, row in distr.iterrows():
                if index in np.unique(pv_per_distr["grid_district"]):
                    pv = pv_per_distr[pv_per_distr["grid_district"] == index]
                    x = pv["installed capacity in kW"].iloc[0]
                    distr["capacity"].loc[index] = x
                else:
                    distr["capacity"].loc[index] = 0
            distr["capacity"] = distr["capacity"] / 1000

            # add pv_rora- and pv_agri-power to district
            pv_rora = pv_rora.set_geometry("centroid")
            pv_agri = pv_agri.set_geometry("centroid")
            overlay_rora = gpd.sjoin(pv_rora, distr)
            overlay_agri = gpd.sjoin(pv_agri, distr)

            for index, row in distr.iterrows():
                o_rora = overlay_rora[overlay_rora["index_right"] == index]
                o_agri = overlay_agri[overlay_agri["index_right"] == index]
                cap_rora = o_rora["installed capacity in kW"].sum() / 1000
                cap_agri = o_agri["installed capacity in kW"].sum() / 1000
            distr["capacity"].loc[index] = (
                distr["capacity"].loc[index] + cap_rora + cap_agri
            )

            from matplotlib import pyplot as plt

            fig, ax = plt.subplots(1, 1)
            distr.boundary.plot(linewidth=0.2, ax=ax, color="black")
            distr.plot(
                ax=ax,
                column="capacity",
                cmap="magma_r",
                legend=True,
                legend_kwds={
                    "label": "Installed capacity in MW",
                    "orientation": "vertical",
                },
            )
            plt.savefig("pv_per_distr_map_eGon2035.png", dpi=300)

            # 2) nep2037_2025

            # get MV grid districts
            sql = "SELECT bus_id, geom FROM grid.egon_mv_grid_district"
            distr = gpd.GeoDataFrame.from_postgis(sql, con)
            distr = distr.set_index("bus_id")

            # assign pv_per_distr-power to districts
            distr["capacity"] = pd.Series()
            for index, row in distr.iterrows():
                if index in np.unique(pv_per_distr_nep2037_2025["grid_district"]):
                    pv = pv_per_distr_nep2037_2025[pv_per_distr_nep2037_2025["grid_district"] == index]
                    x = pv["installed capacity in kW"].iloc[0]
                    distr["capacity"].loc[index] = x
                else:
                    distr["capacity"].loc[index] = 0
            distr["capacity"] = distr["capacity"] / 1000

            # add pv_rora- and pv_agri-power to district
            pv_rora_nep2037_2025 = pv_rora_nep2037_2025.set_geometry("centroid")
            pv_agri_nep2037_2025 = pv_agri_nep2037_2025.set_geometry("centroid")
            overlay_rora = gpd.sjoin(pv_rora_nep2037_2025, distr)
            overlay_agri = gpd.sjoin(pv_agri_nep2037_2025, distr)

            for index, row in distr.iterrows():
                o_rora = overlay_rora[overlay_rora["index_right"] == index]
                o_agri = overlay_agri[overlay_agri["index_right"] == index]
                cap_rora = o_rora["installed capacity in kW"].sum() / 1000
                cap_agri = o_agri["installed capacity in kW"].sum() / 1000
            distr["capacity"].loc[index] = (
                distr["capacity"].loc[index] + cap_rora + cap_agri
            )

            from matplotlib import pyplot as plt

            fig, ax = plt.subplots(1, 1)
            distr.boundary.plot(linewidth=0.2, ax=ax, color="black")
            distr.plot(
                ax=ax,
                column="capacity",
                cmap="magma_r",
                legend=True,
                legend_kwds={
                    "label": "Installed capacity in MW",
                    "orientation": "vertical",
                },
            )
            plt.savefig("pv_per_distr_map__nep2037_2025.png", dpi=300)

            # 3) eGon100RE

            # get MV grid districts
            sql = "SELECT bus_id, geom FROM grid.egon_mv_grid_district"
            distr = gpd.GeoDataFrame.from_postgis(sql, con)
            distr = distr.set_index("bus_id")

            # assign pv_per_distr-power to districts
            distr["capacity"] = pd.Series()
            for index, row in distr.iterrows():
                if index in np.unique(pv_per_distr_100RE["grid_district"]):
                    pv = pv_per_distr_100RE[
                        pv_per_distr_100RE["grid_district"] == index
                    ]
                    x = pv["installed capacity in kW"].iloc[0]
                    distr["capacity"].loc[index] = x
                else:
                    distr["capacity"].loc[index] = 0
            distr["capacity"] = distr["capacity"] / 1000

            # add pv_rora- and pv_agri-power to district
            pv_rora_100RE = pv_rora_100RE.set_geometry("centroid")
            pv_agri_100RE = pv_agri_100RE.set_geometry("centroid")
            overlay_rora = gpd.sjoin(pv_rora_100RE, distr)
            overlay_agri = gpd.sjoin(pv_agri_100RE, distr)

            for index, row in distr.iterrows():
                o_rora = overlay_rora[overlay_rora["index_right"] == index]
                o_agri = overlay_agri[overlay_agri["index_right"] == index]
                cap_rora = o_rora["installed capacity in kW"].sum() / 1000
                cap_agri = o_agri["installed capacity in kW"].sum() / 1000
            distr["capacity"].loc[index] = (
                distr["capacity"].loc[index] + cap_rora + cap_agri
            )

            from matplotlib import pyplot as plt

            fig, ax = plt.subplots(1, 1)
            distr.boundary.plot(linewidth=0.2, ax=ax, color="black")
            distr.plot(
                ax=ax,
                column="capacity",
                cmap="magma_r",
                legend=True,
                legend_kwds={
                    "label": "Installed capacity in MW",
                    "orientation": "vertical",
                },
            )
            plt.savefig("pv_per_distr_map_eGon100RE.png", dpi=300)

        pv_rora_100RE = pv_rora_100RE[
            pv_rora_100RE["installed capacity in kW"] > 0
        ]
        pv_agri_100RE = pv_agri_100RE[
            pv_agri_100RE["installed capacity in kW"] > 0
        ]
        pv_per_distr_100RE = pv_per_distr_100RE[
            pv_per_distr_100RE["installed capacity in kW"] > 0
        ]

        return (
            pv_rora,
            pv_agri,
            pv_exist,
            pv_per_distr,
            pv_rora_nep2037_2025,
            pv_agri_nep2037_2025,
            pv_exist_nep2037_2025,
            pv_per_distr_nep2037_2025,
            pv_rora_100RE,
            pv_agri_100RE,
            pv_exist_100RE,
            pv_per_distr_100RE,
        )

    def insert_pv_parks(
        pv_rora, pv_agri, pv_exist, pv_per_distr, scenario_name
    ):
        """Write to database.

        Parameters
        ----------
        pv_rora : gpd.GeoDataFrame()
            Pv parks on selected potential areas of raod and railway
        pv_agri : gpd.GeoDataFrame()
            Pv parks on selected potential areas of raod and railway
        pv_exist : gpd.GeoDataFrame()
            Existing Pv parks on selected areas
        pv_per_distr: gpd.GeoDataFrame()
            Additionally built pv parks on potential areas per mv grid
            district
        scenario_name:
            Scenario name of calculation

        """

        # prepare dataframe for integration in supply.egon_power_plants

        pv_parks = pd.concat(
            [pv_rora, pv_agri, pv_exist, pv_per_distr], ignore_index=True
        )
        pv_parks["el_capacity"] = pv_parks["installed capacity in kW"] / 1000
        pv_parks.rename(columns={"centroid": "geometry"}, inplace=True)
        pv_parks = gpd.GeoDataFrame(pv_parks, geometry="geometry", crs=3035)
        pv_parks = pv_parks[["el_capacity", "voltage_level", "geometry"]]

        # integration in supply.egon_power_plants

        con = db.engine()

        # maximum ID in egon_power_plants
        sql = "SELECT MAX(id) FROM supply.egon_power_plants"
        max_id = pd.read_sql(sql, con)
        max_id = max_id["max"].iat[0]
        if max_id is None:
            max_id = 1

        pv_park_id = max_id + 1

        # copy relevant columns from pv_parks
        insert_pv_parks = pv_parks[
            ["el_capacity", "voltage_level", "geometry"]
        ]
        insert_pv_parks = insert_pv_parks.set_geometry("geometry")
        insert_pv_parks["voltage_level"] = insert_pv_parks[
            "voltage_level"
        ].apply(int)

        # set static column values
        insert_pv_parks["carrier"] = "solar"
        insert_pv_parks["scenario"] = scenario_name

        # change name and crs of geometry column
        insert_pv_parks.set_crs(epsg=3035, allow_override=True, inplace=True)
        insert_pv_parks = (
            insert_pv_parks.rename({"geometry": "geom"}, axis=1)
            .set_geometry("geom")
            .to_crs(4326)
        )

        # reset index
        insert_pv_parks.index = pd.RangeIndex(
            start=pv_park_id, stop=pv_park_id + len(insert_pv_parks), name="id"
        )

        # insert into database
        insert_pv_parks.reset_index().to_postgis(
            "egon_power_plants",
            schema="supply",
            con=db.engine(),
            if_exists="append",
        )

        return pv_parks

    # ########################################################################

    # execute methodology

    (
        pv_rora,
        pv_agri,
        pv_exist,
        pv_per_distr,
        pv_rora_100RE,
        pv_agri_100RE,
        pv_exist_100RE,
        pv_per_distr_100RE,
    ) = run_methodology(
        con=db.engine(),
        pow_per_area=0.04,
        join_buffer=10,
        max_dist_hv=20000,
        show_map=False,
    )

    # ### examination of results
    if len(pv_per_distr) > 0:
        pv_per_distr_mv = pv_per_distr[pv_per_distr["voltage_level"] == 5]
        pv_per_distr_hv = pv_per_distr[pv_per_distr["voltage_level"] == 4]
    if len(pv_rora) > 0:
        pv_rora_mv = pv_rora[pv_rora["voltage_level"] == 5]
        pv_rora_hv = pv_rora[pv_rora["voltage_level"] == 4]
        pv_agri_mv = pv_agri[pv_agri["voltage_level"] == 5]
        pv_agri_hv = pv_agri[pv_agri["voltage_level"] == 4]

        print(" ")
        print("eGon2035: Examination of overall voltage levels:")
        print("a) PVs on potential areas Road & Railway: ")
        print(
            "Total installed capacity: "
            + str(pv_rora["installed capacity in kW"].sum() / 1000)
            + " MW"
        )
        print("Number of PV farms: " + str(len(pv_rora)))
        print(" - thereof MV: " + str(len(pv_rora_mv)))
        print(" - thereof HV: " + str(len(pv_rora_hv)))
        print("b) PVs on potential areas Agriculture: ")
        print(
            "Total installed capacity: "
            + str(pv_agri["installed capacity in kW"].sum() / 1000)
            + " MW"
        )
        print("Number of PV farms: " + str(len(pv_agri)))
        print(" - thereof MV: " + str(len(pv_agri_mv)))
        print(" - thereof HV: " + str(len(pv_agri_hv)))
        print("c) Existing PVs not in potential areas: ")
        print("Number of PV farms: " + str(len(pv_exist)))
        print("d) PVs on additional potential areas per MV-District: ")
        if len(pv_per_distr) > 0:
            print(
                "Total installed capacity: "
                + str(pv_per_distr["installed capacity in kW"].sum() / 1000)
                + " MW"
            )
            print("Number of PV farms: " + str(len(pv_per_distr)))
            print(" - thereof MV: " + str(len(pv_per_distr_mv)))
            print(" - thereof HV: " + str(len(pv_per_distr_hv)))
        else:
            print(" -> No additional expansion needed")
        print(" ")
        ###

    # save to DB
    if "eGon2035" in egon.data.config.settings()["egon-data"]["--scenarios"]:
        if (
            pv_rora["installed capacity in kW"].sum() > 0
            or pv_agri["installed capacity in kW"].sum() > 0
            or pv_per_distr["installed capacity in kW"].sum() > 0
            or pv_exist["installed capacity in kW"].sum() > 0
        ):
            pv_parks = insert_pv_parks(
                pv_rora, pv_agri, pv_exist, pv_per_distr, "eGon2035"
            )

        else:
            pv_parks = gpd.GeoDataFrame()
    else:
        pv_parks = gpd.GeoDataFrame()

    if "nep2037_2025" in egon.data.config.settings()["egon-data"]["--scenarios"]:
        if (
            pv_rora["installed capacity in kW"].sum() > 0
            or pv_agri_nep2037_2025["installed capacity in kW"].sum() > 0
            or pv_per_distr_nep2037_2025["installed capacity in kW"].sum() > 0
            or pv_exist_nep2037_2025["installed capacity in kW"].sum() > 0
        ):
            pv_parks_nep2037_2025 = insert_pv_parks(
                pv_rora_nep2037_2025, pv_agri_nep2037_2025, pv_exist_nep2037_2025, pv_per_distr_nep2037_2025, "nep2037_2025"
            )

        else:
            pv_parks_nep2037_2025 = gpd.GeoDataFrame()
    else:
        pv_parks_nep2037_2025 = gpd.GeoDataFrame()

    if "eGon100RE" in egon.data.config.settings()["egon-data"]["--scenarios"]:
        if (
            pv_rora_100RE["installed capacity in kW"].sum() > 0
            or pv_agri_100RE["installed capacity in kW"].sum() > 0
            or pv_per_distr_100RE["installed capacity in kW"].sum() > 0
            or pv_exist_100RE["installed capacity in kW"].sum() > 0
        ):
            pv_parks_100RE = insert_pv_parks(
                pv_rora_100RE,
                pv_agri_100RE,
                pv_exist_100RE,
                pv_per_distr_100RE,
                "eGon100RE",
            )

        else:
            pv_parks_100RE = gpd.GeoDataFrame()
    else:
        pv_parks_100RE = gpd.GeoDataFrame()

    return pv_parks, pv_parks_nep2037_2025, pv_parks_100RE
