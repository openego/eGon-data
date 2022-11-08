"""
The module containing all code dealing with large chp from NEP list.
"""

from sqlalchemy.orm import sessionmaker
import geopandas
import pandas as pd

from egon.data import config, db
from egon.data.datasets.chp.small_chp import assign_use_case
from egon.data.datasets.power_plants import (
    assign_bus_id,
    assign_voltage_level,
    filter_mastr_geometry,
    select_target,
)
from egon.data.datasets.scenario_capacities import map_carrier


#####################################   NEP treatment   #################################
def select_chp_from_nep(sources):
    """Select CHP plants with location from NEP's list of power plants

    Returns
    -------
    pandas.DataFrame
        CHP plants from NEP list

    """

    # Select CHP plants with geolocation from list of conventional power plants
    chp_NEP_data = db.select_dataframe(
        f"""
        SELECT bnetza_id, name, carrier, chp, postcode, capacity, city,
        federal_state, c2035_chp, c2035_capacity
        FROM {sources['list_conv_pp']['schema']}.
        {sources['list_conv_pp']['table']}
        WHERE bnetza_id != 'KW<10 MW'
        AND (chp = 'Ja' OR c2035_chp = 'Ja')
        AND c2035_capacity > 0
        AND postcode != 'None'
        """
    )

    # Removing CHP out of Germany
    chp_NEP_data["postcode"] = chp_NEP_data["postcode"].astype(str)
    chp_NEP_data = chp_NEP_data[~chp_NEP_data["postcode"].str.contains("A")]
    chp_NEP_data = chp_NEP_data[~chp_NEP_data["postcode"].str.contains("L")]
    chp_NEP_data = chp_NEP_data[~chp_NEP_data["postcode"].str.contains("nan")]

    # Remove the subunits from the bnetza_id
    chp_NEP_data["bnetza_id"] = chp_NEP_data["bnetza_id"].str[0:7]

    # Initalize DataFrame
    chp_NEP = pd.DataFrame(
        columns=[
            "name",
            "postcode",
            "carrier",
            "capacity",
            "c2035_capacity",
            "c2035_chp",
            "city",
        ]
    )

    # Insert rows from list without a name
    chp_NEP = chp_NEP.append(
        chp_NEP_data[chp_NEP_data.name.isnull()].loc[
            :,
            [
                "name",
                "postcode",
                "carrier",
                "capacity",
                "c2035_capacity",
                "c2035_chp",
                "city",
                "federal_state",
            ],
        ]
    )
    # Insert rows from list with a name
    chp_NEP = chp_NEP.append(
        chp_NEP_data.groupby(
            [
                "carrier",
                "name",
                "postcode",
                "c2035_chp",
                "city",
                "federal_state",
            ]
        )["capacity", "c2035_capacity", "city", "federal_state"]
        .sum()
        .reset_index()
    ).reset_index()

    return chp_NEP.drop("index", axis=1)


#####################################   MaStR treatment   #################################
def select_chp_from_mastr(sources):
    """Select combustion CHP plants from MaStR

    Returns
    -------
    MaStR_konv : pd.DataFrame
        CHP plants from MaStR

    """

    # Read-in data from MaStR
    MaStR_konv = pd.read_csv(
        sources["mastr_combustion"],
        delimiter=",",
        usecols=[
            "Nettonennleistung",
            "EinheitMastrNummer",
            "Kraftwerksnummer",
            "Energietraeger",
            "Postleitzahl",
            "Laengengrad",
            "Breitengrad",
            "ThermischeNutzleistung",
            "EinheitBetriebsstatus",
            "LokationMastrNummer",
            "Ort",
            "Bundesland",
        ],
    )

    # Rename columns
    MaStR_konv = MaStR_konv.rename(
        columns={
            "Kraftwerksnummer": "bnetza_id",
            "Energietraeger": "carrier",
            "Postleitzahl": "plz",
            "Ort": "city",
            "Bundesland": "federal_state",
            "Nettonennleistung": "el_capacity",
            "ThermischeNutzleistung": "th_capacity",
        }
    )

    # Select only CHP plants which are in operation
    MaStR_konv = MaStR_konv[MaStR_konv.EinheitBetriebsstatus == "InBetrieb"]

    # Insert geometry column
    MaStR_konv = MaStR_konv[~(MaStR_konv["Laengengrad"].isnull())]
    MaStR_konv = geopandas.GeoDataFrame(
        MaStR_konv,
        geometry=geopandas.points_from_xy(
            MaStR_konv["Laengengrad"], MaStR_konv["Breitengrad"]
        ),
    )

    # Delete from Mastr_kov where carrier is not conventional
    MaStR_konv = MaStR_konv[MaStR_konv.carrier.isin(map_carrier().keys())]

    # Update carrier to match to eGon
    MaStR_konv["carrier"] = map_carrier()[MaStR_konv["carrier"].values].values

    # Drop individual CHP
    MaStR_konv = MaStR_konv[(MaStR_konv["el_capacity"] >= 100)]

    # Drop rows without post code and update datatype of postcode
    MaStR_konv = MaStR_konv[~MaStR_konv["plz"].isnull()]
    MaStR_konv["plz"] = MaStR_konv["plz"].astype(int)

    # Calculate power in MW
    MaStR_konv.loc[:, "el_capacity"] *= 1e-3
    MaStR_konv.loc[:, "th_capacity"] *= 1e-3

    MaStR_konv = MaStR_konv.set_crs(4326)

    # Drop CHP outside of Germany
    MaStR_konv = filter_mastr_geometry(MaStR_konv, federal_state=None)

    return MaStR_konv


# ############################################   Match with plz and K   ############################################
def match_nep_chp(
    chp_NEP,
    MaStR_konv,
    chp_NEP_matched,
    buffer_capacity=0.1,
    consider_location="plz",
    consider_carrier=True,
    consider_capacity=True,
):
    """Match CHP plants from MaStR to list of power plants from NEP

    Parameters
    ----------
    chp_NEP : pandas.DataFrame
        CHP plants from NEP which are not matched to MaStR
    MaStR_konv : pandas.DataFrame
        CHP plants from MaStR which are not matched to NEP
    chp_NEP_matched : pandas.DataFrame
        Already matched CHP
    buffer_capacity : float, optional
        Maximum difference in capacity in p.u. The default is 0.1.

    Returns
    -------
    chp_NEP_matched : pandas.DataFrame
        Matched CHP
    MaStR_konv : pandas.DataFrame
        CHP plants from MaStR which are not matched to NEP
    chp_NEP : pandas.DataFrame
        CHP plants from NEP which are not matched to MaStR

    """

    list_federal_states = pd.Series(
        {
            "Hamburg": "HH",
            "Sachsen": "SN",
            "MecklenburgVorpommern": "MV",
            "Thueringen": "TH",
            "SchleswigHolstein": "SH",
            "Bremen": "HB",
            "Saarland": "SL",
            "Bayern": "BY",
            "BadenWuerttemberg": "BW",
            "Brandenburg": "BB",
            "Hessen": "HE",
            "NordrheinWestfalen": "NW",
            "Berlin": "BE",
            "Niedersachsen": "NI",
            "SachsenAnhalt": "ST",
            "RheinlandPfalz": "RP",
        }
    )

    for ET in chp_NEP["carrier"].unique():

        for index, row in chp_NEP[
            (chp_NEP["carrier"] == ET) & (chp_NEP["postcode"] != "None")
        ].iterrows():

            # Select plants from MaStR that match carrier, PLZ
            # and have a similar capacity
            # Create a copy of all power plants from MaStR
            selected = MaStR_konv.copy()

            # Set capacity constraint using buffer
            if consider_capacity:
                selected = selected[
                    (
                        selected.el_capacity
                        <= row["capacity"] * (1 + buffer_capacity)
                    )
                    & (
                        selected.el_capacity
                        >= row["capacity"] * (1 - buffer_capacity)
                    )
                ]

            # Set geographic constraint, either chosse power plants
            # with the same postcode, city or federal state
            if consider_location == "plz":
                selected = selected[
                    selected.plz.astype(str) == row["postcode"]
                ]
            elif consider_location == "city":
                selected = selected[
                    selected.city == row.city.replace("\n", " ")
                ]
            elif consider_location == "federal_state":
                selected.loc[:, "federal_state"] = list_federal_states[
                    selected.federal_state
                ].values
                selected = selected[
                    selected.federal_state == row.federal_state
                ]

            # Set capacity constraint if selected
            if consider_carrier:
                selected = selected[selected.carrier == ET]

            # If a plant could be matched, add this to chp_NEP_matched
            if len(selected) > 0:
                chp_NEP_matched = chp_NEP_matched.append(
                    geopandas.GeoDataFrame(
                        data={
                            "source": "MaStR scaled with NEP 2021 list",
                            "MaStRNummer": selected.EinheitMastrNummer.head(1),
                            "carrier": (
                                ET if row.c2035_chp == "Nein" else "gas"
                            ),
                            "chp": True,
                            "el_capacity": row.c2035_capacity,
                            "th_capacity": selected.th_capacity.head(1),
                            "scenario": "eGon2035",
                            "geometry": selected.geometry.head(1),
                            "voltage_level": selected.voltage_level.head(1),
                        }
                    )
                )

                # Drop matched CHP from chp_NEP
                chp_NEP = chp_NEP.drop(index)

                # Drop matched CHP from MaStR list if the location is accurate
                if consider_capacity & consider_carrier:
                    MaStR_konv = MaStR_konv.drop(selected.index)

    return chp_NEP_matched, MaStR_konv, chp_NEP


################################################### Final table ###################################################
def insert_large_chp(sources, target, EgonChp):
    # Select CHP from NEP list
    chp_NEP = select_chp_from_nep(sources)

    # Select CHP from MaStR
    MaStR_konv = select_chp_from_mastr(sources)

    # Assign voltage level to MaStR
    MaStR_konv["voltage_level"] = assign_voltage_level(
        MaStR_konv.rename({"el_capacity": "Nettonennleistung"}, axis=1),
        config.datasets()["chp_location"],
    )

    # Initalize DataFrame for match CHPs
    chp_NEP_matched = geopandas.GeoDataFrame(
        columns=[
            "carrier",
            "chp",
            "el_capacity",
            "th_capacity",
            "scenario",
            "geometry",
            "MaStRNummer",
            "source",
            "voltage_level",
        ]
    )

    # Match CHP from NEP list using PLZ, carrier and capacity
    chp_NEP_matched, MaStR_konv, chp_NEP = match_nep_chp(
        chp_NEP, MaStR_konv, chp_NEP_matched, buffer_capacity=0.1
    )

    # Match CHP from NEP list using first 4 numbers of PLZ,
    # carrier and capacity
    chp_NEP_matched, MaStR_konv, chp_NEP = match_nep_chp(
        chp_NEP,
        MaStR_konv,
        chp_NEP_matched,
        buffer_capacity=0.1,
        consider_location="city",
    )

    # Aggregate units from MaStR to one power plant
    MaStR_konv = (
        MaStR_konv.groupby(
            [
                "plz",
                "Laengengrad",
                "Breitengrad",
                "carrier",
                "city",
                "federal_state",
            ]
        )[["el_capacity", "th_capacity", "EinheitMastrNummer"]]
        .sum(numeric_only=False)
        .reset_index()
    )
    MaStR_konv["geometry"] = geopandas.points_from_xy(
        MaStR_konv["Laengengrad"], MaStR_konv["Breitengrad"]
    )
    MaStR_konv["voltage_level"] = assign_voltage_level(
        MaStR_konv.rename({"el_capacity": "Nettonennleistung"}, axis=1),
        config.datasets()["chp_location"],
    )

    # Match CHP from NEP list with aggregated MaStR units
    chp_NEP_matched, MaStR_konv, chp_NEP = match_nep_chp(
        chp_NEP, MaStR_konv, chp_NEP_matched, buffer_capacity=0.1
    )

    # Match CHP from NEP list with aggregated MaStR units
    chp_NEP_matched, MaStR_konv, chp_NEP = match_nep_chp(
        chp_NEP,
        MaStR_konv,
        chp_NEP_matched,
        buffer_capacity=0.1,
        consider_location="city",
    )

    # Aggregate units from NEP to one power plant
    chp_NEP = (
        chp_NEP.groupby(
            ["postcode", "carrier", "city", "c2035_chp", "federal_state"]
        )[["capacity", "c2035_capacity"]]
        .sum()
        .reset_index()
    )

    # Match CHP from NEP list with aggregated MaStR units
    chp_NEP_matched, MaStR_konv, chp_NEP = match_nep_chp(
        chp_NEP, MaStR_konv, chp_NEP_matched, buffer_capacity=0.1
    )

    # Match CHP from NEP list with aggregated MaStR units
    chp_NEP_matched, MaStR_konv, chp_NEP = match_nep_chp(
        chp_NEP,
        MaStR_konv,
        chp_NEP_matched,
        buffer_capacity=0.1,
        consider_location="city",
    )

    chp_NEP_matched, MaStR_konv, chp_NEP = match_nep_chp(
        chp_NEP,
        MaStR_konv,
        chp_NEP_matched,
        buffer_capacity=0.3,
        consider_location="city",
    )

    chp_NEP_matched, MaStR_konv, chp_NEP = match_nep_chp(
        chp_NEP,
        MaStR_konv,
        chp_NEP_matched,
        buffer_capacity=0.3,
        consider_location="city",
        consider_carrier=False,
    )

    chp_NEP_matched, MaStR_konv, chp_NEP = match_nep_chp(
        chp_NEP,
        MaStR_konv,
        chp_NEP_matched,
        buffer_capacity=0.3,
        consider_location="city",
        consider_carrier=True,
        consider_capacity=False,
    )

    chp_NEP_matched, MaStR_konv, chp_NEP = match_nep_chp(
        chp_NEP,
        MaStR_konv,
        chp_NEP_matched,
        consider_location="city",
        consider_carrier=True,
        consider_capacity=False,
    )

    chp_NEP_matched, MaStR_konv, chp_NEP = match_nep_chp(
        chp_NEP,
        MaStR_konv,
        chp_NEP_matched,
        consider_location="city",
        consider_carrier=False,
        consider_capacity=False,
    )

    chp_NEP_matched, MaStR_konv, chp_NEP = match_nep_chp(
        chp_NEP,
        MaStR_konv,
        chp_NEP_matched,
        consider_location="federal_state",
        consider_carrier=False,
        consider_capacity=False,
    )

    # Prepare geometry for database import
    chp_NEP_matched["geometry_wkt"] = chp_NEP_matched["geometry"].apply(
        lambda geom: geom.wkt
    )

    print(f"{chp_NEP_matched.el_capacity.sum()} MW matched")
    print(f"{chp_NEP.c2035_capacity.sum()} MW not matched")

    chp_NEP.to_csv("not_matched_chp.csv")

    # Aggregate chp per location and carrier
    insert_chp = (
        chp_NEP_matched.groupby(["carrier", "geometry_wkt", "voltage_level"])[
            ["el_capacity", "th_capacity", "geometry", "MaStRNummer", "source"]
        ]
        .sum(numeric_only=False)
        .reset_index()
    )
    insert_chp.loc[:, "geometry"] = (
        chp_NEP_matched.drop_duplicates(subset="geometry_wkt")
        .set_index("geometry_wkt")
        .loc[insert_chp.set_index("geometry_wkt").index, "geometry"]
        .values
    )
    insert_chp.crs = "EPSG:4326"
    insert_chp_c = insert_chp.copy()

    # Assign bus_id
    insert_chp["bus_id"] = assign_bus_id(
        insert_chp, config.datasets()["chp_location"]
    ).bus_id

    # Assign gas bus_id
    insert_chp["gas_bus_id"] = db.assign_gas_bus_id(
        insert_chp_c, "eGon2035", "CH4"
    ).bus

    insert_chp = assign_use_case(insert_chp, sources)

    # Delete existing CHP in the target table
    db.execute_sql(
        f""" DELETE FROM {target['schema']}.{target['table']}
        WHERE carrier IN ('gas', 'other_non_renewable', 'oil')
        AND scenario='eGon2035';"""
    )

    # Insert into target table
    session = sessionmaker(bind=db.engine())()
    for i, row in insert_chp.iterrows():
        entry = EgonChp(
            sources={
                "chp": "MaStR",
                "el_capacity": row.source,
                "th_capacity": "MaStR",
            },
            source_id={"MastrNummer": row.MaStRNummer},
            carrier=row.carrier,
            el_capacity=row.el_capacity,
            th_capacity=row.th_capacity,
            voltage_level=row.voltage_level,
            electrical_bus_id=row.bus_id,
            ch4_bus_id=row.gas_bus_id,
            district_heating=row.district_heating,
            scenario="eGon2035",
            geom=f"SRID=4326;POINT({row.geometry.x} {row.geometry.y})",
        )
        session.add(entry)
    session.commit()

    return MaStR_konv
