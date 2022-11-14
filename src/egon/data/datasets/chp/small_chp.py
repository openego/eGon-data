"""
The module containing all code dealing with chp < 10MW.
"""
import geopandas as gpd
import numpy as np

from egon.data import config, db
from egon.data.datasets.power_plants import (
    assign_bus_id,
    filter_mastr_geometry,
    select_target,
)


@db.session_scoped
def insert_mastr_chp(mastr_chp, EgonChp, session=None):
    """Insert MaStR data from exising CHPs into database table

    Parameters
    ----------
    mastr_chp : pandas.DataFrame
        List of existing CHPs in MaStR.
    EgonChp : class
        Class definition of daabase table for CHPs
    session : sqlalchemy.orm.Session
        The session inside which this function operates. Ignore this, because
        it will be supplied automatically.

    Returns
    -------
    None.

    """

    for i, row in mastr_chp.iterrows():
        entry = EgonChp(
            sources={
                "chp": "MaStR",
                "el_capacity": "MaStR",
                "th_capacity": "MaStR",
            },
            source_id={"MastrNummer": row.EinheitMastrNummer},
            carrier="gas",
            el_capacity=row.el_capacity,
            th_capacity=row.th_capacity,
            electrical_bus_id=row.bus_id,
            ch4_bus_id=row.gas_bus_id,
            district_heating=row.district_heating,
            voltage_level=row.voltage_level,
            scenario="eGon2035",
            geom=f"SRID=4326;POINT({row.geometry.x} {row.geometry.y})",
        )
        session.add(entry)


def existing_chp_smaller_10mw(sources, MaStR_konv, EgonChp):
    """Insert existing small CHPs based on MaStR and target values

    Parameters
    ----------
    MaStR_konv : pandas.DataFrame
        List of conevntional CHPs in MaStR whoes locateion is not used
    EgonChp : class
        Class definition of daabase table for CHPs

    Returns
    -------
    additional_capacitiy : pandas.Series
        Capacity of new locations for small chp per federal state

    """

    existsting_chp_smaller_10mw = MaStR_konv[
        # (MaStR_konv.Nettonennleistung>0.1)
        (MaStR_konv.el_capacity <= 10)
        & (MaStR_konv.th_capacity > 0)
    ]

    targets = select_target("small_chp", "eGon2035")

    for federal_state in targets.index:
        mastr_chp = gpd.GeoDataFrame(
            filter_mastr_geometry(existsting_chp_smaller_10mw, federal_state)
        )

        mastr_chp.crs = "EPSG:4326"

        # Assign gas bus_id
        mastr_chp_c = mastr_chp.copy()
        mastr_chp["gas_bus_id"] = db.assign_gas_bus_id(
            mastr_chp_c, "eGon2035", "CH4"
        ).bus

        # Assign bus_id
        mastr_chp["bus_id"] = assign_bus_id(
            mastr_chp, config.datasets()["chp_location"]
        ).bus_id

        mastr_chp = assign_use_case(mastr_chp, sources)

        insert_mastr_chp(mastr_chp, EgonChp)


@db.session_scoped
def extension_to_areas(
    areas,
    additional_capacity,
    existing_chp,
    flh,
    EgonChp,
    district_heating=True,
    scenario="eGon2035",
    session=None,
):
    """Builds new CHPs on potential industry or district heating areas.

    This method can be used to distrectly extend and spatial allocate CHP
    for industry or district heating areas.
    The following steps are running in a loop until the additional
    capacity is reached:

        1. Randomly select an existing CHP < 10MW and its parameters.

        2. Select possible areas where the CHP can be located.
        It is assumed that CHPs are only build if the demand of the industry
        or district heating grid exceeds the annual energy output of the CHP.
        The energy output is calculated using the installed capacity and
        estimated full load hours.
        The thermal output is used for district heating areas. Since there are
        no explicit heat demands for industry, the electricity output and
        demands are used.

        3. Randomly select one of the possible areas.
        The areas are weighted by the annal demand, assuming that the
        possibility of building a CHP plant is higher when for large consumers.

        4. Insert allocated CHP plant into the database

        5. Substract capacity of new build CHP from the additional capacity.
        The energy demands of the areas are reduced by the estimated energy
        output of the CHP plant.

    Parameters
    ----------
    areas : geopandas.GeoDataFrame
        Possible areas for a new CHP plant, including their energy demand
    additional_capacity : float
        Overall eletcrical capacity of CHPs that should be build in MW.
    existing_chp : pandas.DataFrame
        List of existing CHP plants including electrical and thermal capacity
    flh : int
        Assumed electrical or thermal full load hours.
    EgonChp : class
        ORM-class definition of CHP database-table.
    district_heating : boolean, optional
        State if the areas are district heating areas. The default is True.
    session : sqlalchemy.orm.Session
        The session inside which this function operates. Ignore this, because
        it will be supplied automatically.

    Returns
    -------
    None.

    """
    np.random.seed(seed=config.settings()["egon-data"]["--random-seed"])

    # Add new CHP as long as the additional capacity is not reached
    while additional_capacity > existing_chp.el_capacity.min():

        if district_heating:
            selected_areas = areas.loc[
                areas.demand > existing_chp.th_capacity.min() * flh, :
            ]
        else:
            selected_areas = areas.loc[
                areas.demand > existing_chp.el_capacity.min() * flh, :
            ]

        if len(selected_areas) > 0:

            selected_areas = selected_areas.to_crs(4326)
            # Assign gas bus_id
            selected_areas["gas_bus_id"] = db.assign_gas_bus_id(
                selected_areas.copy(), "eGon2035", "CH4"
            ).bus

            # Select randomly one area from the list of possible areas
            # weighted by the share of demand
            id_area = np.random.choice(
                selected_areas.index,
                p=selected_areas.demand / selected_areas.demand.sum(),
            )
            selected_areas.drop(
                selected_areas.loc[selected_areas.index != id_area, :].index,
                inplace=True,
            )
            # selected_area = possible_areas.loc[possible_areas.index==id_area, :]

            if district_heating:
                possible_chp = existing_chp[
                    (
                        existing_chp.th_capacity * flh
                        <= selected_areas.demand.values[0]
                    )
                    & (existing_chp.el_capacity <= additional_capacity)
                ]
            else:
                possible_chp = existing_chp[
                    (
                        existing_chp.el_capacity * flh
                        <= selected_areas.demand.values[0]
                    )
                    & (existing_chp.el_capacity <= additional_capacity)
                ]

            # Select random new build CHP from list of existing CHP
            # which is smaller than the remaining capacity to distribute

            if len(possible_chp) > 0:
                id_chp = np.random.choice(range(len(possible_chp)))
                selected_chp = possible_chp.iloc[id_chp]

                # Assign bus_id
                selected_areas["voltage_level"] = selected_chp["voltage_level"]

                selected_areas.loc[:, "bus_id"] = assign_bus_id(
                    selected_areas, config.datasets()["chp_location"]
                ).bus_id

                entry = EgonChp(
                    sources={
                        "chp": "MaStR",
                        "el_capacity": "MaStR",
                        "th_capacity": "MaStR",
                        "CHP extension algorithm": "",
                    },
                    carrier="gas extended",
                    el_capacity=selected_chp.el_capacity,
                    th_capacity=selected_chp.th_capacity,
                    district_heating=district_heating,
                    voltage_level=selected_chp.voltage_level,
                    electrical_bus_id=int(selected_areas.bus_id),
                    ch4_bus_id=int(selected_areas.gas_bus_id),
                    scenario=scenario,
                    geom=f"""
                            SRID=4326;
                            POINT({selected_areas.geom.values[0].x} {selected_areas.geom.values[0].y})
                            """,
                )
                if district_heating:
                    entry.district_heating_area_id = int(
                        selected_areas.area_id
                    )

                session.add(entry)
                session.commit()

                # Reduce additional capacity by newly build CHP
                additional_capacity -= selected_chp.el_capacity

                # Reduce the demand of the selected area by the estimated
                # enrgy output of the CHP
                if district_heating:
                    areas.loc[
                        areas.index[
                            areas.area_id == selected_areas.area_id.values[0]
                        ],
                        "demand",
                    ] -= (
                        selected_chp.th_capacity * flh
                    )
                else:
                    areas.loc[
                        areas.index[
                            areas.osm_id == selected_areas.osm_id.values[0]
                        ],
                        "demand",
                    ] -= (
                        selected_chp.th_capacity * flh
                    )
                areas = areas[areas.demand > 0]

        else:
            print(f"{additional_capacity} MW are not matched to an area.")
            break

    return additional_capacity


def extension_district_heating(
    federal_state,
    additional_capacity,
    flh_chp,
    EgonChp,
    areas_without_chp_only=False,
):
    """Build new CHP < 10 MW for district areas considering existing CHP
    and the heat demand.

    For more details on the placement alogrithm have a look at the description
    of extension_to_areas().

    Parameters
    ----------
    federal_state : str
        Name of the federal state.
    additional_capacity : float
        Additional electrical capacity of new CHP plants in district heating
    flh_chp : int
        Assumed number of full load hours of heat output.
    EgonChp : class
        ORM-class definition of CHP database-table.
    areas_without_chp_only : boolean, optional
        Set if CHPs are only assigned to district heating areas which don't
        have an existing CHP. The default is True.

    Returns
    -------
    None.

    """

    sources = config.datasets()["chp_location"]["sources"]
    targets = config.datasets()["chp_location"]["targets"]

    existing_chp = db.select_dataframe(
        f"""
        SELECT el_capacity, th_capacity, voltage_level, b.area_id
        FROM
        {targets['chp_table']['schema']}.
        {targets['chp_table']['table']} a,
        {sources['district_heating_areas']['schema']}.
        {sources['district_heating_areas']['table']} b
        WHERE a.scenario = 'eGon2035'
        AND b.scenario = 'eGon2035'
        AND district_heating = True
        AND ST_Intersects(
            ST_Transform(
                ST_Centroid(geom_polygon), 4326),
            (SELECT ST_Union(geometry)
             FROM {sources['vg250_lan']['schema']}.
             {sources['vg250_lan']['table']}
            WHERE REPLACE(REPLACE(gen, '-', ''), 'ü', 'ue') ='{federal_state}'))
        AND el_capacity < 10
        ORDER BY el_capacity, residential_and_service_demand

        """
    )

    # Select all district heating areas without CHP

    try:
        dh_areas = db.select_geodataframe(
            f"""
            SELECT
            residential_and_service_demand as demand, area_id,
            ST_Transform(ST_PointOnSurface(geom_polygon), 4326)  as geom
            FROM
            {sources['district_heating_areas']['schema']}.
            {sources['district_heating_areas']['table']}
            WHERE scenario = 'eGon2035'
            AND ST_Intersects(ST_Transform(ST_Centroid(geom_polygon), 4326), (
                SELECT ST_Union(d.geometry)
                FROM
                {sources['vg250_lan']['schema']}.{sources['vg250_lan']['table']} d
                WHERE REPLACE(REPLACE(gen, '-', ''), 'ü', 'ue') ='{federal_state}'))
            AND area_id NOT IN (
                SELECT district_heating_area_id
                FROM {targets['chp_table']['schema']}.
                {targets['chp_table']['table']}
                WHERE scenario = 'eGon2035'
                AND district_heating = TRUE)
            """
        )
    except:
        dh_areas = gpd.GeoDataFrame(
            columns=["demand", "area_id", "geom"]
        ).set_geometry("geom")
        dh_areas = dh_areas.set_crs(3035)

    if not areas_without_chp_only:
        # Append district heating areas with CHP
        # assumed dispatch of existing CHP is substracted from remaining demand
        dh_areas = dh_areas.append(
            db.select_geodataframe(
                f"""
                SELECT
                b.residential_and_service_demand - sum(a.el_capacity)*{flh_chp}
                as demand, b.area_id,
                ST_Transform(ST_PointOnSurface(geom_polygon), 4326) as geom
                FROM
                {targets['chp_table']['schema']}.
                {targets['chp_table']['table']} a,
                {sources['district_heating_areas']['schema']}.
                {sources['district_heating_areas']['table']} b
                WHERE b.scenario = 'eGon2035'
                AND a.scenario = 'eGon2035'
                AND ST_Intersects(
                    ST_Transform(ST_Centroid(geom_polygon), 4326),
                    (SELECT ST_Union(d.geometry)
                     FROM {sources['vg250_lan']['schema']}.
                      {sources['vg250_lan']['table']} d
                    WHERE REPLACE(REPLACE(gen, '-', ''), 'ü', 'ue') ='{federal_state}'))
                AND a.district_heating_area_id = b.area_id
                GROUP BY (
                    b.residential_and_service_demand,
                    b.area_id, geom_polygon)
                """,
                epsg=3035,
            ),
            ignore_index=True,
        ).set_crs(3035, allow_override=True)

    not_distributed_capacity = extension_to_areas(
        dh_areas,
        additional_capacity,
        existing_chp,
        flh_chp,
        EgonChp,
        district_heating=True,
    )

    return not_distributed_capacity


def extension_industrial(federal_state, additional_capacity, flh_chp, EgonChp):
    """Build new CHP < 10 MW for industry considering existing CHP,
    osm landuse areas and electricity demands.

    For more details on the placement alogrithm have a look at the description
    of extension_to_areas().

    Parameters
    ----------
    federal_state : str
        Name of the federal state.
    additional_capacity : float
        Additional electrical capacity of new CHP plants in indsutry.
    flh_chp : int
        Assumed number of full load hours of electricity output.
    EgonChp : class
        ORM-class definition of CHP database-table.

    Returns
    -------
    None.

    """

    sources = config.datasets()["chp_location"]["sources"]
    targets = config.datasets()["chp_location"]["targets"]

    existing_chp = db.select_dataframe(
        f"""
        SELECT el_capacity, th_capacity, voltage_level
        FROM
        {targets['chp_table']['schema']}.
        {targets['chp_table']['table']} a
        WHERE a.scenario = 'eGon2035'
        AND district_heating = False
        AND el_capacity < 10
        ORDER BY el_capacity

        """
    )

    # Select all industrial areas without CHP
    industry_areas = db.select_geodataframe(
        f"""
        SELECT
        SUM(demand) as demand, a.osm_id, ST_Centroid(b.geom) as geom, b.name
        FROM
        {sources['industrial_demand_osm']['schema']}.
        {sources['industrial_demand_osm']['table']} a,
        {sources['osm_landuse']['schema']}.
        {sources['osm_landuse']['table']} b
        WHERE a.scenario = 'eGon2035'
        AND b.id = a.osm_id
        AND NOT ST_Intersects(
            ST_Transform(b.geom, 4326),
            (SELECT ST_Union(geom) FROM
              {targets['chp_table']['schema']}.
              {targets['chp_table']['table']}
              ))
        AND b.tags::json->>'landuse' = 'industrial'
        AND b.name NOT LIKE '%%kraftwerk%%'
        AND b.name NOT LIKE '%%Stadtwerke%%'
        AND b.name NOT LIKE '%%Müllverbrennung%%'
        AND b.name NOT LIKE '%%Müllverwertung%%'
        AND b.name NOT LIKE '%%Abfall%%'
        AND b.name NOT LIKE '%%Kraftwerk%%'
        AND b.name NOT LIKE '%%Wertstoff%%'
        AND b.name NOT LIKE '%%olarpark%%'
        AND b.name NOT LIKE '%%Gewerbegebiet%%'
        AND b.name NOT LIKE '%%Gewerbepark%%'
        AND ST_Intersects(
            ST_Transform(ST_Centroid(b.geom), 4326),
            (SELECT ST_Union(d.geometry)
             FROM {sources['vg250_lan']['schema']}.
             {sources['vg250_lan']['table']} d
             WHERE REPLACE(REPLACE(gen, '-', ''), 'ü', 'ue') ='{federal_state}'))

        GROUP BY (a.osm_id, b.geom, b.name)
        ORDER BY SUM(demand)

        """
    )

    not_distributed_capacity = extension_to_areas(
        industry_areas,
        additional_capacity,
        existing_chp,
        flh_chp,
        EgonChp,
        district_heating=False,
    )

    return not_distributed_capacity


def extension_per_federal_state(federal_state, EgonChp):
    """Adds new CHP plants to meet target value per federal state.

    The additional capacity for CHPs < 10 MW is distributed discretly.
    Therefore, existing CHPs and their parameters from Marktstammdatenregister
    are randomly selected and allocated in a district heating grid.
    In order to generate a reasonable distribution, new CHPs can only
    be assigned to a district heating grid which needs additional supply
    technologies. This is estimated by the substraction of demand, and the
    assumed dispatch oof a CHP considering the capacitiy and full load hours
    of each CHPs.

    Parameters
    ----------
    additional_capacity : float
        Capacity to distribute.
    federal_state : str
        Name of the federal state
    EgonChp : class
        ORM-class definition of CHP table

    Returns
    -------
    None.

    """

    sources = config.datasets()["chp_location"]["sources"]
    target_table = config.datasets()["chp_location"]["targets"]["chp_table"]

    targets = select_target("small_chp", "eGon2035")

    existing_capacity = db.select_dataframe(
        f"""
            SELECT SUM(el_capacity) as capacity, district_heating
            FROM {target_table['schema']}.
            {target_table['table']}
            WHERE sources::json->>'el_capacity' = 'MaStR'
            AND carrier != 'biomass'
            AND scenario = 'eGon2035'
            AND ST_Intersects(geom, (
            SELECT ST_Union(geometry) FROM
            {sources['vg250_lan']['schema']}.{sources['vg250_lan']['table']} b
            WHERE REPLACE(REPLACE(gen, '-', ''), 'ü', 'ue') ='{federal_state}'))
            GROUP BY district_heating
            """
    )

    print(f"Target capacity in {federal_state}: {targets[federal_state]}")
    print(
        f"Existing capacity in {federal_state}: {existing_capacity.capacity.sum()}"
    )

    additional_capacity = (
        targets[federal_state] - existing_capacity.capacity.sum()
    )

    if additional_capacity > 0:

        share_dh = (
            existing_capacity[
                existing_capacity.district_heating
            ].capacity.values[0]
            / existing_capacity.capacity.sum()
        )

        flh_chp = 6000

        capacity_district_heating = additional_capacity * share_dh
        capacity_industry = additional_capacity * (1 - share_dh)

        print(f"Distributing {additional_capacity} MW_el in {federal_state}")
        print(
            f"Distributing {capacity_district_heating} MW_el to district heating"
        )
        not_distributed_capacity_dh = extension_district_heating(
            federal_state, capacity_district_heating, flh_chp, EgonChp
        )

        if not_distributed_capacity_dh > 1:
            print(
                f"{not_distributed_capacity_dh} MW_el were not matched to district "
                "heating. This capacity is added to industry"
            )
            capacity_industry += not_distributed_capacity_dh

        print(f"Distributing {capacity_industry} MW_el to industry")
        not_distributed_capacity_industry = extension_industrial(
            federal_state,
            additional_capacity * (1 - share_dh),
            flh_chp,
            EgonChp,
        )

        print(
            f"{not_distributed_capacity_industry} MW_el were not matched to "
            "industry. This capacity is added to district heating"
        )

        if not_distributed_capacity_industry > 1:
            print(
                f"{not_distributed_capacity_industry} MW_el were not matched to "
                "industry. This capacity is added to district heating"
            )

            extension_district_heating(
                federal_state,
                not_distributed_capacity_industry,
                flh_chp,
                EgonChp,
            )

    else:
        print("Decommissioning of CHP plants is not implemented.")


def assign_use_case(chp, sources):
    """Identifies CHPs used in district heating areas.

    A CHP plant is assigned to a district heating area if
    - it is closer than 1km to the borders of the district heating area
    - the name of the osm landuse area where the CHP is located indicates
    that it feeds in to a district heating area (e.g. 'Stadtwerke')
    - it is not closer than 100m to an industrial area

    Parameters
    ----------
    chp : pandas.DataFrame
        CHPs without district_heating flag

    Returns
    -------
    chp : pandas.DataFrame
        CHPs with identification of district_heating CHPs


    """
    # Select osm industrial areas which don't include power or heat supply
    # (name not includes 'Stadtwerke', 'Kraftwerk', 'Müllverbrennung'...)
    landuse_industrial = db.select_geodataframe(
        f"""
        SELECT ST_Buffer(geom, 100) as geom,
         tags::json->>'name' as name
         FROM {sources['osm_landuse']['schema']}.
        {sources['osm_landuse']['table']}
        WHERE tags::json->>'landuse' = 'industrial'
        AND(name NOT LIKE '%%kraftwerk%%'
        OR name NOT LIKE '%%Müllverbrennung%%'
        OR name LIKE '%%Müllverwertung%%'
        OR name NOT LIKE '%%Abfall%%'
        OR name NOT LIKE '%%Kraftwerk%%'
        OR name NOT LIKE '%%Wertstoff%%')
        """,
        epsg=4326,
    )

    # Select osm polygons where a district heating chp is likely
    # (name includes 'Stadtwerke', 'Kraftwerk', 'Müllverbrennung'...)
    possible_dh_locations = db.select_geodataframe(
        f"""
        SELECT ST_Buffer(geom, 100) as geom,
         tags::json->>'name' as name
         FROM {sources['osm_polygon']['schema']}.
        {sources['osm_polygon']['table']}
        WHERE name LIKE '%%Stadtwerke%%'
        OR name LIKE '%%kraftwerk%%'
        OR name LIKE '%%Müllverbrennung%%'
        OR name LIKE '%%Müllverwertung%%'
        OR name LIKE '%%Abfall%%'
        OR name LIKE '%%Kraftwerk%%'
        OR name LIKE '%%Wertstoff%%'
        """,
        epsg=4326,
    )

    # Initilize district_heating argument
    chp["district_heating"] = False
    # chp.loc[chp[chp.Nettonennleistung <= 0.15].index, 'use_case'] = 'individual'
    # Select district heating areas with buffer of 1 km
    district_heating = db.select_geodataframe(
        f"""
        SELECT area_id, ST_Buffer(geom_polygon, 1000) as geom
        FROM {sources['district_heating_areas']['schema']}.
        {sources['district_heating_areas']['table']}
        WHERE scenario = 'eGon2035'
        """,
        epsg=4326,
    )

    # Select all CHP closer than 1km to a district heating area
    # these are possible district heating chp
    # Chps which are not close to a district heating area get use_case='industrial'
    close_to_dh = chp[chp.index.isin(gpd.sjoin(chp, district_heating).index)]

    # All chp which are close to a district heating grid and intersect with
    # osm polygons whoes name indicates that it could be a district heating location
    # (e.g. Stadtwerke, Heizraftwerk, Müllverbrennung)
    # are assigned as district heating chp
    district_heating_chp = chp[
        chp.index.isin(gpd.sjoin(close_to_dh, possible_dh_locations).index)
    ]

    # Assigned district heating chps are dropped from list of possible
    # district heating chp
    close_to_dh.drop(district_heating_chp.index, inplace=True)

    # Select all CHP closer than 100m to a industrial location its name
    # doesn't indicate that it could be a district heating location
    # these chp get use_case='industrial'
    close_to_industry = chp[
        chp.index.isin(gpd.sjoin(close_to_dh, landuse_industrial).index)
    ]

    # Chp which are close to a district heating area and not close to an
    # industrial location are assigned as district_heating_chp
    district_heating_chp = district_heating_chp.append(
        close_to_dh[~close_to_dh.index.isin(close_to_industry.index)]
    )

    # Set district_heating = True for all district heating chp
    chp.loc[district_heating_chp.index, "district_heating"] = True

    return chp
