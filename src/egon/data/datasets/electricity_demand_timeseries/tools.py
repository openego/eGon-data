from io import StringIO
import csv

from shapely.geometry import Point
import geopandas as gpd
import numpy as np
import pandas as pd

from egon.data import db, logger

engine = db.engine()


def random_point_in_square(geom, tol):
    """
    Generate a random point within a square

    Parameters
    ----------
    geom: gpd.Series
        Geometries of square
    tol: float
        tolerance to square bounds

    Returns
    -------
    points: gpd.Series
        Series of random points
    """
    # cell bounds - half edge_length to not build buildings on the cell border
    xmin = geom.bounds["minx"] + tol / 2
    xmax = geom.bounds["maxx"] - tol / 2
    ymin = geom.bounds["miny"] + tol / 2
    ymax = geom.bounds["maxy"] - tol / 2

    # generate random coordinates within bounds - half edge_length
    x = (xmax - xmin) * np.random.rand(geom.shape[0]) + xmin
    y = (ymax - ymin) * np.random.rand(geom.shape[0]) + ymin

    points = pd.Series([Point(cords) for cords in zip(x, y)])
    points = gpd.GeoSeries(points, crs="epsg:3035")

    return points


# distribute amenities evenly
def specific_int_until_sum(s_sum, i_int):
    """
    Generate list `i_int` summing to `s_sum`. Last value will be <= `i_int`
    """
    list_i = [] if [s_sum % i_int] == [0] else [s_sum % i_int]
    list_i += s_sum // i_int * [i_int]
    return list_i


def random_ints_until_sum(s_sum, m_max):
    """
    Generate non-negative random integers < `m_max` summing to `s_sum`.
    """
    list_r = []
    while s_sum > 0:
        r = np.random.randint(1, m_max + 1)
        r = r if r <= m_max and r < s_sum else s_sum
        list_r.append(r)
        s_sum -= r
    return list_r


def write_table_to_postgis(gdf, table, engine=db.engine(), drop=True):
    """
    Helper function to append df data to table in db. Only predefined columns
    are passed. Error will raise if column is missing. Dtype of columns are
    taken from table definition.

    Parameters
    ----------
    gdf: gpd.DataFrame
        Table of data
    table: declarative_base
        Metadata of db table to export to
    engine:
        connection to database db.engine()
    drop: bool
        Drop table before appending

    """

    # Only take in db table defined columns
    columns = [column.key for column in table.__table__.columns]
    gdf = gdf.loc[:, columns]

    if drop:
        table.__table__.drop(bind=engine, checkfirst=True)
        table.__table__.create(bind=engine)

    dtypes = {
        i: table.__table__.columns[i].type
        for i in table.__table__.columns.keys()
    }

    # Write new buildings incl coord into db
    gdf.to_postgis(
        name=table.__tablename__,
        con=engine,
        if_exists="append",
        schema=table.__table_args__["schema"],
        dtype=dtypes,
    )


def psql_insert_copy(table, conn, keys, data_iter):
    """
    Execute SQL statement inserting data

    Parameters
    ----------
    table : pandas.io.sql.SQLTable
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
    keys : list of str
        Column names
    data_iter : Iterable that iterates the values to be inserted
    """
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ", ".join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = "{}.{}".format(table.schema, table.name)
        else:
            table_name = table.name

        sql = "COPY {} ({}) FROM STDIN WITH CSV".format(table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)


def write_table_to_postgres(
    df, db_table, drop=False, index=False, if_exists="append"
):
    """
    Helper function to append df data to table in db. Fast string-copy is used.
    Only predefined columns are passed. If column is missing in dataframe a
    warning is logged. Dtypes of columns are taken from table definition. The
    writing process happens in a scoped session.

    Parameters
    ----------
    df: pd.DataFrame
        Table of data
    db_table: declarative_base
        Metadata of db table to export to
    drop: boolean, default False
        Drop db-table before appending
    index: boolean, default False
        Write DataFrame index as a column.
    if_exists: {'fail', 'replace', 'append'}, default 'append'
        - fail: If table exists, do nothing.
        - replace: If table exists, drop it, recreate it, and insert data.
        - append: If table exists, insert data. Create if does not exist.

    """
    logger.info("Write table to db")
    # Only take in db table defined columns and dtypes
    columns = {
        column.key: column.type for column in db_table.__table__.columns
    }

    # Take only the columns defined in class
    # pandas raises an error if column is missing
    try:
        df = df.loc[:, columns.keys()]
    except KeyError:
        same = df.columns.intersection(columns.keys())
        missing = same.symmetric_difference(df.columns)
        logger.warning(f"Columns: {missing.values} missing!")
        df = df.loc[:, same]

    if drop:
        db_table.__table__.drop(bind=engine, checkfirst=True)
        db_table.__table__.create(bind=engine)

    with db.session_scope() as session:
        df.to_sql(
            name=db_table.__table__.name,
            schema=db_table.__table__.schema,
            con=session.connection(),
            if_exists=if_exists,
            index=index,
            method=psql_insert_copy,
            dtype=columns,
        )
