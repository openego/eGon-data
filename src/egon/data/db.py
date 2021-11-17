import codecs
import functools
from contextlib import contextmanager

import geopandas as gpd
import pandas as pd
from egon.data import config
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
from time import sleep


def credentials():
    """Return local database connection parameters.

    Returns
    -------
    dict
        Complete DB connection information
    """
    translated = {
        "--database-name": "POSTGRES_DB",
        "--database-password": "POSTGRES_PASSWORD",
        "--database-host": "HOST",
        "--database-port": "PORT",
        "--database-user": "POSTGRES_USER",
    }
    configuration = config.settings()["egon-data"]
    update = {
        translated[flag]: configuration[flag]
        for flag in configuration
        if flag in translated
    }
    configuration.update(update)
    return configuration


def engine():
    """Engine for local database."""
    db_config = credentials()
    return create_engine(
        f"postgresql+psycopg2://{db_config['POSTGRES_USER']}:"
        f"{db_config['POSTGRES_PASSWORD']}@{db_config['HOST']}:"
        f"{db_config['PORT']}/{db_config['POSTGRES_DB']}",
        echo=False,
    )


def execute_sql(sql_string):
    """Execute a SQL expression given as string.

    The SQL expression passed as plain string is convert to a
    `sqlalchemy.sql.expression.TextClause`.

    Parameters
    ----------
    sql_string : str
        SQL expression

    """
    engine_local = engine()

    with engine_local.connect().execution_options(autocommit=True) as con:
        con.execute(text(sql_string))


def submit_comment(json, schema, table):
    """Add comment to table.

    We use `Open Energy Metadata <https://github.com/OpenEnergyPlatform/
    oemetadata/blob/develop/metadata/v141/metadata_key_description.md>`_
    standard for describing our data. Metadata is stored as JSON in the table
    comment.

    Parameters
    ----------
    json : str
        JSON string reflecting comment
    schema : str
        The target table's database schema
    table : str
        Database table on which to put the given comment
    """
    prefix_str = "COMMENT ON TABLE {0}.{1} IS ".format(schema, table)

    check_json_str = "SELECT obj_description('{0}.{1}'::regclass)::json".format(
        schema, table
    )

    execute_sql(prefix_str + json + ";")

    # Query table comment and cast it into JSON
    # The query throws an error if JSON is invalid
    execute_sql(check_json_str)


def execute_sql_script(script, encoding="utf-8-sig"):
    """Execute a SQL script given as a file name.

    Parameters
    ----------
    script : str
        Path of the SQL-script
    encoding : str
        Encoding which is used for the SQL file. The default is "utf-8-sig".
    Returns
    -------
    None.

    """

    with codecs.open(script, "r", encoding) as fd:
        sqlfile = fd.read()

    execute_sql(sqlfile)


@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    Session = sessionmaker(bind=engine())
    session = Session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def session_scoped(function):
    """Provide a session scope to a function.

    Can be used as a decorator like this:

    >>> @session_scoped
    ... def get_bind(session):
    ...     return session.get_bind()
    ...
    >>> get_bind()
    Engine(postgresql+psycopg2://egon:***@127.0.0.1:59734/egon-data)

    Note that the decorated function needs to accept a parameter named
    `session`, but is called without supplying a value for that parameter
    because the parameter's value will be filled in by `session_scoped`.
    Using this decorator allows saving an indentation level when defining
    such functions but it also has other usages.
    """

    @functools.wraps(function)
    def wrapped(*xs, **ks):
        with session_scope() as session:
            return function(session=session, *xs, **ks)

    return wrapped


def select_dataframe(sql, index_col=None):
    """ Select data from local database as pandas.DataFrame

    Parameters
    ----------
    sql : str
        SQL query to be executed.
    index_col : str, optional
        Column(s) to set as index(MultiIndex). The default is None.

    Returns
    -------
    df : pandas.DataFrame
        Data returned from SQL statement.

    """

    df = pd.read_sql(sql, engine(), index_col=index_col)

    if df.size == 0:
        print(f"WARNING: No data returned by statement: \n {sql}")

    return df


def select_geodataframe(sql, index_col=None, geom_col="geom", epsg=3035):
    """ Select data from local database as geopandas.GeoDataFrame

    Parameters
    ----------
    sql : str
        SQL query to be executed.
    index_col : str, optional
        Column(s) to set as index(MultiIndex). The default is None.
    geom_col : str, optional
        column name to convert to shapely geometries. The default is 'geom'.
    epsg : int, optional
        EPSG code specifying output projection. The default is 3035.

    Returns
    -------
    gdf : pandas.DataFrame
        Data returned from SQL statement.

    """

    gdf = gpd.read_postgis(
        sql, engine(), index_col=index_col, geom_col=geom_col
    ).to_crs(epsg=epsg)

    if gdf.size == 0:
        print(f"WARNING: No data returned by statement: \n {sql}")

    return gdf


def next_etrago_id(component):
    """ Select next id value for components in etrago tables

    Parameters
    ----------
    component : str
        Name of componenet

    Returns
    -------
    next_id : int
        Next index value

    """
    max_id = select_dataframe(
        f"""
        SELECT MAX({component}_id) FROM grid.egon_etrago_{component}
        """
    )["max"][0]

    if max_id:
        next_id = max_id + 1
    else:
        next_id = 1

    return next_id


def to_db(data, component, retry=0, **kwargs):
    """Write DataFrame or GeoDataFrame to database and allocate *_id column.

    This method prevents errors, when multiple methods try to fetch their
    *_id column values simultaneously before writing to the db. Blabla

    Parameters
    ----------
    data : pandas.core.frame.DataFrame, geopandas.geodataframe.GeoDataFrame
        Frame with data to write to db

    component : str
        Name of componenet

    retry : int
        Number of retries

    kwargs : dict
        Keyword arguments for to_postgis or to_sql method

    Returns
    -------
    component_id : pandas.core.series.Series
        *_id column values

    """
    next_id = next_etrago_id(component)
    data[component + '_id'] = range(next_id, next_id + len(data))
    try:
        if isinstance(data, gpd.GeoDataFrame):
                # test if geometry column is set: https://github.com/geopandas/geopandas/blob/17fe21ed15442d2cd30bd3d39171e1e6e2b44b68/geopandas/geodataframe.py#L201-L207
                # if not, treat identical to DataFrame
            if data._geometry_column_name in data:
                data.to_postgis(**kwargs)
            else:
                data.to_sql(**kwargs)
        elif isinstance(data, pd.DataFrame):
            data.to_sql(**kwargs)
        else:
            msg = (
                "Parameter 'data' must be a pandas.DataFrame or a "
                "geopandas.GeoDataFrame."
            )
            raise TypeError(msg)
    except IntegrityError:
        if retry < 5:
            # wait a second or two
            # consider making this wait a random amount of hours and sometime
            # in the future changing it to a small amount to claim we have
            # accelerated the code.
            # Maybe try sleep(-86400) so the run will have finished by yesterday
            sleep(1)
            to_db(data, component, retry=retry + 1, **kwargs)
        else:
            msg = "Some other error message"
            raise ValueError(msg)

    # return the assigned ids for further use
    return data[component + '_id']
