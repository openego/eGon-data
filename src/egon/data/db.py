from contextlib import contextmanager

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import pandas as pd
import geopandas as gpd

from egon.data import config


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
    oemetadata/blob/develop/metadata/v140/metadata_key_description.md>`_
    standard for describging our data. Metadata is stored as JSON in the table
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

    check_json_str = (
        "SELECT obj_description('{0}.{1}'::regclass)::json".format(
            schema, table
        )
    )

    execute_sql(prefix_str + json + ";")

    # Query table comment and cast it into JSON
    # The query throws an error if JSON is invalid
    execute_sql(check_json_str)


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

def select_geodataframe(sql, index_col=None, geom_col='geom', epsg=3035):
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
