from egon.data import db
import egon.data.config


def modify_tables():
    """Adjust primary keys, indices and schema of OSM tables.

    * The Column "gid" is added and used as the new primary key.
    * Indices (GIST, GIN) are reset
    * The tables are moved to the schema configured as the "output_schema".
    """
    # Get dataset config
    data_config = egon.data.config.datasets()["openstreetmap"]

    # Replace indices and primary keys
    for table in [
        f"{data_config['original_data']['target']['table_prefix']}_" + suffix
        for suffix in ["line", "point", "polygon", "roads"]
    ]:

        # Drop indices
        sql_statements = [f"DROP INDEX IF EXISTS {table}_index;"]

        # Drop primary keys
        sql_statements.append(f"DROP INDEX IF EXISTS {table}_pkey;")

        # Add primary key on newly created column "gid"
        sql_statements.append(f"ALTER TABLE public.{table} ADD gid SERIAL;")
        sql_statements.append(
            f"ALTER TABLE public.{table} ADD PRIMARY KEY (gid);"
        )
        sql_statements.append(
            f"ALTER TABLE public.{table} RENAME COLUMN way TO geom;"
        )

        # Add indices (GIST and GIN)
        sql_statements.append(
            f"CREATE INDEX {table}_geom_idx ON public.{table} "
            f"USING gist (geom);"
        )
        sql_statements.append(
            f"CREATE INDEX {table}_tags_idx ON public.{table} "
            f"USING GIN (tags);"
        )

        # Execute collected SQL statements
        for statement in sql_statements:
            db.execute_sql(statement)

    # Move table to schema "openstreetmap"
    db.execute_sql(
        f"CREATE SCHEMA IF NOT EXISTS {data_config['processed']['schema']};"
    )

    for out_table in data_config["processed"]["tables"]:
        db.execute_sql(f"DROP TABLE IF EXISTS "
                       f"{data_config['processed']['schema']}.{out_table};")

        sql_statement = (
            f"ALTER TABLE public.{out_table} "
            f"SET SCHEMA {data_config['processed']['schema']};"
        )

        db.execute_sql(sql_statement)
