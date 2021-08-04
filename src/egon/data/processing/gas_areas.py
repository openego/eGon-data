"""The central module containing code to create gas voronoi polygones

"""
from egon.data import db
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

def create_voronoi():
    '''
    Creates voronoi polygons for gas buses

    Returns
    -------
    None.

    '''
        
    db.execute_sql(
        """
        DROP TABLE IF EXISTS grid.egon_gas_voronoi;
                
        SELECT bus_id, bus_id as id, geom as point
        INTO grid.egon_gas_voronoi
        FROM grid.egon_etrago_bus 
        WHERE carrier = 'gas';
        
        ALTER TABLE grid.egon_gas_voronoi ADD geom Geometry('Multipolygon', 4326);     
        
        DROP TABLE IF EXISTS grid.egon_gas_bus CASCADE;
        
        SELECT bus_id, bus_id as id, geom as point 
        INTO grid.egon_gas_bus
        FROM grid.egon_etrago_bus 
        WHERE carrier = 'gas';
        """
                    )

    schema = 'grid'
    substation_table = 'egon_gas_bus'
    voronoi_table = 'egon_gas_voronoi'
    view = 'grid.egon_voronoi_no_borders'
    boundary = 'boundaries.vg250_sta_union'
    
    # Create view for Voronoi polygons without taking borders into account
    db.execute_sql(
        f"DROP VIEW IF EXISTS {schema}.egon_voronoi_no_borders CASCADE;"
                   )
    
    db.execute_sql(
        f"""
        CREATE VIEW {view} AS
           SELECT (ST_Dump(ST_VoronoiPolygons(ST_collect(a.point)))).geom
           FROM {schema}.{substation_table} a;
        """
        )
    
    # Clip Voronoi with boundaries
    db.execute_sql(
        f"""
        INSERT INTO {schema}.{voronoi_table} (geom)
        (SELECT ST_Multi(ST_Intersection(
            ST_Transform(a.geometry, 4326), b.geom)) AS geom
         FROM {boundary} a
         CROSS JOIN {view} b);
        """
        )
    
    # Assign substation id as foreign key
    db.execute_sql(
        f"""
        UPDATE {schema}.{voronoi_table} AS t1
            SET  	bus_id = t2.bus_id
    	            FROM	(SELECT	voi.id AS id,
    			                sub.bus_id ::integer AS bus_id
    		            FROM	{schema}.{voronoi_table} AS voi,
    			                {schema}.{substation_table} AS sub
    		            WHERE  	voi.geom && sub.point AND
    			                ST_CONTAINS(voi.geom,sub.point)
    		           GROUP BY voi.id,sub.bus_id
    		           )AS t2
    	            WHERE  	t1.id = t2.id;
        """
        )
    
    db.execute_sql(
        f"""
        CREATE INDEX  	{voronoi_table}_idx
            ON          {schema}.{voronoi_table} USING gist (geom);
        """
        )
    
    db.execute_sql(
        f"""
        DROP VIEW IF EXISTS {view} CASCADE;
        DROP TABLE IF EXISTS grid.egon_gas_bus;
        """
                    )
