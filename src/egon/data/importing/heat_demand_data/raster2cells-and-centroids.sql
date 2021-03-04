DROP TABLE IF EXISTS demand.heat;
CREATE TABLE demand.heat (
  id SERIAL PRIMARY KEY,
  demand DOUBLE PRECISION,
  sector TEXT,
  scenario TEXT,
  centroid GEOMETRY,
  polygon GEOMETRY,
  population_gid INTEGER
    REFERENCES society.destatis_zensus_population_per_ha (gid)
);

CREATE INDEX ON demand.heat USING gist (centroid);
CREATE INDEX ON demand.heat USING gist (polygon);

INSERT INTO demand.heat (
  demand, sector, scenario, centroid, polygon, population_gid
) SELECT
  (demands.centroid).val AS demand,
  sector,
  (regexp_matches(filename, '(ser|res)_hd_([^.]*).*', 'i'))[2] AS scenario,
  (demands.centroid).geom AS centroid,
  (demands.polygon).geom AS polygon,
  population.gid as population_gid
FROM (
  SELECT
    ST_PixelAsCentroids(rast, 1) AS centroid,
    ST_PixelAsPolygons(rast, 1) AS polygon,
    '{"res": "residential", "ser": "service"}'::json
    ->> substring(filename, 1, 3) AS sector,
    filename
  FROM heat_demand_rasters
) AS demands
LEFT JOIN (
  SELECT
    gid,
    geom_point AS point
  FROM society.destatis_zensus_population_per_ha
) AS population
ON ST_Intersects((demands.polygon).geom, population.point)
;
