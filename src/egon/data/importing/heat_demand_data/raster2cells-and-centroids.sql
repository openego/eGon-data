DROP TABLE IF EXISTS demand.heat;
CREATE TABLE demand.heat (
  id SERIAL PRIMARY KEY,
  demand DOUBLE PRECISION,
  sector TEXT,
  scenario TEXT,
  version TEXT,
  zensus_population_id INTEGER
    REFERENCES society.destatis_zensus_population_per_ha (id)
);

INSERT INTO demand.heat (
  demand, sector, scenario, version, zensus_population_id
) SELECT
  (demands.centroid).val AS demand,
  sector,
  (regexp_matches(filename, '(ser|res)_hd_([^.]*).*', 'i'))[2] AS scenario,
  '{{ version }}' AS version,
  population.id AS zensus_population_id
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
