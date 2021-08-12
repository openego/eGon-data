INSERT INTO demand.egon_peta_heat (
  demand, sector, scenario, zensus_population_id
) SELECT
  (demands.centroid).val AS demand,
  sector,
  (regexp_matches(filename, '(ser|res)_hd_([^.]*).*', 'i'))[2] AS scenario,
  population.id AS zensus_population_id
FROM (
  SELECT
    ST_PixelAsCentroids(rast, 1) AS centroid,
    ST_PixelAsPolygons(rast, 1) AS polygon,
    '{"res": "residential", "ser": "service"}'::json
    ->> substring(filename, 1, 3) AS sector,
    filename
  FROM {{ source }}
) AS demands
LEFT JOIN (
  SELECT
    id,
    geom_point AS point
  FROM society.destatis_zensus_population_per_ha
) AS population
ON ST_Intersects((demands.polygon).geom, population.point)
;
