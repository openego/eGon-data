DELETE FROM demand.egon_peta_heat;

INSERT INTO demand.egon_peta_heat (
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
    id,
    geom_point AS point
  FROM society.destatis_zensus_population_per_ha
) AS population
ON ST_Intersects((demands.polygon).geom, population.point)
;
