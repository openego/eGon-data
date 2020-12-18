-- Create MView with NUTS IDs

DROP MATERIALIZED VIEW IF EXISTS boundaries.vg250_lan_nuts_id;

CREATE MATERIALIZED VIEW boundaries.vg250_lan_nuts_id AS
 SELECT lan.ags_0,
    lan.gen,
    lan.nuts,
    st_union(st_transform(lan.geometry, 3035)) AS geometry
   FROM ( SELECT vg.ags_0,
            vg.nuts,
            replace(vg.gen::text, ' (Bodensee)'::text, ''::text) AS gen,
            vg.geometry
           FROM boundaries.vg250_lan vg) lan
  GROUP BY lan.ags_0, lan.gen, lan.nuts
  ORDER BY lan.ags_0
WITH DATA;
