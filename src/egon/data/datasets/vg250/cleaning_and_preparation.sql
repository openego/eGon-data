/*
Setup borders
Inputs are german administrative borders (boundaries.bkg_vg250)
Create mviews with transformed CRS (EPSG:3035) and corrected geometries
Municipalities / Gemeinden are fragmented and cleaned from ringholes (vg250_gem_clean)

__copyright__ = "Reiner Lemoine Institut"
__license__ = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__ = "https://github.com/openego/data_processing/blob/master/LICENSE"
__author__ = "Ludee"

*/

-- 1. Nationalstaat (sta) - country (cntry)
-- 1. country - mview with tiny buffer because of intersection (in official data)
DROP MATERIALIZED VIEW IF EXISTS	boundaries.vg250_sta_tiny_buffer CASCADE;
CREATE MATERIALIZED VIEW		boundaries.vg250_sta_tiny_buffer AS
	SELECT
		vg.id ::integer,
		vg.bez ::text,
		vg.gf ::double precision,
		ST_AREA(ST_TRANSFORM(vg.geometry, 3035)) / 10000 ::double precision AS area_ha,
		ST_MULTI(ST_BUFFER(ST_TRANSFORM(vg.geometry,3035),-0.001)) ::geometry(MultiPolygon,3035) AS geometry
	FROM	boundaries.vg250_sta AS vg
	ORDER BY vg.id;

-- index (id)
CREATE UNIQUE INDEX  	vg250_sta_tiny_buffer_id_idx
		ON	boundaries.vg250_sta_tiny_buffer (id);

-- index GIST (geometry)
CREATE INDEX  	vg250_sta_tiny_buffer_geometry_idx
	ON	boundaries.vg250_sta_tiny_buffer USING gist (geometry);

/*
-- metadata
COMMENT ON MATERIALIZED VIEW boundaries.vg250_sta_tiny_buffer IS '{
	"title": "BKG - Verwaltungsgebiete 1:250.000 - country mview",
	"description": "Country mview with tiny buffer",
	"language": [ "eng", "ger" ],
	"reference_date": "2016-01-01",
	"sources": [
		{"name": "Dienstleistungszentrum des Bundes für Geoinformation und Geodäsie - Open Data", "description": "Dieser Datenbestand steht über Geodatendienste gemäß Geodatenzugangsgesetz (GeoZG) (http://www.geodatenzentrum.de/auftrag/pdf/geodatenzugangsgesetz.pdf) für die kommerzielle und nicht kommerzielle Nutzung geldleistungsfrei zum Download und zur Online-Nutzung zur Verfügung. Die Nutzung der Geodaten und Geodatendienste wird durch die Verordnung zur Festlegung der Nutzungsbestimmungen für die Bereitstellung von Geodaten des Bundes (GeoNutzV) (http://www.geodatenzentrum.de/auftrag/pdf/geonutz.pdf) geregelt. Insbesondere hat jeder Nutzer den Quellenvermerk zu allen Geodaten, Metadaten und Geodatendiensten erkennbar und in optischem Zusammenhang zu platzieren. Veränderungen, Bearbeitungen, neue Gestaltungen oder sonstige Abwandlungen sind mit einem Veränderungshinweis im Quellenvermerk zu versehen. Quellenvermerk und Veränderungshinweis sind wie folgt zu gestalten. Bei der Darstellung auf einer Webseite ist der Quellenvermerk mit der URL http://www.bkg.bund.de zu verlinken. © GeoBasis-DE / BKG <Jahr des letzten Datenbezugs> © GeoBasis-DE / BKG <Jahr des letzten Datenbezugs> (Daten verändert) Beispiel: © GeoBasis-DE / BKG 2013",
		"url": "http://www.geodatenzentrum.de/geodaten/gdz_rahmen.gdz_div?gdz_spr=deu&gdz_akt_zeile=5&gdz_anz_zeile=1&gdz_unt_zeile=14&gdz_user_id=0", "license": "Geodatenzugangsgesetz (GeoZG)"},
		{"name": "BKG - Verwaltungsgebiete 1:250.000 (vg250)", "description": "© GeoBasis-DE / BKG 2016 (Daten verändert)",
		"url": "http://www.geodatenzentrum.de/", "license": "Geodatenzugangsgesetz (GeoZG)"} ],
	"spatial": [
		{"extend": "Germany",
		"resolution": ""} ],
	"license": [
		{"id": "ODbL-1.0",
		"name": "Open Data Commons Open Database License 1.0",
		"version": "1.0",
		"url": "https://opendatacommons.org/licenses/odbl/1.0/",
		"instruction": "You are free: To Share, To Create, To Adapt; As long as you: Attribute, Share-Alike, Keep open!"} ],
	"contributors": [
		{"Name": "Ludee", "Mail": "",
		"Date":  "02.09.2015", "Comment": "Create MView (with tiny buffer because of intersection in official data)" },
		{"Name": "Ludee", "Mail": "",
		"Date":  "16.11.2016", "Comment": "Add metadata" },
		{"name": "Ludee", "email": "",
		"date": "21.03.2017", "comment": "Update metadata to 1.1"} ],
	"resources": [{
		"schema": {
			"fields": [
				{"name": "reference_date", "description": "Reference date", "unit": "" },
				{"name": "id", "description": "Unique identifier", "unit": "" },
				{"name": "bez", "description": "Bezeichnung der Verwaltungseinheit", "unit": "" },
				{"name": "gf", "description": "Geofaktor", "unit": "" },
				{"name": "area_ha", "description": "Area", "unit": "ha" },
				{"name": "geometry", "description": "geometry", "unit": "" } ]},
		"meta_version": "1.1" }] }';

-- select description
SELECT obj_description('boundaries.vg250_sta_tiny_buffer' ::regclass) ::json;
*/

-- 1. country - error geometry
DROP MATERIALIZED VIEW IF EXISTS	boundaries.vg250_sta_invalid_geometry CASCADE;
CREATE MATERIALIZED VIEW		boundaries.vg250_sta_invalid_geometry AS 
	SELECT	sub.id AS id,
		sub.error AS error,
		sub.error_reason AS error_reason,
		ST_SETSRID(location(ST_IsValidDetail(sub.geometry)),3035) ::geometry(Point,3035) AS geometry
	FROM	(
		SELECT	source.id AS id,				-- PK
			ST_IsValid(source.geometry) AS error,
			reason(ST_IsValidDetail(source.geometry)) AS error_reason,
			source.geometry AS geometry
		FROM	boundaries.vg250_sta AS source	-- Table
		) AS sub
	WHERE	sub.error = FALSE;

-- index (id)
CREATE UNIQUE INDEX  	vg250_sta_invalid_geometry_id_idx
		ON	boundaries.vg250_sta_invalid_geometry (id);

-- index GIST (geometry)
CREATE INDEX  	vg250_sta_invalid_geometry_geometry_idx
	ON	boundaries.vg250_sta_invalid_geometry USING gist (geometry);

/*
-- metadata
COMMENT ON MATERIALIZED VIEW boundaries.vg250_sta_invalid_geometry IS '{
    "Name": "BKG - Verwaltungsgebiete 1:250.000 - country mview errors",
    "Source":   [{
	"Name": "Dienstleistungszentrum des Bundes für Geoinformation und Geodäsie - Open Data",
	"URL": "http://www.geodatenzentrum.de/geodaten/gdz_rahmen.gdz_div?gdz_spr=deu&gdz_akt_zeile=5&gdz_anz_zeile=1&gdz_unt_zeile=14&gdz_user_id=0"}],
    "Reference date": "2016-01-01",
    "Date of collection": "02.09.2015",
    "Original file": ["vg250_2015-01-01.gk3.shape.ebenen.zip"],
    "Spatial": [{
	"Resolution": "1:250.000",
	"Extend": "Germany; Nationalstaat (sta) - country (cntry)" }],
    "Description": ["Errors in country border"],
    "Column":[
        {"Name": "id", "Description": "Unique identifier", "Unit": " " },
        {"Name": "error", "Description": "Error", "Unit": " " },
	{"Name": "error_reason", "Description": "Error reason", "Unit": " " },
	{"Name": "geometry", "Description": "geometry", "Unit": " " } ],
    "Changes":	[
        {"Name": "Ludee", "Mail": "",
	"Date":  "02.09.2015", "Comment": "Created mview" },
	{"Name": "Ludee", "Mail": "",
	"Date":  "16.11.2016", "Comment": "Added metadata" } ],
    "Notes": [""],
    "Licence": [{
	"Name": "Geodatenzugangsgesetz (GeoZG) © GeoBasis-DE / BKG 2016 (Daten verändert)", 
	"URL": "http://www.geodatenzentrum.de/auftrag/pdf/geodatenzugangsgesetz.pdf" }],
    "Instructions for proper use": ["Dieser Datenbestand steht über Geodatendienste gemäß Geodatenzugangsgesetz (GeoZG) (http://www.geodatenzentrum.de/auftrag/pdf/geodatenzugangsgesetz.pdf) für die kommerzielle und nicht kommerzielle Nutzung geldleistungsfrei zum Download und zur Online-Nutzung zur Verfügung. Die Nutzung der Geodaten und Geodatendienste wird durch die Verordnung zur Festlegung der Nutzungsbestimmungen für die Bereitstellung von Geodaten des Bundes (GeoNutzV) (http://www.geodatenzentrum.de/auftrag/pdf/geonutz.pdf) geregelt. Insbesondere hat jeder Nutzer den Quellenvermerk zu allen Geodaten, Metadaten und Geodatendiensten erkennbar und in optischem Zusammenhang zu platzieren. Veränderungen, Bearbeitungen, neue Gestaltungen oder sonstige Abwandlungen sind mit einem Veränderungshinweis im Quellenvermerk zu versehen. Quellenvermerk und Veränderungshinweis sind wie folgt zu gestalten. Bei der Darstellung auf einer Webseite ist der Quellenvermerk mit der URL http://www.bkg.bund.de zu verlinken. © GeoBasis-DE / BKG <Jahr des letzten Datenbezugs> © GeoBasis-DE / BKG <Jahr des letzten Datenbezugs> (Daten verändert) Beispiel: © GeoBasis-DE / BKG 2013"]
    }' ;

-- select description
SELECT obj_description('boundaries.vg250_sta_invalid_geometry' ::regclass) ::json;
*/

-- 1. country - union
DROP MATERIALIZED VIEW IF EXISTS	boundaries.vg250_sta_union CASCADE;
CREATE MATERIALIZED VIEW		boundaries.vg250_sta_union AS
	SELECT
		'1' ::integer AS id,
		'Bundesrepublik' ::text AS bez,
		ST_AREA(un.geometry) / 10000 ::double precision AS area_ha,
		un.geometry ::geometry(MultiPolygon,3035) AS geometry
	FROM	(SELECT	ST_MULTI(ST_MakeValid(ST_UNION(ST_TRANSFORM(vg.geometry,3035)))) ::geometry(MultiPolygon,3035) AS geometry
		FROM	boundaries.vg250_sta AS vg
		WHERE	vg.bez = 'Bundesrepublik'
		) AS un;

-- index (id)
CREATE UNIQUE INDEX  	vg250_sta_union_id_idx
		ON	boundaries.vg250_sta_union (id);

-- index GIST (geometry)
CREATE INDEX  	vg250_sta_union_geometry_idx
	ON	boundaries.vg250_sta_union USING gist (geometry);

/*
-- metadata
COMMENT ON MATERIALIZED VIEW boundaries.vg250_sta_union IS '{
    "Name": "BKG - Verwaltungsgebiete 1:250.000 - country mview union",
    "Source":   [{
	"Name": "Dienstleistungszentrum des Bundes für Geoinformation und Geodäsie - Open Data",
	"URL": "http://www.geodatenzentrum.de/geodaten/gdz_rahmen.gdz_div?gdz_spr=deu&gdz_akt_zeile=5&gdz_anz_zeile=1&gdz_unt_zeile=14&gdz_user_id=0"}],
    "Reference date": "2016-01-01",
    "Date of collection": "02.09.2015",
    "Original file": ["vg250_2015-01-01.gk3.shape.ebenen.zip"],
    "Spatial": [{
	"Resolution": "1:250.000",
	"Extend": "Germany; Nationalstaat (sta) - country (cntry)" }],
    "Description": ["geometry union"],
    "Column":[
        {"Name": "reference_date", "Description": "Reference Year", "Unit": " " },
        {"Name": "id", "Description": "Unique identifier", "Unit": " " },
        {"Name": "bez", "Description": "Bezeichnung der Verwaltungseinheit", "Unit": " " },
	{"Name": "area_ha", "Description": "Area in ha", "Unit": "ha" },
	{"Name": "geometry", "Description": "geometry", "Unit": " " } ],
    "Changes":	[
        {"Name": "Ludee", "Mail": "",
	"Date":  "02.09.2015", "Comment": "Created mview" },
	{"Name": "Ludee", "Mail": "",
	"Date":  "16.11.2016", "Comment": "Added metadata" } ],
    "Notes": [""],
    "Licence": [{
	"Name": "Geodatenzugangsgesetz (GeoZG) © GeoBasis-DE / BKG 2016 (Daten verändert)", 
	"URL": "http://www.geodatenzentrum.de/auftrag/pdf/geodatenzugangsgesetz.pdf" }],
    "Instructions for proper use": ["Dieser Datenbestand steht über Geodatendienste gemäß Geodatenzugangsgesetz (GeoZG) (http://www.geodatenzentrum.de/auftrag/pdf/geodatenzugangsgesetz.pdf) für die kommerzielle und nicht kommerzielle Nutzung geldleistungsfrei zum Download und zur Online-Nutzung zur Verfügung. Die Nutzung der Geodaten und Geodatendienste wird durch die Verordnung zur Festlegung der Nutzungsbestimmungen für die Bereitstellung von Geodaten des Bundes (GeoNutzV) (http://www.geodatenzentrum.de/auftrag/pdf/geonutz.pdf) geregelt. Insbesondere hat jeder Nutzer den Quellenvermerk zu allen Geodaten, Metadaten und Geodatendiensten erkennbar und in optischem Zusammenhang zu platzieren. Veränderungen, Bearbeitungen, neue Gestaltungen oder sonstige Abwandlungen sind mit einem Veränderungshinweis im Quellenvermerk zu versehen. Quellenvermerk und Veränderungshinweis sind wie folgt zu gestalten. Bei der Darstellung auf einer Webseite ist der Quellenvermerk mit der URL http://www.bkg.bund.de zu verlinken. © GeoBasis-DE / BKG <Jahr des letzten Datenbezugs> © GeoBasis-DE / BKG <Jahr des letzten Datenbezugs> (Daten verändert) Beispiel: © GeoBasis-DE / BKG 2013"]
    }' ;

-- select description
SELECT obj_description('boundaries.vg250_sta_union' ::regclass) ::json;
*/

-- 1. state borders - bounding box
DROP MATERIALIZED VIEW IF EXISTS	boundaries.vg250_sta_bbox CASCADE;
CREATE MATERIALIZED VIEW		boundaries.vg250_sta_bbox AS
	SELECT
		'1' ::integer AS id,
		'Bundesrepublik' ::text AS bez,
		ST_AREA(un.geometry) / 10000 ::double precision AS area_ha,
		un.geometry ::geometry(Polygon,3035) AS geometry
	FROM	(SELECT	ST_SetSRID(Box2D(vg.geometry),3035) ::geometry(Polygon,3035) AS geometry
		FROM	boundaries.vg250_sta_union AS vg
		) AS un;

-- index (id)
CREATE UNIQUE INDEX  	vg250_sta_bbox_id_idx
		ON	boundaries.vg250_sta_bbox (id);

-- index GIST (geometry)
CREATE INDEX  	vg250_sta_bbox_geometry_idx
	ON	boundaries.vg250_sta_bbox USING gist (geometry);

/*
-- metadata
COMMENT ON MATERIALIZED VIEW boundaries.vg250_sta_bbox IS '{
    "Name": "BKG - Verwaltungsgebiete 1:250.000 - country mview bounding box",
    "Source":   [{
	"Name": "Dienstleistungszentrum des Bundes für Geoinformation und Geodäsie - Open Data",
	"URL": "http://www.geodatenzentrum.de/geodaten/gdz_rahmen.gdz_div?gdz_spr=deu&gdz_akt_zeile=5&gdz_anz_zeile=1&gdz_unt_zeile=14&gdz_user_id=0"}],
    "Reference date": "2016-01-01",
    "Date of collection": "02.09.2015",
    "Original file": ["vg250_2015-01-01.gk3.shape.ebenen.zip"],
    "Spatial": [{
	"Resolution": "1:250.000",
	"Extend": "Germany; Nationalstaat (sta) - country (cntry)" }],
    "Description": ["geometry bounding box"],
    "Column":[
        {"Name": "reference_date", "Description": "Reference Year", "Unit": " " },
        {"Name": "id", "Description": "Unique identifier", "Unit": " " },
        {"Name": "bez", "Description": "Bezeichnung der Verwaltungseinheit", "Unit": " " },
	{"Name": "area_ha", "Description": "Area in ha", "Unit": "ha" },
	{"Name": "geometry", "Description": "geometry", "Unit": " " } ],
    "Changes":	[
        {"Name": "Ludee", "Mail": "",
	"Date":  "02.09.2015", "Comment": "Created mview" },
	{"Name": "Ludee", "Mail": "",
	"Date":  "16.11.2016", "Comment": "Added metadata" } ],
    "Notes": [""],
    "Licence": [{
	"Name": "Geodatenzugangsgesetz (GeoZG) © GeoBasis-DE / BKG 2016 (Daten verändert)", 
	"URL": "http://www.geodatenzentrum.de/auftrag/pdf/geodatenzugangsgesetz.pdf" }],
    "Instructions for proper use": ["Dieser Datenbestand steht über Geodatendienste gemäß Geodatenzugangsgesetz (GeoZG) (http://www.geodatenzentrum.de/auftrag/pdf/geodatenzugangsgesetz.pdf) für die kommerzielle und nicht kommerzielle Nutzung geldleistungsfrei zum Download und zur Online-Nutzung zur Verfügung. Die Nutzung der Geodaten und Geodatendienste wird durch die Verordnung zur Festlegung der Nutzungsbestimmungen für die Bereitstellung von Geodaten des Bundes (GeoNutzV) (http://www.geodatenzentrum.de/auftrag/pdf/geonutz.pdf) geregelt. Insbesondere hat jeder Nutzer den Quellenvermerk zu allen Geodaten, Metadaten und Geodatendiensten erkennbar und in optischem Zusammenhang zu platzieren. Veränderungen, Bearbeitungen, neue Gestaltungen oder sonstige Abwandlungen sind mit einem Veränderungshinweis im Quellenvermerk zu versehen. Quellenvermerk und Veränderungshinweis sind wie folgt zu gestalten. Bei der Darstellung auf einer Webseite ist der Quellenvermerk mit der URL http://www.bkg.bund.de zu verlinken. © GeoBasis-DE / BKG <Jahr des letzten Datenbezugs> © GeoBasis-DE / BKG <Jahr des letzten Datenbezugs> (Daten verändert) Beispiel: © GeoBasis-DE / BKG 2013"]
    }' ;

-- select description
SELECT obj_description('boundaries.vg250_sta_bbox' ::regclass) ::json;
*/

	
-- 2. Bundesland (lan) - federal state (fst)
-- 2. federal state - mview with tiny buffer because of intersection (in official data)
DROP MATERIALIZED VIEW IF EXISTS	boundaries.vg250_lan_union CASCADE;
CREATE MATERIALIZED VIEW		boundaries.vg250_lan_union AS
	SELECT
		lan.ags_0 ::character varying(12) AS ags_0,
		lan.gen ::text AS gen,
		ST_MULTI(ST_UNION(ST_TRANSFORM(lan.geometry,3035))) ::geometry(Multipolygon,3035) AS geometry
	FROM	(SELECT	vg.ags_0,
			replace( vg.gen, ' (Bodensee)', '') as gen,
			vg.geometry
		FROM	boundaries.vg250_lan AS vg
		) AS lan
	GROUP BY lan.ags_0,lan.gen
	ORDER BY lan.ags_0;

-- index (id)
CREATE UNIQUE INDEX  	vg250_lan_union_ags_0_idx
		ON	boundaries.vg250_lan_union (ags_0);

-- index GIST (geometry)
CREATE INDEX  	vg250_lan_union_geometry_idx
	ON	boundaries.vg250_lan_union USING gist (geometry);

/*
-- metadata
COMMENT ON MATERIALIZED VIEW boundaries.vg250_lan_union IS '{
    "Name": "BKG - Verwaltungsgebiete 1:250.000 - federal state mview",
    "Source":   [{
	"Name": "Dienstleistungszentrum des Bundes für Geoinformation und Geodäsie - Open Data",
	"URL": "http://www.geodatenzentrum.de/geodaten/gdz_rahmen.gdz_div?gdz_spr=deu&gdz_akt_zeile=5&gdz_anz_zeile=1&gdz_unt_zeile=14&gdz_user_id=0"}],
    "Reference date": "2016-01-01",
    "Date of collection": "02.09.2015",
    "Original file": ["vg250_2015-01-01.gk3.shape.ebenen.zip"],
    "Spatial": [{
	"Resolution": "1:250.000",
	"Extend": "Germany; Bundesland (lan) - federal state (fst)" }],
    "Description": ["Federal state mview"],
    "Column":[
        {"Name": "reference_date", "Description": "Reference Year", "Unit": " " },
        {"Name": "ags_0", "Description": "Amtlicher Gemeindeschlüssel", "Unit": " " },
        {"Name": "gen", "Description": "Geografischer Name", "Unit": " " },
	{"Name": "area_ha", "Description": "Area in ha", "Unit": "ha" },
	{"Name": "geometry", "Description": "geometry", "Unit": " " } ],
    "Changes":	[
        {"Name": "Ludee", "Mail": "",
	"Date":  "02.09.2015", "Comment": "Created mview" },
	{"Name": "Ludee", "Mail": "",
	"Date":  "16.11.2016", "Comment": "Added metadata" } ],
    "Notes": [""],
    "Licence": [{
	"Name": "Geodatenzugangsgesetz (GeoZG) © GeoBasis-DE / BKG 2016 (Daten verändert)", 
	"URL": "http://www.geodatenzentrum.de/auftrag/pdf/geodatenzugangsgesetz.pdf" }],
    "Instructions for proper use": ["Dieser Datenbestand steht über Geodatendienste gemäß Geodatenzugangsgesetz (GeoZG) (http://www.geodatenzentrum.de/auftrag/pdf/geodatenzugangsgesetz.pdf) für die kommerzielle und nicht kommerzielle Nutzung geldleistungsfrei zum Download und zur Online-Nutzung zur Verfügung. Die Nutzung der Geodaten und Geodatendienste wird durch die Verordnung zur Festlegung der Nutzungsbestimmungen für die Bereitstellung von Geodaten des Bundes (GeoNutzV) (http://www.geodatenzentrum.de/auftrag/pdf/geonutz.pdf) geregelt. Insbesondere hat jeder Nutzer den Quellenvermerk zu allen Geodaten, Metadaten und Geodatendiensten erkennbar und in optischem Zusammenhang zu platzieren. Veränderungen, Bearbeitungen, neue Gestaltungen oder sonstige Abwandlungen sind mit einem Veränderungshinweis im Quellenvermerk zu versehen. Quellenvermerk und Veränderungshinweis sind wie folgt zu gestalten. Bei der Darstellung auf einer Webseite ist der Quellenvermerk mit der URL http://www.bkg.bund.de zu verlinken. © GeoBasis-DE / BKG <Jahr des letzten Datenbezugs> © GeoBasis-DE / BKG <Jahr des letzten Datenbezugs> (Daten verändert) Beispiel: © GeoBasis-DE / BKG 2013"]
    }' ;

-- select description
SELECT obj_description('boundaries.vg250_lan_union' ::regclass) ::json;
*/

-- 4. Landkreis (krs) - district (dist)
-- 4. district - mview 
DROP MATERIALIZED VIEW IF EXISTS	boundaries.vg250_krs_area CASCADE;
CREATE MATERIALIZED VIEW		boundaries.vg250_krs_area AS
	SELECT
		vg.id ::integer AS id,
		vg.gen ::text AS gen,
		vg.bez ::text AS bez,
		vg.nuts ::varchar(5) AS nuts,
		vg.rs_0 ::varchar(12) AS rs_0,
		vg.ags_0 ::varchar(12) AS ags_0,
		ST_AREA(vg.geometry) / 10000 ::double precision AS area_ha,
		ST_TRANSFORM(vg.geometry,3035) AS geometry
	FROM	boundaries.vg250_krs AS vg
	ORDER BY vg.id;

-- index (id)
CREATE UNIQUE INDEX  	vg250_krs_area_id_idx
		ON	boundaries.vg250_krs_area (id);

-- index GIST (geometry)
CREATE INDEX  	vg250_krs_area_geometry_idx
	ON	boundaries.vg250_krs_area USING gist (geometry);

/*
-- metadata
COMMENT ON MATERIALIZED VIEW boundaries.vg250_krs_area IS '{
    "Name": "BKG - Verwaltungsgebiete 1:250.000 - federal state mview",
    "Source":   [{
	"Name": "Dienstleistungszentrum des Bundes für Geoinformation und Geodäsie - Open Data",
	"URL": "http://www.geodatenzentrum.de/geodaten/gdz_rahmen.gdz_div?gdz_spr=deu&gdz_akt_zeile=5&gdz_anz_zeile=1&gdz_unt_zeile=14&gdz_user_id=0"}],
    "Reference date": "2016-01-01",
    "Date of collection": "02.09.2015",
    "Original file": ["vg250_2015-01-01.gk3.shape.ebenen.zip"],
    "Spatial": [{
	"Resolution": "1:250.000",
	"Extend": "Germany; Landkreis (krs) - district (dist)" }],
    "Description": ["Federal state mview"],
    "Column":[
        {"Name": "reference_date", "Description": "Reference Year", "Unit": " " },
	{"Name": "id", "Description": "Unique identifier", "Unit": " " },
        {"Name": "gen", "Description": "Geografischer Name", "Unit": " " },
	{"Name": "bez", "Description": "Bezeichnung der Verwaltungseinheit", "Unit": " " },
	{"Name": "nuts", "Description": "Europäischer Statistikschlüssel", "Unit": " " },
	{"Name": "rs_0", "Description": "Aufgefüllter Regionalschlüssel", "Unit": " " },
	{"Name": "ags_0", "Description": "Amtlicher Gemeindeschlüssel", "Unit": " " },
	{"Name": "area_ha", "Description": "Area in ha", "Unit": "ha" },
	{"Name": "geometry", "Description": "geometry", "Unit": " " } ],
    "Changes":	[
        {"Name": "Ludee", "Mail": "",
	"Date":  "02.09.2015", "Comment": "Created mview" },
	{"Name": "Ludee", "Mail": "",
	"Date":  "16.11.2016", "Comment": "Added metadata" } ],
    "Notes": [""],
    "Licence": [{
	"Name": "Geodatenzugangsgesetz (GeoZG) © GeoBasis-DE / BKG 2016 (Daten verändert)", 
	"URL": "http://www.geodatenzentrum.de/auftrag/pdf/geodatenzugangsgesetz.pdf" }],
    "Instructions for proper use": ["Dieser Datenbestand steht über Geodatendienste gemäß Geodatenzugangsgesetz (GeoZG) (http://www.geodatenzentrum.de/auftrag/pdf/geodatenzugangsgesetz.pdf) für die kommerzielle und nicht kommerzielle Nutzung geldleistungsfrei zum Download und zur Online-Nutzung zur Verfügung. Die Nutzung der Geodaten und Geodatendienste wird durch die Verordnung zur Festlegung der Nutzungsbestimmungen für die Bereitstellung von Geodaten des Bundes (GeoNutzV) (http://www.geodatenzentrum.de/auftrag/pdf/geonutz.pdf) geregelt. Insbesondere hat jeder Nutzer den Quellenvermerk zu allen Geodaten, Metadaten und Geodatendiensten erkennbar und in optischem Zusammenhang zu platzieren. Veränderungen, Bearbeitungen, neue Gestaltungen oder sonstige Abwandlungen sind mit einem Veränderungshinweis im Quellenvermerk zu versehen. Quellenvermerk und Veränderungshinweis sind wie folgt zu gestalten. Bei der Darstellung auf einer Webseite ist der Quellenvermerk mit der URL http://www.bkg.bund.de zu verlinken. © GeoBasis-DE / BKG <Jahr des letzten Datenbezugs> © GeoBasis-DE / BKG <Jahr des letzten Datenbezugs> (Daten verändert) Beispiel: © GeoBasis-DE / BKG 2013"]
    }' ;

-- select description
SELECT obj_description('boundaries.vg250_krs_area' ::regclass) ::json;
*/

CREATE SEQUENCE IF NOT EXISTS boundaries.vg250_gem_valid_id;

-- Transform bkg_vg250 Gemeinden   (OK!) -> 5.000ms =12.521
DROP MATERIALIZED VIEW IF EXISTS	boundaries.vg250_gem_valid CASCADE;
CREATE MATERIALIZED VIEW		boundaries.vg250_gem_valid AS
	SELECT	nextval('boundaries.vg250_gem_valid_id') AS id,
		vg.id ::integer AS old_id,
		vg.gen ::text AS gen,
		vg.bez ::text AS bez,
		vg.bem ::text AS bem,
		vg.nuts ::varchar(5) AS nuts,
		vg.rs_0 ::varchar(12) AS rs_0,
		vg.ags_0 ::varchar(12) AS ags_0,
		ST_AREA(vg.geometry) / 10000 ::double precision AS area_ha,
		ST_MakeValid((ST_DUMP(ST_TRANSFORM(vg.geometry,3035))).geom) ::geometry(Polygon,3035) AS geometry
	FROM	boundaries.vg250_gem AS vg
	WHERE	gf = '4' -- Without water
	ORDER BY vg.id;

-- index (id)
CREATE UNIQUE INDEX  	vg250_gem_valid_id_idx
	ON	boundaries.vg250_gem_valid (id);

-- index GIST (geometry)
CREATE INDEX  	vg250_gem_valid_geometry_idx
	ON	boundaries.vg250_gem_valid USING gist (geometry);

/*
-- metadata
COMMENT ON MATERIALIZED VIEW boundaries.vg250_gem_valid IS '{
    "Name": "BKG - Verwaltungsgebiete 1:250.000 - municipality mview",
    "Source":   [{
	"Name": "Dienstleistungszentrum des Bundes für Geoinformation und Geodäsie - Open Data",
	"URL": "http://www.geodatenzentrum.de/geodaten/gdz_rahmen.gdz_div?gdz_spr=deu&gdz_akt_zeile=5&gdz_anz_zeile=1&gdz_unt_zeile=14&gdz_user_id=0"}],
    "Reference date": "2016-01-01",
    "Date of collection": "02.09.2015",
    "Original file": ["vg250_2015-01-01.gk3.shape.ebenen.zip"],
    "Spatial": [{
	"Resolution": "1:250.000",
	"Extend": "Germany; Gemeinde (gem) - municipality (mun)" }],
    "Description": ["Municipality mview dump without water"],
    "Column":[
        {"Name": "reference_date", "Description": "Reference Year", "Unit": " " },
	{"Name": "id", "Description": "Unique identifier", "Unit": " " },
        {"Name": "gen", "Description": "Geografischer Name", "Unit": " " },
	{"Name": "bez", "Description": "Bezeichnung der Verwaltungseinheit", "Unit": " " },
	{"Name": "bem", "Description": "Bemerkung", "Unit": " " },
	{"Name": "nuts", "Description": "Europäischer Statistikschlüssel", "Unit": " " },
	{"Name": "rs_0", "Description": "Aufgefüllter Regionalschlüssel", "Unit": " " },
	{"Name": "ags_0", "Description": "Amtlicher Gemeindeschlüssel", "Unit": " " },
	{"Name": "area_ha", "Description": "Area in ha", "Unit": "ha" },
	{"Name": "geometry", "Description": "geometry", "Unit": " " } ],
    "Changes":	[
        {"Name": "Ludee", "Mail": "",
	"Date":  "02.09.2015", "Comment": "Created mview" },
	{"Name": "Ludee", "Mail": "",
	"Date":  "16.11.2016", "Comment": "Added metadata" } ],
    "Notes": [""],
    "Licence": [{
	"Name": "Geodatenzugangsgesetz (GeoZG) © GeoBasis-DE / BKG 2016 (Daten verändert)", 
	"URL": "http://www.geodatenzentrum.de/auftrag/pdf/geodatenzugangsgesetz.pdf" }],
    "Instructions for proper use": ["Dieser Datenbestand steht über Geodatendienste gemäß Geodatenzugangsgesetz (GeoZG) (http://www.geodatenzentrum.de/auftrag/pdf/geodatenzugangsgesetz.pdf) für die kommerzielle und nicht kommerzielle Nutzung geldleistungsfrei zum Download und zur Online-Nutzung zur Verfügung. Die Nutzung der Geodaten und Geodatendienste wird durch die Verordnung zur Festlegung der Nutzungsbestimmungen für die Bereitstellung von Geodaten des Bundes (GeoNutzV) (http://www.geodatenzentrum.de/auftrag/pdf/geonutz.pdf) geregelt. Insbesondere hat jeder Nutzer den Quellenvermerk zu allen Geodaten, Metadaten und Geodatendiensten erkennbar und in optischem Zusammenhang zu platzieren. Veränderungen, Bearbeitungen, neue Gestaltungen oder sonstige Abwandlungen sind mit einem Veränderungshinweis im Quellenvermerk zu versehen. Quellenvermerk und Veränderungshinweis sind wie folgt zu gestalten. Bei der Darstellung auf einer Webseite ist der Quellenvermerk mit der URL http://www.bkg.bund.de zu verlinken. © GeoBasis-DE / BKG <Jahr des letzten Datenbezugs> © GeoBasis-DE / BKG <Jahr des letzten Datenbezugs> (Daten verändert) Beispiel: © GeoBasis-DE / BKG 2013"]
    }' ;

-- select description
SELECT obj_description('boundaries.vg250_gem_valid' ::regclass) ::json;
*/


-- 6. municipality - geometry clean of holes
DROP TABLE IF EXISTS	boundaries.vg250_gem_clean CASCADE;
CREATE TABLE		boundaries.vg250_gem_clean (
	id SERIAL,
	old_id integer,
	gen text,
	bez text,
	bem text,
	nuts varchar(5),
	rs_0 varchar(12),
	ags_0 varchar(12),
	area_ha decimal,
	count_hole integer,
	path integer[],
	is_hole boolean,
	geometry geometry(Polygon,3035),
	CONSTRAINT boundaries_vg250_gem_clean_pkey PRIMARY KEY (id));

-- insert municipalities with rings
INSERT INTO	boundaries.vg250_gem_clean (old_id,gen,bez,bem,nuts,rs_0,ags_0,area_ha,count_hole,path,geometry)
	SELECT	dump.id ::integer AS old_id,
		dump.gen ::text AS gen,
		dump.bez ::text AS bez,
		dump.bem ::text AS bem,
		dump.nuts ::varchar(5) AS nuts,
		dump.rs_0 ::varchar(12) AS rs_0,
		dump.ags_0 ::varchar(12) AS ags_0,
		ST_AREA(dump.geometry) / 10000 ::decimal AS area_ha,
		dump.count_hole ::integer,
		dump.path ::integer[] AS path,
		dump.geometry ::geometry(Polygon,3035) AS geometry		
	FROM	(SELECT vg.id,
			vg.gen,
			vg.bez,
			vg.bem,
			vg.nuts,
			vg.rs_0,
			vg.ags_0,
			ST_NumInteriorRings(vg.geometry) AS count_hole,
			(ST_DumpRings(vg.geometry)).path AS path,
			(ST_DumpRings(vg.geometry)).geom AS geometry
		FROM	boundaries.vg250_gem_valid AS vg ) AS dump;

-- index GIST (geometry)
CREATE INDEX  	vg250_gem_clean_geometry_idx
	ON	boundaries.vg250_gem_clean USING gist (geometry);


-- separate holes
DROP MATERIALIZED VIEW IF EXISTS	boundaries.vg250_gem_hole CASCADE;
CREATE MATERIALIZED VIEW 		boundaries.vg250_gem_hole AS 
SELECT 	mun.*
FROM	boundaries.vg250_gem_clean AS mun
WHERE	mun.path[1] <> 0;

-- index (id)
CREATE UNIQUE INDEX  	vg250_gem_hole_id_idx
		ON	boundaries.vg250_gem_hole (id);

-- index GIST (geometry)
CREATE INDEX  	vg250_gem_hole_geometry_idx
	ON	boundaries.vg250_gem_hole USING gist (geometry);

/*
-- metadata
COMMENT ON MATERIALIZED VIEW boundaries.vg250_gem_hole IS '{
    "Name": "ego municipality holes",
    "Source":   [{
	"Name": "open_eGo",
	"URL": "https://github.com/openego/data_processing"}],
    "Reference date": "2016",
    "Date of collection": "02.09.2016",
    "Original file": ["boundaries.vg250_gem"],
    "Spatial": [{
	"Resolution": "1:250.000",
	"Extend": "Germany; Gemeinde (gem) - municipality (mun)" }],
    "Description": ["Municipality holes"],
    "Column":[
        {"Name": "id", "Description": "Unique identifier", "Unit": " " },
	{"Name": "old_id", "Description": "vg250 identifier", "Unit": " " },
        {"Name": "gen", "Description": "Geografischer Name", "Unit": " " },
	{"Name": "bez", "Description": "Bezeichnung der Verwaltungseinheit", "Unit": " " },
	{"Name": "bem", "Description": "Bemerkung", "Unit": " " },
	{"Name": "nuts", "Description": "Europäischer Statistikschlüssel", "Unit": " " },
	{"Name": "rs_0", "Description": "Aufgefüllter Regionalschlüssel", "Unit": " " },
	{"Name": "ags_0", "Description": "Amtlicher Gemeindeschlüssel", "Unit": " " },
	{"Name": "area_ha", "Description": "Area in ha", "Unit": "ha" },
	{"Name": "count_hole", "Description": "Number of holes", "Unit": " " },
	{"Name": "path", "Description": "Path number", "Unit": " " },
	{"Name": "hole", "Description": "True if hole", "Unit": " " },
	{"Name": "geometry", "Description": "geometry", "Unit": " " } ],
    "Changes":	[
        {"Name": "Ludee", "Mail": "",
	"Date":  "02.09.2015", "Comment": "Created mview" },
	{"Name": "Ludee", "Mail": "",
	"Date":  "16.11.2016", "Comment": "Added metadata" } ],
    "Notes": [""],
    "Licence": [{
	"Name": "Geodatenzugangsgesetz (GeoZG) © GeoBasis-DE / BKG 2016 (Daten verändert)", 
	"URL": "http://www.geodatenzentrum.de/auftrag/pdf/geodatenzugangsgesetz.pdf" }],
    "Instructions for proper use": ["Dieser Datenbestand steht über Geodatendienste gemäß Geodatenzugangsgesetz (GeoZG) (http://www.geodatenzentrum.de/auftrag/pdf/geodatenzugangsgesetz.pdf) für die kommerzielle und nicht kommerzielle Nutzung geldleistungsfrei zum Download und zur Online-Nutzung zur Verfügung. Die Nutzung der Geodaten und Geodatendienste wird durch die Verordnung zur Festlegung der Nutzungsbestimmungen für die Bereitstellung von Geodaten des Bundes (GeoNutzV) (http://www.geodatenzentrum.de/auftrag/pdf/geonutz.pdf) geregelt. Insbesondere hat jeder Nutzer den Quellenvermerk zu allen Geodaten, Metadaten und Geodatendiensten erkennbar und in optischem Zusammenhang zu platzieren. Veränderungen, Bearbeitungen, neue Gestaltungen oder sonstige Abwandlungen sind mit einem Veränderungshinweis im Quellenvermerk zu versehen. Quellenvermerk und Veränderungshinweis sind wie folgt zu gestalten. Bei der Darstellung auf einer Webseite ist der Quellenvermerk mit der URL http://www.bkg.bund.de zu verlinken. © GeoBasis-DE / BKG <Jahr des letzten Datenbezugs> © GeoBasis-DE / BKG <Jahr des letzten Datenbezugs> (Daten verändert) Beispiel: © GeoBasis-DE / BKG 2013"]
    }' ;

-- select description
SELECT obj_description('boundaries.vg250_gem_hole' ::regclass) ::json;
*/

-- update holes
UPDATE 	boundaries.vg250_gem_clean AS t1
	SET  	is_hole = t2.is_hole
	FROM    (
		SELECT	gem.id AS id,
			'TRUE' ::boolean AS is_hole
		FROM	boundaries.vg250_gem_clean AS gem,
			boundaries.vg250_gem_hole AS hole
		WHERE  	gem.geometry = hole.geometry
		) AS t2
	WHERE  	t1.id = t2.id;

-- remove holes
DELETE FROM 	boundaries.vg250_gem_clean
WHERE		is_hole IS TRUE;

/*
-- metadata
COMMENT ON TABLE boundaries.vg250_gem_clean IS '{
    "Name": "ego municipality clean",
    "Source":   [{
	"Name": "open_eGo",
	"URL": "https://github.com/openego/data_processing"}],
    "Reference date": "2016",
    "Date of collection": "02.09.2016",
    "Original file": ["boundaries.vg250_gem"],
    "Spatial": [{
	"Resolution": "1:250.000",
	"Extend": "Germany; Gemeinde (gem) - municipality (mun)" }],
    "Description": ["Municipality without holes"],
    "Column":[
        {"Name": "id", "Description": "Unique identifier", "Unit": " " },
	{"Name": "old_id", "Description": "vg250 identifier", "Unit": " " },
        {"Name": "gen", "Description": "Geografischer Name", "Unit": " " },
	{"Name": "bez", "Description": "Bezeichnung der Verwaltungseinheit", "Unit": " " },
	{"Name": "bem", "Description": "Bemerkung", "Unit": " " },
	{"Name": "nuts", "Description": "Europäischer Statistikschlüssel", "Unit": " " },
	{"Name": "rs_0", "Description": "Aufgefüllter Regionalschlüssel", "Unit": " " },
	{"Name": "ags_0", "Description": "Amtlicher Gemeindeschlüssel", "Unit": " " },
	{"Name": "area_ha", "Description": "Area in ha", "Unit": "ha" },
	{"Name": "count_hole", "Description": "Number of holes", "Unit": " " },
	{"Name": "path", "Description": "Path number", "Unit": " " },
	{"Name": "hole", "Description": "True if hole", "Unit": " " },
	{"Name": "geometry", "Description": "geometry", "Unit": " " } ],
    "Changes":	[
        {"Name": "Ludee", "Mail": "",
	"Date":  "02.09.2015", "Comment": "Created mview" },
	{"Name": "Ludee", "Mail": "",
	"Date":  "16.11.2016", "Comment": "Added metadata" } ],
    "Notes": [""],
    "Licence": [{
	"Name": "Geodatenzugangsgesetz (GeoZG) © GeoBasis-DE / BKG 2016 (Daten verändert)", 
	"URL": "http://www.geodatenzentrum.de/auftrag/pdf/geodatenzugangsgesetz.pdf" }],
    "Instructions for proper use": ["Dieser Datenbestand steht über Geodatendienste gemäß Geodatenzugangsgesetz (GeoZG) (http://www.geodatenzentrum.de/auftrag/pdf/geodatenzugangsgesetz.pdf) für die kommerzielle und nicht kommerzielle Nutzung geldleistungsfrei zum Download und zur Online-Nutzung zur Verfügung. Die Nutzung der Geodaten und Geodatendienste wird durch die Verordnung zur Festlegung der Nutzungsbestimmungen für die Bereitstellung von Geodaten des Bundes (GeoNutzV) (http://www.geodatenzentrum.de/auftrag/pdf/geonutz.pdf) geregelt. Insbesondere hat jeder Nutzer den Quellenvermerk zu allen Geodaten, Metadaten und Geodatendiensten erkennbar und in optischem Zusammenhang zu platzieren. Veränderungen, Bearbeitungen, neue Gestaltungen oder sonstige Abwandlungen sind mit einem Veränderungshinweis im Quellenvermerk zu versehen. Quellenvermerk und Veränderungshinweis sind wie folgt zu gestalten. Bei der Darstellung auf einer Webseite ist der Quellenvermerk mit der URL http://www.bkg.bund.de zu verlinken. © GeoBasis-DE / BKG <Jahr des letzten Datenbezugs> © GeoBasis-DE / BKG <Jahr des letzten Datenbezugs> (Daten verändert) Beispiel: © GeoBasis-DE / BKG 2013"]
    }' ;

-- select description
SELECT obj_description('boundaries.vg250_gem_clean' ::regclass) ::json;
*/

-- validation
CREATE OR REPLACE VIEW boundaries.bkg_vg250_statistics_view AS
-- Area Sum
-- 38162814 ha
SELECT	'vg' ::text AS id,
	SUM(vg.area_ha) ::integer AS area_sum_ha
FROM	boundaries.vg250_sta_tiny_buffer AS vg
UNION ALL
-- 38141292 ha
SELECT	'deu' ::text AS id,
	SUM(vg.area_ha) ::integer AS area_sum_ha
FROM	boundaries.vg250_sta_tiny_buffer AS vg
WHERE	bez='Bundesrepublik'
UNION ALL
-- 38141292 ha
SELECT	'NOT deu' ::text AS id,
	SUM(vg.area_ha) ::integer AS area_sum_ha
FROM	boundaries.vg250_sta_tiny_buffer AS vg
WHERE	bez='--'
UNION ALL
-- 35718841 ha
SELECT	'land' ::text AS id,
	SUM(vg.area_ha) ::integer AS area_sum_ha
FROM	boundaries.vg250_sta_tiny_buffer AS vg
WHERE	gf='3' OR gf='4'
UNION ALL
-- 35718841 ha
SELECT	'water' ::text AS id,
	SUM(vg.area_ha) ::integer AS area_sum_ha
FROM	boundaries.vg250_sta_tiny_buffer AS vg
WHERE	gf='1' OR gf='2';


-- metadata
COMMENT ON VIEW boundaries.bkg_vg250_statistics_view IS '{
	"comment": "eGoDP - Temporary table",
	"version": "v0.3.0" }' ;
