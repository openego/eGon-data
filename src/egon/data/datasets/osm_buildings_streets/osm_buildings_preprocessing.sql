/*
 * extract all buildings
 * calculate area
 * create centroids
*/
DROP TABLE IF EXISTS openstreetmap.osm_buildings;
CREATE TABLE openstreetmap.osm_buildings AS
	SELECT
		poly.osm_id,
		poly.amenity,
		poly.building,
		poly.name,
		ST_TRANSFORM(poly.geom, 3035) AS geom,
		ST_AREA(ST_TRANSFORM(poly.geom, 3035)) AS area,
		ST_TRANSFORM(ST_CENTROID(poly.geom), 3035) AS geom_point,
		poly.tags
	FROM openstreetmap.osm_polygon poly
	WHERE poly.building IS NOT NULL;
	
CREATE INDEX ON openstreetmap.osm_buildings USING gist (geom);
CREATE INDEX ON openstreetmap.osm_buildings USING gist (geom_point);

/*
 * extract specific buildings only
 * calculate area
 * create centroids
*/
DROP TABLE if exists openstreetmap.osm_buildings_filtered;
CREATE TABLE openstreetmap.osm_buildings_filtered as
    select *
    from openstreetmap.osm_buildings bld
    where
		bld.building like 'yes'
		or bld.building like 'apartments'
		or bld.building like 'detached'
		or bld.building like 'dormitory'
		or bld.building like 'farm'
		or bld.building like 'hotel'
		or bld.building like 'house'
		or bld.building like 'residential'
		or bld.building like 'semidetached_house'
		or bld.building like 'static_caravan'
		or bld.building like 'terrace'
		or bld.building like 'commercial'
		or bld.building like 'industrial'
		or bld.building like 'kiosk'
		or bld.building like 'office'
		or bld.building like 'retail'
		or bld.building like 'supermarket'
		or bld.building like 'warehouse'
		or bld.building like 'cathedral'
		or bld.building like 'chapel'
		or bld.building like 'church'
		or bld.building like 'monastery'
		or bld.building like 'mosque'
		or bld.building like 'presbytery'
		or bld.building like 'religious'
		or bld.building like 'shrine'
		or bld.building like 'synagogue'
		or bld.building like 'temple'
		or bld.building like 'bakehouse'
		or bld.building like 'civic'
		or bld.building like 'fire_station'
		or bld.building like 'government'
		or bld.building like 'hospital'
		or bld.building like 'public'
		or bld.building like 'train_station'
		or bld.building like 'transportation'
		or bld.building like 'kindergarten'
		or bld.building like 'school'
		or bld.building like 'university'
		or bld.building like 'college'
		or bld.building like 'barn'
		or bld.building like 'conservatory'
		or bld.building like 'farm_auxiliary'
		or bld.building like 'stable'
		or bld.building like 'pavilion'
		or bld.building like 'riding_hall'
		or bld.building like 'sports_hall'
		or bld.building like 'stadium'
		or bld.building like 'digester'
		or bld.building like 'service'
		or bld.building like 'transformer_tower'
		or bld.building like 'military'
		or bld.building like 'gatehouse'
		or bld.amenity like 'bar'
		or bld.amenity like 'biergarten'
		or bld.amenity like 'cafe'
		or bld.amenity like 'fast_food'
		or bld.amenity like 'food_court'
		or bld.amenity like 'ice_cream'
		or bld.amenity like 'pub'
		or bld.amenity like 'restaurant'
		or bld.amenity like 'college'
		or bld.amenity like 'driving_school'
		or bld.amenity like 'kindergarten'
		or bld.amenity like 'language_school'
		or bld.amenity like 'library'
		or bld.amenity like 'toy_library'
		or bld.amenity like 'music_school'
		or bld.amenity like 'school'
		or bld.amenity like 'university'
		or bld.amenity like 'car_wash'
		or bld.amenity like 'vehicle_inspection'
		or bld.amenity like 'charging_station'
		or bld.amenity like 'fuel'
		or bld.amenity like 'bank'
		or bld.amenity like 'clinic'
		or bld.amenity like 'dentist'
		or bld.amenity like 'doctors'
		or bld.amenity like 'hospital'
		or bld.amenity like 'nursing_home'
		or bld.amenity like 'pharmacy'
		or bld.amenity like 'social_facility'
		or bld.amenity like 'veterinary'
		or bld.amenity like 'arts_centre'
		or bld.amenity like 'brothel'
		or bld.amenity like 'casino'
		or bld.amenity like 'cinema'
		or bld.amenity like 'community_centre'
		or bld.amenity like 'conference_centre'
		or bld.amenity like 'events_venue'
		or bld.amenity like 'gambling'
		or bld.amenity like 'love_hotel'
		or bld.amenity like 'nightclub'
		or bld.amenity like 'planetarium'
		or bld.amenity like 'social_centre'
		or bld.amenity like 'stripclub'
		or bld.amenity like 'studio'
		or bld.amenity like 'swingerclub'
		or bld.amenity like 'exhibition_centre'
		or bld.amenity like 'theatre'
		or bld.amenity like 'courthouse'
		or bld.amenity like 'embassy'
		or bld.amenity like 'fire_station'
		or bld.amenity like 'police'
		or bld.amenity like 'post_depot'
		or bld.amenity like 'post_office'
		or bld.amenity like 'prison'
		or bld.amenity like 'ranger_station'
		or bld.amenity like 'townhall'
		or bld.amenity like 'animal_boarding'
		or bld.amenity like 'childcare'
		or bld.amenity like 'dive_centre'
		or bld.amenity like 'funeral_hall'
		or bld.amenity like 'gym'
		or bld.amenity like 'internet_cafe'
		or bld.amenity like 'kitchen'
		or bld.amenity like 'monastery'
		or bld.amenity like 'place_of_mourning'
		or bld.amenity like 'place_of_worship';

CREATE INDEX ON openstreetmap.osm_buildings_filtered USING gist (geom);
CREATE INDEX ON openstreetmap.osm_buildings_filtered USING gist (geom_point);

-- merge n_apartments representing households to buildings_filtered
-- first a temp table is needed due to wanna add a count to share n_apartments for n_buildings
-- all buildings within a radius of ~77.7 meters

-- create temporary table of buildings containing zensus data (residentials)
drop table if exists openstreetmap.osm_buildings_with_res_tmp;
CREATE TABLE openstreetmap.osm_buildings_with_res_tmp as
    select * from (
        select
			bld.osm_id,
			bld.amenity,
			bld.building,
			bld.name,
			bld.geom,
			bld.area,
			bld.tags,
			zensus_apartments.apartment_count,
			zensus_apartments.grid_id
        from openstreetmap.osm_buildings_filtered bld
        left join society.egon_destatis_zensus_apartment_building_population_per_ha zensus_apartments
        on ST_DWithin(zensus_apartments.geom, bld.geom, 0.0007) -- radius is around 77.7 meters
    ) buildings_with_apartments
    where buildings_with_apartments.osm_id is not null;

-- ^- this table will be droppped at a later point due to the need is only for preprocessing steps

drop table if exists openstreetmap.osm_buildings_with_res_tmp2;
create table openstreetmap.osm_buildings_with_res_tmp2 as
    select
		bld.osm_id,
		bld.amenity,
		bld.building,
		bld.name,
		bld.geom,
		bld.area,
		bld.tags,
		bld.apartment_count,
		bld.grid_id,
		bwa.n_apartments_in_n_buildings -- number of buildings the apartments are shared by
    from openstreetmap.osm_buildings_with_res_tmp as bld
    left join (
        select * from (
            select
				b_res.grid_id,
				count(b_res.*) as n_apartments_in_n_buildings
			from openstreetmap.osm_buildings_with_res_tmp as b_res
            group by b_res.grid_id
        ) bwa ) bwa
    on bld.grid_id = bwa.grid_id;
CREATE INDEX ON openstreetmap.osm_buildings_with_res_tmp2 USING gist (geom);


drop table if exists openstreetmap.osm_amenities_in_buildings_tmp;
CREATE TABLE openstreetmap.osm_amenities_in_buildings_tmp as
    with amenity as (select * from openstreetmap.osm_amenities_shops_filtered af)
    select
		bf.osm_id as osm_id_building,
		bf.building,
		bf.area,
		bf.geom as geom_building,
		amenity.osm_id as osm_id_amenity,
		amenity.amenity,
		amenity.name,
		amenity.geom as geom_amenity,
		bf.tags as tags_building,
		amenity.tags as tags_amenity,
		bf.apartment_count,
		bf.grid_id,
		bf.n_apartments_in_n_buildings
    from amenity, openstreetmap.osm_buildings_with_res_tmp2 bf
    where st_intersects(bf.geom, amenity.geom);


-- buildings containing amenities
drop table if exists openstreetmap.osm_buildings_with_amenities;
CREATE TABLE openstreetmap.osm_buildings_with_amenities as
    select
		bwa.osm_id_amenity,
		bwa.osm_id_building,
		bwa.building,
		bwa.area,
		bwa.geom_building,
		bwa.geom_amenity,
    CASE
       WHEN (ST_Contains(bwa.geom_building, ST_Centroid(bwa.geom_building))) IS TRUE
       THEN ST_Centroid(bwa.geom_building)
       ELSE ST_PointOnSurface(bwa.geom_building)
    END AS geom_point,
    bwa."name",
	bwa.tags_building,
	bwa.tags_amenity,
	bwa.n_amenities_inside,
    case
        when apartment_count > 0
        then bwa.apartment_count / bwa.n_apartments_in_n_buildings
        else 0
    end as apartment_count
    from (
        select
			bwa.osm_id_amenity,
			bwa.osm_id_building,
			bwa.building,
			bwa.area,
			bwa.geom_building,
			bwa.geom_amenity,
			bwa.name,
			bwa.tags_building,
			bwa.tags_amenity,
			bwa.n_amenities_inside,
			SUM(bwa.apartment_count) as apartment_count,
			SUM(bwa.n_apartments_in_n_buildings) as n_apartments_in_n_buildings
        from (
            select
				b.osm_id_amenity,
				b.osm_id_building,
				coalesce(b.amenity, b.building) as building,
				b.area,
				b.geom_building,
				b.geom_amenity,
				b.name,
				b.tags_building,
				b.tags_amenity,
				coalesce(b.apartment_count, 0) as apartment_count,
				coalesce(b.n_apartments_in_n_buildings, 0) as n_apartments_in_n_buildings,
				ainb.n_amenities_inside
            from openstreetmap.osm_amenities_in_buildings_tmp b
            left join (
                select
					ainb.osm_id_building,
					count(*) as n_amenities_inside
					from openstreetmap.osm_amenities_in_buildings_tmp ainb
                group by ainb.osm_id_building ) ainb
            on b.osm_id_building = ainb.osm_id_building
        ) bwa
        group by
			bwa.osm_id_amenity,
			bwa.osm_id_building,
			bwa.building,
			bwa.area,
			bwa.geom_building,
			bwa.geom_amenity,
			bwa.name,
			bwa.tags_building,
			bwa.tags_amenity,
			bwa.n_amenities_inside
    ) bwa;
CREATE INDEX ON openstreetmap.osm_buildings_with_amenities USING gist (geom_building);


-- buildings containing no amenities
drop table if exists openstreetmap.osm_buildings_without_amenities;
CREATE TABLE openstreetmap.osm_buildings_without_amenities as
    select
		bwa.osm_id,
		bwa.building,
		bwa.area,
		bwa.geom,
    CASE
       WHEN (ST_Contains(bwa.geom, ST_Centroid(bwa.geom))) IS TRUE
       THEN ST_Centroid(bwa.geom)
       ELSE ST_PointOnSurface(bwa.geom)
    END AS geom_point,
    bwa.name,
	bwa.tags,
	CASE
		WHEN apartment_count > 0
		THEN bwa.apartment_count / bwa.n_apartments_in_n_buildings
		ELSE 0
	END AS apartment_count
    from (
        select
			bwa.osm_id,
			bwa.building,
			bwa.area,
			bwa.geom,
			bwa.name,
			bwa.tags,
			SUM(bwa.apartment_count) as apartment_count,
			SUM(bwa.n_apartments_in_n_buildings) as n_apartments_in_n_buildings
        from (
            select
				bf.osm_id,
				coalesce(bf.amenity, bf.building) as building,
				bf.name,
				bf.area,
				bf.geom,
				bf.tags,
				coalesce(bf.apartment_count, 0) as apartment_count,
				coalesce(bf.n_apartments_in_n_buildings, 0) as n_apartments_in_n_buildings
            from openstreetmap.osm_buildings_with_res_tmp2 bf
            where bf.osm_id not in (
				select aib.osm_id_building
				from openstreetmap.osm_amenities_in_buildings_tmp aib
			)
        ) bwa
        group by
			bwa.osm_id,
			bwa.building,
			bwa.area, bwa.geom,
			bwa.name,
		bwa.tags
    ) bwa;
CREATE INDEX ON openstreetmap.osm_buildings_without_amenities USING gist (geom);


-- amenities not located in a building
drop table if exists openstreetmap.osm_amenities_not_in_buildings;
CREATE TABLE openstreetmap.osm_amenities_not_in_buildings as
    select *
	from openstreetmap.osm_amenities_shops_filtered af
    where af.osm_id not in (
		select aib.osm_id_amenity
		from openstreetmap.osm_amenities_in_buildings_tmp aib
	);
CREATE INDEX ON openstreetmap.osm_amenities_not_in_buildings USING gist (geom);
