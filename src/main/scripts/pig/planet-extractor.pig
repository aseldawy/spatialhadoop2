/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
REGISTER spatialhadoop-2.4.jar;
REGISTER pigeon-0.2.1.jar;
REGISTER esri-geometry-api-1.2.jar;
REGISTER jts-1.8.jar;

IMPORT 'osmx.pig';

all_nodes = LoadOSMNodes('$input');
STORE all_nodes INTO '$output/all_nodes.bz2';

pois = FILTER all_nodes BY edu.umn.cs.spatialHadoop.osm.HasTag(tags, CONCAT(
  'name,amenity,cuisine,highway,railway,barrier,power,noexit,man_name, manmade,',
  'light_rail,traffic_calming,turning_circle,natural,public_transport,entrane,',
  'building,emergence,tourism,shop,attraction,histroic,advertising,usage,',
  'addr:street,addr:housenumber'));
STORE pois INTO '$output/pois.bz2';

-- Extract road network

road_network = LoadOSMWaysWithSegments('$input');
road_network = FILTER road_network BY edu.umn.cs.spatialHadoop.osm.HasTag(tags,
  'highway,junction,ford,route,cutting,tunnel,amenity',
  CONCAT('yes,street,highway,service,parking_aisle,motorway,motorway_link,trunk,',
    'trunk_link,primary,primary_link,secondary,secondary_link,tertiary,',
    'tertiary_link,living_street,residential,unclassified,track,road,roundabout,',
    'escape,mini_roundabout,motorway_junction,passing_place,rest_area,',
    'turning_circle,detour,parking_entrance'));
road_network = FOREACH road_network GENERATE segment_id,
  id1, latitude1, longitude1,
  id2, latitude2, longitude2,
  way_id, edu.umn.cs.spatialHadoop.osm.MapToJson(tags) AS tags;

STORE road_network INTO '$output/road_network.bz2';

all_objects = LoadOSMObjects('$input');

STORE all_objects INTO '$output/all_objects.bz2';

buildings = FILTER all_objects BY edu.umn.cs.spatialHadoop.osm.HasTag(tags,
  'amenity,building',
  'yes');
STORE buildings INTO '$output/buildings.bz2';

lakes = FILTER all_objects BY edu.umn.cs.spatialHadoop.osm.HasTag(tags,
  'natural,waterway',
  'bay,wetland,water,coastline,riverbank,dock,boatyard');
STORE lakes INTO '$output/lakes.bz2';

parks = FILTER all_objects BY edu.umn.cs.spatialHadoop.osm.HasTag(tags,
  'leisure,boundary,landuse,natural',
  CONCAT('wood,tree_row,grassland,park,golf_course,national_park,',
    'garden,nature_reserve,forest,grass,tree,orchard,farmland,protected_area'));
STORE parks INTO '$output/parks.bz2';

cemetery = FILTER all_objects BY edu.umn.cs.spatialHadoop.osm.HasTag(tags,
  'landuse', 'cemetery');
STORE cemetery INTO '$output/cemetery.bz2';

sports = FILTER all_objects BY edu.umn.cs.spatialHadoop.osm.HasTag(tags,
  'leisure,landuse',
  'sports_centre,stadium,track,pitch,golf_course,water_park,swimming_pool,recreation_ground,piste');
STORE sports INTO '$output/sports.bz2';

postal_codes = FILTER all_objects BY edu.umn.cs.spatialHadoop.osm.HasTag(tags,
  'admin_level', '8');
STORE postal_codes INTO '$output/postal_codes.bz2';
