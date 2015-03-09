/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/

REGISTER spatialhadoop-2.3-rc1.jar;
REGISTER pigeon-0.1.0-rc1.jar;
REGISTER esri-geometry-api-1.2.jar;
REGISTER jts-1.8.jar;
REGISTER piggybank.jar;

IMPORT 'pigeon_import.pig';

/* Read and parse nodes */
xml_nodes = LOAD '$input'
  USING org.apache.pig.piggybank.storage.XMLLoader('node')
  AS (node:chararray);

parsed_nodes = FOREACH xml_nodes
  GENERATE edu.umn.cs.spatialHadoop.osm.OSMNode(node) AS node;
  
/*filtered_nodes = FILTER parsed_nodes BY node.tags#'highway' == 'traffic_signals';*/
filtered_nodes = parsed_nodes; /* No filter */
/*filtered_nodes = FILTER parsed_nodes BY ST_Contains(ST_MakeBox(-92.3,46.6,-92.2,46.8), ST_MakePoint(node.lon, node.lat));*/

flattened_nodes = FOREACH filtered_nodes
  GENERATE node.id, node.lon, node.lat, node.tags;

/*STORE flattened_nodes into 'points';*/

flattened_nodes_wkt = FOREACH flattened_nodes
  GENERATE id, ST_AsText(ST_MakePoint(lon, lat)), tags;

/*STORE flattened_nodes_wkt into '/all_points.tsv';*/

/******************************************************/
/* Read and parse ways */
xml_ways = LOAD '$input'
  USING org.apache.pig.piggybank.storage.XMLLoader('way') AS (way:chararray);

parsed_ways = FOREACH xml_ways
  GENERATE edu.umn.cs.spatialHadoop.osm.OSMWay(way) AS way;
  
/* Project columns of interest in ways*/
flattened_ways = FOREACH parsed_ways
  GENERATE way.id AS way_id, FLATTEN(way.nodes), way.tags AS tags;

/* Project node ID and point location from nodes extracted earlier*/
node_locations = FOREACH parsed_nodes
  GENERATE node.id, ST_MakePoint(node.lon, node.lat) AS location;

/* Join ways with nodes to find the location of each node (lat, lon)*/
joined_ways = JOIN node_locations BY id, flattened_ways BY node_id PARALLEL 70;

/* Group all node locations of each way*/
ways_with_nodes = GROUP joined_ways BY way_id PARALLEL 70;

/* For each way, generate a shape out of every list of points*/
ways_with_shapes = FOREACH ways_with_nodes {
  /* order points by position */
  ordered = ORDER joined_ways BY pos;
  /* All tags are similar. Just grab the first one*/
  tags = FOREACH joined_ways GENERATE tags;
  GENERATE group AS way_id, ST_MakeLinePolygon(ordered.node_id, ordered.location) AS geom,
    FLATTEN(TOP(1, 0, tags)) AS tags;
};

/**** Generate road network ****/
road_network = FILTER joined_ways BY edu.umn.cs.spatialHadoop.osm.HasTag(tags,
  'highway,junction,ford,route,cutting,tunnel,amenity',
  'yes,street,highway,service,parking_aisle,motorway,motorway_link,trunk,trunk_link,primary,primary_link,secondary,secondary_link,tertiary,tertiary_link,living_street,residential,unclassified,track,road,roundabout,escape,mini_roundabout,motorway_junction,passing_place,rest_area,turning_circle,detour,parking_entrance');
  
roads_with_nodes = GROUP road_network BY way_id PARALLEL 70;

raod_segments = FOREACH roads_with_nodes {
  /* order points by position */
  ordered = ORDER road_network BY pos;
  /* All tags are similar. Just grab the first one*/
  tags = FOREACH road_network GENERATE tags;
  GENERATE group AS way_id, ST_MakeSegments(ordered.node_id, ordered.location) AS geom,
    FLATTEN(TOP(1, 0, tags)) AS tags;
};

raod_segments = FOREACH raod_segments GENERATE way_id, FLATTEN(geom), tags;
raod_segments = FOREACH raod_segments GENERATE CONCAT((CHARARRAY)way_id, (CHARARRAY)position), id1, x1, y1, id2, x2, y2, way_id, edu.umn.cs.spatialHadoop.osm.MapToJson(tags);

STORE raod_segments into '$output/road_network.bz2' USING PigStorage(',');

/******************************************************/
/* Read and parse relations */
xml_relations = LOAD '$input'
  USING org.apache.pig.piggybank.storage.XMLLoader('relation') AS (relation:chararray);

parsed_relations = FOREACH xml_relations
  GENERATE edu.umn.cs.spatialHadoop.osm.OSMRelation(relation) AS relation;

/* Filter relations according to tags or space */
/* filtered_relations = FILTER parsed_relations BY relation.tags#'admin_level' == '6'; */
/* filtered_relations = parsed_relations;*/
filtered_relations = FILTER parsed_relations BY edu.umn.cs.spatialHadoop.osm.HasTag(relation.tags,
  'amenity,building,natural,waterway,leisure,boundary,landuse,admin_level',
  'yes,bay,wetland,water,coastline,riverbank,dock,boatyard,wood,tree_row,grassland,park,golf_course,national_park,garden,nature_reserve,forest,grass,tree,orchard,farmland,protected_area,cemetery,sports_centre,stadium,track,pitch,golf_course,water_park,swimming_pool,recreation_ground,piste,administrative,8');


/* Flatten the members so that we have one member per line */
flattened_relations = FOREACH filtered_relations
  GENERATE relation.id AS relation_id, FLATTEN(relation.members), relation.tags AS tags;

/* Limit flattened relations to those associated with ways */
flattened_relations = FILTER flattened_relations BY members::member_type == 'way';

/* For each way, generate a shape out of every list of points. Keep first and last node IDs to be able to connect ways later*/
ways_with_ordered_shapes = FOREACH ways_with_nodes {
  /* order points by position */
  ordered_asc = ORDER joined_ways BY pos ASC;
  first_node = LIMIT ordered_asc 1;
  ordered_desc = ORDER joined_ways BY pos DESC;
  last_node = LIMIT ordered_desc 1;
  tags = FOREACh joined_ways GENERATE tags;

  GENERATE group AS way_id, FLATTEN(first_node.node_id) AS first_node_id,
     FLATTEN(last_node.node_id) AS last_node_id,
     ST_MakeLinePolygon(ordered_asc.node_id, ordered_asc.location) AS way_shape,
     FLATTEN(TOP(1, 0, tags)) AS way_tags;
};

/* Join relations with ways to get the shape of each way */
relations_join_ways = JOIN flattened_relations BY member_id RIGHT OUTER, ways_with_ordered_shapes BY way_id PARALLEL 70;

dangled_ways = FILTER relations_join_ways BY relation_id IS NULL;

relations_with_ways = FILTER relations_join_ways BY relation_id IS NOT NULL;

/* Group nodes by relation_id and way_role */
relations_by_role = GROUP relations_with_ways BY (relation_id, member_role) PARALLEL 70;

relations_with_shapes = FOREACH relations_by_role {
  /* All tags are similar. Just grab the first one*/
  tags = FOREACH relations_with_ways GENERATE tags;
  GENERATE group.relation_id AS relation_id, group.member_role AS way_role,
    ST_Connect(relations_with_ways.first_node_id, relations_with_ways.last_node_id, relations_with_ways.way_shape) AS shape,
    FLATTEN(TOP(1, 0, tags)) AS tags;
};

good_relations = FILTER relations_with_shapes BY shape is not null;
bad_relations = FILTER relations_with_shapes BY shape is null;

relation_objects = FOREACH good_relations
  GENERATE relation_id AS id, ST_AsText(shape) AS shape, tags;

way_objects = FOREACH dangled_ways
  GENERATE way_id AS id, ST_AsText(way_shape) AS shape,  way_tags AS tags;

all_objects = UNION ONSCHEMA relation_objects, way_objects;

/*
buildings = FILTER all_objects BY edu.umn.cs.spatialHadoop.osm.HasTag(tags,
  'amenity,building.bz2',
  'yes');
STORE buildings INTO '$output/buildings.bz2';

lakes = FILTER all_objects BY edu.umn.cs.spatialHadoop.osm.HasTag(tags,
  'natural,waterway',
  'bay,wetland,water,coastline,riverbank,dock,boatyard');
STORE lakes INTO '$output/lakes.bz2';

parks = FILTER all_objects BY edu.umn.cs.spatialHadoop.osm.HasTag(tags,
  'leisure,boundary,landuse,natural',
  'wood,tree_row,grassland,park,golf_course,national_park,garden,nature_reserve,forest,grass,tree,orchard,farmland,protected_area');
STORE parks INTO '$output/parks.bz2';

cemetery = FILTER all_objects BY edu.umn.cs.spatialHadoop.osm.HasTag(tags,
  'landuse',
  'cemetery');
STORE cemetery INTO '$output/cemetery.bz2';

sports = FILTER all_objects BY edu.umn.cs.spatialHadoop.osm.HasTag(tags,
  'leisure,landuse',
  'sports_centre,stadium,track,pitch,golf_course,water_park,swimming_pool,recreation_ground,piste');
STORE sports INTO '$output/sports.bz2';

postal = FILTER all_objects BY edu.umn.cs.spatialHadoop.osm.HasTag(tags,
  'admin_level',
  '8');
STORE postal INTO '$output/postal_codes.bz2';

STORE all_objects INTO '$output/all_objects.bz2';
STORE bad_relations INTO '$output/bad_relations.bz2';
*/
