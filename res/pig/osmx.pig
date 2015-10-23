/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/

REGISTER spatialhadoop-2.3.jar;
REGISTER pigeon-0.2.0.jar;
REGISTER esri-geometry-api-1.2.jar;
REGISTER jts-1.8.jar;
REGISTER piggybank.jar;

IMPORT 'pigeon_import.pig';

DEFINE XMLLoader org.apache.pig.piggybank.storage.XMLLoader;
DEFINE OSMNode edu.umn.cs.spatialHadoop.osm.OSMNode;
DEFINE OSMWay edu.umn.cs.spatialHadoop.osm.OSMWay;
DEFINE OSMRelation edu.umn.cs.spatialHadoop.osm.OSMRelation;

/* Read and parse nodes */
xml_nodes = LOAD '$input' USING XMLLoader('node') AS (node:chararray);
parsed_nodes = FOREACH xml_nodes GENERATE OSMNode(node) AS node;

/*Add any node filtering here */
/*parsed_nodes = FILTER parsed_nodes BY node.tags#'highway' == 'traffic_signals';*/
/*parsed_nodes = FILTER parsed_nodes BY ST_Contains(ST_MakeBox(-92.3,46.6,-92.2,46.8), ST_MakePoint(node.lon, node.lat));*/

parsed_nodes = FOREACH parsed_nodes
  GENERATE node.id, node.lon, node.lat, node.tags;

/* Store the nodes to a CSV text file */
STORE parsed_nodes into '$output/all_nodes.bz2' USING PigStorage('\t');

/******************************************************/
/* Read and parse ways */
xml_ways = LOAD '$input' USING XMLLoader('way') AS (way:chararray);
parsed_ways = FOREACH xml_ways GENERATE OSMWay(way) AS way;
  
/* Project columns of interest in ways and flatten nodes*/
parsed_ways = FOREACH parsed_ways
  GENERATE way.id AS way_id, FLATTEN(way.nodes), way.tags AS tags;

/* Project node ID and point location from nodes extracted earlier*/
node_locations = FOREACH parsed_nodes
  GENERATE id, ST_MakePoint(lon, lat) AS location;

/* Join ways with nodes to find the location of each node (lat, lon)*/
joined_ways = JOIN node_locations BY id, parsed_ways BY node_id PARALLEL 70;

/* Group all node locations of each way by way ID*/
ways_with_nodes = GROUP joined_ways BY way_id PARALLEL 70;

/* For each way, generate a shape out of every list of points. Keep first and last node IDs to be able to connect ways later*/
ways_with_shapes = FOREACH ways_with_nodes {
  /* order points by position */
  ordered_asc = ORDER joined_ways BY pos ASC;
  first_node = LIMIT ordered_asc 1;
  ordered_desc = ORDER joined_ways BY pos DESC;
  last_node = LIMIT ordered_desc 1;
  tags = FOREACh joined_ways GENERATE tags;

  GENERATE group AS way_id, FLATTEN(first_node.node_id) AS first_node_id,
     FLATTEN(last_node.node_id) AS last_node_id,
     ST_MakeLinePolygon(ordered_asc.node_id, ordered_asc.location) AS geom,
     FLATTEN(TOP(1, 0, tags)) AS way_tags;
};

ways_with_wkt_shapes = FOREACH ways_with_shapes GENERATE way_id, ST_AsText(geom), way_tags AS tags;

/*Filter by tags and store each dataset in a file*/

/**** Generate roads ****/
road_network = FILTER ways_with_wkt_shapes BY edu.umn.cs.spatialHadoop.osm.HasTag(tags,
  'highway,junction,ford,route,cutting,tunnel,amenity',
  'yes,street,highway,service,parking_aisle,motorway,motorway_link,trunk,trunk_link,primary,primary_link,secondary,secondary_link,tertiary,tertiary_link,living_street,residential,unclassified,track,road,roundabout,escape,mini_roundabout,motorway_junction,passing_place,rest_area,turning_circle,detour,parking_entrance');

STORE road_network INTO '$output/roads.bz2';

/**** Generate road network ****/
road_network = FILTER joined_ways BY edu.umn.cs.spatialHadoop.osm.HasTag(tags,
  'highway,junction,ford,route,cutting,tunnel,amenity',
  'yes,street,highway,service,parking_aisle,motorway,motorway_link,trunk,trunk_link,primary,primary_link,secondary,secondary_link,tertiary,tertiary_link,living_street,residential,unclassified,track,road,roundabout,escape,mini_roundabout,motorway_junction,passing_place,rest_area,turning_circle,detour,parking_entrance');
  
roads_with_nodes = GROUP road_network BY way_id PARALLEL 70;

way_segments = FOREACH ways_with_nodes {
  /* order points by position */
  ordered = ORDER joined_ways BY pos;
  /* All tags are similar. Just grab the first one*/
  tags = FOREACH joined_ways GENERATE tags;
  GENERATE group AS way_id, ST_MakeSegments(ordered.node_id, ordered.location) AS segments,
    FLATTEN(TOP(1, 0, tags)) AS tags;
};

road_segments = FILTER way_segments BY edu.umn.cs.spatialHadoop.osm.HasTag(tags,
  'highway,junction,ford,route,cutting,tunnel,amenity',
  'yes,street,highway,service,parking_aisle,motorway,motorway_link,trunk,trunk_link,primary,primary_link,secondary,secondary_link,tertiary,tertiary_link,living_street,residential,unclassified,track,road,roundabout,escape,mini_roundabout,motorway_junction,passing_place,rest_area,turning_circle,detour,parking_entrance');

road_segments = FOREACH road_segments GENERATE way_id, FLATTEN(segments), tags;
road_segments = FOREACH road_segments GENERATE CONCAT((CHARARRAY)way_id, (CHARARRAY)position), id1, x1, y1, id2, x2, y2, way_id, edu.umn.cs.spatialHadoop.osm.MapToJson(tags);

STORE road_segments into '$output/road_network.bz2' USING PigStorage('\t');

/******************************************************/
/* Read and parse relations */
xml_relations = LOAD '$input' USING XMLLoader('relation') AS (relation:chararray);
parsed_relations = FOREACH xml_relations GENERATE OSMRelation(relation) AS relation;

/* Flatten the members so that we have one member per line */
flattened_relations = FOREACH parsed_relations
  GENERATE relation.id AS relation_id, FLATTEN(relation.members), relation.tags AS tags;

/* Limit flattened relations to those associated with ways */
flattened_relations = FILTER flattened_relations BY members::member_type == 'way';

/* Join relations with ways to get the shape of each way */
relations_join_ways = JOIN flattened_relations BY member_id RIGHT OUTER, ways_with_shapes BY way_id PARALLEL 70;

dangled_ways = FILTER relations_join_ways BY relation_id IS NULL;

relations_with_ways = FILTER relations_join_ways BY relation_id IS NOT NULL;

/* Group nodes by relation_id and way_role */
relations_by_role = GROUP relations_with_ways BY (relation_id, member_role) PARALLEL 70;

relations_with_shapes = FOREACH relations_by_role {
  /* All tags are similar. Just grab the first one*/
  tags = FOREACH relations_with_ways GENERATE tags;
  GENERATE group.relation_id AS relation_id, group.member_role AS way_role,
    ST_Connect(relations_with_ways.first_node_id, relations_with_ways.last_node_id, relations_with_ways.geom) AS shape,
    FLATTEN(TOP(1, 0, tags)) AS tags;
};

good_relations = FILTER relations_with_shapes BY shape is not null;
bad_relations = FILTER relations_with_shapes BY shape is null;

relation_objects = FOREACH good_relations
  GENERATE relation_id AS id, ST_AsText(shape) AS shape, tags;

way_objects = FOREACH dangled_ways
  GENERATE way_id AS id, ST_AsText(geom) AS shape,  way_tags AS tags;

all_objects = UNION ONSCHEMA relation_objects, way_objects;
STORE all_objects INTO '$output/all_objects.bz2';

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
