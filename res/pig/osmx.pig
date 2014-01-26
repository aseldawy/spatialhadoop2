/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the
 * NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

REGISTER spatialhadoop-2-b3.jar;
REGISTER pigeon.jar
REGISTER esri-geometry-api-1.1.1.jar;
REGISTER piggybank-0.12.0.jar;

IMPORT 'pigeon_import.pig';

/* Read and parse nodes */
xml_nodes = LOAD 'mpls-stpaul.osm.bz2'
  USING org.apache.pig.piggybank.storage.XMLLoader('node')
  AS (node:chararray);

parsed_nodes = FOREACH xml_nodes
  GENERATE edu.umn.cs.spatialHadoop.osm.OSMNode(node) AS node;
  
/*filtered_nodes = FILTER parsed_nodes BY node.tags#'highway' == 'traffic_signals';*/
filtered_nodes = parsed_nodes; /* No filter */

flattened_nodes = FOREACH filtered_nodes
  GENERATE node.id, node.lon, node.lat, node.tags;

/*STORE flattened_nodes into 'points';*/

flattened_nodes_wkt = FOREACH flattened_nodes
  GENERATE id, ST_AsText(ST_MakePoint(lon, lat)), tags;

/*STORE flattened_nodes_wkt into '/all_points.tsv';*/

/******************************************************/
/* Read and parse ways */
xml_ways = LOAD 'mpls-stpaul.osm.bz2'
  USING org.apache.pig.piggybank.storage.XMLLoader('way') AS (way:chararray);

parsed_ways = FOREACH xml_ways
  GENERATE edu.umn.cs.spatialHadoop.osm.OSMWay(way) AS way;
  
/* Filter ways to keep only ways of interest */
/*filtered_ways = FILTER parsed_ways BY way.tags#'boundary' == 'administrative';*/
filtered_ways = parsed_ways;

/* Project columns of interest in ways*/
flattened_ways = FOREACH filtered_ways
  GENERATE way.id AS way_id, FLATTEN(way.nodes), way.tags AS tags;

/* Project node ID and point location from nodes extracted earlier*/
node_locations = FOREACH parsed_nodes
  GENERATE node.id, ST_MakePoint(node.lon, node.lat) AS location;

/* Join ways with nodes to find the location of each node (lat, lon)*/
joined_ways = JOIN node_locations BY id, flattened_ways BY node_id;

/* Group all node locations of each way*/
ways_with_nodes = GROUP joined_ways BY way_id;

/* For each way, generate a shape out of every list of points*/
ways_with_shapes = FOREACH ways_with_nodes {
  /* order points by position */
  ordered = ORDER joined_ways BY pos;
  /* All tags are similar. Just grab the first one*/
  tags = FOREACH joined_ways GENERATE tags;
  GENERATE group AS way_id, ST_AsText(ST_MakeLinePolygon(ordered.node_id, ordered.location)) AS shape,
    FLATTEN(TOP(1, 0, tags)) AS tags;
};

/* STORE ways_with_shapes into 'all_ways.tsv'; */


/******************************************************/
/* Read and parse relations */
xml_relations = LOAD 'mpls-stpaul.osm.bz2'
  USING org.apache.pig.piggybank.storage.XMLLoader('relation') AS (relation:chararray);

parsed_relations = FOREACH xml_relations
  GENERATE edu.umn.cs.spatialHadoop.osm.OSMRelation(relation) AS relation;

/* Filter relations according to tags or space */
filtered_relations = FILTER parsed_relations BY relation.tags#'admin_level' == '6';

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
relations_join_ways = JOIN flattened_relations BY member_id RIGHT OUTER, ways_with_ordered_shapes BY way_id;

dangled_ways = FILTER relations_join_ways BY relation_id IS NULL AND way_tags#'admin_level' == '6';

relations_with_ways = FILTER relations_join_ways BY relation_id IS NOT NULL;

/* Group nodes by relation_id and way_role */
relations_by_role = GROUP relations_with_ways BY (relation_id, member_role);

relations_with_shapes = FOREACH relations_by_role {
  /* All tags are similar. Just grab the first one*/
  tags = FOREACH relations_with_ways GENERATE tags;
  GENERATE group.relation_id AS relation_id, group.member_role AS way_role,
    ST_Connect(relations_with_ways.first_node_id, relations_with_ways.last_node_id, relations_with_ways.way_shape) AS shape,
    FLATTEN(TOP(1, 0, tags)) AS tags;
};

good_relations = FILTER relations_with_shapes BY shape is not null;

relation_objects = FOREACH good_relations
  GENERATE 'relation' AS source, relation_id, ST_AsText(shape) AS shape, tags;

way_objects = FOREACH dangled_ways
  GENERATE 'way' AS source, way_id, ST_AsText(way_shape) AS shape, way_tags AS tags;

all_objects = UNION ONSCHEMA relation_objects, way_objects;

STORE all_objects INTO 'all_counties.tsv';


