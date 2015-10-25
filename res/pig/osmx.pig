/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/

IMPORT 'pigeon_import.pig';

DEFINE OSMNode edu.umn.cs.spatialHadoop.osm.OSMNode;
DEFINE OSMWay edu.umn.cs.spatialHadoop.osm.OSMWay;
DEFINE OSMRelation edu.umn.cs.spatialHadoop.osm.OSMRelation;

/*
 * Reads all OSM nodes from an XML file and return one dataset that contains
 * - node_id: ID of the node as appears in XML
 * - latitude:
 * - longitude:
 * - tags: A map of tags as appear in the XML file
 */
DEFINE LoadOSMNodes(osm_file) RETURNS nodes {
  xml_nodes = LOAD '$osm_file' USING org.apache.pig.piggybank.storage.XMLLoader('node') AS (node:chararray);
  osm_nodes = FOREACH xml_nodes GENERATE OSMNode(node) AS node;
  $nodes = FOREACH osm_nodes
    GENERATE node.id AS osm_node_id, node.lon AS longitude, node.lat AS latitude, node.tags AS tags;
};

/*
 * Loads all ways in an OSM XML file. Returns one dataset that conatins:
 * - segment_id: A generated ID which is unique for each segment
 * - id1: ID of the first node
 * - latitude1: Latitude of the first node
 * - longitude1: Longitude of the first node
 * - id2: ID of the second node
 * - latitude2: Latitude of the second node
 * - longitude2: Longitude of the second node
 * - way_id: The ID of the way which contains this segment
 * - tags: Tags of the way that contains this segment
 */
DEFINE LoadOSMWaysWithSegments(osm_file) RETURNS ways_with_segments {
  xml_ways = LOAD '$osm_file' USING org.apache.pig.piggybank.storage.XMLLoader('way') AS (way:chararray);
  osm_ways = FOREACH xml_ways GENERATE OSMWay(way) AS way;

  -- Project columns of interest in ways and flatten nodes
  osm_ways = FOREACH osm_ways
    GENERATE way.id AS way_id, FLATTEN(way.nodes), way.tags AS tags;

  -- Project node ID and point location from nodes
  
  osm_nodes = LoadOSMNodes('$osm_file');
  node_locations = FOREACH osm_nodes
    GENERATE osm_node_id, ST_MakePoint(longitude, latitude) AS location;

  -- Join ways with nodes to find the location of each node (lat, lon)
  joined_ways = JOIN node_locations BY osm_node_id, osm_ways BY nodes::node_id PARALLEL 70;
  
  -- Group all node locations of each way by way ID
  ways_with_nodes = GROUP joined_ways BY way_id PARALLEL 70;
  
  way_segments = FOREACH ways_with_nodes {
    -- order points by position
    ordered = ORDER joined_ways BY pos;
    -- All tags are similar. Just grab the first one
    tags = FOREACH joined_ways GENERATE tags;
    GENERATE group AS way_id, ST_MakeSegments(ordered.osm_node_id, ordered.location) AS segments,
      FLATTEN(TOP(1, 0, tags)) AS tags;
  };
  
  way_segments = FOREACH way_segments GENERATE way_id, FLATTEN(segments), tags;
  
  $ways_with_segments = FOREACH way_segments
    GENERATE CONCAT((CHARARRAY)way_id, (CHARARRAY)position) AS segment_id,
    id1, x1 AS longitude1, y1 AS latitude1,
    id2, x2 AS longitude2, y2 AS latitude2, way_id, tags;
};

/*
 * Loads all ways in an OSM XML file. Returns one dataset that conatins:
 * - way_id: ID of the way as appears in the OSM file
 * - first_node_id: ID of the first node in that way
 * - last_node_id: ID of the last node in that way
 * - geom: Geometry of the way
 * - tags: Key-value pairs of tags as appear in the OSM file
 */
DEFINE LoadOSMWaysWithGeoms(osm_file) RETURNS ways_with_geoms {
  xml_ways = LOAD '$osm_file' USING org.apache.pig.piggybank.storage.XMLLoader('way') AS (way:chararray);
  osm_ways = FOREACH xml_ways GENERATE OSMWay(way) AS way;
  -- Project columns of interest in ways and flatten nodes
  osm_ways = FOREACH osm_ways
    GENERATE way.id AS way_id, FLATTEN(way.nodes), way.tags AS tags;
  
  -- Project node ID and point location from nodes
  osm_nodes = LoadOSMNodes('$osm_file');
  node_locations = FOREACH osm_nodes
    GENERATE osm_node_id, ST_MakePoint(longitude, latitude) AS location;

  -- Join ways with nodes to find the location of each node (lat, lon)
  joined_ways = JOIN node_locations BY osm_node_id, osm_ways BY node_id PARALLEL 70;
  
  -- Group all node locations of each way by way ID
  ways_with_nodes = GROUP joined_ways BY way_id PARALLEL 70;
  
  -- For each way, generate a shape out of every list of points.
  -- Keep first and last node IDs to be able to connect ways later
  $ways_with_geoms = FOREACH ways_with_nodes {
    -- order points by position
    ordered_asc = ORDER joined_ways BY pos ASC;
    first_node = LIMIT ordered_asc 1;
    ordered_desc = ORDER joined_ways BY pos DESC;
    last_node = LIMIT ordered_desc 1;
    tags = FOREACh joined_ways GENERATE tags;
  
    GENERATE group AS way_id, FLATTEN(first_node.osm_node_id) AS first_node_id,
       FLATTEN(last_node.osm_node_id) AS last_node_id,
       ST_MakeLinePolygon(ordered_asc.osm_node_id, ordered_asc.location) AS geom,
       FLATTEN(TOP(1, 0, tags)) AS tags;
  };
};

/*
 * Loads all objects in the osm_file which include:
 * - First level relations: All relations computed by connecting a set of ways.
 * - Dangled ways: Ways that are not included in any relations
 * This macro returns a single relation with the following columns:
 * - object_id: Either relation_id or way_id
 * - geom: Geometry of the objects
 * - tags: Tags of the object, whether they are way or relations tags.
 */
DEFINE LoadOSMObjects(osm_file) RETURNS osm_objects {
  -- Read and parse relations
  xml_relations = LOAD '$osm_file' USING org.apache.pig.piggybank.storage.XMLLoader('relation') AS (relation:chararray);
  osm_relations = FOREACH xml_relations GENERATE OSMRelation(relation) AS relation;
  
  -- Flatten the members so that we have one member per line
  flattened_relations = FOREACH osm_relations
    GENERATE relation.id AS relation_id, FLATTEN(relation.members), relation.tags AS relation_tags;
  
  -- Limit flattened relations to those associated with ways
  flattened_relations = FILTER flattened_relations BY members::member_type == 'way';
  
  ways_with_geoms = LoadOSMWaysWithGeoms('$osm_file');
  -- Join relations with ways to get the shape of each way
  relations_join_ways = JOIN flattened_relations BY member_id RIGHT OUTER, ways_with_geoms BY way_id PARALLEL 70;
  
  dangled_ways = FILTER relations_join_ways BY relation_id IS NULL;
  
  relations_with_ways = FILTER relations_join_ways BY relation_id IS NOT NULL;
  
  -- Group relations by relation_id and way_role
  relations_by_role = GROUP relations_with_ways BY (relation_id, member_role) PARALLEL 70;
  
  relations_with_geoms = FOREACH relations_by_role {
    -- All tags are similar. Just grab the first one
    tags = FOREACH relations_with_ways GENERATE relation_tags;
    GENERATE group.relation_id AS relation_id, group.member_role AS way_role,
      ST_Connect(relations_with_ways.first_node_id, relations_with_ways.last_node_id, relations_with_ways.geom) AS geom,
      FLATTEN(TOP(1, 0, tags)) AS tags;
  };

  -- Find relations that have geometries
  good_relations = FILTER relations_with_geoms BY geom IS NOT NULL;
  
  -- Unify relations and ways schemas to union them
  relation_objects = FOREACH good_relations
    GENERATE relation_id AS id, geom, tags;
  
  way_objects = FOREACH dangled_ways
    GENERATE way_id AS id, geom, tags;
  
  $osm_objects = UNION ONSCHEMA relation_objects, way_objects;
};
