/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/

REGISTER osmx.jar;
REGISTER spig.jar
REGISTER jts-1.8.jar;

DEFINE XMLLoader org.apache.pig.piggybank.storage.XMLLoader;
DEFINE OSMNode edu.umn.cs.spatialHadoop.udf.OSMNode;
DEFINE OSMWay edu.umn.cs.spatialHadoop.udf.OSMWay;
DEFINE ST_MakePoint edu.umn.cs.spig.ST_MakePoint;
DEFINE ST_AsText edu.umn.cs.spig.ST_AsText;
DEFINE ST_MakeLine edu.umn.cs.spig.ST_MakeLine;

/* Read and parse nodes */
xml_nodes = LOAD 'map.osm.xml'
  USING org.apache.pig.piggybank.storage.XMLLoader('node')
  AS (node:chararray);

parsed_nodes = FOREACH xml_nodes GENERATE FLATTEN(OSMNode(node));

flattened_nodes = FOREACH parsed_nodes
  GENERATE id AS node_id, lat, lon;

STORE flattened_nodes into 'nodes.csv' USING PigStorage(',');

/* Read and parse ways */
xml_ways = LOAD 'map.osm.xml' USING org.apache.pig.piggybank.storage.XMLLoader('way') AS (way:chararray);

parsed_ways = FOREACH xml_ways GENERATE OSMWay(way) AS way;

/* Keep only way_id with tags to be joined later*/
flattened_ways = FOREACH parsed_ways
  GENERATE way.id AS way_id, FLATTEN(way.nodes), way.tags AS tags;

STORE flattened_ways INTO 'edges.csv' using PigStorage(',');
