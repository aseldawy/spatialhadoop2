/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/

nodes = LOAD '/node.csv' USING PigStorage(',')
  AS (node_id:long, lat:double, lon:double);

edges = LOAD '/road_edges.csv' USING PigStorage(',')
  AS (edge_id:long, node_id1:long, node_id2:long, way_id:long, tags);

edges_join1 = JOIN edges BY node_id1,
              nodes BY node_id;

edges_join2 = JOIN edges_join1 BY node_id2,
              nodes BY node_id;

edges_join = FOREACH edges_join2
  GENERATE edge_id AS edge_id,
    node_id1 AS node_id1, edges_join1::nodes::lat AS lat1, edges_join1::nodes::lon AS lon1,
    node_id2 AS node_id2, nodes::lat AS lat2, nodes::lon AS lon2,
    way_id AS way_id, tags AS tags;

STORE edges_join INTO 'edges_join.csv' USING PigStorage(',');