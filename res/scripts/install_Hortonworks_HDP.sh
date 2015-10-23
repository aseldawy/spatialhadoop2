#!/bin/bash

# This script installs the redistributable package (in this case spatialhadoop-2.3-2 onto a Hortonworks HDP cluster)

$HDP_VERSION="2.2.6.0-2800"
$SPATIALHADOOP_VERSION="spatialhadoop-2.4-rc1.jar"

mkdir spatialhadoop2
cd spatialhadoop2
wget http://spatialhadoop.cs.umn.edu/downloads/$SPATIALHADOOP_VERSION
tar -xzvf $SPATIALHADOOP_VERSION

sudo cp bin/* /usr/bin 
sudo cp etc/hadoop/conf/* /etc/hadoop/conf
sudo cp -r share/hadoop/common/lib/* /usr/local/lib/hadoop/lib
sudo cp share/hadoop/common/lib/spatialhadoop-2.4-rc1.jar /usr/hdp/$HDP_VERSION/hadoop/lib/
sudo cp spatialhadoop-main.jar /usr/hdp/$HDP_VERSION/hadoop-mapreduce