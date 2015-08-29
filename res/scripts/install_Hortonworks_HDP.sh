#!/bin/bash

# This script installs the redistributable package (in this case spatialhadoop-2.3-2 onto a Hortonworks HDP cluster)

$HDP_VERSION="2.2.6.0-2800"

mkdir spatialhadoop2
cd spatialhadoop2
wget http://spatialhadoop.cs.umn.edu/downloads/spatialhadoop-2.3-2.tar.gz
tar -xzvf spatialhadoop-2.3-2.tar.gz

sudo cp bin/* /usr/bin 
sudo cp etc/hadoop/conf/* /etc/hadoop/conf
sudo cp -r share/hadoop/common/lib/* /usr/local/lib/hadoop/lib
sudo cp spatialhadoop-main.jar /usr/hdp/$HDP_VERSION/hadoop-mapreduce