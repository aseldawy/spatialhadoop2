#!/bin/sh

mvn assembly:assembly -DskipTests
scp target/spatialhadoop-2.4.1-SNAPSHOT.jar tvu032@ec-hn:/home/tvu032/shadoop/benchmarks/
