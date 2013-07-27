SpatialHadoop
=============

[SpatialHadoop](http://spatialhadoop.cs.umn.edu) is an extension to Hadoop that provides efficient processing of
spatial data using MapReduce. It provides spatial data types to be used in
MapReduce jobs including point, rectangle and polygon. It also adds low level
spatial indexes in HDFS such as Grid file, R-tree and R+-tree. Some new
InputFormats and RecordReaders are also provided to allow reading and processing
spatial indexes efficiently in MapReduce jobs. SpatialHadoop also ships with
a bunch of spatial operations that are implemented as efficient MapReduce jobs
which access spatial indexes using the new components. Developers can implement
myriad spatial operations that run efficiently using spatial indexes.


How it works
============

SpatialHadoop is used in the same way as Hadoop. Data files are first loaded
into HDFS and indexed using the *index* command which builds a spatial index
of your choice over the input file. Once the file is indexed, you can execute
any of the spatial operations provided in SpatialHadoop such as range query,
k-nearest neighbor and spatial join. New operations are added occasionally
to SpatialHadoop such as polygon union and convex hull.


Install
=======

SpatialHadoop is packaged as a single jar file which contains all the required
classes to run. All operations including building the index can be accessed
through this jar file and it gets automatically distributed to all slave nodes
by the Hadoop framework. In addition the *spatial-site.xml* configuration file
needs to be placed in the *conf* directory of your Hadoop installation. This
allows you to configure the cluster accordingly.


Examples
========

Here are a few examples of how to use SpatialHadoop.

Generate a non-indexed spatial file with rectangles in a rectangular area of 1M x 1M

    hadoop spatialhadoop*.jar generate test.rects size:1.gb shape:rect mbr:0,0,1000000,1000000 

Build a grid index over the generated file

    hadoop spatialhadoop*.jar index test.rects sindex:grid test.grid

Run a range query that selects rectangles overlapping the query area defined
by the box with the two corners (10, 20) and (2000, 3000). Results are stored
in the output file *rangequery.out*

    hadoop spatialhadoop*.jar rangequery test.grid rect:10,10,2000,3000 rangequery.out
    

