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

    shadoop generate test.rects size:1.gb shape:rect mbr:0,0,1000000,1000000 

Build a grid index over the generated file

    shadoop index test.rects sindex:grid test.grid shape:rect

Run a range query that selects rectangles overlapping the query area defined
by the box with the two corners (10, 20) and (2000, 3000). Results are stored
in the output file *rangequery.out*

    shadoop rangequery test.grid rect:10,10,2000,3000 rangequery.out shape:rect
    
Compile
=======

Advanced users and contributors might like to compile SpatialHadoop on their own machines.
SpatialHadoop can be compiled via [Maven](http://maven.apache.org/).
First, you need to grab your own version of the source code. You can do this through [git](http://git-scm.com/).
The source code resides on [github](http://github.com). To clone the repository, run the following command

    git clone https://github.com/aseldawy/spatialhadoop2.git
    
If you do not want to use git, you can still download it as a
[single archive](https://github.com/aseldawy/spatialhadoop2/archive/master.zip) provided by github.

Once you downloaded the source code, you need to make sure you have Any and Ivy installed on your system.
Please check the installation guide of [Maven](http://maven.apache.org/install.html) if you do not have it installed.

To compile SpatialHadoop, navigate to the source code and run the command:

    mvn compile

This will automatically retrieve all dependencies and compile the source code.

To build a redistribution package, run the command:

    mvn assembly:assembly

This Maven command will package all classes of SpatialHadoop along with the dependent jars
not included in Hadoop into an archive. This archive can be used to install SpatialHadoop
on any existing Hadoop cluster.

