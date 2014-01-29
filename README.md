CascadingSpatial
================

[Cascading](http://www.cascading.org/) workflow with [spatial](http://esri.github.io/gis-tools-for-hadoop/) binning function.

This Hadoop based [Cascading](http://www.cascading.org/) workflow enables me to take zip code locations in the continental US (not very big BTW, this is just a PoC :-)

![CascadingBefore](https://dl.dropboxusercontent.com/u/2193160/CascadingBefore.png "CascadingBefore")

overlay it with a set of hexagon cell in an [Albers equal area conic](http://resources.arcgis.com/en/help/main/10.1/index.html#//003r0000001n000000) projection

![CascadingBetween](https://dl.dropboxusercontent.com/u/2193160/CascadingBetween.png "CascadingBetween")

to produce a spatial density set of bins

![CascadingAfter](https://dl.dropboxusercontent.com/u/2193160/CascadingAfter.png "CascadingAfter")

## Dependencies
* https://github.com/Esri/geometry-api-java
* https://github.com/mraad/Shapefile.git

```
$ git clone https://github.com/Esri/geometry-api-java.git
$ cd geometry-api-java
$ mvn install
```

```
$ git clone https://github.com/mraad/Shapefile.git
$ cd Shapefile
$ mvn install
```

## Data Preparation

I've placed some sample data in the ```data``` folder. I'm assuming that you have a Hadoop cluster. If you do not have one, you can download the [Cloudera Quick Start VM](http://www.cloudera.com/content/cloudera-content/cloudera-docs/DemoVMs/Cloudera-QuickStart-VM/cloudera_quickstart_vm.html)

```
$ hadoop fs -put data/zipcodes.tsv zipcodes.tsv
$ hadoop fs -put data/hexalbers.shp hexalbers.shp
```

## Build and Run

```
$ mvn package
$ hadoop jar target/CascadingSpatial-1.0-job.jar zipcodes.tsv hexalbers.shp output
```

## View Output

```
$ hadoop fs -cat output/part* | more
ORIGID,POPULATION
136,3
137,1
188,17
189,13
213,1
214,2
263,2
264,8
265,7
266,3
...
```

Save the output to a local file

```
$ hadoop fs -cat output/part* > density.csv
```

In [ArcGIS for Desktop](http://www.esri.com/software/arcgis/arcgis-for-desktop), add the ```density.csv``` as table, and join it with the ```hexalbers``` layer for symbolization on the ```POPULATION``` field.

![CascadingJoin](https://dl.dropboxusercontent.com/u/2193160/CascadingJoin.png "CascadingJoin")
