package edu.upf.inequality.pipeline

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import geotrellis.spark._

import geotrellis.raster._
import geotrellis.vector._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io._
import geotrellis.shapefile.ShapeFileReader
import geotrellis.raster.io.geotiff._

object Pipeline {
  def main(args: Array[String]) {

    if (args.length != 1) {
      System.err.println(s"""
        |Usage: Pipeline <mobile>
        |  <tilePath> Path to tiles from ETL
        """.stripMargin)
      System.exit(1)
    }
    val Array(tilePath) = args

    implicit val spark : SparkSession = SparkSession.builder()
      .appName("Etl-Example")
      .getOrCreate()

    implicit val isc : SparkContext = spark.sparkContext

    def makeRDD(layerName: String, path: String, num: Int) = {
      val inLayerId = LayerId(layerName, num)
      require(HadoopAttributeStore(path).layerExists(inLayerId))

      HadoopLayerReader(path)
        .query[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](inLayerId)
        .result
    }

    def writeTiff(
      rdd: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]],
      f: String) : Unit = {
      GeoTiff(rdd.stitch, rdd.metadata.extent, rdd.metadata.crs).write(f)
    }

    val nl = makeRDD("nl_2013", tilePath, 8)
    val munis = makeRDD("municipalities", tilePath, 9)

    writeTiff(nl, "test-etl-nl.tif")
    writeTiff(munis, "test-etl-munis.tif")
  }
}
