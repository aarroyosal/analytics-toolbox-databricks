package com.carto.analyticstoolbox.index.h3

import com.carto.analyticstoolbox.index.h3.H3UDFs.{geoToH3UDF, h3ToString, multiPolygonToH3UDF, polygonToH3UDF, wktToH3, wktToH3Polyfill}
import org.apache.spark.sql.SparkSession

/**
 * Helper class with the goal of registering all the UDFs in spark in order to use the in spark sql
 */
object CartoUdfRegistrator {
  def registerAll(spark: SparkSession): Unit = {
    spark.udf.register("multiPolygonToH3UDF", multiPolygonToH3UDF)
    spark.udf.register("geoToH3UDF", geoToH3UDF)
    spark.udf.register("polygonToH3UDF", polygonToH3UDF)
    spark.udf.register("wktToH3", wktToH3)
    spark.udf.register("wktToH3Polyfill", wktToH3Polyfill)
    spark.udf.register("h3ToString", h3ToString)

  }
}


