package com.carto.analyticstoolbox.index.h3

import com.carto.analyticstoolbox.index.h3.H3UDFs.{geoToH3UDF, multiPolygonToH3UDF, polygonToH3UDF, wktToH3, wktToH3Polyfill}
import org.apache.spark.sql.SparkSession

object CartoUdfRegistrator {
  def registerAll(spark: SparkSession): Unit = {
    spark.udf.register("multiPolygonToH3UDF", multiPolygonToH3UDF)
    spark.udf.register("geoToH3UDF", geoToH3UDF)
    spark.udf.register("polygonToH3UDF", polygonToH3UDF)
    spark.udf.register("wktToH3", wktToH3)
    spark.udf.register("wktToH3Polyfill", wktToH3Polyfill)

  }
}


