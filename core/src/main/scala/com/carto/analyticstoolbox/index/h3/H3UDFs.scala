package com.carto.analyticstoolbox.index.h3

import com.carto.analyticstoolbox.index.h3.H3Functs.{geoToH3, h32String, multiPolygonToH3, polygonToH3}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.locationtech.jts.geom.{Geometry, MultiPolygon, Polygon}
import org.locationtech.jts.io.{ParseException, WKTReader}

/**
 * UDF functions
 */
object H3UDFs {

  /**
   * UDF that calls the geoToH3 function
   */
  val geoToH3UDF: UserDefinedFunction = udf { (latitude: Double, longitude: Double, resolution: Int) =>
    geoToH3(latitude, longitude, resolution)
  }

  /**
   * UDF that calls the polygonToH3 function
   */
  val polygonToH3UDF: UserDefinedFunction = udf { (geometry: Geometry, resolution: Int) =>
    polygonToH3(geometry.asInstanceOf[Polygon], resolution)
  }

  /**
   * UDF that calls the multiPolygonToH3 function
   */
  val multiPolygonToH3UDF: UserDefinedFunction = udf { (geometry: Geometry, resolution: Int) =>
    multiPolygonToH3(geometry.asInstanceOf[MultiPolygon], resolution)
  }

  /**
   * UDF that takes a wkt and transform it to a h3 code for a given resolution
   */
  val wktToH3: UserDefinedFunction = udf { (wkt: String, resolution: Int) =>
    try {
      val reader = new WKTReader()
      val geometry = reader.read(wkt)
      if (geometry.isEmpty) {
        null.asInstanceOf[Long]
      } else {
        val coordinate = geometry.getCoordinate
        if (coordinate == null) {
          null.asInstanceOf[Long]
        } else {
          geoToH3(coordinate.y, coordinate.x, resolution)
        }
      }
    } catch {
      case _: ParseException | _: IllegalArgumentException => null.asInstanceOf[Long]
    }
  }

  /**
   * UDF that takes a wkt and apply the H3 polyfill function to it.
   * Returns an array of longs containing all the H3 indexes
   */
  val wktToH3Polyfill: UserDefinedFunction = udf { (wkt: String, resolution: Int) =>
    try {
      val reader = new WKTReader()
      val geometry = reader.read(wkt)
      if (geometry.isEmpty) {
        null
      } else {
        geometry.getGeometryType match {
          case "MultiPolygon" => multiPolygonToH3(geometry.asInstanceOf[MultiPolygon], resolution)
          case "Polygon" => polygonToH3(geometry.asInstanceOf[Polygon], resolution)
          case _ => null //not supported yet
        }
      }
    } catch {
      case _: ParseException | _: IllegalArgumentException => null
    }
  }


  /**
   * UDF that calls the h32String function
   */
  val h3ToString: UserDefinedFunction = udf { (id: Long) =>
    h32String(id)
  }
}
