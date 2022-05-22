package com.carto.analyticstoolbox.index.h3

import com.carto.analyticstoolbox.index.h3.H3Functs.{geoToH3, multiPolygonToH3, polygonToH3}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.{ParseException, WKTReader}


object H3UDFs {
  val geoToH3UDF: UserDefinedFunction = udf { (latitude: Double, longitude: Double, resolution: Int) =>
    geoToH3(latitude, longitude, resolution)
  }

  val polygonToH3UDF: UserDefinedFunction = udf { (geometry: Geometry, resolution: Int) =>
    polygonToH3(geometry, resolution)
  }

  val multiPolygonToH3UDF: UserDefinedFunction = udf { (geometry: Geometry, resolution: Int) =>
    multiPolygonToH3(geometry, resolution)
  }

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

  val wktToH3Polyfill: UserDefinedFunction = udf { (wkt: String, resolution: Int) =>
    try {
      val reader = new WKTReader()
      val geometry = reader.read(wkt)
      if (geometry.isEmpty) {
        null
      } else {
        geometry.getGeometryType match {
          case "MultiPolygon" => multiPolygonToH3(geometry, resolution)
          case "Polygon" => polygonToH3(geometry, resolution)
          case _ => null //not supported yet
        }
      }
    } catch {
      case _: ParseException | _: IllegalArgumentException => null
    }
  }

}
