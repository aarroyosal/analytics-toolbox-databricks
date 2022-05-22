package com.carto.analyticstoolbox.index.h3

import com.uber.h3core.H3CoreV3
import com.uber.h3core.util.LatLng
import org.locationtech.jts.geom.Geometry

import java.lang
import scala.collection.JavaConverters._


object H3Functs {

  object H3 extends Serializable {
    val instance: H3CoreV3 = H3CoreV3.newInstance()
  }

  val geoToH3: (Double, Double, Int) => Long = (latitude: Double, longitude: Double, resolution: Int) => {
    H3.instance.geoToH3(latitude, longitude, resolution)
  }

  val polygonToH3: (Geometry, Int) => Array[lang.Long] = (geometry: Geometry, resolution: Int) => {
    var points: List[LatLng] = List()
    val holes: List[java.util.List[LatLng]] = List()
    if (geometry.getGeometryType == "Polygon") {
      points = List(
        geometry
          .getCoordinates
          .toList
          .map(coord => new LatLng(coord.getY, coord.getX)): _*)
    }
    H3.instance.polyfill(points.asJava, holes.asJava, resolution).asScala.toArray
  }

  val multiPolygonToH3: (Geometry, Int) => Array[lang.Long] = (geometry: Geometry, resolution: Int) => {
    var points: List[LatLng] = List()
    var holes: List[java.util.List[LatLng]] = List()
    if (geometry.getGeometryType == "MultiPolygon") {
      val numGeometries = geometry.getNumGeometries
      if (numGeometries > 0) {
        points = List(
          geometry
            .getGeometryN(0)
            .getCoordinates
            .toList
            .map(coord => new LatLng(coord.y, coord.x)): _*)
      }
      if (numGeometries > 1) {
        holes = (1 until numGeometries).toList.map(n => {
          List(
            geometry
              .getGeometryN(n)
              .getCoordinates
              .toList
              .map(coord => new LatLng(coord.y, coord.x)): _*).asJava
        })
      }
    }
    H3.instance.polyfill(points.asJava, holes.asJava, resolution).asScala.toArray
  }

}
