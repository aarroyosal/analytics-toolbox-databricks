package com.carto.analyticstoolbox.index.h3

import com.uber.h3core.util.LatLng
import org.locationtech.jts.geom.{LinearRing, MultiPolygon, Polygon}

import java.lang
import scala.collection.JavaConverters._

/**
 * Object with the function logic
 */
object H3Functs {
  /**
   * return an h3 index for a given coordinates and resolution
   */
  val geoToH3: (Double, Double, Int) => Long = (latitude: Double, longitude: Double, resolution: Int) => {
    H3CoreV3Producer.get.geoToH3(latitude, longitude, resolution)
  }

  /**
   * Return an array of the s3 index for a polygon object
   */
  val polygonToH3: (Polygon, Int) => Array[lang.Long] = (polygon: Polygon, resolution: Int) => {
    val toGeoJavaList = (ring: LinearRing) => ring.getCoordinates.map(c => new LatLng(c.y, c.x)).toList.asJava
    val coordinates = toGeoJavaList(polygon.getExteriorRing)
    val holes = (0 until polygon.getNumInteriorRing).map(i => toGeoJavaList(polygon.getInteriorRingN(i))).toList.asJava
    H3CoreV3Producer.get.polyfill(coordinates, holes, resolution).asScala.toArray
  }

  /**
   * Return an array of the s3 index for a multipolygon object
   */
  val multiPolygonToH3: (MultiPolygon, Int) => Array[lang.Long] = (multiPolygon: MultiPolygon, resolution: Int) => {
    (0 until multiPolygon.getNumGeometries)
      .map(multiPolygon.getGeometryN(_).asInstanceOf[Polygon])
      .flatMap(polygonToH3( _, resolution))
      .distinct
      .toArray
  }

  /**
   * Transform a h3 int index to string
   */
  val h32String: Long => String = (h3: Long) => {
    H3CoreV3Producer.get.h3ToString(h3)
  }

  /**
   * Return if the 1st element of the tuple is contained within the second
   */
  val isH3Parent: ((Long, Long)) => Boolean = tuple => {
    val childId = tuple._1
    val parentId = tuple._2
    val parentRes = H3CoreV3Producer.get.h3GetResolution(parentId)
    parentId.equals(H3CoreV3Producer.get.h3ToParent(childId, parentRes))
    }

}
