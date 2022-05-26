package com.carto.analyticstoolbox.index

import com.azavea.hiveless.HUDF
import com.carto.analyticstoolbox.index.h3.H3Functs.isH3Parent

/**
 * function that takes 2 h3 index and answers if the 1st is contained by the second
 */
class H3_IsParent extends HUDF[(Long, Long), Boolean] {
  def function: ((Long, Long)) => Boolean = isH3Parent
}
