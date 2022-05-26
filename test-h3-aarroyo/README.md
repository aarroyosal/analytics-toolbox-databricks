# Carto H3 Test 
### by Ángel Arroyo

This delivery is supposed to contain the development of functions a carto programmer and the use of those functions by the use in databricks notebooks

There have been added some functions to use in scala or sql code.

In order to use it with spark scala api you just need to import them:
*import com.carto.aarroyo.utils.H3UDFs._*

If you would rather use the SQL api all you need to do is call the new registrator
*CartoUdfRegistrator.registerAll(spark)*

The catalog of the new functions:
 * geoToH3UDF - Gets the H3 id for a given coords 
 * multiPolygonToH3UDF - Polyfills in H3 a Multipolygon object for a given resolution 
 * polygonToH3UDF - Polyfills in H3 a Multipolygon object for a given resolution
 * wktToH3Polyfill - Polyfills in H3 a WKT string for a given resolution. Accepted types are Polygon and Multipolygon so far
 * h3ToString - Transform a h3 int id to string
 * H3_IsParent - Return a boolean if the first h3 index is contained by the second

In order to check the result of the tests, you need to follow the next steps:
 * Get the jar with dependencies with the assembly command
 * Install this jar in a databricks cluster
 * Imports the notebooks in databricks and execute the in order. Each notebook is associated with one task
You can see if you are getting the right responses comparing your notebook with the html documents present in this folder