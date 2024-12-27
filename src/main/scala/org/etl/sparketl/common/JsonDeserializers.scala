package org.etl.sparketl.common

import com.google.gson.reflect.TypeToken
import com.google.gson.{JsonDeserializationContext, JsonDeserializer, JsonElement}
import org.etl.sparketl.conf.{ColumnTransformation, DataFrameTransformation, Transformation}

import java.lang.reflect.Type
import scala.collection.mutable
import scala.jdk.CollectionConverters.iterableAsScalaIterableConverter
import scala.util.Try

class GenericMapDeserializer extends JsonDeserializer[Any] {

  override def deserialize(json: JsonElement, typeOfT: Type, context: JsonDeserializationContext): Any = {
    val jsonObject = json.getAsJsonObject

    // Get the raw class of the map's value type (generic part)
    val typeToken = TypeToken.get(typeOfT)

    // Check whether the type is mutable or immutable map
    typeToken.getRawType match {
      // Handle mutable.Map[String, _] type
      case clazz if clazz == classOf[mutable.Map[String, _]] =>
        var map = Map[String, Any]()
        jsonObject.entrySet().forEach(entry => {
          map += (entry.getKey -> deserializeValue(entry.getValue, context))
        })
        mutable.Map(map.toSeq: _*)


      // Handle immutable Map[String, _]
      case clazz if clazz == classOf[Map[_, _]] =>
        var map = Map[String, Any]()
        jsonObject.entrySet().forEach(entry => {
          map += (entry.getKey -> deserializeValue(entry.getValue, context))
        })
        map

      case _ =>
        throw new IllegalArgumentException(s"Unsupported map type: $typeOfT")
    }
  }

  // Helper method to deserialize different value types based on their JSON type
  private def deserializeValue(jsonElement: JsonElement, context: JsonDeserializationContext): Any = {
    if (jsonElement.isJsonPrimitive) {
      // If the value is a primitive (string, number, etc.), we handle it here.
      val primitive = jsonElement.getAsJsonPrimitive
      if (primitive.isString) {
        primitive.getAsString
      } else if (primitive.isNumber) {
        // Check if it can be an integer
        Try(primitive.getAsInt).getOrElse(primitive.getAsDouble) // Return as Int or Double if not Int
      } else {
        primitive.getAsString // Fallback
      }
    } else if (jsonElement.isJsonArray) {
      // If the value is an array, we deserialize it as an array of strings
      val jsonArray = jsonElement.getAsJsonArray
      jsonArray.asScala.map {
        case e if e.isJsonPrimitive && e.getAsJsonPrimitive.isString => e.getAsString
        case e => e.toString // Fallback for non-string array values
      }.toArray
    } else if (jsonElement.isJsonObject) {
      // If the value is another map (nested map), we recursively deserialize it.
      val nestedMapType = new TypeToken[Map[String, Array[String]]]() {}.getType
      context.deserialize[Map[String, Array[String]]](jsonElement, nestedMapType)
    } else {
      // Fallback for unsupported or unknown value types
      jsonElement.toString
    }
  }
}


class ImmutableMapDeserializer extends JsonDeserializer[Map[String, String]] {
  override def deserialize(json: JsonElement, typeOfT: Type, context: JsonDeserializationContext): Map[String, String] = {
    var map = Map[String, String]() // Start with an empty immutable Map
    val jsonObject = json.getAsJsonObject

    // Iterate over the JSON object entries and add them to the immutable map
    jsonObject.entrySet().forEach(entry => {
      map += (entry.getKey -> entry.getValue.getAsString) // Use the `+` operator to add entries to the immutable map
    })

    map
  }
}


class MutableMapDeserializer extends JsonDeserializer[mutable.Map[String, String]] {
  override def deserialize(json: JsonElement, typeOfT: Type, context: JsonDeserializationContext): mutable.Map[String, String] = {
    val map = mutable.Map[String, String]() // Use mutable.Map instead of immutable Map
    val jsonObject = json.getAsJsonObject

    // Iterate over the JSON object entries and add them to the mutable map
    jsonObject.entrySet().forEach(entry => {
      map.put(entry.getKey, entry.getValue.getAsString)
    })

    map
  }
}

class TransformationDeserializer extends JsonDeserializer[Transformation] {
  override def deserialize(json: JsonElement, typeOfT: Type, context: JsonDeserializationContext): Transformation = {
    val jsonObject = json.getAsJsonObject
    val transformationType = jsonObject.get("transformationType").getAsString

    transformationType match {
      case "DataframeLevel" =>
        context.deserialize[DataFrameTransformation](jsonObject, classOf[DataFrameTransformation])
      case "ColumnLevel" =>
        context.deserialize[ColumnTransformation](jsonObject, classOf[ColumnTransformation])
      case _ =>
        throw new IllegalArgumentException(s"Unknown transformation type: $transformationType")
    }
  }
}


