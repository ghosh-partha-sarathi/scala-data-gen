package com.app

import java.io.{File, FileInputStream, InputStream}

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.databind.node.{ArrayNode, NullNode, ObjectNode, TextNode}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import scala.collection.JavaConverters._
import scala.jdk.CollectionConverters

class ConfigReader(val jf:JsonFactory = new JsonFactory()) {
  @transient private val mapper = new ObjectMapper(jf)

  def readFromStream(is:InputStream, expanded:Boolean = false): JsonNode = mapper.readTree(is) match {
    case objNode:ObjectNode if expanded => expandObject(objNode)
    case arrNode: ArrayNode if expanded => expandArray(arrNode)
    case node:JsonNode => node
  }

  def readFromString(str:String): JsonNode = {
    mapper.readTree(str)
  }

  def expandArray(arr: ArrayNode): ArrayNode = {
    arr.elements.asScala.zipWithIndex.foreach{case (jsonNode, i) =>
      jsonNode match {
        case objNode: ObjectNode if isRefNode(objNode) => arr.set(i, expandRef(objNode))
        case objNode: ObjectNode => arr.set(i, expandObject(objNode))
        case arrNode: ArrayNode => arr.set(i, expandArray(arrNode))
        case _ =>
      }
    }
    arr
  }

  def expandObject(obj: ObjectNode): ObjectNode = {
    obj.fields().asScala foreach{entry =>
      val node = entry.getValue
      node match {
        case objNode: ObjectNode if isRefNode(objNode) =>
          obj.replace(entry.getKey, expandRef(node.asInstanceOf[ObjectNode]))
        case objNode: ObjectNode => expandObject(objNode)
        case arrNode: ArrayNode => obj.replace(entry.getKey, expandArray(arrNode))
        case textNode: TextNode =>
          val text = textNode.asText
          if (text.contains("\n")) {
            val trimed = text.replace("\r\n","\n")
              .replace("\t","  ")
              .replaceAll("\\s+\n", "\n")
              .replaceAll("\\s+$", "")
            obj.replace(entry.getKey, new TextNode(trimed))
          }
        case _ =>
      }
    }
    obj
  }

  private def isRefNode(node: JsonNode): Boolean = node match {
    case objNode: ObjectNode if objNode.get("$ref") != null => true
    case _ => false
  }

  private def expandRef(node: ObjectNode): JsonNode = {
    val ref = node.get("$ref").asText
    val is = {
      val refFile = new File(ref)
      if (scala.util.Try(refFile.canRead).getOrElse(false)) new FileInputStream(refFile)
      else getClass.getClassLoader.getResourceAsStream(ref)
    }
    if (is != null) {
      val nd = if (ref.toLowerCase.contains(".yaml")) {
        readFromStream(is, true)
      } else {
        val str = scala.io.Source.fromInputStream(is).getLines().mkString("\n")
        new TextNode(str)
      }
      is.close()
      nd
    } else NullNode.getInstance
  }    
}
