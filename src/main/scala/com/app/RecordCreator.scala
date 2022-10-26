package com.app

import org.apache.avro.Schema
import org.apache.avro.Schema.Type.{ARRAY, BOOLEAN, BYTES, DOUBLE, FLOAT, ENUM, INT, LONG, MAP, RECORD, STRING, UNION}
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericData.{ Record, EnumSymbol }
import org.slf4j.LoggerFactory
import java.util.Arrays

import scala.None
import scala.collection.{ immutable, mutable }
import scala.collection.mutable._
import scala.jdk.CollectionConverters._

class RecordCreator {
  private val logger = LoggerFactory.getLogger(classOf[RecordCreator])
  private val defaultMap: Map[String, Any] = Map()
  private var inputMap: Map[String, Any] = Map()
  private val elemTypes: List[String] = List("STRING", "INT", "LONG", "DOUBLE")
  private val keyListBuilder: mutable.StringBuilder = new StringBuilder()
  private var runType: String = ""
  private val outHandler: OutputHandler = new OutputHandler

  def createDefaultRecord(defaultContent: String): Unit = {
    defaultContent.split('|').foreach(entry => {
      val field = entry.split("::")
      defaultMap.put(field(0), field(1))
    })
  }

  def createDefaultRecord(defaultContentBuffer: ListBuffer[String]): Unit = {
    defaultContentBuffer.toList.foreach(line => {
      val field = line.split("::")
      defaultMap.put(field(0), field(1))
    })
  }

  def createKeyList(baseSchema: Schema): String = {
    runType="_key_"
    parseSchema(baseSchema)
    logger.info("=================================================================")
    // logger.info(keyListBuilder.toString())
    keyListBuilder.toString()
  }

  def createStringRecord(baseSchema: Schema
                   , inputContent: Map[String, Any]
                   , outputFile: String):Unit = {
    val recordListBuff: ListBuffer[Record] = new ListBuffer()               
    if(inputMap.size > 0){
      inputMap.empty
    }
    inputMap = inputContent

    val aRecord = parseSchema(baseSchema)
    recordListBuff.addOne(aRecord.asInstanceOf[Record])
    outHandler.writeListToFile(outputFile, recordListBuff.toList)
  }

  def createSerializedRecord(baseSchema: Schema
                   , inputContent: Map[String, Any]
                   , outputFile: String):Unit = {
    val recordListBuff: ListBuffer[Record] = new ListBuffer()               
    if(inputMap.size > 0){
      inputMap.empty
    }
    inputMap = inputContent

    val aRecord = parseSchema(baseSchema)
    recordListBuff.addOne(aRecord.asInstanceOf[Record])
    outHandler.writeToFileSerialized(baseSchema, outputFile, recordListBuff.toList)
  }

  private def parseSchema(
                            schema: Schema, parentFieldName: String = ""): Any = {

    schema.getType match {
      case RECORD => {
        val fields: List[Schema.Field] = schema.getFields().asScala.toList
        val childRecord = new GenericData.Record(schema)

        fields.foreach(field => {
          var aSchemaField = None: Option[Schema.Field]
          aSchemaField = Some(field)
          if(aSchemaField.nonEmpty){
            var concatFieldName : String = ""
            if(parentFieldName.isEmpty){
              concatFieldName = field.name()
            } else {
              concatFieldName = parentFieldName.concat(".").concat(field.name())
            }
            if(defaultMap.contains(concatFieldName) && defaultMap.get(concatFieldName).get.equals("null")){
              childRecord.put(field.name(), null)
            } else {
              childRecord.put(field.name(), parseSchema(field.schema(), concatFieldName))
            }
          }
        })
        childRecord
      }
      case UNION => {
        var newRecord : Any = null
        val typeList = schema.getTypes().asScala.toList
        typeList.foreach( f = x => {
          var name = None: Option[Schema]
          name = Some(x)

          if (!name.get.isNullable) {
            var concatFieldName: String = ""

            if (parentFieldName.isEmpty) {
              concatFieldName = x.getName
            } else {
              concatFieldName = parentFieldName.concat(".").concat(x.getName)
            }
            newRecord = parseSchema(x, concatFieldName)
          }
        })
        newRecord
      }
      case BOOLEAN => {
        val key = getKey(parentFieldName)
        var booleanVal : Any = false
        if(runType.equalsIgnoreCase("_key_")){
          addKey(key, false)    
        } else {
          booleanVal = getFieldVal(key, false)
        }  
        if(null==booleanVal) null else booleanVal.toString().toBoolean
      }
      case STRING => {
        val key = getKey(parentFieldName)
        var strVal : Any = " "
        if(runType.equalsIgnoreCase("_key_")){
          addKey(key, "-NA-")    
        } else {
          strVal = getFieldVal(key, "-NA-")
        } 
        if(null==strVal) null else strVal.toString()
      }
      case INT => {
        val key = getKey(parentFieldName)
        var fieldVal: Any = 1
        if(runType.equalsIgnoreCase("_key_")){
          addKey(key, 0)    
        } else {
          fieldVal = getFieldVal(key, 0)
        }
        if(null==fieldVal) null else fieldVal.toString.toInt
      }
      case LONG => {
        val key = getKey(parentFieldName)
        var fieldVal : Any = 0
        if(runType.equalsIgnoreCase("_key_")){
          addKey(key, 0)    
        } else {
          fieldVal = getFieldVal(key, 0)
        } 
        if(null==fieldVal) null else fieldVal.toString.toLong
      }
      case DOUBLE => {
        val key = getKey(parentFieldName)
        var fieldVal : Any = 0.0
        if(runType.equalsIgnoreCase("_key_")){
          addKey(key, 0.0)    
        } else {
          fieldVal = getFieldVal(key, 0.0)
        } 
        if(null==fieldVal) null else fieldVal.toString.toDouble
      }
      case FLOAT => {
        val key = getKey(parentFieldName)
        var fieldVal : Any = 0.0
        if(runType.equalsIgnoreCase("_key_")){
          addKey(key, 0.0)    
        } else {
          fieldVal = getFieldVal(key, 0.0)
        } 
        if(null==fieldVal) null else fieldVal.toString.toFloat
      }
      case ENUM => {
        val key = getKey(parentFieldName)
        val defaultVal = schema.getEnumSymbols.get(0)
        var enumRecord : EnumSymbol = null
        if(runType.equalsIgnoreCase("_key_")){
          addKey(key, defaultVal)    
        } else {
          val fieldVal = getFieldVal(key, defaultVal)
          enumRecord = if(null==fieldVal) null else new EnumSymbol(schema, fieldVal)
        } 
        enumRecord
      }
      case ARRAY => {
        val key = getKey(parentFieldName)
        val elementType: Schema = schema.getElementType

        var arrRecord: Any = null
        if(elemTypes.contains(elementType.getType.toString.toUpperCase)) {
          if(runType.equalsIgnoreCase("_key_")){
            addKey(key, "ARRELEM")    
          } else {
            arrRecord = getFieldVal(key, "ARRELEM")
            if(arrRecord.!=(null)) {
              val fieldElem = arrRecord.toString.split('|')
              arrRecord = new GenericData.Array(schema, Arrays.asList(fieldElem: _*))
            }
          }  
        } else {
          val recordListBuffer: ListBuffer[Record] = new ListBuffer[Record]()
          val recordCount = getFieldVal(key, 1)
          if(runType.equalsIgnoreCase("_key_")){
            addKey(key, 1)    
          }
          if(recordCount!=null && recordCount.toString.toIntOption.ne(None)){
            val count = recordCount.toString.toIntOption
            for( count <- 0 to count.get-1){
              val recordKey = key.concat("["+count+"]")
              val aRecordElem = parseSchema(elementType, recordKey)
              if(aRecordElem.isInstanceOf[Record]){
                recordListBuffer.addOne(aRecordElem.asInstanceOf[Record])
              }
              
            }
            arrRecord = new GenericData.Array(schema, recordListBuffer.asJavaCollection)
          } else {
            arrRecord = null
          }
        }
        arrRecord
      }
      case MAP => {
        val key = getKey(parentFieldName)
        var fieldVal : Any = " "
        var mapRecord: mutable.Map[String, Any] = Map()
        if(runType.equalsIgnoreCase("_key_")){
          addKey(key, "KEY,VAL")    
        } else {
          fieldVal = getFieldVal(key, "KEY,VAL")
          if(fieldVal.toString.contains("|")) {
            val mapEntries = fieldVal.toString.split('|')
            val count = mapEntries.size
            for( count <- 0 to count-1){
              val anEntry = mapEntries(count)
              val aRecordElem = anEntry.toString.split(',')
              mapRecord+=(aRecordElem(0).toString() -> aRecordElem(1))
            }
          } else {
            val aRecordElem = fieldVal.toString.split(',')
            mapRecord+=(aRecordElem(0).toString() -> aRecordElem(1))
          }
        } 
        if(null==fieldVal) null else mapRecord.asJava
      }
      case BYTES => {
        val key = getKey(parentFieldName)
        var fieldVal: Option[Byte] = Some(0xa)
        var defaultVal : Any = 0xa
        if(runType.equalsIgnoreCase("_key_")){
          addKey(key, fieldVal)    
        } else {
          fieldVal = getFieldVal(key, defaultVal).toString.toByteOption
        }  
        logger.info("[[ "+fieldVal+" ]]")
        // fieldVal.get
        null
      }
      case _ => {
         logger.info("[[ Parse schema default case... ]]")
         logger.info("[[ Type: "+ schema.getType +", Name: " +schema.getName+ " ]]")
      }
    }
  }

  def getKey(parentFieldName: String): String = {
    val endIndex = parentFieldName.lastIndexOf('.')
    val startIndex = parentFieldName.lastIndexOf('.', endIndex-1)+1
    val key = parentFieldName.substring(0, endIndex)
    val fieldName = parentFieldName.substring(startIndex, endIndex)
    // logger.info("[[ "+key+" ]]")
    key
  }

  def getFieldVal(key: String, defaultVal: Any): Any = {
    var fieldVal: Any = defaultVal
    if(inputMap != null && inputMap.size>0) {      
      fieldVal = inputMap.getOrElse(key, defaultMap.getOrElse(key, defaultVal))
      if(fieldVal.toString.toLowerCase.equals("null")) fieldVal=null
    }  
    fieldVal
  }

  def addKey(key: String, defaultVal: Any): Any = {
    var fieldVal: Any = Nil
    fieldVal = defaultVal
    keyListBuilder.append(key).append("::").append(fieldVal).append("\n")  
  }
}

