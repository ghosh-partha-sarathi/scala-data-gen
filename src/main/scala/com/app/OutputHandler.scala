package com.app

import java.io.{BufferedWriter, File, FileWriter}
import java.util.Properties

import com.fasterxml.jackson.databind.JsonNode
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{GenericData, GenericRecord, GenericDatumWriter}
import org.apache.avro.io.DatumWriter

import org.slf4j.LoggerFactory
import org.slf4j.Logger

import scala.util.Using
import scala.util.Using.Releasable;
import org.apache.avro.file.DataFileWriter

class OutputHandler {
  private val logger = LoggerFactory.getLogger(classOf[OutputHandler])

  def writeListToFile(file: String, recordList: List[Record]): Unit = {
    logger.info("[[ writeToFile method reached ]]")
    try {
      Using(new BufferedWriter(new FileWriter(file))) { fileWriter =>
        recordList.foreach(record => {
          fileWriter.write(record.toString)
          fileWriter.write("\n")
        })
      }
      logger.info("[[ Data written to output file successfully ]]")
    } catch {
      case exp: Exception =>
        logger.error(exp.getStackTrace().toString())
    }
  }

  def writeStringToFile(file: String, strContent: String): Unit = {
    try {
      Using(new BufferedWriter(new FileWriter(file))) { fileWriter =>
          fileWriter.write(strContent)
      }
    } catch {
      case exp: Exception =>
        logger.error(exp.getStackTrace().toString())
    }
  }

  def writeToFileSerialized(schema: Schema, outputFile: String, recordList: List[Record]): Unit = {
    logger.info("[[ writeToFileSerialized method called... ]]")
    var dataFileWriter: DataFileWriter[GenericRecord] = null
    try {
      val datumWriter = new GenericDatumWriter[GenericRecord](schema);
      dataFileWriter = new DataFileWriter[GenericRecord](datumWriter);
      dataFileWriter.create(schema, new File(outputFile));
      recordList.foreach(record => {
        dataFileWriter.append(record);
      })
      logger.info("[[ Records appended to file writer ]]")

      dataFileWriter.flush()
    } catch {
      case exp: Exception =>
        logger.error("[[ Exception Message: ", exp)
    } finally {
      if(null != dataFileWriter) dataFileWriter.close()
    }
  }
}
