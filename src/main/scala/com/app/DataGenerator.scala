package com.app

import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.io.File
import java.io.FileInputStream
import java.nio.file.Files
import java.nio.file.Paths
import java.util.Properties
import scala.App
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.io.Source

class DataGenerator()

object DataGenerator {  
    private val logger = LoggerFactory.getLogger(classOf[DataGenerator])
    private val outHandler: OutputHandler = new OutputHandler
    private val outputRecordsBuff: ListBuffer[Record] = new ListBuffer()

    def main(args: Array[String]): Unit = {
        val START_TIME = System.nanoTime()
        try {
            if(args.size > 0) {
                val runType = args(0)
                val configReader = new ConfigReader(new YAMLFactory())
                val confFilePath = "src/main/resources/config.yaml"
                val rootNode = configReader.readFromStream(new FileInputStream(new File(confFilePath)))
                val props = new Properties()
                rootNode.get("Input").fields.asScala foreach (entry => props.put(entry.getKey, entry.getValue.asText))

                val recordCreator = new RecordCreator()
                val schemaFile = props.get("schema.file").toString()
                val schemaContent = new String(Files.readAllBytes(Paths.get(schemaFile)))
                val baseSchema = new Schema.Parser().parse(schemaContent)
                if(runType.toLowerCase.equals("key")) {
                    val keyListFile = props.get("generated.key.file").toString()
                    val listKeys = recordCreator.createKeyList(baseSchema)
                    outHandler.writeStringToFile(keyListFile, listKeys)
                    logger.info("[[ Keyfile generated successfully... ]]")
                } else if(runType.toLowerCase.equals("data")) {
                    val defaultFile = props.get("default.file").toString()
                    val bufferedDefaultSrc = Source.fromFile(defaultFile)
                    val defaultBuffer : ListBuffer[String] = ListBuffer()
                    for (line <- bufferedDefaultSrc.getLines) {
                        defaultBuffer.addOne(line)
                    }

                    recordCreator.createDefaultRecord(defaultBuffer)
                    
                    val inputReader = new InputReader()
                    val inputValueList = inputReader.getDataFromInputFile(props)

                    val outputFile = props.get("output.file").toString()
                    val serialize = props.get("serialize.output").toString().toBoolean
                    
                    inputValueList.foreach(inputMap => {
                        recordCreator.createStringRecord(baseSchema, inputMap, outputFile)
                        if(serialize) {
                            val outputFileSer = props.get("output.file.serialized").toString()
                            recordCreator.createSerializedRecord(baseSchema, inputMap, outputFileSer)
                        }
                    })
                } else {
                    logger.error("[[ Invalid arguments provided. Acceptable arguments: data|key ]]")
                    logger.error("[[ Program Exiting. ]]")
                }
            } else {
                logger.error("[[ No arguments received. Please provide argument: data|key ]]")
                logger.error("[[ Program Exiting. ]]")
            }
        } catch {
            case e: Exception =>
            logger.error(e.toString)
        } finally {
            val END_TIME = System.nanoTime()

            val TIME_ELAPSED = (END_TIME - START_TIME) / 1e9d
            logger.info("[[ Time taken by the process: " + TIME_ELAPSED + " ]]")
        }
     
    }
}
