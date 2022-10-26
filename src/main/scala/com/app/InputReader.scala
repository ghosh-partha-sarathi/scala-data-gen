package com.app

import java.io.{File, FileInputStream}
import java.util
import java.util.Properties

import org.apache.poi.ss.usermodel.{Cell, Row}
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.slf4j.LoggerFactory

import scala.collection.mutable.{ArrayBuffer, ListBuffer, Map}

class InputReader {
  private val logger = LoggerFactory.getLogger(classOf[InputReader])

  def getDataFromInputFile(props: Properties): List[Map[String, Any]] = {
    val inputValueList: ListBuffer[Map[String, Any]] = ListBuffer()
    val fieldNamesBuffer: ArrayBuffer[String] = ArrayBuffer()
    val myFile = new File(props.getProperty("input.file"))
    val fis = new FileInputStream(myFile)
    val myWorkBook = new XSSFWorkbook(fis)
    val mySheet = myWorkBook.getSheet(props.getProperty("input.sheet"))
    logger.info("[[ No. of input rows: " +mySheet.getLastRowNum+ " ]]")
    val rowIterator = mySheet.iterator()
    while (rowIterator.hasNext()) {
      val row = rowIterator.next()
      val cellIterator = row.cellIterator()
      val colCount = row.getLastCellNum
      if(row.getRowNum.equals(0)){
        while (cellIterator.hasNext()) {
          val cell = cellIterator.next()
          fieldNamesBuffer.addOne(cell.getStringCellValue)
        }
      } else {
        val fieldNameArr = fieldNamesBuffer.toArray
        var fieldNameIndex: Int = -1
        val inputValueMap: Map[String, Any] = Map()
        var count=0
        for (count <- 0 until colCount) {
          val cell = row.getCell(count)
          fieldNameIndex+=1
          val fieldName = fieldNameArr(fieldNameIndex)
          if (cell != null) {
            cell.getCellType match {
              case Cell.CELL_TYPE_STRING => {
                inputValueMap.put(fieldName, cell.getStringCellValue)
              }
              case Cell.CELL_TYPE_NUMERIC => {
                inputValueMap.put(fieldName, cell.getNumericCellValue.toLong)
              }
              case Cell.CELL_TYPE_BOOLEAN => {
                inputValueMap.put(fieldName, cell.getBooleanCellValue)
              }
              case Cell.CELL_TYPE_BLANK => {
                inputValueMap.put(fieldName, "")
              }
            }
          } 
        }
        inputValueList.addOne(inputValueMap)
      }
    }
    inputValueList.toList
  }
}
