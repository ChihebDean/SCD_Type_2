package com.databeans
import java.util.Date
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Utils {

  def compareIds(history: DataFrame, update: DataFrame): Boolean ={

    val joinCondition = history("id") === update("update_id")
    val joinedDF = history.join(update, joinCondition, "inner")
    val existsInBothDFs = joinedDF.count() > 0

    if (existsInBothDFs)
    {
      true
    }
    else
    {
      false
    }
  }
  def compareAddress(history: DataFrame, update: DataFrame): Boolean = {

    val joinCondition = history("id") === update("update_id")
    val joinedDF = history.join(update, joinCondition, "inner")
    val resultDf = joinedDF.withColumn("comparison_result", expr("update_address == address"))
    val allEqual = resultDf.agg(expr("count(case when comparison_result = true then 1 end) = count(*)")).collect()(0)(0)
    if (allEqual == true)
    {
      println("same address")
      true
    }
    else
    {
      println("new address")
      false
    }
  }
  def compareDates(history: DataFrame, update: DataFrame, historyDateColName: String, updateDateColName: String): Option[Boolean] ={

    val joinCondition = history("id") === update("update_id")
    val joinedDF = history.join(update, joinCondition, "inner")

    val historyMoveIn = joinedDF.select(col(historyDateColName)).first.getDate(0)
    val updateMovedIn = joinedDF.select(col(updateDateColName)).first.getDate(0)

    if (historyMoveIn.compareTo(updateMovedIn) > 0 )
    {
      println("early arriving date")
      Some(true)
    }
    else if (historyMoveIn.compareTo(updateMovedIn) == 0 ) { None }
    else
    {
      println("late arriving date")
      Some(false)
    }
  }
  def checkIfColumnExists(update: DataFrame, colNameToCheck: String): Boolean ={

    if (update.columns.contains(colNameToCheck)) {true}
    else {false}
  }
}
