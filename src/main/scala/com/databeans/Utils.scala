package com.databeans
import java.util.Date
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Utils {

  def compareIds(history: DataFrame, update: DataFrame): Boolean ={

    val joinCondition = history("id") === update("update_id")
    val joinedDF = history.join(update, joinCondition, "inner")

    if (joinedDF.count() > 0)
    {
      true
    }
    else
    {
      false
    }
  }
  def compareAddress(history: DataFrame, update: DataFrame): Boolean = {

    val joinCondition = history("id") === update("update_id") && history("address") === update("update_address")
    val joinedDF = history.join(update, joinCondition, "inner")

    if (joinedDF.count() > 0)
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
    val updateMovedIn = joinedDF.select(col(updateDateColName)).first.getDate(0) //filter

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
