package com.databeans
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import java.sql.Date

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
  def compareDates(history: DataFrame, update: DataFrame, exp1: String, exp2: String): Option[Boolean] ={

    val joinCondition = history("id") === update("update_id")
    val joinedDF = history.join(update, joinCondition, "inner")
      .withColumn("is_early", expr(exp1))
      .withColumn("is_equal", expr(exp2))
    val rowsWhereColumnsNotEqual = joinedDF.filter(col("is_early") === true)
    val rowsWhereColumnsEqual = joinedDF.filter(col("is_equal") === true)

    if (rowsWhereColumnsNotEqual.count() > 0 && rowsWhereColumnsEqual.count() <= 0) {Some(true)}
    else if (rowsWhereColumnsNotEqual.count() <= 0 && rowsWhereColumnsEqual.count() > 0) {None}
    else {Some(false)}
  }
  def checkIfColumnExists(update: DataFrame, colNameToCheck: String): Boolean ={

    if (update.columns.contains(colNameToCheck)) {true}
    else {false}
  }
}
