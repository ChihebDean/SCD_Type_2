package com.databeans
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.databeans.Utils._

object AddressHistoryBuilder {
  def addressHistoryBuilder (history: DataFrame, update: DataFrame): DataFrame = {

    if (!compareIds(history, update))
    {
      val newUpdate = update
        .withColumn("update_moved_out", lit(null))
        .withColumn("update_moved_out",col("update_moved_out").cast("Date"))
        .withColumn("update_current", lit(true))
      val res = history.union(newUpdate)
      res
    }
    else if (compareIds(history, update) && !compareAddress(history, update))
    {
      if (compareDates(history, update, "moved_in", "update_moved_in").contains(false))
      {
        val newUpdate = update
          .withColumn("update_moved_out", lit(null))
          .withColumn("update_moved_out",col("update_moved_out").cast("Date"))
          .withColumn("update_current", lit(true))
        val joinCondition = history.col("id") === update.col("update_id")
        val newHistory = history.join(update, joinCondition, "inner")
        .select(col("id"),
          col("firstName"),
          col("lastName"),
          col("address"),
          col("moved_in"),
          col("update_moved_in" ).alias("moved_out"),
          col("current")).withColumn("current", lit(false))
        val res = newHistory.union(history).union(newUpdate).dropDuplicates("id", "address")
        res
      }
      else if(compareDates(history, update, "moved_in", "update_moved_in").contains(true)
      && !checkIfColumnExists(update, "update_moved_out"))
      {
        val joinCondition = history.col("id") === update.col("update_id")
        val newUpdate = update.join(history, joinCondition, "inner")
          .select(col("id"),
            col("firstName"),
            col("lastName"),
            col("update_address"),
            col("update_moved_in"),
            col("moved_in" ).alias("update_moved_out"),
            col("current")).withColumn("current", lit(false))
        val res = history.union(newUpdate)
        res
      }
      else
      {
        val newUpdate = update.withColumn("update_current", lit(false))
        val res = history.union(newUpdate)
        res
      }
    }
    else if (compareIds(history, update) && compareAddress(history, update))
    {
        if (compareDates(history, update, "moved_in", "update_moved_in").contains(true)
        && !checkIfColumnExists(update, "update_moved_out"))
         {
           val joinCondition = history.col("id") === update.col("update_id")
           val newUpdate = update.join(history, joinCondition, "inner")
             .select(col("id"),
               col("firstName"),
               col("lastName"),
               col("address"),
               col("update_moved_in").alias("moved_in"),
               col("moved_out" ),
               col("current"))
           val res = newUpdate.union(history).dropDuplicates("id", "address")
           res
         }
        else if (compareDates(history, update, "moved_in", "update_moved_in").contains(false))
          {
            history
          }
        else if (compareDates(history, update, "moved_in", "update_moved_in").isEmpty
        && compareDates(history, update, "moved_in", "update_moved_out").contains(false))
          {
            val joinCondition = history.col("id") === update.col("update_id")
            val newUpdate = update.join(history, joinCondition, "inner")
              .select(col("id"),
                col("firstName"),
                col("lastName"),
                col("address"),
                col("moved_in"),
                col("update_moved_out").alias("moved_out"),
                col("current")).withColumn("current", lit(false))
            val newHistory = history.join(update, joinCondition, "left_anti")
            val res = newHistory.union(newUpdate)
            res
          }
        else
          {
            val newUpdate = update.withColumn("update_current", lit(false))
            val res = history.union(newUpdate)
            res
          }
    }
    else
    {
      println("new case")
      history
    }
  }
}
