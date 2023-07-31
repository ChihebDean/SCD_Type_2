package com.databeans
import java.sql.Date
import com.databeans.Utils._
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class HistoryUtils(id: Long, firstName: String, lastName: String, address: String, moved_in: Date , moved_out: Date, current: Boolean)
case class UpdateUtils(id: Long, firstName: String, lastName: String, address: String, moved_in: Date, moved_out: Date, current: Boolean)

class UtilsSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Utils_Test")
    .getOrCreate()
  import spark.implicits._
  implicit def date(str: String): Date = Date.valueOf(str)

  "compareId"should "check if the update id exists in the history" in {
    Given("The history and update DataFrames")
    val history = Seq(
      (5, "Bilel", "Ben Amor", "Sfax", date("2020-12-10"), null, true),
      (1, "Mohsen", "Abidi", "Sousse", date("1995-12-23"), date("2000-12-12"), false)
    ).toDF("id", "firstName", "lastName", "address", "moved_in", "moved_out", "current")
    val update = Seq(
      (3, "Chi", "Bou", "Sousse", date("2020-05-05"))
    ).toDF("update_id", "update_firstName", "update_lastName", "update_address", "update_moved_in")
    When("compareId is invoked")
    val Result = compareIds(history, update)
    Then("false")
    val expectedResult = false
    Seq(expectedResult).toDF.collect() should contain theSameElementsAs(Seq(Result).toDF.collect())
  }
  "compareDates"should "compare the history and the update arriving dates" in {
    Given("The history and update DataFrames, and column names")
    val history = Seq(
      (5, "Bilel", "Ben Amor", "Sfax", date("2020-12-10"), null, true),
      (1, "Mohsen", "Abidi", "Sousse", date("1995-12-23"), date("2000-12-12"), false)
    ).toDF("id", "firstName", "lastName", "address", "moved_in", "moved_out", "current")
    val update = Seq(
      (5, "Bilel", "Ben Amor", "Sousse", date("2022-10-10"))
    ).toDF("update_id", "update_firstName", "update_lastName", "update_address", "update_moved_in")
    val historyDateColName = "moved_in"
    val updateDateColName = "update_moved_in"
    When("compareDates is invoked")
    val Result = compareDates(history, update, historyDateColName, updateDateColName)
    Then("if it's a late arriving date the value false should be returned")
    val expectedResult = false
    Seq(expectedResult).toDF.collect() should contain theSameElementsAs(Seq(Result).toDF.collect())
  }
  "compareAddress"should "check if an existing person is having a new address" in {
    Given("The history and update DataFrames")
    val history = Seq(
      (5, "Bilel", "Ben Amor", "Sfax", date("2020-12-10"), null, true),
      (1, "Mohsen", "Abidi", "Sousse", date("1995-12-23"), date("2000-12-12"), false)
    ).toDF("id", "firstName", "lastName", "address", "moved_in", "moved_out", "current")
    val update = Seq(
      (5, "Bilel", "Ben Amor", "Sousse")
    ).toDF("update_id", "update_firstName", "update_lastName", "update_address")
    When("compareAddress is invoked")
    val Result = compareAddress(history, update)
    Then("true if the person has the same address and false if he has a new address")
    val expectedResult = false
    Seq(expectedResult).toDF.collect() should contain theSameElementsAs (Seq(Result).toDF.collect())
  }
  "checkIfColumnExists" should "check whether a column exists or not" in {
    Given("The update Dataframe and the column name")
    val update = Seq(
      (5, "Bilel", "Ben Amor", "Sousse")
    ).toDF("update_id", "update_firstName", "update_lastName", "update_address")
    val colName = "update_moved_out"
    When("checkIfColumnExists is invoked")
    val Result = checkIfColumnExists(update, colName)
    Then("true if the column exists and false if not")
    val expectedResult = false
    Seq(expectedResult).toDF.collect() should contain theSameElementsAs (Seq(Result).toDF.collect())
  }
}