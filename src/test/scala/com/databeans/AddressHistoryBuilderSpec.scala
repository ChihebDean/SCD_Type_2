package com.databeans
import java.sql.Date
import com.databeans.AddressHistoryBuilder.addressHistoryBuilder
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class History(id: Long, firstName: String, lastName: String, address: String, moved_in: Date , moved_out: Date, current: Boolean)
case class Update(update_id: Long, update_firstName: String, update_lastName: String, update_address: String, update_moved_in: Date, update_moved_out: Date, update_current: Boolean)

class AddressHistoryBuilderSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("AddressHistoryBuilder_Test")
    .getOrCreate()
  import spark.implicits._
  implicit def date(str: String): Date = Date.valueOf(str)

  "addressHistoryBuilder"should "add new id to history" in {
    Given("The history and update DataFrames")
    val history = Seq(
      (5, "Bilel", "Ben Amor", "Sfax", date("2020-12-10"), null, true),
      (1, "Mohsen", "Abidi", "Sousse", date("1995-12-23"), date("2000-12-12"), false)
    ).toDF("id", "firstName", "lastName", "address", "moved_in", "moved_out", "current")
    val update = Seq(
      (3, "Chi", "Bou", "Sousse", date("2020-05-05"))
    ).toDF("update_id", "update_firstName", "update_lastName", "update_address", "update_moved_in")
    When("addressHistoryBuilder is invoked")
    val updatedResult = addressHistoryBuilder(history, update)
    Then("a row of new history data should be added")
    val expectedResult = Seq(
      (5, "Bilel", "Ben Amor", "Sfax", date("2020-12-10"), null, true),
      (1, "Mohsen", "Abidi", "Sousse", date("1995-12-23"), date("2000-12-12"), false),
      (3, "Chi", "Bou", "Sousse", date("2020-05-05"), null, true)
    ).toDF("id", "firstName", "lastName", "address", "moved_in", "moved_out", "current")
    expectedResult.collect() should contain theSameElementsAs(updatedResult.collect())
  }
  "addressHistoryBuilder"should "update existing person's data and add his new address" in {
    Given("The history and the update DataFrames")
    val history = Seq(
      (5, "Bilel", "Ben Amor", "Sfax", date("2020-12-01"), null, true),
      (1, "Mohsen", "Abidi", "Sousse", date("1995-12-23"), date("2000-12-12"), false)
    ).toDF("id", "firstName", "lastName", "address", "moved_in", "moved_out", "current")
    val update = Seq(
      (5, "Bilel", "Ben Amor", "Sousse", date("2022-05-05"))
    ).toDF("update_id", "update_firstName", "update_lastName", "update_address", "update_moved_in")
    When("addressHistoryBuilder is invoked")
    val updatedResult = addressHistoryBuilder(history, update)
    Then("existing person's data should be updated, and a row of person's new address should be added")
    val expectedResult = Seq(
      (5, "Bilel", "Ben Amor", "Sfax", date("2020-12-01"), date("2022-05-05"), false),
      (1, "Mohsen", "Abidi", "Sousse", date("1995-12-23"), date("2000-12-12"), false),
      (5, "Bilel", "Ben Amor", "Sousse", date("2022-05-05"), null, true)
    ).toDF("id", "firstName", "lastName", "address", "moved_in", "moved_out", "current")
    expectedResult.collect() should contain theSameElementsAs(updatedResult.collect())
  }
  "addressHistoryBuilder"should "add a person with new address" in {
    Given("The history and the update DataFrames")
    val history = Seq(
      (5, "Bilel", "Ben Amor", "Sfax", date("2020-12-01"), null, true),
      (1, "Mohsen", "Abidi", "Sousse", date("1995-12-23"), date("2000-12-12"), false)
    ).toDF("id", "firstName", "lastName", "address", "moved_in", "moved_out", "current")
    val update = Seq(
      (5, "Bilel", "Ben Amor", "Sousse", date("2015-05-05"))
    ).toDF("update_id", "update_firstName", "update_lastName", "update_address", "update_moved_in")
    When("addressHistoryBuilder is invoked")
    val updatedResult = addressHistoryBuilder(history, update)
    Then("a row holding an existing person's new address and leaving date should be added")
    val expectedResult = Seq(
      (5, "Bilel", "Ben Amor", "Sfax", date("2020-12-01"), null, true),
      (1, "Mohsen", "Abidi", "Sousse", date("1995-12-23"), date("2000-12-12"), false),
      (5, "Bilel", "Ben Amor", "Sousse", date("2015-05-05"), date("2020-12-01"), false)
    ).toDF("id", "firstName", "lastName", "address", "moved_in", "moved_out", "current")
    expectedResult.collect() should contain theSameElementsAs(updatedResult.collect())
  }
  "addressHistoryBuilder"should "update the existing peron's arriving date" in {
    Given("The history and update DataFrames")
    val history = Seq(
      (5, "Bilel", "Ben Amor", "Sfax", date("2020-12-10"), null, true),
      (1, "Mohsen", "Abidi", "Sousse", date("1995-12-23"), date("2000-12-12"), false)
    ).toDF("id", "firstName", "lastName", "address", "moved_in", "moved_out", "current")
    val update = Seq(
      (5, "Bilel", "Ben Amor", "Sfax", date("2015-12-12"))
    ).toDF("update_id", "update_firstName", "update_lastName", "update_address", "update_moved_in")
    When("addressHistoryBuilder is invoked")
    val updatedResult = addressHistoryBuilder(history, update)
    Then("the arriving data should be modified to an earlier arriving date")
    val expectedResult = Seq(
      (5, "Bilel", "Ben Amor", "Sfax", date("2015-12-12"), null, true),
      (1, "Mohsen", "Abidi", "Sousse", date("1995-12-23"), date("2000-12-12"), false)
    ).toDF("id", "firstName", "lastName", "address", "moved_in", "moved_out", "current")
    expectedResult.collect() should contain theSameElementsAs(updatedResult.collect())
  }
  "addressHistoryBuilder"should "change nothing" in{
    Given("The history and update DataFrames")
    val history = Seq(
      (5, "Bilel", "Ben Amor", "Sfax", date("2020-12-10"), null, true),
      (1, "Mohsen", "Abidi", "Sousse", date("1995-12-23"), date("2000-12-12"), false)
    ).toDF("id", "firstName", "lastName", "address", "moved_in", "moved_out", "current")
    val update = Seq(
      (5, "Bilel", "Ben Amor", "Sfax", date("2023-01-01"))
    ).toDF("update_id", "update_firstName", "update_lastName", "update_address", "update_moved_in")
    When("addressHistoryBuilder is invoked")
    val updatedResult = addressHistoryBuilder(history, update)
    Then("nothing changes")
    val expectedResult = Seq(
      (5, "Bilel", "Ben Amor", "Sfax", date("2020-12-10"), null, true),
      (1, "Mohsen", "Abidi", "Sousse", date("1995-12-23"), date("2000-12-12"), false)
    ).toDF("id", "firstName", "lastName", "address", "moved_in", "moved_out", "current")
    expectedResult.collect() should contain theSameElementsAs(updatedResult.collect())
  }
  "addressHistoryBuilder"should "update the existing peron's leaving date" in {
    Given("The history and update DataFrames")
    val history = Seq(
      (5, "Bilel", "Ben Amor", "Sfax", date("2020-12-10"), null, true),
      (1, "Mohsen", "Abidi", "Sousse", date("1995-12-23"), date("2000-12-12"), false)
    ).toDF("id", "firstName", "lastName", "address", "moved_in", "moved_out", "current")
    val update = Seq(
      (5, "Bilel", "Ben Amor", "Sfax", date("2020-12-10"), date("2022-02-05"))
    ).toDF("update_id", "update_firstName", "update_lastName", "update_address", "update_moved_in", "update_moved_out")
    When("addressHistoryBuilder is invoked")
    val updatedResult = addressHistoryBuilder(history, update)
    Then("the leaving date should be added to the history")
    val expectedResult = Seq(
      (5, "Bilel", "Ben Amor", "Sfax", date("2020-12-10"), date("2022-02-05"), false),
      (1, "Mohsen", "Abidi", "Sousse", date("1995-12-23"), date("2000-12-12"), false)
    ).toDF("id", "firstName", "lastName", "address", "moved_in", "moved_out", "current")
    expectedResult.collect() should contain theSameElementsAs(updatedResult.collect())
  }
  "addressHistoryBuilder"should "add an existing person with new leaving date" in{
    Given("The history and update DataFrames")
    val history = Seq(
      (5, "Bilel", "Ben Amor", "Sfax", date("2020-12-10"), null, true),
      (1, "Mohsen", "Abidi", "Sousse", date("1995-12-23"), date("2000-12-12"), false)
    ).toDF("id", "firstName", "lastName", "address", "moved_in", "moved_out", "current")
    val update = Seq(
      (5, "Bilel", "Ben Amor", "Sfax", date("2010-04-05"), date("2018-02-05"))
    ).toDF("update_id", "update_firstName", "update_lastName", "update_address", "update_moved_in", "update_moved_out")
    When("addressHistoryBuilder is invoked")
    val updatedResult = addressHistoryBuilder(history, update)
    Then("a row holding an existing person's leaving date should be added")
    val expectedResult = Seq(
      (5, "Bilel", "Ben Amor", "Sfax", date("2020-12-10"), null, true),
      (1, "Mohsen", "Abidi", "Sousse", date("1995-12-23"), date("2000-12-12"), false),
      (5, "Bilel", "Ben Amor", "Sfax", date("2010-04-05"), date("2018-02-05"), false)
    ).toDF("id", "firstName", "lastName", "address", "moved_in", "moved_out", "current")
    expectedResult.collect() should contain theSameElementsAs(updatedResult.collect())
  }
  "addressHistoryBuilder"should "add an existing person with a new address and a new leaving date" in{
    Given("The history and update DataFrames")
    val history = Seq(
      (5, "Bilel", "Ben Amor", "Sfax", date("2020-12-10"), null, true),
      (1, "Mohsen", "Abidi", "Sousse", date("1995-12-23"), date("2000-12-12"), false)
    ).toDF("id", "firstName", "lastName", "address", "moved_in", "moved_out", "current")
    val update = Seq(
      (5, "Bilel", "Ben Amor", "Sousse", date("2010-04-05"), date("2018-02-05"))
    ).toDF("update_id", "update_firstName", "update_lastName", "update_address", "update_moved_in", "update_moved_out")
    When("addressHistoryBuilder is invoked")
    val updatedResult = addressHistoryBuilder(history, update)
    Then("a row holding an existing person should be added with an updated current status")
    val expectedResult = Seq(
      (5, "Bilel", "Ben Amor", "Sfax", date("2020-12-10"), null, true),
      (1, "Mohsen", "Abidi", "Sousse", date("1995-12-23"), date("2000-12-12"), false),
      (5, "Bilel", "Ben Amor", "Sousse", date("2010-04-05"), date("2018-02-05"), false)
    ).toDF("id", "firstName", "lastName", "address", "moved_in", "moved_out", "current")
    expectedResult.collect() should contain theSameElementsAs(updatedResult.collect())
  }
}