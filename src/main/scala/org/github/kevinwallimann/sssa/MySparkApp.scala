/*
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.github.kevinwallimann.sssa

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming._

import java.time.LocalDate

object MySparkApp extends App {
  val parser = new scopt.OptionParser[Config]("MySparkApp") {
    head("MySparkApp", "1.0")
    help("help").text("Prints this help message.")

    opt[String]('c', "checkpoint")
      .required()
      .valueName("<path>")
      .action((value, config) => config.copy(checkpointPath = value))
      .text("Checkpoint path for the streaming query.")

    opt[String]('o', "output")
      .required()
      .valueName("<path>")
      .action((value, config) => config.copy(outputPath = value))
      .text("Output path for writing Parquet files.")

    opt[String]('i', "interval")
      .required()
      .valueName("<interval>")
      .action((value, config) => config.copy(processingInterval = value))
      .text("Processing time interval for triggering Parquet writes (e.g., '5 seconds', '1 minute').")
  }

  parser.parse(args, Config()) match {
    case Some(config) =>
      run(config.checkpointPath, config.outputPath, config.processingInterval)
    case None =>
      sys.exit(1)
  }

  def run(checkpointPath: String, outputPath: String, processingInterval: String): Unit = {
    val spark = SparkSession
      .builder
      .appName("ParquetStreamingApp")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val input = MemoryStream[Transaction](1, spark.sqlContext)


    // Configure the sink to write data in Parquet format
    val query = input.toDF()
      .writeStream
      .format("parquet")
      .option("checkpointLocation", checkpointPath)
      .option("path", outputPath)
      .outputMode("append")
      .partitionBy("date")
      .trigger(Trigger.ProcessingTime(processingInterval))
      .start()

    // Generate and add fake transaction data to MemoryStream
    var transactionId = 1
    while (true) {
      val categories = List("food", "electronics", "clothes", "travel", "utilities")
      for (category <- categories) {
        // Generate a transaction with a random amount between 1 and 1000
        val transaction = Transaction(s"t$transactionId", 1 + Math.random() * 1000, category, java.sql.Date.valueOf(LocalDate.now()))
        input.addData(transaction)
        transactionId += 1
      }
      Thread.sleep(500) // sleep for a second to simulate real streaming data
    }

    // Start the streaming query
    query.awaitTermination()
  }
}

