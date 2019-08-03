/**
  * Bespin: reference implementations of "big data" algorithms
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package edu.umd.analytics.mr.spark

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

import java.text.SimpleDateFormat


class Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  verify()
}

object IpTimeline {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("IpTimeline")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args.input())

    val dateParser = new SimpleDateFormat("dd/MMM/yyyy HH:mm:ss")

    val timed_tups = textFile
      .filter(line => line.length > 0 && !line.startsWith("Date/time"))  // Remove headers
      .map(line => {  // Parse the date/time in each line, flattening seconds
        val d = dateParser.parse(line.split(",")(0))
        d.setSeconds(0)

        (d.getTime, 1)
      }).reduceByKey((l,r) => l+r)  // sum the number of times a time appears
      .sortBy(tup => tup._1)  // sort output
      .map(tup => "%d\t%d".format(tup._1, tup._2))  // convert to string
      .saveAsTextFile(args.output())  // save to file
  }
}
