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

import org.apache.spark.graphx._

import org.rogach.scallop._

import java.text.SimpleDateFormat


class MyConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, topk)
  val input = opt[String](descr = "input path", required = true)
  val topk = opt[String](descr = "Print top k nodes", required = true)
  verify()
}

object PageRank {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new MyConf(argv)

    val topk = args.topk().toInt
    log.info("Input: " + args.input())
    log.info("Top-K: " + args.topk())

    val conf = new SparkConf().setAppName("PageRank")
    val sc = new SparkContext(conf)

    val df = sc.textFile(args.input()).filter(line => !line.startsWith("Date"))
    val adj_list = df.filter(line => line.length > 0)
    .map(line => {
      val row = line.split(",")
      val tup = List[String](row(5), row(6)).sorted

      ((tup(0), tup(1)), 1)
    }).reduceByKey(_+_)

    val users = adj_list.flatMap(edge => List(
      (edge._1._1, 1), (edge._1._2, 1))
    ).reduceByKey(_+_)
    .map(tup => (tup._1.hashCode.toLong, tup._1))

    val edges = adj_list.map(edge => 
      Edge(edge._1._1.hashCode.toLong, edge._1._2.hashCode.toLong, edge._2))

    val g = Graph(users, edges)

    val ranks = g.pageRank(0.0001).vertices

    val ranksByUsername = users.join(ranks).map({
          case (id, (username, rank)) => (username, rank)
        }).sortBy(tup => tup._2, ascending=false)

    // Print the result
    println(ranksByUsername.take(topk).map(tup => "%s,%f".format(tup._1, tup._2)).mkString("\n"))
  }
}
