package mx.cic

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.scala._
import scala.io.Source
import scala.collection.mutable.ArrayBuffer
import java.io._
import java.util.Properties
/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram
 * over some sample data
 *
 * This example shows how to:
 *
 *   - write a simple Flink program.
 *   - use Tuple data types.
 *   - write and use user-defined functions.
 */
object WordCount {

  // set up the execution environment
  val env = ExecutionEnvironment.getExecutionEnvironment

  def main(args: Array[String]) {

    val file = new File("/Users/joseonofre/Documents/BigData/Modulo4/onofre_flink/src/main/output/output.txt")
    val bw = new BufferedWriter(new FileWriter(file))

      val DataFiles = getListFiles(args(1))
      println(DataFiles)

      //file1, file2, file3, #Files(3)
      bw.write(DataFiles.toString()+", " + DataFiles.length)
      bw.write("\n" + "*************"+"\n")

      //#totalWordsOfFile1, #totalWordsOfFile2, #totalWordsOfFile3
      for( files <- DataFiles) {
        println(files)
        val myFile2 = Totalperfile(files.toString)
        bw.write(myFile2+',')
      }
      bw.write("\n" + "*************" + "\n")

    //#TotalWordsRead(totalWordsOfFile1+totalWordsOfFile2+totalWordsOfFile3)
      val myFile3 = Totalfiles(args(1))
      println(myFile3)
      bw.write(myFile3)
      bw.write("\n" + "*************" + "\n")

      //nameFile1
      //word, value
      for( files <- DataFiles) {

        bw.write(files.toString+"\n")

        val myFile = wordsperfile(files.toString)

        bw.write(myFile+"\n")
      }
      bw.close()
  }


  def wordsperfile(path:String): String = {

    // read text file from local files system
    val localLines = env.readTextFile(path)

    val counts = localLines.flatMap { _.toLowerCase.split("\\W+") }
      .map { (_, 1) }
      .groupBy(0)
      .sum(1)

    //counts.writeAsText("/Users/joseonofre/Documents/BigData/Modulo4/onofre_flink/src/main/resources/output")

    return counts.collect().toString()

  }
  def Totalperfile(path:String): String = {

    // read text file from local files system
    val localLines = env.readTextFile(path)

    val counts = localLines.flatMap { _.toLowerCase.split("\\W+") }
      .map { (_, 1) }
      .sum(1)
    //  .KeyBy(0)
    //  .sum(1)

    return counts.collect().toString()

    //counts.toString

  }
  def getListFiles(directory: String): List[File] = {
    val d = new File(directory)
    if (d.exists && d.isDirectory) {//coun//counts.saveAsTextFile(salida)ts.saveAsTextFile(salida)
      d.listFiles.filter(_.isFile).toList
    }
    else List[File]()
  }

  def Totalfiles(path:String): String = {

    // read text file from local files system
    val localLines = env.readTextFile(path)

    val counts = localLines.flatMap { _.toLowerCase.split("\\W+") }
      .map { (_, 1) }
      .count()
    //  .KeyBy(0)
    //  .sum(1)

    return counts.intValue().toString

    //counts.toString

  }
  //def getListOfFiles(dir: String):List[File] = {
  //  val d = new File(dir)
  //  if (d.exists && d.isDirectory) {
  //    d.listFiles.filter(_.isFile).toList
  //  } else {
  //    List[File]()
  //  }
  //}

}

