package com.seq.stat

import org.apache.spark.sql.SparkSession
import java.text.DecimalFormat

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.HashPartitioner


/**
  * A Main to run
  */
object MyRouteMain {
  val nf = new DecimalFormat
  nf.setMaximumIntegerDigits(4)
  nf.setMinimumIntegerDigits(4)
  nf.setDecimalSeparatorAlwaysShown(false)
  nf.setGroupingSize(0)
  case class CommandLineArgs(
                              rootPath: String = "", // required
                              inputDate: Seq[String] = Seq(), // required
                              groupNums: Int = 10, //required default is 10
                              groupLpad: Int = 4,
                              hasTitle: Boolean = false,
                              verbose: Boolean = false,
                              debug: Boolean = false,
                              checkCount: Boolean = false,
                              clear: Boolean = false
                            )


  def main(args: Array[String]) {

    val parser = new scopt.OptionParser[CommandLineArgs]("MyRouteMain") {
      head("MyRouteMain", "1.0")
      opt[String]('r', "rootPath").required().action((x, c) =>
        c.copy(rootPath = x)).text("rootPath is required")

      opt[Int]('n', "nums").valueName("<nums>").
        action((x, c) => c.copy(groupNums = x)).
        text("nums is a required group property")

      opt[Int]('l', "lpad").valueName("<lpad>").
        action((x, c) => {
          nf.setMaximumIntegerDigits(x)
          nf.setMinimumIntegerDigits(x)
          c.copy(groupLpad = x)
        }
        ).
        text("ldap is a required group property")

      opt[Seq[String]]('d', "dates").required().valueName("<date1>,<date2>...").action((x, c) =>
        c.copy(inputDate = x)).text("dates to include")

      opt[Boolean]('t', "hasTitle").action((x, c) =>
        c.copy(hasTitle = x)).text("default is false")

      opt[Boolean]('v', "verify").action((x, c) =>
        c.copy(checkCount = x)).text("check the result")

      opt[Boolean]('c', "clear").action((x, c) =>
        c.copy(clear = x)).text("clean up result paths")

      opt[Unit]("verbose").action((_, c) =>
        c.copy(verbose = true)).text("verbose is a flag")

      opt[Unit]("debug").hidden().action((_, c) =>
        c.copy(debug = true)).text("this option is hidden in the usage text")
      help("help").text("prints this usage text")
    }

    // parser.parse returns Option[C]
    parser.parse(args, CommandLineArgs()) match {
      case Some(c@CommandLineArgs(_,_,_,_,_,_,_,_,true)) =>
        val configuration = new Configuration()
        val fileSystem = FileSystem.get(configuration)
        c.inputDate.map(x => new Path(c.rootPath, "TRAFF_" + x)).map(p => fileSystem.listStatus(p).filter(l =>l.getPath.getName.equals("Group")).map(a => fileSystem.delete(a.getPath,true)))

      case Some(c@CommandLineArgs(_,_,_,_,_,_,_,true,_)) =>
        val spark = SparkSession.builder()
          .appName("RouteMain").config("spark.hadoop.validateOutputSpecs", false)
          .getOrCreate()
        val sc = spark.sparkContext
        val fileSystem = FileSystem.get(sc.hadoopConfiguration)
        val sourceFiles = c.inputDate.map(x => new Path(c.rootPath, "TRAFF_" + x)).map(p => fileSystem.listStatus(p).filter(l => !l.getPath.getName.equals("Group")).map(a => a.getPath.toString)).flatten
        val processRdds = sourceFiles.map(f => sc.textFile(f))
        val processRdd = sc.union(processRdds)
        val srouceCount = processRdd.count()

        val resultFiles = c.inputDate.map(x => new Path(c.rootPath, "TRAFF_" + x)).map(p => fileSystem.listStatus(new Path(p,"Group")).filter(a => a.getPath.getName.length == c.groupLpad).map(a => fileSystem.listStatus(a.getPath)).flatten).flatten
        val process2Rdds = resultFiles.map(a => a.getPath.toString).map(f => sc.textFile(f))
        val process2Rdd = sc.union(process2Rdds)
        val resultCount = process2Rdd.count()

        (srouceCount == resultCount) match {
          case true =>
            val msg = "srouce and result count is %s"
            println(String.format(msg,srouceCount.toString))
          case false =>
            val msg = "srouce count is %s, but result count is %s"
            throw new Exception(String.format(msg,srouceCount.toString,resultCount.toString))
        }

        spark.stop()
      case Some(commandLineArgs) =>
        // do stuff
        val spark = SparkSession.builder()
          .appName("RouteMain").config("spark.hadoop.validateOutputSpecs", false)
          .getOrCreate()
        val sc = spark.sparkContext
        val fileSystem = FileSystem.get(sc.hadoopConfiguration)
        val pathToFile = commandLineArgs.inputDate.map(x => new Path(commandLineArgs.rootPath, "TRAFF_" + x)).map(p => fileSystem.listStatus(p).filter(l => !l.getPath.getName.equals("Group")).map(a => (a.getPath.getParent.toString, a.getPath.toString))).flatten
        val processRdds = pathToFile.map(a => sc.textFile(a._2).mapPartitionsWithIndex((idx, iter) => if (commandLineArgs.hasTitle && (idx == 0)) iter.drop(1) else iter).map(p => (diyHexToDec(p, commandLineArgs.groupNums), (a._1, new Path(a._2).getName, p))))
        val processRdd = sc.union(processRdds)
        processRdd.partitionBy(new HashPartitioner(commandLineArgs.groupNums)).saveAsHadoopFile("/tmp", classOf[String], classOf[String],
          classOf[RDDMultipleTextOutputFormat])
        spark.stop()
      case None =>
      // arguments are bad, error message will have been displayed
    }

  }

  def diyHexToDec(string: String, num: Int): String = {
    val number = Integer.valueOf(string.split(",")(0).substring(string.split(",")(0).length - 6, string.split(",")(0).length), 16) & num
    nf.format(number)
  }
}

