import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.*

import java.io.IOException
import java.time.LocalTime
import java.util
import scala.util.matching.Regex
import scala.jdk.CollectionConverters.*


// object for the first mapreduce task
// It shows the distribution of different types of messages across a predefined time interval
// containing injected string instances of the regex pattern
object MapReduceProgram1:
  val logger = CreateLogger(classOf[MainRunner.type])
  logger.info("Inside The First Job")
  // Mapper class
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]:
    logger.info("Inside Mapper Class of Job 1")
    private final val one = new IntWritable(1)
    private val word = new Text()
    // getting config values
    val applicationConf: Config = ConfigFactory.load("application.conf")
    // gets the start time mentioned in config, which is used to filter all the records in the log files
    val StartTime: LocalTime = LocalTime.parse(applicationConf.getString("randomLogGenerator.StartTime"))
    logger.info("Configured Start Time " + StartTime)
    // gets the end time mentioned in config, which is used to filter all the records in the log files
    val EndTime: LocalTime = LocalTime.parse(applicationConf.getString("randomLogGenerator.EndTime"))
    logger.info("Configured End Time " + EndTime)
    val Pattern: Regex = applicationConf.getString("randomLogGenerator.Pattern").r

    // will produce values like (DEBUG, 1) (DEBUG, 1) (ERROR, 1)
    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val line: String = value.toString
      val lst = line.split(" ")
      val FileTime = LocalTime.parse(lst(0)) // gets the time of record
      // checking if valid log record and whether time inside range and whether pattern exists
      if (lst(1).substring(0, 6) == "[scala" && Pattern.findAllIn(line.substring(81)).nonEmpty && FileTime.compareTo(StartTime) >= 0 && FileTime.compareTo(EndTime) <= 0)
        word.set(line.split(" ")(2)) // get the log level
        output.collect(word, one)


  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]:
    logger.info("Inside Reducer Class of Job 1")

    // will combine values like (DEBUG, {1, 1}) (ERROR, {1}) into (DEBUG, 2) (ERROR, 1) and write them to output
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get())) // gets the sum of the set
      output.collect(key,  new IntWritable(sum.get()))
