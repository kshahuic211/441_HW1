import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.*

import java.io.IOException
import java.time.LocalTime
import java.util
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters.*



// object for the second mapreduce task
// Part 1 of the second Job
// It will compute time intervals and the number of log messages 
// of the type ERROR with injected regex pattern string instances
object MapReduceProgram2:
  val logger = CreateLogger(classOf[MainRunner.type])
  logger.info("Inside The Second Job")
  // Mapper class
  class Map1 extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]:
    logger.info("Inside Mapper Class of Job 2")
    
    private final val one = new IntWritable(1)
    private final val zero = new IntWritable(0)
    private val word = new Text()
    val StartTime = LocalTime.parse("00:00:00.000")  // initially set of minimum time
    val applicationConf: Config = ConfigFactory.load("application.conf")
    val TimeInterval: Long = applicationConf.getLong("randomLogGenerator.TimeInterval")  // gets the time interval
    logger.info("Configured Interval " + TimeInterval)
    val Pattern = applicationConf.getString("randomLogGenerator.Pattern").r  // gets the pattern used to generate the log file
    logger.info("Pattern " + Pattern)


    // will produce values like (11:29:58.827 to 11:30:09.827, 1) (11:29:58.827 to 11:30:09.827, 1)
    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val line: String = value.toString
      val lst = line.split(" ")
      val FileTime = LocalTime.parse(lst(0))  // gets the time
      val slot = FileTime.toSecondOfDay / TimeInterval // used to find appropriate time bracket
      // setting word to the appropriate interval using slot * timeinterval seconds
      word.set(StartTime.plusSeconds(slot * TimeInterval).toString + " to " + StartTime.plusSeconds((slot + 1) * TimeInterval).toString)
      // checking if a match found
      if (lst(1).substring(0, 6) == "[scala" && lst(2) == "ERROR" && Pattern.findAllIn(line.substring(81)).nonEmpty)
          output.collect(word, one)
      else
          output.collect(word, zero)


  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]:
    logger.info("Inside Reducer Class of Job 2")
    // will combine values like (11:29:58.827 to 11:30:09.827, 1) (11:29:58.827 to 11:30:09.827, 1) 
    // into (11:29:58.827 to 11:30:09.827, 2) and write them to intermediate output
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val sum = values.asScala.map(value => value.get()).sum  // gets the sum
      output.collect(key, new IntWritable(sum))
