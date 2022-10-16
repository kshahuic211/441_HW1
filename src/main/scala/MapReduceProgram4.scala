import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.*

import java.io.IOException
import java.util
import scala.jdk.CollectionConverters.*


// object for the fourth mapreduce task
// It will produce the number of characters in each log message for each log message type that 
// contains the highest number of characters in the detected instances of the designated regex pattern
object MapReduceProgram4:
  val logger = CreateLogger(classOf[MainRunner.type])
  logger.info("Inside The Fourth Job")

  // Mapper class
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] :
    logger.info("Inside Mapper Class of Job 4")
    private val word = new Text()
    val applicationConf: Config = ConfigFactory.load("application.conf")
    val Pattern = applicationConf.getString("randomLogGenerator.Pattern").r
    logger.info("Pattern " + Pattern)

    // will produce values like (DEBUG, 4) (DEBUG, 3) (ERROR, 3) (ERROR, 5)
    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val line: String = value.toString
      val lst = line.split(" ")

      // checks if valid record
      if (lst(1).substring(0, 6) == "[scala")
        // uses the regex to find any matching patterns
        val pattern_instance = Pattern.findAllIn(line.substring(81))
        if (pattern_instance.nonEmpty)
          word.set(lst(2))
          output.collect(word, new IntWritable(pattern_instance.max.length)) // writes the length of the message


  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] :
    logger.info("Inside Reducer Class of Job 4")

    // will combine values like (DEBUG, {4, 3}) (ERROR, {3, 5}) into (DEBUG, 4) (ERROR, 5) and write them to output
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      // gets the max value for that log message type
      val max = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(if (valueOne.get() > valueTwo.get()) valueOne.get() else valueTwo.get()))
      output.collect(key, new IntWritable(max.get()))
