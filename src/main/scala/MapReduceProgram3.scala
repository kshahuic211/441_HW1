import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.*

import java.io.IOException
import java.util
import scala.jdk.CollectionConverters.*


// object for the third mapreduce task
// similar to WordCount
// For each message type it will produce the number of the generated log messages
object MapReduceProgram3:
  val logger = CreateLogger(classOf[MainRunner.type])
  logger.info("Inside The Third Job")
  // Mapper class
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]:
    logger.info("Inside Mapper Class of Job 3")

    private final val one = new IntWritable(1)
    private val word = new Text()

    // will produce values like (DEBUG, 1) (DEBUG,1) (ERROR, 1)
    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val line: String = value.toString
      val lst = line.split(" ")
      // checking if valid log entry
      if (lst(1).substring(0, 6) == "[scala")
        word.set(line.split(" ")(2))
        output.collect(word, one)


  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]:
    logger.info("Inside Reducer Class of Job 3")

    // will combine values like (DEBUG, {1, 1}) (ERROR, {1}) into (DEBUG, 2) (ERROR, 1) and write them to output
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key, new IntWritable(sum.get()))
