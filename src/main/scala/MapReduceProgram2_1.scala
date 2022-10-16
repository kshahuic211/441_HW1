import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.*

import java.io.IOException
import java.util

object MapReduceProgram2_1:
  val logger = CreateLogger(classOf[MainRunner.type])
  logger.info("Inside The Second_One Job")

  // Mapper class
  class Map extends MapReduceBase with Mapper[LongWritable, Text, IntWritable, Text] :
    private val word = new Text()

    // will produce values like (-3, 11:29:58.827 to 11:30:09.827) (-2, 11:30:09.827 to 11:30:20.827)
    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[IntWritable, Text], reporter: Reporter): Unit =
      val line: String = value.toString
      val lst = line.split(", ")
      word.set(lst(0)) // i.e time interval
      // here the key is the number of messages and value is time interval
      // it is flipped due the limitation of reducer output type
      // the number is made negative to ensure descending order
      output.collect(new IntWritable(-1 * lst(1).toInt), word)


  class Reduce extends MapReduceBase with Reducer[IntWritable, Text, IntWritable, Text] :
    logger.info("Inside Reducer Class of Job 2_1")

    // will combine values like (-3, 11:29:58.827 to 11:30:09.827) (-2, 11:30:09.827 to 11:30:20.827)
    // into (3, 11:29:58.827 to 11:30:09.827) (2, 11:30:09.827 to 11:30:20.827) and write them to the output
    override def reduce(key: IntWritable, values: util.Iterator[Text], output: OutputCollector[IntWritable, Text], reporter: Reporter): Unit =
      values.forEachRemaining(value => output.collect(new IntWritable(Math.abs(key.get())), value))
