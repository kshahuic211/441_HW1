import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.*
import org.apache.hadoop.io.*
import org.apache.hadoop.util.*
import org.apache.hadoop.mapred.*

import java.io.IOException
import java.util
import scala.jdk.CollectionConverters.*
import com.typesafe.config.{Config, ConfigFactory}

// The class responsible for Configuring the MapReduce Jobs and running them
object MainRunner:
  // creating logger
  val logger = CreateLogger(classOf[MainRunner.type])
  logger.info("The Map Reduce HW program has started")
  


  // main method, calls all the four jobs one by one
  def main(args: Array[String]): Unit = {
    val applicationConf: Config = ConfigFactory.load("application.conf")
    val InputPath = if (args.length >= 2) args(0) else applicationConf.getString("randomLogGenerator.InputPath")
    val OutputPath = if (args.length >= 2) args(1) else applicationConf.getString("randomLogGenerator.OutputPath")
    JobRunner(1, InputPath, OutputPath)
    JobRunner(2, args(0), args(1))
    JobRunner(3, args(0), args(1))
    JobRunner(4, args(0), args(1))
  }

  // runs the specified job
  def JobRunner(JobNum: Int, inputPath: String, outputPath: String) =
    logger.info("In Job " + JobNum)
    // setting the classes according to JobNum
    val conf: JobConf = JobNum match {
      case 1 => new JobConf(MapReduceProgram1.getClass)
      case 2 => new JobConf(MapReduceProgram2.getClass)
      case 3 => new JobConf(MapReduceProgram3.getClass)
      case 4 => new JobConf(MapReduceProgram4.getClass)
    }
    conf.setJobName("MapReduce" + JobNum)
    conf.set("mapreduce.job.maps", "1")
    conf.set("mapreduce.job.reduces", "1")
    // changing output separator to commas
    conf.set("mapred.textoutputformat.separator", ", ")
    // setting output, input classes
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])
    // setting Map/Reduce classes according to JobNum
    JobNum match {
      case 1 => conf.setMapperClass(classOf[MapReduceProgram1.Map])
        conf.setCombinerClass(classOf[MapReduceProgram1.Reduce])
        conf.setReducerClass(classOf[MapReduceProgram1.Reduce])
      case 2 => conf.setMapperClass(classOf[MapReduceProgram2.Map1])
        conf.setCombinerClass(classOf[MapReduceProgram2.Reduce])
        conf.setReducerClass(classOf[MapReduceProgram2.Reduce])
      case 3 => conf.setMapperClass(classOf[MapReduceProgram3.Map])
        conf.setCombinerClass(classOf[MapReduceProgram3.Reduce])
        conf.setReducerClass(classOf[MapReduceProgram3.Reduce])
      case 4 => conf.setMapperClass(classOf[MapReduceProgram4.Map])
        conf.setCombinerClass(classOf[MapReduceProgram4.Reduce])
        conf.setReducerClass(classOf[MapReduceProgram4.Reduce])
    }
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    // handling case for Job 2
    if (JobNum != 2)
      FileOutputFormat.setOutputPath(conf, new Path(outputPath + "/" + JobNum.toString))
    else
      FileOutputFormat.setOutputPath(conf, new Path(outputPath + "/intermediate"))
    logger.info("Attempting to Run Job:  " + JobNum)
    JobClient.runJob(conf)

    // For Job 2, the task in divided into 2 tasks, the second part is executed below
    if (JobNum == 2)
      val conf2: JobConf = new JobConf(MapReduceProgram2_1.getClass)
      conf2.setJobName("MapReduce2_1")
      conf2.set("mapreduce.job.maps", "1")
      conf2.set("mapreduce.job.reduces", "1")
      conf2.set("mapred.textoutputformat.separator", ", ")
      conf2.setOutputKeyClass(classOf[IntWritable])
      conf2.setOutputValueClass(classOf[Text])
      conf2.setMapperClass(classOf[MapReduceProgram2_1.Map])
      conf2.setCombinerClass(classOf[MapReduceProgram2_1.Reduce])
      conf2.setReducerClass(classOf[MapReduceProgram2_1.Reduce])
      conf2.setInputFormat(classOf[TextInputFormat])
      conf2.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
      FileInputFormat.setInputPaths(conf2, new Path(outputPath + "/intermediate/"))
      FileOutputFormat.setOutputPath(conf2, new Path(outputPath + "/" + JobNum.toString))
      logger.info("Attempting to Run Job: 2_1")
      JobClient.runJob(conf2)

    logger.info("Finished the execution of all jobs")