import MapReduceProgram4.logger
import com.mifmif.common.regex.Generex
import org.scalatest.PrivateMethodTester
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.typesafe.config.{Config, ConfigFactory}

import java.io.IOException
import java.time.LocalTime
import scala.io.Source
import scala.language.deprecated.symbolLiterals


// code for basic testing of the MapReduce output on a small log file
class HW1Tester extends AnyFlatSpec with Matchers with PrivateMethodTester {
  val applicationConf: Config = ConfigFactory.load("application.conf")
  val TestInputPath = applicationConf.getString("randomLogGenerator.TestInputPath")
  val TestOutputPath = applicationConf.getString("randomLogGenerator.TestOutputPath")

  // run the jobs
  MainRunner.JobRunner(1, TestInputPath, TestOutputPath)
  MainRunner.JobRunner(2, TestInputPath, TestOutputPath)
  MainRunner.JobRunner(3, TestInputPath, TestOutputPath)
  MainRunner.JobRunner(4, TestInputPath, TestOutputPath)

  val lines1 = Source.fromFile(TestOutputPath + "/1/part-00000").getLines.toList
  val lines2 = Source.fromFile(TestOutputPath + "/2/part-00000").getLines.toList
  val lines3 = Source.fromFile(TestOutputPath + "/3/part-00000").getLines.toList
  val lines4 = Source.fromFile(TestOutputPath + "/4/part-00000").getLines.toList

  // Check Job 1 Num of Lines
  it should "check Job 1 Length" in {
    assert(lines1.length === (1))
  }

  // Check Job 1 Validity
  it should "check Job 1 Validity" in {
    val line = lines1(0).split(", ")
    assert(line.length === (2))
    assert(line(0) === ("ERROR"))
    assert(line(1) === ("1"))
  }

  // Check Job 2
  it should "check Job 2 Num of Lines" in {
    assert(lines2.length === (3))
  }

  // Check Job 2
  it should "check Job 2 Order" in {
    val line = lines2(0).split(", ")
    assert(line(0) === ("1"))
    assert(line(1) === ("11:30:04 to 11:30:15"))
  }


  // Check Job 3
  it should "check Job 3 Num Lines" in {
    assert(lines3.length === (4))
  }

  // Check Job 3
  it should "check Job 3" in {
    val line1 = lines3(0).split(", ")
    val line2 = lines3(1).split(", ")
    val line3 = lines3(2).split(", ")
    val line4 = lines3(3).split(", ")
    assert(line1(0) === ("DEBUG"))
    assert(line1(1) === ("1"))
    assert(line2(0) === ("ERROR"))
    assert(line2(1) === ("1"))
    assert(line3(0) === ("INFO"))
    assert(line3(1) === ("5"))
    assert(line4(0) === ("WARN"))
    assert(line4(1) === ("2"))
  }

  // Check Job 4
  it should "check Job 4 Num Lines" in {
    assert(lines4.length === (1))
  }

  // Check Job 4
  it should "check Job 4" in {
    val line = lines4(0).split(", ")
    assert(line(0) === ("ERROR"))
    assert(line(1) === ("24"))
  }

  // Check Time Slot Alg
  it should "check Time Slot Alg" in {
    val time1 = LocalTime.parse("11:12:10")  // i
    val time2 = LocalTime.parse("13:14:18")  // i
    val time3 = LocalTime.parse("10:04:02")  // i
    val slot1 = time1.toSecondOfDay / 15 // used to find appropriate time bracket
    val slot2 = time2.toSecondOfDay / 150 // used to find appropriate time bracket
    val slot3 = time3.toSecondOfDay / 50 // used to find appropriate time bracket
    assert(slot1 === (2688))
    assert(slot2 === (317))
    assert(slot3 === (724))
  }

  it should "check Pattern logic" in {
    val Pattern1 = "abcd".r
    val pattern_instance1 = Pattern1.findAllIn("aabcdefghi")
    val Pattern2 = "([a-c][e-g][0-3]|[A-Z][5-9][f-w]){5,15}".r
    val pattern_instance2 = Pattern2.findAllIn("ae2ae2A5wA5wbf2")
    assert(pattern_instance1.nonEmpty)
    assert(pattern_instance1.max.length === (4))
    assert(pattern_instance2.nonEmpty)
    assert(pattern_instance2.max.length === (15))
  }

}
