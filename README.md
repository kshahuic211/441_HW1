441_HW1 Readme
Karan Shah kshah211@uic.edu 678212638

Youtube Video Link-


Instruction on how to run-
1) As mentioned in the description, it should take inputs via the configuration file. So, the input and output paths are specified in there. But the program can also optionally take two commandline arguments InputPath and OutputPath which will override the config setting.
2) The project runs with commands sbt compile run and sbt compile test
3) sbt compile run will runs all the 4 tasks on the specified input
4) The testoutput directory is mentioned in the config file
5) If the directory for testoutput or output is already there, then it will produce errors. So, they need to be deleted before running the program.

Project Description-
Important files- log, log_test, application.conf, logback.xml, CreateLogger, MainRunner, MapReduceProgram1, MapReduceProgram2, MapReduceProgram2_1, MapReduceProgram3, MapReduceProgram4, HW1Tester

- The MainRunner class contains the main method for running the tasks. It supplies the JobRunner method with the Job number and input and output files for all four tasks one after the another. JobRunner configures the MapReduce Job depending on the Job number. For example- it calls MapReduceProgram1 for the first task.
- The default input path is the log directory, which has 10000 log records divided into 21 files (500 lines each), created using the Pattern mentioned in the config file
- MapReduceProgram1- 
  -  Creates a file that shows the distribution of different types of messages across a predefined time interval (mentioned in the config file) containing      injected string instances of the regex pattern. 
  -  The Mapper in that class reads each line and produces values like (DEBUG, 1) (DEBUG, 1) (ERROR, 1), which are then combined in the Reducer that takes values like (DEBUG, {1, 1}) (ERROR, {1}) and outputs them like (DEBUG, 2) (ERROR, 1) by using the scala.reduce method to get the sum
 
- MapReduceProgram2 and MapReduceProgram2_2- These two classes work together to produce the results for task 2. 
  - MapReduceProgram2 computes time intervals (interval length mentioned in config) and the number of log messages of the type ERROR with injected regex pattern string instances in that interval. This output is not sorted, so MapReduceProgram2_2 takes the output and swaps the key and value to get the records in sorted order (-values to produce descending order). 
  - Ex- MapReduceProgram2 converts (11:29:58.827 to 11:30:09.827, 1) (11:29:58.827 to 11:30:09.827, 1) to into (11:29:58.827 to 11:30:09.827, 2) and MapReduceProgram2 converts this to (2, 11:29:58.827 to 11:30:09.827)

- MapReduceProgram3- Creates a file that shows for each message type, the number of the generated log messages.
  - The Mapper in that class reads each line and produces values like (DEBUG, 1) (DEBUG, 1) (ERROR, 1), which are then combined in the Reducer that takes values like (DEBUG, {1, 1}) (ERROR, {1}) and outputs them like (DEBUG, 2) (ERROR, 1) by using the scala.reduce method to get the sum

- MapReduceProgram4- Creates a file that shows the number of characters in each log message for each log message type that contains the highest number of characters in the detected instances of the designated regex pattern.
  - The Mapper in that class reads each line and produces values like (DEBUG, 4) (DEBUG, 3) (ERROR, 3) (ERROR, 5), which are then combined in the Reducer that takes values like (DEBUG, {4, 3}) (ERROR, {3, 5}) and turns them into (DEBUG, 4) (ERROR, 5) (max length of pattern instance by type) using the > operator to get the maximum sum
 
- The HW1Tester tests the four tasks output on a smaller log file with 10 entries found in log_test. It also tests the logic of computing the interval interval slot and Regex pattern matching.
- The config file contains settings for InputPath, OutputPath, timeInterval, stringlength, test I/O, Interval times for the first task and finally the log file pattern
