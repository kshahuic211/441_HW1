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
- MapReduceProgram1- Creates a file that shows the distribution of different types of messages across a predefined time interval (mentioned in the config file) containing injected string instances of the regex pattern. It 
