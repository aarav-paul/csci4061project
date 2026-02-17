# Programming Assignment 1 MapReduce  
CSCI 4061  
Intermediate Submission  

## Overview

This project builds a simplified MapReduce pipeline that processes a set of log files and counts how many requests come from each IP address. The system is organized into three main parts which are the main controller process, a group of mapper processes, and a group of reducer processes. These parts work together to split the workload, process the data in parallel, and then combine the results into a final output. The goal is to show how large log data can be handled efficiently using multiple processes instead of a single program doing all the work.

## Main Process

The main process is responsible for controlling the entire MapReduce workflow. It reads the command line arguments which include the log directory, the number of mappers, and the number of reducers. After reading the directory, it distributes the files evenly across the mapper processes. It then creates mapper child processes using fork and exec and waits for all of them to finish before continuing. Once mapping is complete, the main process divides the IP address key space among the reducer processes, starts the reducers, and again waits for them to finish. After all reducers complete successfully, the main process reads the reducer output files and prints the final aggregated results. This structure ensures the map stage fully finishes before the reduce stage begins.

## Mapper Design

Each mapper process receives an output file along with one or more input log files. The mapper reads the log files line by line so that memory usage stays low even if the logs are large. For every log entry, the mapper extracts the IP address and updates a hash table that keeps track of how many requests came from that address. After processing all assigned files, the mapper writes the contents of the hash table to an intermediate table file. Because multiple mappers run at the same time, the system can process many log files in parallel which improves overall performance.

## Reducer Design

Each reducer process reads all of the intermediate mapper table files and focuses only on the range of IP addresses assigned to it. The reducer aggregates the counts for IP addresses that fall within its range and stores the results in a new hash table. After finishing the aggregation, the reducer writes its results to an output table file. Since each reducer works on a different portion of the key space, the final outputs do not need any additional merging and can be printed directly by the main process.

## Data Flow

The overall data flow begins with raw input log files that are passed to the mapper processes. The mappers transform the raw logs into intermediate tables that contain request counts by IP address. These intermediate tables are then read by the reducer processes which combine counts within specific IP ranges. The reducers produce final output tables that represent the completed aggregation, and the main process prints these results. This structure allows large datasets to be split, processed in parallel, and recombined efficiently.

## Assumptions

The design assumes that the number of reducers will not exceed the number of mappers and that the number of processes will not exceed the number of available files. It also assumes that all directory entries being processed are valid log files and that the intermediate and output directories already exist before the program is run.

## Purpose of MapReduce

MapReduce simplifies large scale log processing by dividing the work into smaller independent tasks, running those tasks in parallel, and combining the results in structured stages. This approach avoids the limitations of a single process handling all data and makes the system more scalable and efficient when working with large amounts of information.

## Work Distribution



## AI Usage

AI tools were used only for small and trivial support tasks such as reviewing code changes, helping with Git related commands, and assisting with minor documentation wording. AI was not used to write project code, generate core logic, or complete the assignment itself. All implementation and design decisions were done by the student team.
