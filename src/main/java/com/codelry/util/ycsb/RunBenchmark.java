package com.codelry.util.ycsb;

import org.apache.commons.cli.*;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.help.HelpFormatter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class RunBenchmark {

  private static final Logger logger = LoggerFactory.getLogger(RunBenchmark.class);

  public static void main(String[] args) {
    Options options = new Options();
    CommandLine cmd = null;

    logger.info("Starting YCSB benchmark");

    Option workload = new Option("w", "workload", true, "workload scenario");
    workload.setRequired(false);
    options.addOption(workload);

    CommandLineParser parser = new DefaultParser();
    HelpFormatter.Builder formatterBuilder = HelpFormatter.builder();
    HelpFormatter formatter = formatterBuilder.get();

    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      String header = "Run a YCSB benchmark scenario.";
      String footer = "If a workload scenario is not specified, all scenarios will be run.";
      try {
        formatter.printHelp("RunBenchmark", header, options, footer, true);
      } catch (IOException printHelpError) {
        logger.error("Error printing CLI usage information", printHelpError);
      }
      System.exit(1);
    }

    if (!cmd.hasOption("workload")) {
      logger.info("Running all scenarios");
      for (String s : new String[] { "a", "b", "c", "d", "e", "f" }) {
        logger.info("Running scenario \"{}\"", s);
        Benchmark benchmark = new Benchmark();
        benchmark.runLoadForWorkload(s);
        benchmark.runBenchmarkForWorkload(s);
      }
    } else {
      String scenario = cmd.getOptionValue("workload");
      logger.info("Running single scenario \"{}\"", scenario);
      Benchmark benchmark = new Benchmark();
      benchmark.runLoadForWorkload(scenario);
      benchmark.runBenchmarkForWorkload(scenario);
    }
  }
}
