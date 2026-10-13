/**
 * Copyright (c) 2010-2016 Yahoo! Inc., 2017 YCSB contributors All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.codelry.util.ycsb;

import com.codelry.util.ycsb.measurements.Measurements;
import com.codelry.util.ycsb.measurements.exporter.MeasurementsExporter;
import com.codelry.util.ycsb.measurements.exporter.TextMeasurementsExporter;
import org.apache.htrace.core.HTraceConfiguration;
import org.apache.htrace.core.TraceScope;
import org.apache.htrace.core.Tracer;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Turn seconds remaining into more useful units.
 * i.e. if there are hours or days worth of seconds, use them.
 */
final class RemainingFormatter {
  private RemainingFormatter() {
    // not used
  }

  public static StringBuilder format(long seconds) {
    StringBuilder time = new StringBuilder();
    long days = TimeUnit.SECONDS.toDays(seconds);
    if (days > 0) {
      time.append(days).append(days == 1 ? " day " : " days ");
      seconds -= TimeUnit.DAYS.toSeconds(days);
    }
    long hours = TimeUnit.SECONDS.toHours(seconds);
    if (hours > 0) {
      time.append(hours).append(hours == 1 ? " hour " : " hours ");
      seconds -= TimeUnit.HOURS.toSeconds(hours);
    }
    /* Only include minute granularity if we're < 1 day. */
    if (days < 1) {
      long minutes = TimeUnit.SECONDS.toMinutes(seconds);
      if (minutes > 0) {
        time.append(minutes).append(minutes == 1 ? " minute " : " minutes ");
        seconds -= TimeUnit.MINUTES.toSeconds(seconds);
      }
    }
    /* Only bother to include seconds if we're < 1 minute */
    if (time.length() == 0) {
      time.append(seconds).append(time.length() == 1 ? " second " : " seconds ");
    }
    return time;
  }
}

/**
 * Main class for executing YCSB.
 */
public class Client {
  private static final Logger logger = LoggerFactory.getLogger(Client.class);

  public Client() {
  }

  public static final String DEFAULT_RECORD_COUNT = "0";

  /**
   * The target number of operations to perform.
   */
  public static final String OPERATION_COUNT_PROPERTY = "operationcount";

  /**
   * The number of records to load into the database initially.
   */
  public static final String RECORD_COUNT_PROPERTY = "recordcount";

  /**
   * The workload class to be loaded.
   */
  public static final String WORKLOAD_PROPERTY = "workload";

  /**
   * The database class to be used.
   */
  public static final String DB_PROPERTY = "db";

  /**
   * The exporter class to be used. The default is
   * com.codelry.util.ycsb.measurements.exporter.TextMeasurementsExporter.
   */
  public static final String EXPORTER_PROPERTY = "exporter";

  /**
   * If set to the path of a file, YCSB will write all output to this file
   * instead of STDOUT.
   */
  public static final String EXPORT_FILE_PROPERTY = "exportfile";

  /**
   * The number of YCSB client threads to run. Set to 0 to enable variable thread count.
   */
  public static final String THREAD_COUNT_PROPERTY = "threadcount";

  /**
   * Minimum client threads when variable thread count is enabled. Default: CPU count.
   */
  public static final String MIN_THREAD_COUNT_PROPERTY = ClientThreadPool.MIN_THREAD_COUNT_PROPERTY;

  /**
   * Maximum client threads when variable thread count is enabled. Default: 256.
   */
  public static final String MAX_THREAD_COUNT_PROPERTY = ClientThreadPool.MAX_THREAD_COUNT_PROPERTY;

  /**
   * Interval in seconds between variable thread-count adjustments. Default: 10.
   */
  public static final String THREAD_ADJUST_INTERVAL_PROPERTY = ClientThreadPool.THREAD_ADJUST_INTERVAL_PROPERTY;

  /**
   * Target minimum latency in milliseconds for the variable thread-count controller. Default: 1.
   */
  public static final String MIN_LATENCY_PROPERTY = ClientThreadPool.MIN_LATENCY_PROPERTY;

  /**
   * Indicates how many inserts to do if less than recordcount.
   * Useful for partitioning the load among multiple servers if the client is the bottleneck.
   * Additionally workloads should support the "insertstart" property which tells them which record to start at.
   */
  public static final String INSERT_COUNT_PROPERTY = "insertcount";

  /**
   * Target number of operations per second.
   */
  public static final String TARGET_PROPERTY = "target";

  /**
   * The maximum amount of time (in seconds) for which the benchmark will be run.
   */
  public static final String MAX_EXECUTION_TIME = "maxexecutiontime";

  /**
   * Whether or not this is the transaction phase (run) or not (load).
   */
  public static final String DO_TRANSACTIONS_PROPERTY = "dotransactions";

  /**
   * Whether or not to show status during run.
   */
  public static final String STATUS_PROPERTY = "status";

  /**
   * Use label for status (e.g. to label one experiment out of a whole batch).
   */
  public static final String LABEL_PROPERTY = "label";

  // HTrace integration related constants.

  /**
   * All keys for configuring the tracing system start with this prefix.
   */
  private static final String HTRACE_KEY_PREFIX = "htrace.";
  private static final String CLIENT_WORKLOAD_INIT_SPAN = "Client#workload_init";
  private static final String CLIENT_INIT_SPAN = "Client#init";
  private static final String CLIENT_WORKLOAD_SPAN = "Client#workload";
  private static final String CLIENT_CLEANUP_SPAN = "Client#cleanup";
  private static final String CLIENT_EXPORT_MEASUREMENTS_SPAN = "Client#export_measurements";

  public static void usageMessage() {
    logger.info("Usage: java com.codelry.util.ycsb.Client [options]");
    logger.info("Options:");
    logger.info("  -threads n: execute using n threads (default: 1; 0 = variable) - can also be specified\n" +
        "        as the \"threadcount\" property using -p");
    logger.info("  -target n: attempt to do n operations per second (default: unlimited) - can also\n" +
        "       be specified as the \"target\" property using -p");
    logger.info("  -load:  run the loading phase of the workload");
    logger.info("  -t:  run the transactions phase of the workload (default)");
    logger.info("  -db dbname: specify the name of the DB to use (default: com.codelry.util.ycsb.BasicDB) - \n" +
        "        can also be specified as the \"db\" property using -p");
    logger.info("  -P propertyfile: load properties from the given file. Multiple files can");
    logger.info("           be specified, and will be processed in the order specified");
    logger.info("  -p name=value:  specify a property to be passed to the DB and workloads;");
    logger.info("          multiple properties can be specified, and override any");
    logger.info("          values in the propertyfile");
    logger.info("  -s:  show status during run (default: no status)");
    logger.info("  -l label:  use label for status (e.g. to label one experiment out of a whole batch)");
    logger.info("");
    logger.info("Required properties:");
    logger.info("  {}: the name of the workload class to use (e.g. com.codelry.util.ycsb.workloads.CoreWorkload)",
        WORKLOAD_PROPERTY);
    logger.info("");
    logger.info("To run the transaction phase from multiple servers, start a separate client on each.");
    logger.info("To run the load phase from multiple servers, start a separate client on each; additionally,");
    logger.info("use the \"insertcount\" and \"insertstart\" properties to divide up the records to be inserted");
  }

  public static boolean checkRequiredProperties(Properties props) {
    if (props.getProperty(WORKLOAD_PROPERTY) == null) {
      logger.error("Missing property: {}", WORKLOAD_PROPERTY);
      return false;
    }

    return true;
  }


  /**
   * Exports the measurements to either sysout or a file using the exporter
   * loaded from conf.
   *
   * @throws IOException Either failed to write to output stream or failed to close it.
   */
  private static void exportMeasurements(Properties props, int opcount, long runtime,
                                         StatusThread statusThread, Map<String, Long[]> gcBaseline,
                                         ClientThreadPool threadPool)
      throws IOException {
    MeasurementsExporter exporter = null;
    try {
      // if no destination file is provided the results will be written to stdout
      OutputStream out;
      String exportFile = props.getProperty(EXPORT_FILE_PROPERTY);
      if (exportFile == null) {
        out = System.out;
      } else {
        out = new FileOutputStream(exportFile);
      }

      // if no exporter is provided the default text one will be used
      String exporterStr = props.getProperty(EXPORTER_PROPERTY,
          "com.codelry.util.ycsb.measurements.exporter.TextMeasurementsExporter");
      try {
        exporter = (MeasurementsExporter) Class.forName(exporterStr).getConstructor(OutputStream.class)
            .newInstance(out);
      } catch (Exception e) {
        logger.error("Could not find exporter {}, will use default text reporter.", exporterStr, e);
        exporter = new TextMeasurementsExporter(out);
      }

      exporter.write("OVERALL", "RunTime(ms)", runtime);
      double throughput = 1000.0 * (opcount) / (runtime);
      exporter.write("OVERALL", "Throughput(ops/sec)", throughput);
      exporter.write("OVERALL", "AverageLatency(us)", Measurements.getMeasurements().getAverageLatencyUs());
      if (threadPool != null) {
        exporter.write("OVERALL", "MinThreads", threadPool.getMinClientThreads());
        exporter.write("OVERALL", "MaxThreads", threadPool.getMaxClientThreads());
        exporter.write("OVERALL", "AverageThreads", threadPool.getAverageClientThreads());
      }

      final Map<String, Long[]> gcs = Utils.getGCStatsDelta(gcBaseline);
      long totalGCCount = 0;
      long totalGCTime = 0;
      for (final Entry<String, Long[]> entry : gcs.entrySet()) {
        exporter.write("TOTAL_GCS_" + entry.getKey(), "Count", entry.getValue()[0]);
        exporter.write("TOTAL_GC_TIME_" + entry.getKey(), "Time(ms)", entry.getValue()[1]);
        exporter.write("TOTAL_GC_TIME_%_" + entry.getKey(), "Time(%)",
            ((double) entry.getValue()[1] / runtime) * (double) 100);
        totalGCCount += entry.getValue()[0];
        totalGCTime += entry.getValue()[1];
      }
      exporter.write("TOTAL_GCs", "Count", totalGCCount);

      exporter.write("TOTAL_GC_TIME", "Time(ms)", totalGCTime);
      exporter.write("TOTAL_GC_TIME_%", "Time(%)", ((double) totalGCTime / runtime) * (double) 100);
      if (statusThread != null && statusThread.trackJVMStats()) {
        exporter.write("MAX_MEM_USED", "MBs", statusThread.getMaxUsedMem());
        exporter.write("MIN_MEM_USED", "MBs", statusThread.getMinUsedMem());
        exporter.write("MAX_THREADS", "Count", statusThread.getMaxThreads());
        exporter.write("MIN_THREADS", "Count", statusThread.getMinThreads());
        exporter.write("MAX_SYS_LOAD_AVG", "Load", statusThread.getMaxLoadAvg());
        exporter.write("MIN_SYS_LOAD_AVG", "Load", statusThread.getMinLoadAvg());
      }

      Measurements.getMeasurements().exportMeasurements(exporter);
    } finally {
      if (exporter != null) {
        exporter.close();
      }
    }
  }

  @SuppressWarnings("unchecked")
  public static void main(String[] args) {
    new Client().run(args);
  }

  public void run(String[] args) {
    Properties props = parseArguments(args);

    boolean status = Boolean.valueOf(props.getProperty(STATUS_PROPERTY, String.valueOf(false)));
    String label = props.getProperty(LABEL_PROPERTY, "");

    long maxExecutionTime = Integer.parseInt(props.getProperty(MAX_EXECUTION_TIME, "0"));

    //get number of threads, target and db
    int threadcount = Integer.parseInt(props.getProperty(THREAD_COUNT_PROPERTY, "1"));
    String dbname = props.getProperty(DB_PROPERTY, "com.codelry.util.ycsb.BasicDB");
    int target = Integer.parseInt(props.getProperty(TARGET_PROPERTY, "0"));
    boolean dotransactions = Boolean.valueOf(props.getProperty(DO_TRANSACTIONS_PROPERTY, String.valueOf(true)));

    Thread warningthread = setupWarningThread();
    warningthread.start();

    Measurements.setProperties(props);
    Measurements.reset();

    Workload workload = getWorkload(props);

    final Tracer tracer = getTracer(props, workload);

    initWorkload(props, warningthread, workload, tracer);

    if (dotransactions) {
      runWarmup(dbname, props, tracer);
    }

    int opcount = resolveOpcount(props, dotransactions);

    logger.info("Starting test.");
    final ClientThreadPool threadPool;
    try (final TraceScope span = tracer.newScope(CLIENT_INIT_SPAN)) {
      threadPool = new ClientThreadPool(dbname, props, workload, tracer, threadcount, opcount, target);
    }

    StatusThread statusThread = null;
    if (status) {
      boolean standardstatus = false;
      if (props.getProperty(Measurements.MEASUREMENT_TYPE_PROPERTY, "").compareTo("timeseries") == 0) {
        standardstatus = true;
      }
      int statusIntervalSeconds = Integer.parseInt(props.getProperty("status.interval", "10"));
      boolean trackJVMStats = props.getProperty(Measurements.MEASUREMENT_TRACK_JVM_PROPERTY,
          Measurements.MEASUREMENT_TRACK_JVM_PROPERTY_DEFAULT).equals("true");
      statusThread = new StatusThread(threadPool.getCompleteLatch(), threadPool.getClients(), label,
          standardstatus, statusIntervalSeconds, trackJVMStats, threadPool::getOpsDone, threadPool::getOpsTodo);
      statusThread.start();
    }

    Thread terminator = null;
    long st;
    long en;
    int opsDone;
    Map<String, Long[]> gcBaseline;

    try (final TraceScope span = tracer.newScope(CLIENT_WORKLOAD_SPAN)) {
      gcBaseline = Utils.getGCStatst();
      st = System.currentTimeMillis();

      try {
        threadPool.start();
      } catch (UnknownDBException e) {
        logger.error("Error initializing datastore bindings.", e);
        System.exit(0);
      }

      if (maxExecutionTime > 0) {
        terminator = new TerminatorThread(maxExecutionTime, threadPool.getThreads(), workload);
        terminator.start();
      }

      opsDone = threadPool.awaitCompletion();
      en = System.currentTimeMillis();
    }

    try {
      try (final TraceScope span = tracer.newScope(CLIENT_CLEANUP_SPAN)) {

        if (terminator != null && !terminator.isInterrupted()) {
          terminator.interrupt();
        }

        if (statusThread != null) {
          // wake up status thread if it's asleep
          statusThread.interrupt();
          // at this point we assume all the monitored threads are already gone as per above join loop.
          try {
            statusThread.join();
          } catch (InterruptedException ignored) {
            // ignored
          }
        }

        workload.cleanup();
      }
    } catch (WorkloadException e) {
      e.printStackTrace();
      e.printStackTrace(System.out);
      System.exit(0);
    }

    try {
      try (final TraceScope span = tracer.newScope(CLIENT_EXPORT_MEASUREMENTS_SPAN)) {
        exportMeasurements(props, opsDone, en - st, statusThread, gcBaseline, threadPool);
      }
    } catch (IOException e) {
      logger.error("Could not export measurements, error: {}", e.getMessage(), e);
      System.exit(-1);
    }

  }

  static int resolveOpcount(Properties props, boolean dotransactions) {
    if (dotransactions) {
      return Integer.parseInt(props.getProperty(OPERATION_COUNT_PROPERTY, "0"));
    }
    if (props.containsKey(INSERT_COUNT_PROPERTY)) {
      return Integer.parseInt(props.getProperty(INSERT_COUNT_PROPERTY, "0"));
    }
    return Integer.parseInt(props.getProperty(RECORD_COUNT_PROPERTY, DEFAULT_RECORD_COUNT));
  }

  /**
   * Runs an optional DB warmup before the timed transaction phase. Metrics are recorded as WARMUP
   * and excluded from OVERALL aggregates; this call is outside the run timer.
   */
  static void runWarmup(String dbname, Properties props, Tracer tracer) {
    DB db = null;
    try {
      db = DBFactory.newDB(dbname, props, tracer);
      db.init();
      logger.info("Running database warmup...");
      db.warmup();
      logger.info("Database warmup complete.");
    } catch (UnknownDBException e) {
      logger.error("Unknown DB {} during warmup", dbname);
      System.exit(0);
    } catch (DBException e) {
      logger.error("Warmup failed: {}", e.getMessage(), e);
      System.exit(0);
    } finally {
      if (db != null) {
        try {
          if (db instanceof DBWrapper) {
            ((DBWrapper) db).cleanupWithoutMeasurement();
          } else {
            db.cleanup();
          }
        } catch (DBException e) {
          logger.warn("Warmup cleanup failed: {}", e.getMessage(), e);
        }
      }
    }
  }

  private static Tracer getTracer(Properties props, Workload workload) {
    return new Tracer.Builder("YCSB " + workload.getClass().getSimpleName())
        .conf(getHTraceConfiguration(props))
        .build();
  }

  private static void initWorkload(Properties props, Thread warningthread, Workload workload, Tracer tracer) {
    try {
      try (final TraceScope span = tracer.newScope(CLIENT_WORKLOAD_INIT_SPAN)) {
        workload.init(props);
        warningthread.interrupt();
      }
    } catch (WorkloadException e) {
      e.printStackTrace();
      e.printStackTrace(System.out);
      System.exit(0);
    }
  }

  private static HTraceConfiguration getHTraceConfiguration(Properties props) {
    final Map<String, String> filteredProperties = new HashMap<>();
    for (String key : props.stringPropertyNames()) {
      if (key.startsWith(HTRACE_KEY_PREFIX)) {
        filteredProperties.put(key.substring(HTRACE_KEY_PREFIX.length()), props.getProperty(key));
      }
    }
    return HTraceConfiguration.fromMap(filteredProperties);
  }

  private static Thread setupWarningThread() {
    //show a warning message that creating the workload is taking a while
    //but only do so if it is taking longer than 2 seconds
    //(showing the message right away if the setup wasn't taking very long was confusing people)
    return new Thread() {
      @Override
      public void run() {
        try {
          sleep(2000);
        } catch (InterruptedException e) {
          return;
        }
        logger.info(" (might take a few minutes for large data sets)");
      }
    };
  }

  private static Workload getWorkload(Properties props) {
    ClassLoader classLoader = Client.class.getClassLoader();

    try {
      Properties projectProp = new Properties();
      projectProp.load(classLoader.getResourceAsStream("project.properties"));
      logger.info("YCSB Client {}", projectProp.getProperty("version"));
    } catch (IOException e) {
      logger.error("Unable to retrieve client version.");
    }

    logger.info("Loading workload...");
    try {
      Class workloadclass = classLoader.loadClass(props.getProperty(WORKLOAD_PROPERTY));

      return (Workload) workloadclass.newInstance();
    } catch (Exception e) {
      e.printStackTrace();
      e.printStackTrace(System.out);
      System.exit(0);
    }

    return null;
  }

  protected Properties parseArguments(String[] args) {
    Properties props = new Properties();
    logger.info("Command line: {}", String.join(" ", args));

    Properties fileprops = new Properties();
    int argindex = 0;

    if (args.length == 0) {
      usageMessage();
      logger.error("At least one argument specifying a workload is required.");
      System.exit(0);
    }

    while (args[argindex].startsWith("-")) {
      if (args[argindex].compareTo("-threads") == 0) {
        argindex++;
        if (argindex >= args.length) {
          usageMessage();
          logger.error("Missing argument value for -threads.");
          System.exit(0);
        }
        int tcount = Integer.parseInt(args[argindex]);
        props.setProperty(THREAD_COUNT_PROPERTY, String.valueOf(tcount));
        argindex++;
      } else if (args[argindex].compareTo("-target") == 0) {
        argindex++;
        if (argindex >= args.length) {
          usageMessage();
          logger.error("Missing argument value for -target.");
          System.exit(0);
        }
        int ttarget = Integer.parseInt(args[argindex]);
        props.setProperty(TARGET_PROPERTY, String.valueOf(ttarget));
        argindex++;
      } else if (args[argindex].compareTo("-load") == 0) {
        props.setProperty(DO_TRANSACTIONS_PROPERTY, String.valueOf(false));
        argindex++;
      } else if (args[argindex].compareTo("-t") == 0) {
        props.setProperty(DO_TRANSACTIONS_PROPERTY, String.valueOf(true));
        argindex++;
      } else if (args[argindex].compareTo("-s") == 0) {
        props.setProperty(STATUS_PROPERTY, String.valueOf(true));
        argindex++;
      } else if (args[argindex].compareTo("-db") == 0) {
        argindex++;
        if (argindex >= args.length) {
          usageMessage();
          logger.error("Missing argument value for -db.");
          System.exit(0);
        }
        props.setProperty(DB_PROPERTY, args[argindex]);
        argindex++;
      } else if (args[argindex].compareTo("-l") == 0) {
        argindex++;
        if (argindex >= args.length) {
          usageMessage();
          logger.error("Missing argument value for -l.");
          System.exit(0);
        }
        props.setProperty(LABEL_PROPERTY, args[argindex]);
        argindex++;
      } else if (args[argindex].compareTo("-P") == 0) {
        argindex++;
        if (argindex >= args.length) {
          usageMessage();
          logger.error("Missing argument value for -P.");
          System.exit(0);
        }
        String propfile = args[argindex];
        argindex++;

        Properties myfileprops = new Properties();
        try {
          myfileprops.load(new FileInputStream(propfile));
        } catch (IOException e) {
          logger.error("Unable to open the properties file {}", propfile, e);
          System.exit(0);
        }

        //Issue #5 - remove call to stringPropertyNames to make compilable under Java 1.5
        for (Enumeration e = myfileprops.propertyNames(); e.hasMoreElements();) {
          String prop = (String) e.nextElement();

          fileprops.setProperty(prop, myfileprops.getProperty(prop));
        }

      } else if (args[argindex].compareTo("-p") == 0) {
        argindex++;
        if (argindex >= args.length) {
          usageMessage();
          logger.error("Missing argument value for -p");
          System.exit(0);
        }
        int eq = args[argindex].indexOf('=');
        if (eq < 0) {
          usageMessage();
          logger.error("Argument '-p' expected to be in key=value format (e.g., -p operationcount=99999)");
          System.exit(0);
        }

        String name = args[argindex].substring(0, eq);
        String value = args[argindex].substring(eq + 1);
        props.put(name, value);
        argindex++;
      } else {
        usageMessage();
        logger.error("Unknown option {}", args[argindex]);
        System.exit(0);
      }

      if (argindex >= args.length) {
        break;
      }
    }

    if (argindex != args.length) {
      usageMessage();
      if (argindex < args.length) {
        logger.error("An argument value without corresponding argument specifier (e.g., -p, -s) was found. "
            + "We expected an argument specifier and instead found {}", args[argindex]);
      } else {
        logger.error("An argument specifier without corresponding value was found at the end of the supplied "
            + "command line arguments.");
      }
      System.exit(0);
    }

    //overwrite file properties with properties from the command line

    //Issue #5 - remove call to stringPropertyNames to make compilable under Java 1.5
    for (Enumeration e = props.propertyNames(); e.hasMoreElements();) {
      String prop = (String) e.nextElement();

      fileprops.setProperty(prop, props.getProperty(prop));
    }

    props = fileprops;

    if (!checkRequiredProperties(props)) {
      logger.error("Failed check required properties.");
      System.exit(0);
    }

    return props;
  }
}
