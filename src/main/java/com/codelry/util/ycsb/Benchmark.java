package com.codelry.util.ycsb;

import com.codelry.util.ycsb.measurements.Measurements;
import com.codelry.util.ycsb.measurements.exporter.MeasurementsExporter;
import com.codelry.util.ycsb.measurements.exporter.TextMeasurementsExporter;
import org.apache.commons.cli.*;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.help.HelpFormatter;
import org.apache.htrace.core.HTraceConfiguration;
import org.apache.htrace.core.TraceScope;
import org.apache.htrace.core.Tracer;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Benchmark {
  public Benchmark() {
  }

  private static final Logger logger = LoggerFactory.getLogger(Benchmark.class);

  public static final String DEFAULT_RECORD_COUNT = "0";

  public static final String OPERATION_COUNT_PROPERTY = "operationcount";

  public static final String RECORD_COUNT_PROPERTY = "recordcount";

  public static final String WORKLOAD_PROPERTY = "workload";

  public static final String DB_PROPERTY = "db";

  public static final String EXPORTER_PROPERTY = "exporter";

  public static final String EXPORT_FILE_PROPERTY = "exportfile";

  public static final String THREAD_COUNT_PROPERTY = "threadcount";

  public static final String INSERT_COUNT_PROPERTY = "insertcount";

  public static final String TARGET_PROPERTY = "target";

  public static final String MAX_EXECUTION_TIME = "maxexecutiontime";

  public static final String DO_TRANSACTIONS_PROPERTY = "dotransactions";

  public static final String STATUS_PROPERTY = "status";

  public static final String LABEL_PROPERTY = "label";

  public static final String WORKLOAD_NAME_PROPERTY = "workloadname";

  public static final String TEST_SETUP_PROPERTY = "test.setup";
  public static String testSetup;

  public static final String TEST_CLEANUP_PROPERTY = "test.clean";
  public static String testCleanup;

  private static StatusThread statusthread = null;

  private static final String YCSB_PROPERTY_FILE = "ycsb.properties";

  private static final String HTRACE_KEY_PREFIX = "htrace.";
  private static final String CLIENT_WORKLOAD_INIT_SPAN = "Client#workload_init";
  private static final String CLIENT_INIT_SPAN = "Client#init";
  private static final String CLIENT_WORKLOAD_SPAN = "Client#workload";
  private static final String CLIENT_CLEANUP_SPAN = "Client#cleanup";
  private static final String CLIENT_EXPORT_MEASUREMENTS_SPAN = "Client#export_measurements";

  private static void exportMeasurements(Properties props, int opcount, long runtime)
      throws IOException {
    MeasurementsExporter exporter = null;
    String workloadFileName = props.getProperty(WORKLOAD_NAME_PROPERTY, null);
    boolean loadMode = props.getProperty(DO_TRANSACTIONS_PROPERTY, "true").equals("false");

    try {
      OutputStream out;
      String exportFile = props.getProperty(EXPORT_FILE_PROPERTY);
      if (exportFile != null) {
        logger.info("Writing output to file: {}", exportFile);
        out = Files.newOutputStream(Paths.get(exportFile));
      } else if (workloadFileName != null) {
        String suffix = loadMode ? "-load.dat" : "-run.dat";
        Path path = Paths.get("output", workloadFileName + suffix);
        Files.createDirectories(path.getParent());
        logger.info("Writing output to: {}", path);
        out = Files.newOutputStream(path);
      } else {
        out = System.out;
      }

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

      final Map<String, Long[]> gcs = Utils.getGCStatst();
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
      if (statusthread != null && statusthread.trackJVMStats()) {
        exporter.write("MAX_MEM_USED", "MBs", statusthread.getMaxUsedMem());
        exporter.write("MIN_MEM_USED", "MBs", statusthread.getMinUsedMem());
        exporter.write("MAX_THREADS", "Count", statusthread.getMaxThreads());
        exporter.write("MIN_THREADS", "Count", statusthread.getMinThreads());
        exporter.write("MAX_SYS_LOAD_AVG", "Load", statusthread.getMaxLoadAvg());
        exporter.write("MIN_SYS_LOAD_AVG", "Load", statusthread.getMinLoadAvg());
      }

      Measurements.getMeasurements().exportMeasurements(exporter);
    } finally {
      if (exporter != null) {
        exporter.close();
      }
    }
  }

  public static void main(String[] args) {
    new Benchmark().runFromArgs(args);
  }

  public void runFromArgs(String[] args) {
    Properties props = parseArguments(args);
    run(props);
  }

  public void runLoadForWorkload(String workload) {
    logger.info("Loading data");
    Properties props = new Properties();
    props.setProperty(DO_TRANSACTIONS_PROPERTY, String.valueOf(false));
    loadPropertiesFiles(workload, props);
    run(props);
  }

  public void runBenchmarkForWorkload(String workload) {
    logger.info("Running benchmark");
    Properties props = new Properties();
    props.setProperty(DO_TRANSACTIONS_PROPERTY, String.valueOf(true));
    loadPropertiesFiles(workload, props);
    run(props);
  }

  public void run(Properties props) {
    boolean status = Boolean.parseBoolean(props.getProperty(STATUS_PROPERTY, String.valueOf(false)));
    String label = props.getProperty(LABEL_PROPERTY, "");

    long maxExecutionTime = Integer.parseInt(props.getProperty(MAX_EXECUTION_TIME, "0"));

    //get the number of threads, target and db
    int threadcount = Integer.parseInt(props.getProperty(THREAD_COUNT_PROPERTY, "1"));
    String dbname = props.getProperty(DB_PROPERTY, "com.codelry.util.ycsb.BasicDB");
    int target = Integer.parseInt(props.getProperty(TARGET_PROPERTY, "0"));

    testSetup = props.getProperty(TEST_SETUP_PROPERTY, null);
    testCleanup = props.getProperty(TEST_CLEANUP_PROPERTY, null);
    boolean loadMode = props.getProperty(DO_TRANSACTIONS_PROPERTY, "true").equals("false");

    //compute the target throughput
    double targetperthreadperms = -1;
    if (target > 0) {
      double targetperthread = ((double) target) / ((double) threadcount);
      targetperthreadperms = targetperthread / 1000.0;
    }

    if (testSetup != null && loadMode) {
      try {
        runTestSetup(testSetup, props);
      } catch (Exception e) {
        logger.error(e.getMessage(), e);
        System.exit(1);
      }
    }

    Thread warningthread = setupWarningThread();
    warningthread.start();

    Measurements.setProperties(props);

    Workload workload = getWorkload(props);

    final Tracer tracer = getTracer(props, workload);

    initWorkload(props, warningthread, workload, tracer);

    logger.info("Starting benchmark...");
    final CountDownLatch completeLatch = new CountDownLatch(threadcount);

    final List<ClientThread> clients = initDb(dbname, props, threadcount, targetperthreadperms,
        workload, tracer, completeLatch);

    if (status) {
      boolean standardstatus = props.getProperty(Measurements.MEASUREMENT_TYPE_PROPERTY, "").compareTo("timeseries") == 0;
      int statusIntervalSeconds = Integer.parseInt(props.getProperty("status.interval", "10"));
      boolean trackJVMStats = props.getProperty(Measurements.MEASUREMENT_TRACK_JVM_PROPERTY,
          Measurements.MEASUREMENT_TRACK_JVM_PROPERTY_DEFAULT).equals("true");
      statusthread = new StatusThread(completeLatch, clients, label, standardstatus, statusIntervalSeconds,
          trackJVMStats);
      statusthread.start();
    }

    Thread terminator = null;
    long st;
    long en;
    int opsDone;

    try (final TraceScope ignored1 = tracer.newScope(CLIENT_WORKLOAD_SPAN)) {

      final Map<Thread, ClientThread> threads = new HashMap<>(threadcount);
      for (ClientThread client : clients) {
        threads.put(new Thread(tracer.wrap(client, "ClientThread")), client);
      }

      st = System.currentTimeMillis();

      for (Thread t : threads.keySet()) {
        t.start();
      }

      if (maxExecutionTime > 0) {
        terminator = new TerminatorThread(maxExecutionTime, threads.keySet(), workload);
        terminator.start();
      }

      opsDone = 0;

      for (Entry<Thread, ClientThread> entry : threads.entrySet()) {
        try {
          entry.getKey().join();
          opsDone += entry.getValue().getOpsDone();
        } catch (InterruptedException ignored) {
          // ignored
        }
      }

      en = System.currentTimeMillis();
    }

    try {
      try (final TraceScope ignored1 = tracer.newScope(CLIENT_CLEANUP_SPAN)) {

        if (terminator != null && !terminator.isInterrupted()) {
          terminator.interrupt();
        }

        if (status) {
          // wake up the status thread if it's asleep
          statusthread.interrupt();
          // at this point we assume all the monitored threads are already gone as per the above join loop.
          try {
            statusthread.join();
          } catch (InterruptedException ignored) {
            // ignored
          }
        }

        workload.cleanup();
      }
    } catch (WorkloadException e) {
      logger.error("Workload exception: {}", e.getMessage(), e);
      System.exit(1);
    }

    try {
      try (final TraceScope ignored = tracer.newScope(CLIENT_EXPORT_MEASUREMENTS_SPAN)) {
        exportMeasurements(props, opsDone, en - st);
      }
    } catch (IOException e) {
      logger.error("Could not export measurements, error: {}", e.getMessage(), e);
      System.exit(1);
    }

    if (testCleanup != null && !loadMode) {
      try {
        runTestClean(testCleanup, props);
      } catch (Exception e) {
        logger.error(e.getMessage(), e);
        System.exit(1);
      }
    }
  }

  private static List<ClientThread> initDb(String dbname, Properties props, int threadcount,
                                           double targetperthreadperms, Workload workload, Tracer tracer,
                                           CountDownLatch completeLatch) {
    boolean initFailed = false;
    boolean dotransactions = Boolean.parseBoolean(props.getProperty(DO_TRANSACTIONS_PROPERTY, String.valueOf(true)));

    final List<ClientThread> clients = new ArrayList<>(threadcount);
    try (final TraceScope ignored = tracer.newScope(CLIENT_INIT_SPAN)) {
      int opcount;
      if (dotransactions) {
        opcount = Integer.parseInt(props.getProperty(OPERATION_COUNT_PROPERTY, "0"));
      } else {
        if (props.containsKey(INSERT_COUNT_PROPERTY)) {
          opcount = Integer.parseInt(props.getProperty(INSERT_COUNT_PROPERTY, "0"));
        } else {
          opcount = Integer.parseInt(props.getProperty(RECORD_COUNT_PROPERTY, DEFAULT_RECORD_COUNT));
        }
      }
      if (threadcount > opcount && opcount > 0){
        threadcount = opcount;
        logger.warn("Warning: the threadcount is bigger than recordcount, the threadcount will be recordcount!");
      }
      for (int threadid = 0; threadid < threadcount; threadid++) {
        DB db;
        try {
          db = DBFactory.newDB(dbname, props, tracer);
        } catch (UnknownDBException e) {
          logger.error("Unknown DB {}", dbname);
          initFailed = true;
          break;
        }

        int threadopcount = opcount / threadcount;

        // ensure the correct number of operations, in case opcount is not a multiple of threadcount
        if (threadid < opcount % threadcount) {
          ++threadopcount;
        }

        ClientThread t = new ClientThread(db, dotransactions, workload, props, threadopcount, targetperthreadperms,
            completeLatch);
        t.setThreadId(threadid);
        t.setThreadCount(threadcount);
        clients.add(t);
      }

      if (initFailed) {
        logger.error("Error initializing datastore bindings.");
        System.exit(1);
      }
    }
    return clients;
  }

  private static Tracer getTracer(Properties props, Workload workload) {
    return new Tracer.Builder("YCSB " + workload.getClass().getSimpleName())
        .conf(getHTraceConfiguration(props))
        .build();
  }

  private static void initWorkload(Properties props, Thread warningthread, Workload workload, Tracer tracer) {
    try {
      try (final TraceScope ignored = tracer.newScope(CLIENT_WORKLOAD_INIT_SPAN)) {
        workload.init(props);
        warningthread.interrupt();
      }
    } catch (WorkloadException e) {
      logger.error("Workload exception: {}", e.getMessage(), e);
      System.exit(1);
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
    return new Thread(() -> {
      try {
        Thread.sleep(2000);
      } catch (InterruptedException e) {
        return;
      }
      logger.info(" (might take a few minutes for large data sets)");
    });
  }

  private static Workload getWorkload(Properties props) {
    ClassLoader classLoader = Benchmark.class.getClassLoader();

    try {
      Properties projectProp = new Properties();
      projectProp.load(classLoader.getResourceAsStream("project.properties"));
      logger.info("YCSB Client {}", projectProp.getProperty("version"));
    } catch (IOException e) {
      logger.error("Unable to retrieve client version.");
    }

    logger.info("Loading workload...");
    try {
      Class<?> workloadclass = classLoader.loadClass(props.getProperty(WORKLOAD_PROPERTY));

      return (Workload) workloadclass.newInstance();
    } catch (Exception e) {
      logger.error("Unable to load workload class: {}", e.getMessage(), e);
      System.exit(1);
    }

    return null;
  }

  private static void runTestSetup(String testSetupClass, Properties properties) {
    TestSetup testSetupUtil = TestSetupFactory.newInstance(testSetupClass);
    try {
      testSetupUtil.testSetup(properties);
    } catch (Exception e) {
      logger.error("Error: test setup failure: {}", e.getMessage(), e);
      System.exit(1);
    }
  }

  private static void runTestClean(String testCleanClass, Properties properties) {
    TestCleanup testCleanUtil = TestCleanupFactory.newInstance(testCleanClass);
    try {
      testCleanUtil.testClean(properties);
    } catch (Exception e) {
      logger.error("Error: test clean failure: {}", e.getMessage(), e);
      System.exit(1);
    }
  }

  protected Properties parseArguments(String[] args) {
    Options options = new Options();
    CommandLine cmd = null;
    Properties properties = new Properties();

    getOptions(options);

    CommandLineParser parser = new DefaultParser();
    HelpFormatter.Builder formatterBuilder = HelpFormatter.builder();
    HelpFormatter formatter = formatterBuilder.get();

    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      String header = "Run a YCSB test scenario.";
      String footer = "To run the benchmark on multiple systems, start a separate client on each.";
      try {
        formatter.printHelp("LoadClient", header, options, footer, true);
      } catch (IOException printHelpError) {
        logger.error("Error printing CLI usage information", printHelpError);
      }
      System.exit(1);
    }

    String workload = cmd.getOptionValue("workload", "a").toUpperCase();
    String threads = cmd.getOptionValue("threads", "32");
    String target = cmd.getOptionValue("target", "0");
    Boolean doTransactions = !cmd.hasOption("load");
    String db = cmd.getOptionValue("db", "com.codelry.util.ycsb.BasicDB");
    Boolean status = cmd.hasOption("status");
    String label = cmd.getOptionValue("label", "");

    properties.setProperty(THREAD_COUNT_PROPERTY, threads);
    properties.setProperty(TARGET_PROPERTY, target);
    properties.setProperty(DO_TRANSACTIONS_PROPERTY, String.valueOf(doTransactions));
    properties.setProperty(DB_PROPERTY, db);
    properties.setProperty(STATUS_PROPERTY, String.valueOf(status));
    properties.setProperty(LABEL_PROPERTY, label);

    loadPropertiesFiles(workload, properties);
    return properties;
  }

  protected void loadPropertiesFiles(String workload, Properties properties) {
    String workloadFile;

    switch (workload.toUpperCase()) {
      case "A":
        workloadFile = "workloada";
        break;
      case "B":
        workloadFile = "workloadb";
        break;
      case "C":
        workloadFile = "workloadc";
        break;
      case "D":
        workloadFile = "workloadd";
        break;
      case "E":
        workloadFile = "workloade";
        break;
      case "F":
        workloadFile = "workloadf";
        break;
      default:
        workloadFile = workload;
    }

    logger.info("Workload file: {}", workloadFile);
    properties.setProperty(WORKLOAD_NAME_PROPERTY, workloadFile);
    loadPropertiesFromFile(properties, workloadFile);

    loadPropertiesFromFile(properties, YCSB_PROPERTY_FILE);

    if (properties.getProperty(WORKLOAD_PROPERTY) == null) {
      properties.setProperty(WORKLOAD_PROPERTY, "com.codelry.util.ycsb.workloads.CoreWorkload");
    }

    if (properties.getProperty(DO_TRANSACTIONS_PROPERTY, "true").equals("false")) {
      properties.setProperty(MAX_EXECUTION_TIME, "0");
    }
  }

  private static void getOptions(Options options) {
    Option workload = new Option("w", "workload", true, "workload scenario");
    Option threads = new Option("t", "threads", true, "threads");
    Option target = new Option("T", "target", true, "target throughput (ops/sec)");
    Option load = new Option("l", "load", false, "load data");
    Option db = new Option("d", "db", true, "database class");
    Option propFile = new Option("P", "property-file", true, "property file");
    Option props = new Option("p", "property", true, "property name=value");
    Option status = new Option("s", "status", false, "show status");
    Option label = new Option("L", "label", true, "label for status");
    workload.setRequired(false);
    threads.setRequired(false);
    target.setRequired(false);
    load.setRequired(false);
    db.setRequired(false);
    propFile.setRequired(false);
    props.setRequired(false);
    status.setRequired(false);
    label.setRequired(false);
    options.addOption(workload);
    options.addOption(threads);
    options.addOption(target);
    options.addOption(load);
    options.addOption(db);
    options.addOption(propFile);
    options.addOption(props);
    options.addOption(status);
    options.addOption(label);
  }

  private void loadPropertiesFromFile(Properties properties, String propertyFile) {
    ClassLoader classloader = Thread.currentThread().getContextClassLoader();
    if (classloader == null) {
      classloader = Benchmark.class.getClassLoader();
    }
    try (InputStream in = classloader.getResourceAsStream(propertyFile)) {
      if (in != null) {
        logger.debug("Loading properties from resource {}", propertyFile);
        properties.load(in);
      } else {
        logger.warn("Properties resource {} not found on classpath.", propertyFile);
      }
    } catch (IOException e) {
      logger.error("Can not open properties file {}: {}", propertyFile, e.getMessage());
      System.exit(1);
    }
  }
}
