package net.martinprobson.hiveutils.hqlwrapper;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static net.martinprobson.hiveutils.hqlwrapper.ControllerConfiguration.*;

/**
 * Controller to submit HQL files to Hive via JDBC.
 * <p>
 * <h3>Operation</h3>
 * Takes the JobConfig.root directory specified in framework_config.xml (or via -JobConfig on command line) and executes the HQL files
 * found in that location in lexicographical order.
 * <p>A sub-directory containing additional HQL files is assumed to be a separate set of independent HQL that can be executed in parallel in conjunction
 * with other HQL files stored in sub-directories.
 * <p>For example, given a JobConfig root that points to the following files: -
 * <p>
 * <img src="./doc-files/Example_Task_Chain.png" />
 * <p>
 * <p>
 * <p>The framework will execute: -
 * <uk>
 * <li>00_Init1
 * <li>10_Init1
 * <li>Start a separate thread to execute files in directory 20_Rule1:-
 * <ul><li>00_Rule1<li>20_Rule1</ul>
 * <li>Start a separate thread to execute file in directory 30_Rule2:-
 * <ul><li>10_Rule2<li>20_Rule2<li>99_Rule2</ul>
 * <li>80_Fin1
 * <li>90_Fin2
 * </ul>
 * Note that a failure of a file existing in a sub-directory will not prevent top level or other sub-directory files from running.
 * <p>For example, a failure in file 00_Rule1 will prevent 20_Rule1 from executing but 30_Rule2 plus 80_Fin1 and 2 will still execute.
 * <p>A failure in file 10_Init1 will prevent all other tasks from executing.
 */
public class Controller extends Configured {

    private static Controller controller = null;
    private final static Logger log = LoggerFactory.getLogger(Controller.class);
    private TaskQueue taskQueue = null;

    private void processCmdLine(String[] args) {
        Options options = new Options().addOption("j", "JobConfig", true, "Filename of the root of the job configuration directory (overrides JobConfig.root if set)");
        options.addOption("d", "DryRun", false, "Dryrun - parse and run the jobs but do not submit anything to Hive");
        options.addOption("h", "help", false, "Display help");
        CommandLineParser parser = new DefaultParser();

        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (MissingArgumentException e) {
            System.err.println("Missing argument " + e.getOption());
            System.exit(2);
        } catch (ParseException e) {
            e.printStackTrace();
            System.exit(2);
        }
        if (cmd.hasOption("h")) {
            HelpFormatter h = new HelpFormatter();
            h.printHelp("Controller", options);
            System.exit(0);
        }
        getConf().setBoolean(DRY_RUN, cmd.hasOption("d"));
        if (cmd.hasOption("j"))
            getConf().set(HQL_FILE_ROOT_DIR, cmd.getOptionValue("j"));
    }

    /**
     * Returns the root directory containing the HQL files to be executed.
     *
     * @return root directory as a String.
     * @throws HqlWrapperException If any error occurs in the execution.
     */
    public String getRoot() throws HqlWrapperException {
        String hqlFileDirectory = getConf().get(HQL_FILE_ROOT_DIR);
        log.debug("Checking job configuration root directory: " + hqlFileDirectory);
        if (!FileUtil.exists(hqlFileDirectory))
            throw new HqlWrapperException("Job Configuration root: " + hqlFileDirectory + " file does not exist");

        if (!FileUtil.isDirectory(hqlFileDirectory))
            throw new HqlWrapperException("Job Configuration root: " + hqlFileDirectory + " file is not a directory");

        return hqlFileDirectory;
    }

    /**
     * Main entry point for framework controller.
     */
    public static void main(String[] args) throws HqlWrapperException {
        Controller.getInstance().run(args);
    }

    /**
     * Executes a TaskQueue
     *
     * @param taskQueue The TaskQueue to execute
     */
    private void executeTaskQueue(TaskQueue taskQueue) {
        log.debug("EXECUTE TASK QUEUE: " + taskQueue.getLabel());
        ExecutorService executor = ExecutorPool.getExecutor(taskQueue.getLabel());
        TaskExecutor taskExecutor = new TaskExecutor(taskQueue);
        Future<TaskResult> task = executor.submit(taskExecutor);
        int sleepInterval = getConf().getInt(MONITOR_INTERVAL, 10) * 1000;
        TaskResult result = null;
        while (true) {
            ExecutorPool.monitor();
            try {
                Thread.sleep(sleepInterval);
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
            if (task.isDone()) {
                try {
                    result = task.get();
                } catch (CancellationException e) {
                    log.error("Top level task - Cancelled");
                } catch (ExecutionException e) {
                    log.error("Top level task - ExecutionException");
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    log.error("Top level task - InterruptedException");
                } finally {
                    log.info("Top level task Finished - result = " + result);
                }
                break;
            }
        }
        ExecutorPool.cleanUp();
        log.info("******************* EXECUTE TASK QUEUE END *******************");
    }

    public void run() throws HqlWrapperException {
        this.run(Collections.emptyMap());
    }

    public void run(Map<String, String> params) throws HqlWrapperException {
        log.info("******************* HQL WRAPPER CONTROLLER START *******************");
        String start_ts = Util.getCurrentTimeStamp();
        if (getConf().getBoolean(DRY_RUN, false))
            log.info("** Dry Run is set **");
        // Append current date (YYYYMMDD) to connection URL so HQL can use ${hiveconf:run_date} in scripts.
        DBConnection.addURLAppender(url -> {
                    url = url + "hiveconf:run_date=" + Util.currentDate;
                    return url;
                }
        );
        taskQueue = new TaskQueue(getRoot(), params);
        executeTaskQueue(taskQueue);
        log.info("******************* HQL WRAPPER CONTROLLER END *******************");
        if (getConf().getBoolean(SEND_MAIL_ON_SUCCESS, false)) {
            log.debug("Sending controller end email");
            Util.SendMail(getConf().get(SEND_MAIL_FROM), getConf().getStrings(SEND_MAIL_TO), "Framework end", "Start: " + start_ts + " End: " + Util.getCurrentTimeStamp());
        }
    }

    private void run(String[] args) throws HqlWrapperException {
        processCmdLine(args);
        run();
    }

    public static Controller getInstance() {
        if (controller == null) {
            log.debug("Building new instance of Controller");
            controller = new Controller(new ControllerConfiguration());
        }
        return controller;
    }

    public void reset() {
        controller = null;
    }

    protected Controller(Configuration conf) {
        super(conf);
        controller = this;
    }

    protected Controller() {
        this(null);
    }

    public TaskQueue getTaskQueue() {
        return taskQueue;
    }
}
