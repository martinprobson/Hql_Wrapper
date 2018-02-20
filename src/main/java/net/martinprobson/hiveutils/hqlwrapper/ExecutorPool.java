package net.martinprobson.hiveutils.hqlwrapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Thread.sleep;


/**
 * Maintains a pool of Executors, that can all be shutdown via the cleanUp() method.
 * <p>
 *
 * @author robsom12
 */
public class ExecutorPool {

    private static List<ExecutorService> executors = new ArrayList<>();

    static class ExecThreadFactory implements ThreadFactory {
        private static final String THREAD_GROUP_NAME = "HQL_Executor";
        private static final ThreadGroup group = new ThreadGroup(THREAD_GROUP_NAME);
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;


        ExecThreadFactory(String label) {
            namePrefix = group.getName() + "-[" + label + "]-";
        }

        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r,
                    namePrefix + threadNumber.getAndIncrement(),
                    0);
            if (t.isDaemon())
                t.setDaemon(false);
            if (t.getPriority() != Thread.NORM_PRIORITY)
                t.setPriority(Thread.NORM_PRIORITY);
            return t;
        }

        static int getThreads() {
            return group.activeCount();
        }

        static void listThreads() {
            Thread[] list = new Thread[getThreads()];
            group.enumerate(list, false);
            for (Thread t : list) {
                if ((t != null) && (t.getThreadGroup().getName().equals(THREAD_GROUP_NAME))) log.info(t.toString());
            }
        }
    }

    public static ExecutorService getExecutor(String label) {
        log.trace("New Executor - newSingleThreadExecutor");
        ExecutorService e = Executors.newSingleThreadExecutor(new ExecThreadFactory(label));
        executors.add(e);
        return e;
    }

    public static void cleanUp() {
        log.trace("Executor cleanup - started");

        for (ExecutorService exec : executors) {
            exec.shutdown();
            while (!exec.isTerminated()) try {
                sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        log.debug("All Executors now shutdown");
    }

    public static void monitor() {
        log.info("Monitoring -- ExecutorPool size: " + getSize() + " Active Threads: " + getActiveThreads());
        ExecThreadFactory.listThreads();
    }

    public static int getSize() {
        return executors.size();
    }

    private static int getActiveThreads() {
        return ExecThreadFactory.getThreads();
    }

    private ExecutorPool() {
    }

    private final static Logger log = LoggerFactory.getLogger(ExecutorPool.class);
}
