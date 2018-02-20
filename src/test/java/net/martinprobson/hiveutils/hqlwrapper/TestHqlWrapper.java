package net.martinprobson.hiveutils.hqlwrapper;


import com.inmobi.hive.test.HiveTestSuite;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static net.martinprobson.hiveutils.hqlwrapper.ControllerConfiguration.HQL_FILE_ROOT_DIR;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestHqlWrapper {

    private final static Logger log = LoggerFactory.getLogger(TestHqlWrapper.class);
    private static HiveTestSuite testSuite;
    private static Controller controller;

    @BeforeClass
    public static void setUp() throws Exception {
        log.debug("In TestHqlWrapper setup method");
        testSuite = new HiveTestSuite();
        testSuite.createTestCluster();
        controller = Controller.getInstance();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        log.debug("In TestHqlWrapper tearDown method");
        testSuite.shutdownTestCluster();
        controller.reset();
    }

    @Test
    public void testRun() {
        Controller controller = Controller.getInstance();
        /* We need to force in the full pathname of the test root directory that
         * lives in the maven test/resources area.
         * Also set Dry Run so that nothing is actually executed.
         */
        File file = new File(getClass().getClassLoader().getResource("root2").getFile());
        controller.getConf().set(HQL_FILE_ROOT_DIR, file.getPath());
        Map<String,String> parms = new HashMap();
        String empData = new File(getClass().getClassLoader().getResource("data/emps.csv").getFile()).getPath();
        parms.put("EMP_DATA",empData);
        String deptData = new File(getClass().getClassLoader().getResource("data/dept_emp.csv").getFile()).getPath();
        parms.put("DEPT_EMP_DATA",deptData);
        String outDir = new File(getClass().getClassLoader().getResource("data/").getFile()).getPath() + "output";
        parms.put("OUTPUT_DIR",outDir);

        try {
            controller.run(parms);
        } catch (HqlWrapperException e) {
            fail("HqlWrapperException: " + e);
        }
        assertTrue("TaskQueue did not execute successfully", checkResults(controller.getTaskQueue()));
    }

    private Boolean checkResults(TaskQueue tq) {
        Boolean ok = true;
        for (TaskNode taskNode : tq) {
            if (taskNode.getTasks().size() == 0) {
                //TODO This is messy.......
                if (taskNode.getResult() != TaskResult.SUCCESS) {
                    ok = false;
                    break;
                }
            } else {
                ok = checkResults(taskNode.getTasks());
            }
        }
        return ok;
    }

}
