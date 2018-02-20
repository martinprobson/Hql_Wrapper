package net.martinprobson.hiveutils.hqlwrapper;


import static net.martinprobson.hiveutils.hqlwrapper.ControllerConfiguration.DRY_RUN;
import static org.junit.Assert.*;

import java.io.File;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static net.martinprobson.hiveutils.hqlwrapper.ControllerConfiguration.HQL_FILE_ROOT_DIR;

public class ControllerTest {

	private static Controller controller = null;

	@BeforeClass
	public static void setup() {
		controller = Controller.getInstance();
	}

	@AfterClass
	public static void tearDown() {
		controller.reset();
	}

	@Test
	public void testExecuteTaskQueue() {
		assertTrue(true);
	}

	private Boolean checkResults(TaskQueue tq) {
		Boolean ok = true;
		for(TaskNode taskNode: tq) {
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
	
	@Test
	public void testRun() {
		Controller cont = Controller.getInstance();
		/* We need to force in the full pathname of the test root directory that
		 * lives in the maven test/resources area.
		 * Also set Dry Run so that nothing is actually executed.
		 */
		File file = new File(getClass().getClassLoader().getResource("testconfig").getFile());
		cont.getConf().set(HQL_FILE_ROOT_DIR,file.getPath());
		cont.getConf().setBoolean(DRY_RUN, true);

		
		try {
			cont.run();
		} catch (HqlWrapperException e) {
			fail("HqlWrapperException: " + e);
		}
		assertTrue("TaskQueue did not execute successfully",checkResults(cont.getTaskQueue()));
	}

}
