package net.martinprobson.hiveutils.hqlwrapper;

import java.util.concurrent.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static net.martinprobson.hiveutils.hqlwrapper.ControllerConfiguration.DRY_RUN;
import static net.martinprobson.hiveutils.hqlwrapper.ControllerConfiguration.MONITOR_INTERVAL;


/**
 * Execute a Queue of TaskNodes (see {@link net.martinprobson.hiveutils.hqlwrapper.TaskNode}) potentially in multiple threads.
 * @author martinr
 *
 */
public class TaskExecutor implements Callable<TaskResult> {
	
	private TaskQueue taskQueue;
	private List<Future<TaskResult>> subTasks = new ArrayList<>();
	
	/**
	 * Construct a new TaskExecutor with a new TaskQueue of zero to many TaskNodes (see {@link net.martinprobson.hiveutils.hqlwrapper.TaskQueue})
	 */	
	TaskExecutor(TaskQueue taskQueue) {
		log.debug("New TaskExecutor class for task queue: " + taskQueue);
		this.taskQueue = taskQueue;
	}

	/**
	 * Start execution of a Queue of TaskNodes.
	 * @return TaskResult Results of Task execution (see {@link net.martinprobson.hiveutils.hqlwrapper.TaskResult})
	 */	
	@Override
	public TaskResult call() {
		log.info("Starting execution of task queue: " + taskQueue.getLabel());
		for(TaskNode taskNode : taskQueue) {
			if (taskNode.getTasks().size() == 0) {		// What type of node is this?
				if (subTasks.size() != 0) {				// Have we launched any sub-tasks, if so wait for them to complete (or fail/timeout) before running any more (subsequent) single tasks
					List<TaskResult> results = monitorSubTasks();
					boolean ok = true;
					for (TaskResult r : results) {
						if (r != TaskResult.SUCCESS) {
							ok = false;
							break;
						}
					}
					if (!ok) log.error(taskQueue.getLabel() + ": One or more subtasks failed");
				}
				boolean rc;
				if (Controller.getInstance().getConf().getBoolean(DRY_RUN,false)) {
					log.info("Dry run - " + taskNode.getLabel() + " - skipping execution setting result to SUCCESS");
					taskNode.setResult(TaskResult.SUCCESS);					
					rc = true;
				} else {
					log.info("Execute taskNode with label: " + taskNode.getLabel());
					rc = Hive.ExecuteHqlStmts(taskNode,taskQueue.getParams());
					log.info("Execute taskNode with label: " + taskNode.getLabel() + " - Complete - Result is: " + taskNode.getResult());
				}
				if (!rc) {
					log.error(taskNode.getLabel() + " failed!");
					return TaskResult.FAILURE;
				}
			}
			else {										// this tasknode contains a list of tasks to run, 
				TaskExecutor task = new TaskExecutor(taskNode.getTasks());
				ExecutorService executor = ExecutorPool.getExecutor(taskNode.getTasks().getLabel());
				Future<TaskResult> future = executor.submit(task);	// Start a new TaskExecutor to process the subtasks. 
				subTasks.add(future);				
			}
		}
		return TaskResult.SUCCESS;
	}
	
	private List<TaskResult> monitorSubTasks() {
		
		List<TaskResult> results = new ArrayList<>();
		int sleepInterval = Controller.getInstance().getConf().getInt(MONITOR_INTERVAL,10) * 1000;
		
		while (subTasks.size() != 0) {
			ExecutorPool.monitor();
			try {
				Thread.sleep(sleepInterval);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			Iterator<Future<TaskResult>> ite = subTasks.iterator();
			while(ite.hasNext()) {
				Future<TaskResult> t = ite.next();
				if (t.isDone()) {
					try {
						TaskResult re = t.get();								// this should be get(timeout)?
						results.add(re);
					}
					catch (CancellationException e) {
						results.add(TaskResult.CANCELLED);
					}
					catch (ExecutionException e) {
						results.add(TaskResult.EXECUTION_EX);
					}
					catch (InterruptedException e) {
						results.add(TaskResult.INTERRUPTED_EX);
					}
					finally {
						ite.remove();
					}
				}
			}
		}
		return results;
	}
	
	
	private final static Logger log = LoggerFactory.getLogger(TaskExecutor.class);


}
