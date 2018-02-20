package net.martinprobson.hiveutils.hqlwrapper;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


/**
 * Queue of TaskNodes.
 */
public class TaskQueue implements Iterable<TaskNode> {

    /**
     * Construct an empty TaskQueue.
     */
    TaskQueue() {
        this(null, Collections.emptyMap(), 0);
    }


    TaskQueue(String directory) {
        this(new Path(directory), Collections.emptyMap(), 0);
    }

    TaskQueue(String directory, Map<String, String> params) {
        this(new Path(directory), params, 0);
    }


    /**
     * Construct a new TaskQueue based on contents of directory on file system.
     *
     * @param directory - points to directory on file system containing tasks configuration.
     * @param params    - Map of parameters which will be substituted into the HQL script.
     * @param level     - The depth of the task queue, 0 - root level.
     */
    TaskQueue(Path directory, Map<String, String> params, int level) {
        if (directory == null) {
            log.trace("Level: " + level + " Building empty TaskQueue");
            this.label = "Empty";

        } else {
            log.trace("Level: " + level + " Building task queue from root directory: " + directory + " with params: " + params);
            this.level = level;
            this.label = directory.getName();
            this.params = params;
            log.trace("Level: " + level + " Building task queue from root directory: " + directory + " with params: " + params);
            Path[] paths = FileUtil.listFiles(directory, name -> (FileUtil.isDirectory(name) || name.getName().toLowerCase().endsWith(".hql")));
            assert paths != null;
            Arrays.sort(paths);
            for (Path p : paths) {
                TaskNode taskNode;
                if (FileUtil.isDirectory(p)) {
                    taskNode = new TaskNode(p, new TaskQueue(p, params, this.level + 1));
                } else {
                    taskNode = new TaskNode(p);
                }
                taskQueue.add(taskNode);
            }
        }
    }

    /**
     * Add a TaskNode to a TaskQueue.
     *
     * @param t - TaskNode to be added.
     */
    public void add(TaskNode t) {
        taskQueue.add(t);
    }

    /**
     * Return size of this TaskQueue
     *
     * @return - No of entries in the TaskQueue
     */
    public int size() {
        return taskQueue.size();
    }


    @Override
    public Iterator<TaskNode> iterator() {
        return taskQueue.iterator();
    }

    @Override
    public String toString() {
        StringBuilder s = new StringBuilder();
        if (level == 0) {
            s.append("Params: ").append(getParams());
        }
        for (TaskNode t : taskQueue) {
            String indent = String.join("", Collections.nCopies(level * 3, " "));
            s.append("\n Level: ").append(level).append(indent).append(" - ").append(t);
            if (t.getTasks().size() != 0) s.append(" ").append(t.getTasks());
        }
        return s.toString();
    }

    public String getLabel() {
        return label;
    }

    public Map<String, String> getParams() {
        return params;
    }

    private Queue<TaskNode> taskQueue = new LinkedList<>();
    private final static Logger log = LoggerFactory.getLogger(TaskQueue.class);
    private Map<String, String> params = Collections.emptyMap();
    private final String label;
    private int level;

}
