package net.martinprobson.hiveutils.hqlwrapper;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class representing a Task.
 */
public class TaskNode {

    /**
     * @param hql      - String containing the HQL to be executed.
     * @param subTasks - TaskQueue of sub-tasks for this TaskNode
     * @param label    - String label for this TaskNode.
     */
    TaskNode(String hql, TaskQueue subTasks, String label) {
        this.path = null;
        this.hql = hql;
        this.label = label;
        if (subTasks == null)
            this.subTasks = new TaskQueue();
        else
            this.subTasks = subTasks;
        this.result = TaskResult.NOT_STARTED;
        log.trace("Building new TaskNode: Path = " + path + " hql = " + hql + " label = " + label);
    }

    /**
     * @param hql   - String containing the HQL to be executed.
     * @param label - String label for this TaskNode.
     */
    TaskNode(String hql, String label) {
        this(hql, null, label);
    }


    /**
     * @param path     - Path containing the HQL to be executed.
     * @param subTasks - TaskQueue of sub-task for this TaskNode
     */
    TaskNode(Path path, TaskQueue subTasks) {
        this.path = path;
        this.hql = null;
        this.label = path.getName();
        if (subTasks == null)
            this.subTasks = new TaskQueue();
        else
            this.subTasks = subTasks;
        this.result = TaskResult.NOT_STARTED;
    }

    /**
     * @param path - File containing the HQL to be executed.
     */
    TaskNode(Path path) {
        this(path, null);
    }

    /**
     * @return String label for this TaskNode.
     */
    public String getLabel() {
        return label;
    }

    /**
     * @return String hql statement(s) for this TaskNode
     */
    public String getHql() {
        /* Does this TaskNode represent the parent of a set of sub-tasks or a single HQL file/string? */
        if (getTasks().size() == 0) {
            /* This is a task representing a single HQL file/string. */
            if (hql == null) {
                if (path != null)
                    hql = FileUtil.readFile(path);
            }
        }
        return hql;
    }

    /**
     * @return TaskQueue of TaskNodes representing sub-tasks.
     */
    public TaskQueue getTasks() {
        return subTasks;
    }

    /**
     * @return TaskResult for this TaskNode
     */
    public TaskResult getResult() {
        return result;
    }

    /**
     * @param result Set result for this TaskNode.
     */
    public void setResult(TaskResult result) {
        log.trace("Task node" + this + " status from = " + this.result + " status to = " + result);
        this.result = result;
    }

    public String toString() {
        StringBuilder s = new StringBuilder("Label: " + getLabel());
        if (path != null) s.append(" File: ").append(path.getName()).append(" Type:");
        if (getTasks().size() != 0) {
            s.append(" Branch");
        } else {
            s.append(" Leaf");
        }
        return s.toString().trim();
    }

    private Path path;
    private String hql;
    private String label;
    private TaskQueue subTasks;
    private TaskResult result;
    private final static Logger log = LoggerFactory.getLogger(TaskNode.class);
}
