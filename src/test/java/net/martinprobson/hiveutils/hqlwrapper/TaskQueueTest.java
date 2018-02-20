package net.martinprobson.hiveutils.hqlwrapper;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


public class TaskQueueTest {

    private static TaskQueue tq = null;
    private static String expResult = null;

    @BeforeClass
    public static void setUp() throws Exception {
        File f = new File(TaskQueue.class.getResource("/root").getFile());
        Map<String,String> p = new HashMap<>();
        p.put("TEST_PARAM1","TEST_VAL1");
        p.put("TEST_PARAM2","TEST_VAL2");
        tq = new TaskQueue(f.getAbsolutePath(),p);
        expResult = readFile(new File(TaskQueue.class.getResource("/TaskQueueTest_expected").getFile()),
                                                                   Charset.defaultCharset());
    }

    @Test
    public void testTaskQueue() {
        TaskQueue tq = new TaskQueue();
        assertNotNull("Valid Configuration reference", tq);
        assertEquals(tq.getLabel(), "Empty");
    }

    @Test
    public void testTaskQueueFile() {
        assertEquals(tq.toString().trim(), expResult.trim());
    }

    @Test
    public void testAdd() {
        TaskQueue tq = new TaskQueue();
        tq.add(new TaskNode("test", "testLabel"));
        assertEquals(tq.iterator().next().getLabel(), "testLabel");
    }

    @Test
    public void testSize() {
        assertEquals(tq.size(), 6);
    }

    @Test
    public void testIterator() {
        TaskQueue tq = new TaskQueue();
        tq.add(new TaskNode("1", "1"));
        tq.add(new TaskNode("2", "2"));
        tq.add(new TaskNode("3", "3"));
        Iterator<TaskNode> it = tq.iterator();
        assertEquals(it.next().getLabel(), "1");
        assertEquals(it.next().getLabel(), "2");
        assertEquals(it.next().getLabel(), "3");
    }

    @Test
    public void testToString() {
        assertEquals(expResult.trim(), tq.toString().trim());
    }

    @Test
    public void testGetLabel() {
        assertEquals(tq.getLabel(), "root");
    }

    /**
     * Read a file
     *
     * @param file     - file to be read
     * @param encoding - charset encoding.
     * @return String representation of file.
     */
    private static String readFile(File file, Charset encoding)
            throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(file.getPath()));
        return new String(encoded, encoding);
    }

}
