package net.martinprobson.hiveutils.hqlwrapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

import static net.martinprobson.hiveutils.hqlwrapper.ControllerConfiguration.DEFAULT_HQL_FILESYSTEM;
import static net.martinprobson.hiveutils.hqlwrapper.ControllerConfiguration.HQL_FILESYSTEM;


/**
 * Utility class used for manipulation of HQL files.
 * <p>
 * This utility class is used to process all I/O operations relating to processing of file containing the Hive
 * statements to be executed.
 * <p>
 * It uses the Hadoop abstract class  FileSystem {@link  org.apache.hadoop.fs.FileSystem} allowing HQL files
 * to be stored on the local filesystem or within HDFS itself. This is controlled via the configuration parameter: -
 * <ul>
 * <li><code>Hql.FileSystem</code>
 */
public class FileUtil {

    private static FileSystem fs = null;
    private final static Logger log = LoggerFactory.getLogger(FileUtil.class);

    private FileUtil() {
    }

    private static FileSystem getFs() throws IOException {
        if (fs == null) {
            Configuration conf = Controller.getInstance().getConf();
            String hqlFileSys = conf.get(HQL_FILESYSTEM, DEFAULT_HQL_FILESYSTEM);
            if (hqlFileSys.equals(DEFAULT_HQL_FILESYSTEM)) {
                fs = FileSystem.getLocal(conf);
                log.trace("Getting local filesystem" + fs.getUri());
            } else {
                Kerboros.auth();
                fs = FileSystem.get(conf);
                log.trace("Getting hdfs filesystem" + fs.getUri());
            }
        }
        return fs;
    }

    public static boolean exists(Path path) {
        try {
            getFs().getFileStatus(path);
        } catch (FileNotFoundException e) {
            return false;
        } catch (IOException e) {
            log.error("Error in exists call for file: " + path, e);
            System.exit(2);
        }
        return true;
    }


    public static boolean exists(String path) {
        return exists(new Path(path));
    }

    public static boolean isDirectory(Path path) {
        FileStatus stat;
        boolean rc = false;
        try {
            stat = getFs().getFileStatus(path);
            rc = stat.isDirectory();
        } catch (IOException e) {
            log.error("Error isDirectory call for file: " + path, e);
            System.exit(2);
        }
        return rc;
    }

    public static boolean isDirectory(String path) {
        return isDirectory(new Path(path));
    }

    public static Path[] listFiles(Path directory, PathFilter filter) {
        ArrayList<Path> paths = new ArrayList<>();
        FileStatus[] status = null;
        try {
            status = getFs().listStatus(directory, filter);
        } catch (FileNotFoundException e) {
            return null;
        } catch (IOException e) {
            log.error("Error listing directory: " + directory, e);
            System.exit(2);
        }
        for (FileStatus fs : status)
            paths.add(fs.getPath());
        return paths.toArray(new Path[paths.size()]);
    }

    public static String readFile(Path fileName) {
        StringBuilder line = new StringBuilder();
        try {
            FSDataInputStream in = getFs().open(fileName);
            BufferedReader br = new BufferedReader(new InputStreamReader(in, Charset.defaultCharset()));
            line = new StringBuilder();
            String tmp;
            while ((tmp = br.readLine()) != null)
                line.append(tmp).append("\n");
            br.close();
        } catch (IOException e) {
            log.error("Error reading file: " + fileName, e);
            System.exit(2);
        }
        return (line.toString());

    }

    /**
     * Read a file
     *
     * @param path     - file to be read
     * @param encoding - charset encoding.
     * @return String representation of file.
     */
    public static String readLocalFile(String path, Charset encoding)
            throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded, encoding);
    }

}

