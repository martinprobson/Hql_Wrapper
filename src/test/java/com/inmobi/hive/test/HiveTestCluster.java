package com.inmobi.hive.test;

import java.io.File;
import java.io.FileFilter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.apache.hive.service.cli.HiveSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/*
 * This is class is used to model a minicluster and mini hive server to be 
 * used for testing.  
 */
public class HiveTestCluster {

    private final static Logger log = LoggerFactory.getLogger(HiveTestCluster.class);
    private FileSystem fs;
    private MiniHS2 miniHS2 = null;
    private Map<String, String> confOverlay;
    private HiveConf hiveConf;

    public void start() throws Exception {
        Configuration conf = new Configuration();
        hiveConf = new HiveConf(conf, 
                org.apache.hadoop.hive.ql.exec.CopyTask.class);
        miniHS2 = new MiniHS2(hiveConf, true);
        confOverlay = new HashMap<String, String>();
        confOverlay.put(ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
        confOverlay.put(MRConfig.FRAMEWORK_NAME, MRConfig.LOCAL_FRAMEWORK_NAME);
        // Workaround hashtable mapjoin issue - see https://stackoverflow.com/a/40115876
        confOverlay.put(ConfVars.SUBMITLOCALTASKVIACHILD.varname, "false");
        miniHS2.start(confOverlay);
        fs = miniHS2.getDfs().getFileSystem();
        SessionState ss = new SessionState(hiveConf);
        SessionState.start(ss);
    }
    
    public FileSystem getFS() {
        return this.fs;
    }
    
    public void stop() throws Exception {
        LocalFileSystem localFileSystem = FileSystem.getLocal(miniHS2.getHiveConf());
        miniHS2.stop();
        FileFilter filter = new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                if (pathname.isDirectory() && 
                        pathname.getName().startsWith("MiniMRCluster_")) {
                    return true;
                }
                return false;
            }
        };
        File targetDir = new File("target");
        File[] files = targetDir.listFiles(filter);
        for (File file : files) {
            Path clusterRoot = new Path(file.getAbsolutePath());
            localFileSystem.delete(clusterRoot, true);
        }
    }
    
    public List<String> executeStatements(List<String> statements) throws HiveSQLException {
        List<String> results = new ArrayList<>();
        for (String statement : statements) {
            results.addAll(processStatement(statement));
        }
        return results;
    }
    
    private List<String> processStatement(String statement) throws HiveSQLException {
        List<String> results = new ArrayList<String>();
        String[] tokens = statement.trim().split("\\s+");
        // Hive does special handling for the commands:
        //   SET,RESET,DFS,CRYPTO,ADD,LIST,RELOAD,DELETE,COMPILE
        CommandProcessor proc = null;
        try {
            proc = CommandProcessorFactory.getForHiveCommand(tokens, hiveConf);
        } catch (SQLException ex) {
            throw new HiveSQLException("CommandProcessorFactory.getForHiveCommand",ex);
        }
        if (proc == null) {
            // this is for all other commands
            proc = new Driver(hiveConf);
        }
        try {
            CommandProcessorResponse resp = proc.run(statement);
            if (resp.getResponseCode() != 0)
                if (resp.getException() != null)
                    throw new HiveSQLException("HQL Error: " + resp,resp.getException());
                else
                    throw new HiveSQLException("HQL Error: " + resp);
            if (proc instanceof org.apache.hadoop.hive.ql.Driver) {
                ((Driver) proc).getResults(results);
            }
            // else these are one of specially handled ones with no results
        } catch (CommandNeedRetryException ex) {
            throw new HiveSQLException("Hive SQL exception", ex);
        } catch (java.io.IOException ex) {
            throw new HiveSQLException("Hive SQL exception", ex);
        }
        return results;
    }

}
