package net.martinprobson.hiveutils.hqlwrapper;

import org.apache.commons.lang3.text.StrSubstitutor;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Responsible for executing Hive SQL against a database connection.
 *
 * @author robsom12
 */
public class Hive {

    /**
     * Given a TaskNode containing  hql statements,
     * split the statements and execute each one in turn.
     * <p>
     *
     * @param taskNode TaskNode containing hql statement(s) to be run.
     * @return returns <code>true</code> if hql file successfully executed, <code>false</code> otherwise.
     */
    public static boolean ExecuteHqlStmts(TaskNode taskNode, Map<String, String> params) {
        taskNode.setResult(TaskResult.RUNNING);
        boolean rc = ExecuteHqlStmts(taskNode.getHql(), params);
        if (rc)
            taskNode.setResult(TaskResult.SUCCESS);
        else
            taskNode.setResult(TaskResult.FAILURE);
        return rc;
    }

    /**
     * Given a String containing one or more hql statements (separated by ';' character),
     * split the statements and execute each one in turn.
     * <p>
     *
     * @param hqlStmts String containing hql statement(s) to be run.
     * @return returns <code>true</code> if hql file successfully executed, <code>false</code> otherwise.
     */
    private static boolean ExecuteHqlStmts(String hqlStmts, Map<String, String> params) {
        log = LoggerFactory.getLogger(Hive.class);
        if (params != null) {
            substitutor = new StrSubstitutor(params);
        }


        Connection conn = null;
        boolean rc = true;

        try {
            conn = DBSource.setupDataSource().getConnection();
            int i = 0;
            List<String> stmts = Util.HQLSplit(hqlStmts);
            log.trace("String contains a string" + stmts.size() + " statements");
            for (String stmt : stmts) {
                log.trace("About to execute statment no: " + ++i);
                log.trace("Statement before substitution: " + stmt);
                stmt = replaceParams(stmt);
                log.trace("Statement after substitution: " + stmt);
                if (!ExecHQL(conn, stmt)) {
                    log.error("Statement number: " + i);
                    log.error("HQL statement: " + stmt + " failed");
                    log.trace("Skipping rest of String");
                    rc = false;
                    break;
                } else {
                    log.trace("Statement number: " + i + " success");
                }
            }
        } catch (SQLException e) {
            log.error("SQLException:");
            while (e != null) {
                log.error("SQLException:", e);
                log.error("SQLState: " + e.getSQLState());
                log.error("Message: " + e.getMessage());
                log.error("Vendor: " + e.getErrorCode());
                e = e.getNextException();
            }
            rc = false;
        } finally {
            try {
                if (conn != null) conn.close();
            } catch (Exception e) {
                log.error("Error on conn close", e);
                rc = false;
            }
        }
        return rc;
    }


    /**
     * Read a file containing HQL statements, split it into separate statements and execute each statement in turn.
     * <p>
     *
     * @param file containing hql statements to be run.
     * @return returns <code>true</code> if hql file successfully executed, <code>false</code> otherwise.
     */
    public static boolean ExecuteHqlFile(Path file) {
        log = LoggerFactory.getLogger(Hive.class);
        log.trace("passed filename: " + file.getName());

        String hqlFile;
        boolean rc;

        hqlFile = FileUtil.readFile(file);
        rc = ExecuteHqlStmts(hqlFile, null);

        return rc;
    }


    /**
     * Execute single HQL statement.
     * <p>
     *
     * @param conn    - DB Connection to run against.
     * @param hqlStmt - HQL statement to execute.
     * @return returns <code>true</code> if hql file successfully executed, <code>false</code> otherwise.
     */
    private static boolean ExecHQL(Connection conn, String hqlStmt) {

        boolean rc = true;
        Kerboros.auth();
        log = LoggerFactory.getLogger(Hive.class);
        log.trace("passed statement: " + hqlStmt);
        Statement stmt = null;
        try {
            if (conn == null) {
                DataSource dataSource = DBSource.setupDataSource();
                conn = dataSource.getConnection();
            }
            stmt = conn.createStatement();
            log.debug("About to execute statement: " + hqlStmt);
            stmt.execute(hqlStmt);

        } catch (SQLException e) {
            log.error("SQLException:");
            while (e != null) {
                log.error("SQLException:", e);
                log.error("SQLState: " + e.getSQLState());
                log.error("Message: " + e.getMessage());
                log.error("Vendor: " + e.getErrorCode());
                e = e.getNextException();
            }
            rc = false;

        } finally {
            try {
                if (stmt != null) stmt.close();
            } catch (Exception e) {
                log.error("Error on stmt close");
                rc = false;
            }

        }
        if (!rc) MailFailure(hqlStmt);
        return rc;
    }

    /**
     * Execute single HQL statement.
     * <p>New DB connection will be opened to execute the statement.
     *
     * @param stmt - HQL statement to execute.
     * @return returns <code>true</code> if hql file successfully executed, <code>false</code> otherwise.
     */
    public static boolean ExecHQL(String stmt) {
        return ExecHQL(null, stmt);
    }

    /**
     * Send an email (if configured to do so) on HQL statement failure.
     *
     * @param stmt - HQL statement that failed.
     */
    private static void MailFailure(String stmt) {
        if (Controller.getInstance().getConf().getBoolean(ControllerConfiguration.SEND_MAIL_ON_FAILURE, false)) {
            Util.SendMail(Controller.getInstance().getConf().get(ControllerConfiguration.SEND_MAIL_FROM), Controller.getInstance().getConf().getStrings(ControllerConfiguration.SEND_MAIL_TO), "Hive Execution failure", stmt + "Failed - refer to log file");
        }
    }

    @SuppressWarnings("unused")
    private static void traceResultSetMetaData(ResultSet rs, Logger log) throws SQLException {
        if (log.isTraceEnabled()) {
            if (jdbcTypeMappings == null) jdbcTypeMappings = getJdbcTypeMappings();
            ResultSetMetaData rm = rs.getMetaData();
            log.trace("ResultSet Column Count: " + rm.getColumnCount());
            for (int i = 1; i <= rm.getColumnCount(); i++)
                log.trace("Column Name " + i + ":" + rm.getColumnName(i) + "Column Type :" + jdbcTypeMappings.get(rm.getColumnType(i)));
        }
    }


    private static Map<Integer, String> getJdbcTypeMappings() {

        Map<Integer, String> result = new HashMap<>();
        for (Field field : Types.class.getFields())
            try {
                result.put((Integer) field.get(null), field.getName());
            } catch (IllegalArgumentException | IllegalAccessException e) {
                e.printStackTrace();
            }
        return result;
    }

    private static String replaceParams(String line) {
        if (substitutor == null) {
            return line;
        }
        return substitutor.replace(line);
    }


    private static Map<Integer, String> jdbcTypeMappings;
    private static Logger log;
    private static StrSubstitutor substitutor;
}


