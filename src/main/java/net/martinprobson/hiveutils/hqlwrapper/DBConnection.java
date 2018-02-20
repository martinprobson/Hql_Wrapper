package net.martinprobson.hiveutils.hqlwrapper;

import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.hadoop.conf.Configured;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static net.martinprobson.hiveutils.hqlwrapper.ControllerConfiguration.*;

/**
 * DBConnection implements ConnectionFactory (see
 * <a href="https://commons.apache.org/proper/commons-dbcp/api-2.1.1/org/apache/commons/dbcp2/ConnectionFactory.html">org.apache.commons.dbcp2.ConnectionFactory</a> )
 * to supply DB Connection pool with a new DB Connection when requested.
 *
 * @author martinr
 */
public class DBConnection extends Configured implements ConnectionFactory {

    /**
     * Gives a new DB Connection.
     * <p>
     * Append current date (YYYYMMDD) to connection URL so HQL can use ${hiveconf:run_date} in scripts.
     */
    DBConnection() {
        super(new ControllerConfiguration());
        log = LoggerFactory.getLogger(DBConnection.class);
        url = buildURL();
        username = getConf().get(JDBC_USERNAME);
        password = getConf().get(JDBC_PASSWORD);
    }


    /**
     * @see org.apache.commons.dbcp2.ConnectionFactory#createConnection()
     */
    @Override
    public Connection createConnection() throws SQLException {
        log.trace("Get connection: URL: " + url + " User: " + username);
        Kerboros.auth();
        return DriverManager.getConnection(url, username, password);
    }

    /**
     * Allow callback (URLAppender) class to be added to allow connection URL to be modified with custom hive parameters.
     */
    public static void addURLAppender(URLAppender appender) {
        appenders.add(appender);
    }

    /**
     * Build connection URL and call registered URLAppenders to complete construction of the connection string.
     * <p>Hive connection URL is in the format: -
     * <pre>
     * jdbc:hive2://<host1>:<port1>,<host2>:<port2>/dbName;sess_var_list?hive_conf_list#hive_var_list
     * </pre>
     * <p>where: -
     * <ul>
     * <li><pre>host1:port1,host2:port2</pre> is a server instance or a comma separated list of server instances to connect to (if dynamic service discovery is enabled). If empty, the embedded server will be used.
     * <li><pre>sess_var_list</pre> is a semicolon separated list of key=value pairs of session variables (e.g., user=foo;password=bar).
     * <li><pre>hive_conf_list</pre>is a semicolon separated list of key=value pairs of Hive configuration variables for this session.
     * <li><pre>hive_var_list</pre>is a semicolon separated list of key=value pairs of Hive variables for this session.
     * </ul>
     */
    private String buildURL() {
        String drivers = getConf().get(JDBC_DRIVERS);
        if (drivers != null) {
            log.debug("Got JDBC Driver: " + drivers);
            System.setProperty(JDBC_DRIVERS, drivers);
            try {
                Class.forName(drivers);
            } catch (ClassNotFoundException e) {
                log.error("JDBC Driver class not found" + drivers, e);
                e.printStackTrace();
            }
            log.debug("JDBC Driver " + drivers + " loaded successfully");

        }
        url = getConf().get(JDBC_URL);
        // Allow all the registered callback classes to modify the URL as required
        if (drivers != null) {

            if (appenders.size() > 0) url += "#";
            for (URLAppender appender : appenders) {
                url = appender.appendURL(url) + ";";
            }
        }
        username = getConf().get(JDBC_USERNAME);
        password = getConf().get(JDBC_PASSWORD);

        return url;
    }


    /**
     * Stub for testing only.
     */
    public static void main(String[] args) throws SQLException {
        DBConnection dbc = new DBConnection();
        dbc.createConnection();
    }

    private static List<URLAppender> appenders = new ArrayList<>();
    private static Logger log = LoggerFactory.getLogger(DBConnection.class);
    private String url;
    private String username;
    private String password;
}
