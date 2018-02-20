package net.martinprobson.hiveutils.hqlwrapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.callback.*;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URL;

import static net.martinprobson.hiveutils.hqlwrapper.ControllerConfiguration.*;


/**
 * Get a valid Kerboros ticket based on jdbc.user/jdbc.password and kerboros.principal specified in configuration.
 * <p>
 */
public class Kerboros {

    private static final Kerboros me;

    static {
        me = new Kerboros();
    }


    class MyCallbackHandler implements CallbackHandler {
        public void handle(Callback[] callbacks)
                throws UnsupportedCallbackException {

            for (Callback callback : callbacks) {
                if (callback instanceof NameCallback) {
                    NameCallback nc = (NameCallback) callback;
                    nc.setName(getLoginDomain());
                } else if (callback instanceof PasswordCallback) {
                    PasswordCallback pc = (PasswordCallback) callback;
                    pc.setPassword(getLoginPassword().toCharArray());
                } else throw new UnsupportedCallbackException
                        (callback, "Unrecognised callback");
            }
        }
    }

    private static String getLoginDomain() {
        String s = Controller.getInstance().getConf().get(JDBC_USERNAME) + "@" + Controller.getInstance().getConf().get(KERBOROS_PRINCIPAL);
        log.trace("Kerboros LoginDomain: " + s);
        return s;
    }

    private static String getLoginPassword() {
        return Controller.getInstance().getConf().get(JDBC_PASSWORD);
    }

    //TODO auth_lib is currently not working correctly if a valid ticket does not exist in cache. Replace with auth_cmd for now.
    @SuppressWarnings("unused")
    private static void auth_lib() {
        System.setProperty("sun.security.krb5.debug", "true");
        if (System.getProperty("java.security.auth.login.config") == null) {
            String ConfigFile;
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            URL ConfigURL = cl.getResource("login.conf");
            assert ConfigURL != null;
            ConfigFile = ConfigURL.getFile();
            System.setProperty("java.security.auth.login.config", ConfigFile);
        }
        Subject subject = new Subject();
        LoginContext lc = null;
        try {
            lc = new LoginContext("kerboros", subject, me.new MyCallbackHandler());
        } catch (LoginException e) {
            e.printStackTrace();
            log.error("Kerboros LoginException - LoginContext failure");
            System.exit(2);
        }
        try {
            lc.login();
        } catch (FailedLoginException e) {
            e.printStackTrace();
            log.error("Kerboros FailedLoginException - login failure");
            System.exit(2);
        } catch (LoginException e) {
            e.printStackTrace();
            log.error("Kerboros LoginException - login failure");
            System.exit(2);
        }
        log.trace("Successfully obtained kerboros login context.");
    }

    //TODO Hack to bypass Kerboros auth problems
    private static void auth_cmd() {
        String logon = " kinit " + getLoginDomain();
        log.trace("Attempting Kerboros logon using: " + logon);
        log.trace("Logon: " + runCmd(logon, getLoginPassword()));
        log.trace("klist returns: " + runCmd("klist", null));
    }


    public static void auth() {
        if (Controller.getInstance().getConf().get(KERBOROS_PRINCIPAL) != null)
            auth_cmd();
    }

    private static String runCmd(String cmd, String input) {

        StringBuilder sb = new StringBuilder();
        try {
            Process p = Runtime.getRuntime().exec(cmd);
            if (input != null) {
                OutputStream out = p.getOutputStream();
                String s = input + "\n";
                out.write(s.getBytes());
                out.flush();
            }
            p.waitFor();
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(p.getInputStream())
            );
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }

        } catch (IOException | InterruptedException e1) {
            e1.printStackTrace();
            System.exit(2);
        }
        return sb.toString();
    }

    private Kerboros() {
    }

    private final static Logger log = LoggerFactory.getLogger(Kerboros.class);

    public static void main(String[] args) {
        auth();
    }
}
