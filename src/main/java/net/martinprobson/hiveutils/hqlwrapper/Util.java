/**
 *
 */
package net.martinprobson.hiveutils.hqlwrapper;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.activation.DataHandler;
import javax.activation.FileDataSource;
import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import java.security.Security;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static net.martinprobson.hiveutils.hqlwrapper.ControllerConfiguration.*;


/**
 * Utility class
 *
 * @author robsom12
 */
public class Util {


    /**
     * Split a string into separate HQL statements.
     *
     * @param hql - String containing HQL
     * @return a list of HQL statements
     */
    public static List<String> HQLSplit(String hql) {
        String str = stripComments(hql);
        List<String> stmts = new ArrayList<>();
        boolean in_sQuote = false, in_dQuote = false;
        StringBuilder stmt = new StringBuilder();

        for (final char c : str.toCharArray()) {
            if (c == '\'')
                in_sQuote = !in_sQuote;
            if (c == '\"')
                if (in_dQuote) in_dQuote = false;
                else in_dQuote = true;
            if (c == ';') {
                if (!in_dQuote && in_sQuote == false) {
                    stmts.add(stmt.toString());
                    stmt = new StringBuilder();
                    continue;
                }
            }
            stmt.append(c);
        }
        return stmts;
    }

    private static String stripComments(String str) {
        StringBuilder result = new StringBuilder();
        Pattern p = Pattern.compile("--");
        for (String s : str.split(System.getProperty("line.separator"))) {
            Matcher m = p.matcher(s);
            if (m.find()) {
                s = s.substring(0, m.start());
            }
            if (s.isEmpty()) continue;
            result.append(s).append(" ");
        }
        return result.toString();
    }


    /**
     * @return Current date in YYYYMMDD format
     */
    private static String getCurrentDate() {
        return new SimpleDateFormat("yyyyMMdd").format(NOW);
    }


    /**
     * @return Current timestamp.
     */
    public static String getCurrentTimeStamp() {
        return new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss").format(Calendar.getInstance().getTime());
    }

    /**
     * Send an email message
     *
     * @param from       - email address of sender
     * @param recipients - String array of recipients
     * @param subject    - subject line
     * @param msg        - Message body
     * @param filename   - full path of filename to send as attachment
     */
    public static void SendMail(String from, String[] recipients, String subject, String msg, String filename) {
        String[] s = new String[1];
        s[0] = filename;
        SendMail(from, recipients, subject, msg, s);
    }

    /**
     * Send an email message
     *
     * @param from       - email address of sender
     * @param recipients - String array of recipients
     * @param subject    - subject line
     * @param msg        - Message body
     */
    public static void SendMail(String from, String[] recipients, String subject, String msg) {
        SendMail(from, recipients, subject, msg, new String[0]);
    }


    /**
     * Send an email message
     *
     * @param from     - email address of sender
     * @param to       - email address of recipient
     * @param subject  - subject line
     * @param msg      - Message body
     * @param filename - full path of filename to send as attachment
     */
    public static void SendMail(String from, String to, String subject, String msg, String filename) {
        String[] s = new String[1];
        s[0] = filename;
        String[] recipients = new String[1];
        recipients[0] = to;
        SendMail(from, recipients, subject, msg, s);
    }

    /**
     * Send an email message
     *
     * @param from    - email address of sender
     * @param to      - email address of recipient
     * @param subject - subject line
     * @param msg     - Message body
     */
    public static void SendMail(String from, String to, String subject, String msg) {
        String[] recipients = new String[1];
        recipients[0] = to;
        SendMail(from, recipients, subject, msg, new String[0]);
    }

    /**
     * Send an email message
     *
     * @param from       - email address of sender
     * @param recipients - email address of recipient
     * @param subject    - subject line
     * @param msg        - Message body
     * @param filenames  - string array of filenames to send as attachment(s)
     */
    static void SendMail(String from, String[] recipients, String subject, String msg, String[] filenames) {
        Security.addProvider(new com.sun.net.ssl.internal.ssl.Provider());
        Properties props = new Properties();
        props.put("mail.transport.protocol", "smtp");
        props.put("mail.smtp.host", SMTP_HOST_NAME);
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.port", "25");
        props.put("mail.smtps.auth", "false");
        try {
            Session mailSession = Session.getInstance(props, null);
            // uncomment for debugging infos to stdout
            Transport transport = mailSession.getTransport("smtp");
            // Part one is the email body text
            BodyPart messageBodyPart = new MimeBodyPart();
            messageBodyPart.setText(msg);
            // Create a multipart message
            Multipart multipart = new MimeMultipart();
            multipart.addBodyPart(messageBodyPart);

            // Add the attachments
            for (String filename : filenames) {
                if (filename != null) {
                    messageBodyPart = new MimeBodyPart();
                    FileDataSource source = new FileDataSource(filename);
                    messageBodyPart.setDataHandler(new DataHandler(source));
                    messageBodyPart.setFileName(source.getFile().getName());
                    multipart.addBodyPart(messageBodyPart);

                }
            }
            MimeMessage message = new MimeMessage(mailSession);
            message.setContent(multipart);
            message.setSubject(subject);
            message.setFrom(new InternetAddress(from));
            for (String to : recipients) {
                message.addRecipient(Message.RecipientType.TO,
                        new InternetAddress(to));
            }

            transport.connect(SMTP_AUTH_USER, SMTP_AUTH_PWD);
            transport.sendMessage(message,
                    message.getRecipients(Message.RecipientType.TO));
            transport.close();

        } catch (MessagingException mex) {
            mex.printStackTrace();
            log.error("Sendmail exception");
            System.exit(2);
        }
    }


    private Util() {
    }

    static {
        NOW = Calendar.getInstance().getTime();
        log = LoggerFactory.getLogger(Util.class);
        currentDate = getCurrentDate();
        SMTP_HOST_NAME = Controller.getInstance().getConf().get(MAIL_HOSTNAME);
        SMTP_AUTH_PWD = Controller.getInstance().getConf().get(JDBC_PASSWORD);
        SMTP_AUTH_USER = Controller.getInstance().getConf().get(JDBC_USERNAME);
    }


    /**
     * Holds current date in YYYYMMDD format
     */
    public static final String currentDate;
    private static final Date NOW;
    private static final String SMTP_HOST_NAME;
    private static final String SMTP_AUTH_USER;
    private static final String SMTP_AUTH_PWD;
    private static final Logger log;

}
