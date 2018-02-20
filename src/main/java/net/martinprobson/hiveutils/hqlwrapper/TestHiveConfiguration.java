package net.martinprobson.hiveutils.hqlwrapper;

import org.apache.hadoop.conf.Configuration;


class TestHiveConfiguration extends Configuration {

	TestHiveConfiguration() {
		addResource(FW_CONFIG);
		addResource(USER_PASS);
	}
	
    static final String FW_CONFIG    			= "framework_config.xml";
    static final String USER_PASS    			= "user_password.xml";
    
    // Configuration keys specific to ControllerConfiguration.
    static final String DRY_RUN					= "Controller.DryRun";
    static final String HQL_FILESYSTEM			= "Hql.FileSystem";
	static final String HQL_FILE_ROOT_DIR		= "JobConfig.root";
	static final String MONITOR_INTERVAL		= "monitor.interval";
	static final String SEND_MAIL_ON_SUCCESS 	= "mail.mailSuccess";
	static final String SEND_MAIL_ON_FAILURE 	= "mail.mailFailure";
	static final String SEND_MAIL_FROM			= "mail.mailfrom";
	static final String SEND_MAIL_TO		    = "mail.mailto";
	static final String JDBC_DRIVERS		    = "jdbc.drivers";
	static final String JDBC_USERNAME	    	= "jdbc.username";
	static final String JDBC_PASSWORD	    	= "jdbc.password";
	static final String JDBC_URL	    	    = "jdbc.url";
	static final String KERBOROS_PRINCIPAL   	= "kerboros.principal";
	static final String MAIL_HOSTNAME        	= "mail.hostname";
	
	// Default values if config missing
	static final String DEFAULT_HQL_FILESYSTEM = "file:///";
}
