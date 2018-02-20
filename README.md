# Apache Hive/Hadoop Execution Wrapper

## Summary 
 Basic wrapper code to execute and monitor HQL script(s) against Apache Hive via a JDBC connection.

## Operation
  Takes the JobConfig.root directory specified in framework_config.xml (or via -JobConfig on command line) and executes the HQL files
  found in that location in lexicographical order.
  
  A sub-directory containing additional HQL files is assumed to be a separate set of independent HQL that can be executed in parallel in conjunction
  with other HQL files stored in sub-directories.
  
  For example, given a JobConfig root that points to the following files: -
  
  ![job flow image](src/main/java/net/martinprobson/hiveutils/hqlwrapper/doc-files/Example_Task_Chain.png?raw=true)
  
  
  The wrapper will execute: -
  
  * 00_Init1.hql
  * 10_Init1.hql
  
  and then start a separate thread to execute files in directory 20_Rule1:-
  
  * 00_Rule1
  * 20_Rule1
  
  also start a thread to execute code in 30_Rule2 directory: -
  
  * 10_Rule2.hql
  * 20_Rule2.hql
  * 99_Rule2.hql
  
  and then:-
  
  * 80_Fin1.hql
  * 90_Fin2.hql
  
  Note that a failure of a file existing in a sub-directory will not prevent top level or other sub-directory files from running.
  For example, a failure in file 00_Rule1 will prevent 20_Rule1 from executing but 30_Rule2 plus 80_Fin1 and 2 will still execute.
  
  A failure in file 10_Init1 will prevent all other tasks from executing.

  **See [TestHqlWrapper](test/java/net/martinprobson/hiveutils/TestHqlWrapper.java)** for an example job chain.

## Details
The wrapper is configured via two XML config files that should be somewhere on the classpath. Each config item has an associated description and follows the same schmea as standard Hadoop configuration (e.g. hdfs-site.xml, hive-site.xml etc):

1. `framework.config.xml` 
- `jdbc.drivers` - Java classname of JDBC driver
- `jdbc.url` - JDBC connection URL
- `Hql.FileSystem` - Name of filesystem on which to retrieve HQL files to be run. (see JobConfig.root). Setting to `hdfs://server_details` will make framework look for files on the Hadoop HDFS filesystem. Setting to `file:///` will use the local file system. If this parameter is not set then local filesystem is assumed.
- `JobConfig.root` - Name of root directory where wrapper will look for HQL files to execute. 
- `monitor.interval` - Polling interval (in seconds) for task monitoring. Wrapper will sleep for this length of time before waking up and checking status of running job(s).
- `kerberos.principal` - The Kerberos principal to authenticate against. If blank a non-kerberos system is assumed.
- `mail.hostname` - Hostname used to send emails from the wrapper code.
- `mail.mailfrom` - Name used in From: field of sent emails
- `mailto` - Comma separated list of email addresses to email.
- `mailSuccess` - Emails send on success? (true/false)
- `mailFailure` - Emails sent on failure? (true/false)

2. `user-password.xml` **This file should contain JDBC user/password details for Hive connection and should be kept secure**. A good solution is to add your home directory to the classpath, and store user_password.xml there.
```
	<configuration>
	 
	  <property>
	    <name>jdbc.username</name>
	    <value>user</value>
	    <description>
	      Userid.
	    </description>
	  </property>

	  <property>
	    <name>jdbc.password</name>
	    <value>password</value>
	    <description>
	      Password.
	    </description>
	  </property>
	</configuration>
```

## Command Line Options

- `--JobConfig` - Name of the job configuration directory (overrides `JobConfig.root` in XML config if set).
- `--DryRun` - Parse and run the jobs but do not submit anything to Hive.
- `--help` - Command line help.

## Build Instructions

Maven is used as the build tool with the following goals: -

```bash
mvn clean compile test package install
```

## Acknowledgements

Thanks to [Bob Freitas](http://www.lopakalogic.com/about/) for the [Hive unit test framework](https://github.com/bobfreitas/hiveunit-mr2).
