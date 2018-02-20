/**
 * Allow execution of HQL against a JDBC connection to Hive. With task management/parallel execution and full logging functionality.
 * <p>
 * <ul>
 * <li>{@link net.martinprobson.hiveutils.hqlwrapper.Controller} - main entry point for package - takes the JobConfig.root
 * directory specified in framework-config.xml (or via -JobConfig on command line) and executes the hql files
 * found in that location in lexicographical order.
 * 
 */
package net.martinprobson.hiveutils.hqlwrapper;
