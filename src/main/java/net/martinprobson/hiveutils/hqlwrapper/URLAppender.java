package net.martinprobson.hiveutils.hqlwrapper;

/**
 * Interface to allow extra parameters to be added to JDBC connection string.
 * @author martinr
 *
 */
interface URLAppender {
	/**
	 * Callback method to add extra parameter(s) to connection URL.
	 * <p>
	 * @param url String containing URL to be modified.
	 * @return returns Modified URL. 
	 */
	String appendURL(String url);
}
